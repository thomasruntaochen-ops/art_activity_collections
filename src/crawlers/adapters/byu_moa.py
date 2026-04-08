import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

BYU_MOA_EVENTS_URL = "https://moa.byu.edu/events/"

BYU_TIMEZONE = "America/Denver"
BYU_VENUE_NAME = "BYU Museum of Art"
BYU_CITY = "Provo"
BYU_STATE = "UT"
BYU_DEFAULT_LOCATION = "BYU Museum of Art, Provo, UT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BYU_MOA_EVENTS_URL,
}

DETAIL_DATE_RE = re.compile(
    r"^(?:[A-Za-z]+,\s+)?(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})"
    r"(?:\s*-\s*(?:[A-Za-z]+,\s+)?(?P<end_month>[A-Za-z]+)\s+(?P<end_day>\d{1,2}))?$"
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}:\d{2}\s*[AP]M)\s*-\s*(?P<end>\d{1,2}:\d{2}\s*[AP]M)",
    re.IGNORECASE,
)

INCLUDE_PATTERNS = (
    " gallery talk ",
    " open studio ",
    " slow art day ",
    " workshop ",
)
EXCLUDE_PATTERNS = (
    " closure ",
    " giveaway ",
    " poem ",
    " poetry ",
    " print study room ",
    " stations of the cross ",
    " tour ",
)


@dataclass(frozen=True, slots=True)
class ByuEventStub:
    source_url: str
    teaser_text: str


async def fetch_byu_page(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS)

    last_exception: Exception | None = None
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            if response.status_code < 400:
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch BYU MOA events page") from last_exception
    raise RuntimeError("Unable to fetch BYU MOA events page after retries")


async def load_byu_moa_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        list_html = await fetch_byu_page(BYU_MOA_EVENTS_URL, client=client)
        stubs = _extract_stubs(list_html)
        detail_urls = [stub.source_url for stub in stubs]
        detail_htmls = await asyncio.gather(*(fetch_byu_page(url, client=client) for url in detail_urls))
        detail_pages = {url: html for url, html in zip(detail_urls, detail_htmls, strict=True)}

    return {
        "list_html": list_html,
        "stubs": [
            {
                "source_url": stub.source_url,
                "teaser_text": stub.teaser_text,
            }
            for stub in stubs
        ],
        "detail_pages": detail_pages,
    }


def parse_byu_moa_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(BYU_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    detail_pages: dict[str, str] = payload.get("detail_pages") or {}

    for raw_stub in payload.get("stubs") or []:
        stub = ByuEventStub(
            source_url=raw_stub.get("source_url", ""),
            teaser_text=raw_stub.get("teaser_text", ""),
        )
        detail_html = detail_pages.get(stub.source_url)
        if not detail_html:
            continue
        row = _build_row(stub=stub, html=detail_html)
        if row is None or row.start_at.date() < current_date:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class ByuMoaEventsAdapter(BaseSourceAdapter):
    source_name = "byu_moa_events"

    async def fetch(self) -> list[str]:
        payload = await load_byu_moa_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_byu_moa_payload(json.loads(payload))


def _extract_stubs(html: str) -> list[ByuEventStub]:
    soup = BeautifulSoup(html, "html.parser")
    stubs: list[ByuEventStub] = []
    seen: set[str] = set()

    for card in soup.select(".PromoCardImageOnTop"):
        anchor = card.select_one("a[href]")
        if anchor is None:
            continue
        source_url = urljoin(BYU_MOA_EVENTS_URL, (anchor.get("href") or "").strip())
        teaser_text = _normalize_space(card.get_text(" ", strip=True))
        if not source_url or source_url in seen:
            continue
        seen.add(source_url)
        stubs.append(ByuEventStub(source_url=source_url, teaser_text=teaser_text))

    return stubs


def _build_row(*, stub: ByuEventStub, html: str) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = _normalize_space(_text_or_none(soup.select_one("h1.EventPage-headline")))
    if not title:
        title = _normalize_space(_meta_content(soup, "og:title"))
    if not title:
        return None

    body_text = _extract_body_text(soup)
    meta_description = _normalize_space(_meta_content(soup, "og:description"))
    description = body_text or meta_description or None

    blob = _searchable_blob(" ".join([stub.teaser_text, title, description or ""]))
    if not _should_include_event(blob):
        return None

    event_date_text = _normalize_space(_text_or_none(soup.select_one(".EventPage-eventDate")))
    event_duration_text = _normalize_space(_text_or_none(soup.select_one(".EventPage-eventDuration")))
    start_at, end_at = _parse_event_datetimes(
        source_url=stub.source_url,
        event_date_text=event_date_text,
        event_duration_text=event_duration_text,
    )
    if start_at is None:
        return None

    location_text = _normalize_space(_text_or_none(soup.select_one(".EventPage-eventLocation"))) or BYU_DEFAULT_LOCATION
    is_free, free_status = infer_price_classification(
        description,
        default_is_free=True if " free " in blob else None,
    )

    return ExtractedActivity(
        source_url=stub.source_url,
        title=title,
        description=description,
        venue_name=BYU_VENUE_NAME,
        location_text=location_text,
        city=BYU_CITY,
        state=BYU_STATE,
        activity_type=_infer_activity_type(blob),
        age_min=None,
        age_max=None,
        drop_in=(" drop in " in blob or " drop-in " in blob),
        registration_required=(" registration required " in blob or " rsvp " in blob or " register " in blob)
        and " registration is not required " not in blob,
        start_at=start_at,
        end_at=end_at,
        timezone=BYU_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_body_text(soup: BeautifulSoup) -> str:
    article = soup.select_one(".EventPage-main article")
    if article is None:
        return ""
    info = article.select_one(".EventPage-info")
    if info is not None:
        info.decompose()
    tags = article.select_one(".EventPage-tags")
    if tags is not None:
        tags.decompose()
    texts: list[str] = []
    for node in article.select("p, h5, h6, li"):
        text = _normalize_space(node.get_text(" ", strip=True))
        if text:
            texts.append(text)
    return " ".join(texts)


def _parse_event_datetimes(
    *,
    source_url: str,
    event_date_text: str,
    event_duration_text: str,
) -> tuple[datetime | None, datetime | None]:
    match = DETAIL_DATE_RE.match(event_date_text)
    if match is None:
        return None, None

    year = _extract_year_from_url(source_url)
    start_date = _parse_month_day(match.group("month"), match.group("day"), year)
    if start_date is None:
        return None, None
    end_date = start_date
    if match.group("end_month") and match.group("end_day"):
        parsed_end = _parse_month_day(match.group("end_month"), match.group("end_day"), year)
        if parsed_end is not None:
            end_date = parsed_end

    time_match = TIME_RANGE_RE.search(event_duration_text)
    if time_match is not None:
        start_at = _combine_date_and_time(start_date, time_match.group("start"))
        end_at = _combine_date_and_time(end_date, time_match.group("end"))
        return start_at, end_at

    return datetime.combine(start_date, datetime.min.time()), None


def _should_include_event(blob: str) -> bool:
    if any(pattern in blob for pattern in EXCLUDE_PATTERNS):
        return False
    return any(pattern in blob for pattern in INCLUDE_PATTERNS)


def _infer_activity_type(blob: str) -> str:
    if " talk " in blob:
        return "lecture"
    return "workshop"


def _meta_content(soup: BeautifulSoup, property_name: str) -> str:
    node = soup.find("meta", attrs={"property": property_name}) or soup.find("meta", attrs={"name": property_name})
    if node is None:
        return ""
    return str(node.get("content") or "")


def _text_or_none(node) -> str:
    if node is None:
        return ""
    return node.get_text(" ", strip=True)


def _extract_year_from_url(source_url: str) -> int:
    match = re.search(r"-(\d{4})-(\d{2})-(\d{2})(?:/?$)", source_url)
    if match:
        return int(match.group(1))
    return datetime.now().year


def _parse_month_day(month_text: str, day_text: str, year: int) -> date | None:
    try:
        return datetime.strptime(f"{month_text} {day_text} {year}", "%B %d %Y").date()
    except ValueError:
        return None


def _combine_date_and_time(base_date, time_text: str) -> datetime | None:
    try:
        parsed_time = datetime.strptime(time_text.upper(), "%I:%M %p").time()
    except ValueError:
        return None
    return datetime.combine(base_date, parsed_time)


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _searchable_blob(value: str) -> str:
    return f" {_normalize_space(value).lower()} "
