import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

UMOCA_ARCHIVE_URL_TEMPLATE = "https://utahmoca.org/events/{year}/"

UMOCA_TIMEZONE = "America/Denver"
UMOCA_VENUE_NAME = "Utah Museum of Contemporary Art"
UMOCA_CITY = "Salt Lake City"
UMOCA_STATE = "UT"
UMOCA_DEFAULT_LOCATION = "Utah Museum of Contemporary Art, Salt Lake City, UT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

EVENT_PAGE_RE = re.compile(r"^https://utahmoca\.org/event/[^/?#]+/?$", re.IGNORECASE)
DATE_TIME_BLOCK_RE = re.compile(
    r"Date and time\s+"
    r"(?P<start_day>[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4})"
    r"(?:-\s*(?P<end_day>[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}))?"
    r"(?:\s+(?P<start_time>\d{1,2}:\d{2}\s*[ap]m)-(?P<end_time>\d{1,2}:\d{2}\s*[ap]m)\s*MST)?",
    re.IGNORECASE,
)
LOCATION_BLOCK_RE = re.compile(
    r"Location\s+(?P<location>Utah Museum of Contemporary Art\s+20 S\. West Temple\s+Salt Lake City UT 84101)",
    re.IGNORECASE,
)

STRONG_INCLUDE_PATTERNS = (
    " artist panel ",
    " artist talk ",
    " family art saturday ",
    " panel discussion ",
    " workshop ",
)
STRONG_EXCLUDE_PATTERNS = (
    " gala ",
    " opening reception ",
    " performance ",
    " residency ",
    " yoga ",
)
SOFT_EXCLUDE_PATTERNS = (
    " film screening ",
    " fitness ",
)


@dataclass(frozen=True, slots=True)
class UmocaEventStub:
    source_url: str
    teaser_text: str


async def fetch_umoca_page(
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
        raise RuntimeError("Unable to fetch UMOCA events page") from last_exception
    raise RuntimeError("Unable to fetch UMOCA events page after retries")


async def load_umoca_payload(*, max_pages: int = 5) -> dict:
    current_year = datetime.now(ZoneInfo(UMOCA_TIMEZONE)).year
    archive_url = UMOCA_ARCHIVE_URL_TEMPLATE.format(year=current_year)

    page_htmls: list[tuple[str, str]] = []
    stubs: list[UmocaEventStub] = []
    seen_stub_urls: set[str] = set()
    detail_urls: list[str] = []
    next_url: str | None = archive_url

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while next_url and len(page_htmls) < max_pages:
            html = await fetch_umoca_page(next_url, client=client)
            page_htmls.append((next_url, html))
            current_page_stubs = _extract_stubs(html)
            if not current_page_stubs:
                break
            for stub in current_page_stubs:
                if stub.source_url in seen_stub_urls:
                    continue
                seen_stub_urls.add(stub.source_url)
                stubs.append(stub)
                detail_urls.append(stub.source_url)
            next_url = _extract_next_page_url(html)

        detail_htmls = await asyncio.gather(*(fetch_umoca_page(url, client=client) for url in detail_urls))
        detail_pages = {url: html for url, html in zip(detail_urls, detail_htmls, strict=True)}

    return {
        "archive_url": archive_url,
        "pages": page_htmls,
        "stubs": [
            {
                "source_url": stub.source_url,
                "teaser_text": stub.teaser_text,
            }
            for stub in stubs
        ],
        "detail_pages": detail_pages,
    }


def parse_umoca_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(UMOCA_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    detail_pages: dict[str, str] = payload.get("detail_pages") or {}

    for raw_stub in payload.get("stubs") or []:
        stub = UmocaEventStub(
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


class UmocaEventsAdapter(BaseSourceAdapter):
    source_name = "umoca_events"

    async def fetch(self) -> list[str]:
        payload = await load_umoca_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_umoca_payload(json.loads(payload))


def _extract_stubs(html: str) -> list[UmocaEventStub]:
    soup = BeautifulSoup(html, "html.parser")
    stubs: list[UmocaEventStub] = []
    seen: set[str] = set()

    for anchor in soup.select('a[href^="https://utahmoca.org/event/"]'):
        source_url = (anchor.get("href") or "").strip()
        if not EVENT_PAGE_RE.match(source_url) or source_url in seen:
            continue
        teaser_text = _normalize_space(anchor.get_text(" ", strip=True))
        if not teaser_text:
            continue
        seen.add(source_url)
        stubs.append(UmocaEventStub(source_url=source_url, teaser_text=teaser_text))

    return stubs


def _extract_next_page_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.select('a[href^="https://utahmoca.org/events/"]'):
        href = (anchor.get("href") or "").strip()
        if "/page/" in href:
            return href
    return None


def _build_row(*, stub: UmocaEventStub, html: str) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = _normalize_space(_text_or_none(soup.find("h1")))
    if not title:
        return None

    page_text = _normalize_space(soup.get_text(" ", strip=True))
    start_at, end_at = _parse_datetimes(page_text)
    if start_at is None:
        return None

    location_text = _extract_location_text(page_text) or UMOCA_DEFAULT_LOCATION
    description = _extract_description(page_text)
    blob = _searchable_blob(" ".join([stub.teaser_text, title, description]))
    if not _should_include_event(blob):
        return None
    age_min, age_max = _parse_age_range(description)
    is_free, free_status = infer_price_classification(
        description,
        default_is_free=True if " free " in blob or " suggested " in blob else None,
    )

    return ExtractedActivity(
        source_url=stub.source_url,
        title=title,
        description=description or None,
        venue_name=UMOCA_VENUE_NAME,
        location_text=location_text,
        city=UMOCA_CITY,
        state=UMOCA_STATE,
        activity_type=_infer_activity_type(blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=(" walk-in " in blob or " drop in " in blob or " drop-in " in blob),
        registration_required=(
            " registration " in blob
            or " rsvp " in blob
            or " tickets " in blob
            or " ticket " in blob
        ) and " not required " not in blob,
        start_at=start_at,
        end_at=end_at,
        timezone=UMOCA_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _parse_datetimes(page_text: str) -> tuple[datetime | None, datetime | None]:
    match = DATE_TIME_BLOCK_RE.search(page_text)
    if match is None:
        return None, None

    start_date = _parse_verbose_date(match.group("start_day"))
    if start_date is None:
        return None, None
    end_date = _parse_verbose_date(match.group("end_day")) or start_date

    start_time = match.group("start_time")
    end_time = match.group("end_time")
    if start_time and end_time:
        start_at = _combine_date_and_time(start_date.date(), start_time)
        end_at = _combine_date_and_time(end_date.date(), end_time)
        return start_at, end_at

    return datetime.combine(start_date.date(), datetime.min.time()), None


def _extract_location_text(page_text: str) -> str | None:
    match = LOCATION_BLOCK_RE.search(page_text)
    if match is None:
        return None
    return _normalize_space(match.group("location"))


def _extract_description(page_text: str) -> str:
    start_match = LOCATION_BLOCK_RE.search(page_text)
    if start_match is None:
        return ""
    tail = page_text[start_match.end() :].strip()
    end_markers = (
        " Registration ",
        " Links Add to calendar ",
        " Newsletter ",
    )
    end_index = len(tail)
    for marker in end_markers:
        idx = tail.find(marker)
        if idx != -1:
            end_index = min(end_index, idx)
    return _normalize_space(tail[:end_index])


def _should_include_event(blob: str) -> bool:
    if any(pattern in blob for pattern in STRONG_EXCLUDE_PATTERNS):
        return False
    if any(pattern in blob for pattern in SOFT_EXCLUDE_PATTERNS):
        if (
            " artist talk " not in blob
            and " artist panel " not in blob
            and " panel discussion " not in blob
        ):
            return False
    return any(pattern in blob for pattern in STRONG_INCLUDE_PATTERNS)


def _infer_activity_type(blob: str) -> str:
    if " talk " in blob or " panel " in blob:
        return "lecture"
    return "workshop"


def _parse_age_range(description: str) -> tuple[int | None, int | None]:
    lowered = description.lower()
    if "all ages" in lowered:
        return None, None
    return None, None


def _parse_verbose_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%A, %B %d, %Y")
    except ValueError:
        return None


def _combine_date_and_time(base_date, time_text: str) -> datetime | None:
    try:
        parsed_time = datetime.strptime(time_text.lower(), "%I:%M %p").time()
    except ValueError:
        return None
    return datetime.combine(base_date, parsed_time)


def _text_or_none(node) -> str:
    if node is None:
        return ""
    return node.get_text(" ", strip=True)


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _searchable_blob(value: str) -> str:
    return f" {_normalize_space(value).lower()} "
