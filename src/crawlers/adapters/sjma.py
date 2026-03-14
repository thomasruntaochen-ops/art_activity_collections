import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

SJMA_CALENDAR_URL = "https://sjmusart.org/calendar"

LA_TIMEZONE = "America/Los_Angeles"
SJMA_VENUE_NAME = "San Jose Museum of Art"
SJMA_CITY = "San Jose"
SJMA_STATE = "CA"
SJMA_DEFAULT_LOCATION = "San Jose, CA"

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>a\.?m\.?|p\.?m\.?|am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?|am|pm)\b",
    re.IGNORECASE,
)

HARD_EXCLUDED_KEYWORDS = (
    "camp",
    "auction",
    "gala",
    "museums on us",
    "free days",
    "first fridays",
    "opening celebration",
    "open registration",
    "docent-led public tours",
    "admission",
)
SOFT_EXCLUDED_KEYWORDS = (
    "tour",
    "tours",
    "performance",
    "music",
    "concert",
    "celebration",
    "after-party",
    "after party",
    "first fridays",
)
INCLUDED_KEYWORDS = (
    "activity",
    "activities",
    "art 101",
    "artmaking",
    "class",
    "classes",
    "conversation",
    "family",
    "hands-on",
    "hands on",
    "lab",
    "lecture",
    "maker",
    "talk",
    "teen",
    "teens",
    "workshop",
    "workshops",
    "youth",
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://sjmusart.org/",
}


async def fetch_sjma_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
    client: httpx.AsyncClient | None = None,
) -> str:
    print(f"[sjma-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    owns_client = client is None
    active_client = client or httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers=DEFAULT_HEADERS,
    )
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[sjma-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await active_client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[sjma-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[sjma-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[sjma-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[sjma-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[sjma-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await active_client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch SJMA events page") from last_exception
    raise RuntimeError("Unable to fetch SJMA events page after retries")


async def fetch_sjma_calendar_bundle(url: str = SJMA_CALENDAR_URL) -> tuple[str, dict[str, str]]:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        list_html = await fetch_sjma_events_page(url, client=client)
        detail_urls = _extract_detail_urls(list_html, list_url=url)
        detail_html_by_url: dict[str, str] = {}
        if detail_urls:
            tasks = [fetch_sjma_events_page(detail_url, client=client) for detail_url in detail_urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for detail_url, result in zip(detail_urls, results):
                if isinstance(result, Exception):
                    print(f"[sjma-fetch] detail fetch failed url={detail_url} error={result}")
                    continue
                detail_html_by_url[detail_url] = result
    return list_html, detail_html_by_url


class SjmaCalendarAdapter(BaseSourceAdapter):
    source_name = "sjma_calendar"

    def __init__(self, url: str = SJMA_CALENDAR_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_sjma_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_sjma_events_html(payload, list_url=self.url)


def parse_sjma_events_html(
    html: str,
    *,
    list_url: str,
    detail_html_by_url: dict[str, str] | None = None,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    current_date = (now or datetime.now()).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for article in soup.select(".view-content article"):
        title_node = article.select_one("h2 a[href] span.field--name-title, h2 a[href]")
        anchor = article.select_one("h2 a[href]")
        abstract_node = article.select_one(".field--name-field-abstract .field__item")
        if anchor is None:
            continue

        title = _normalize_text(title_node.get_text(" ", strip=True) if title_node else None)
        if not title or is_irrelevant_item_text(title):
            continue

        source_url = urljoin(list_url, anchor["href"])
        abstract = _normalize_text(abstract_node.get_text(" ", strip=True) if abstract_node else None)
        listing_start = _parse_listing_date(article)
        if listing_start is not None and listing_start.date() < current_date:
            continue

        detail_html = (detail_html_by_url or {}).get(source_url)
        detail = _parse_detail_page(detail_html) if detail_html else None
        if detail is not None and detail["start_at"] is not None and detail["start_at"].date() < current_date:
            continue

        admission_text = detail["admission"] if detail is not None else None
        detail_description = detail["description"] if detail is not None else None
        text_blob = " ".join(
            part for part in [title, abstract or "", detail_description or "", admission_text or ""] if part
        )
        if _should_exclude_event(text_blob) and not _should_include_event(text_blob):
            continue
        if not _should_include_event(text_blob):
            continue

        start_at = detail["start_at"] if detail is not None else listing_start
        end_at = detail["end_at"] if detail is not None else None
        if start_at is None:
            continue

        description = _join_non_empty(
            [
                abstract,
                detail_description if detail_description and detail_description != abstract else None,
                f"Admission: {admission_text}" if admission_text else None,
            ]
        )
        age_min, age_max = _parse_age_range(title=title, description=description)
        registration_required = _is_registration_required(text_blob)
        is_free, free_status = infer_price_classification(
            " | ".join(part for part in [admission_text, text_blob] if part)
        )

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=SJMA_VENUE_NAME,
                location_text=SJMA_DEFAULT_LOCATION,
                city=SJMA_CITY,
                state=SJMA_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob.lower() or "drop in" in text_blob.lower()),
                registration_required=registration_required,
                start_at=start_at,
                end_at=end_at,
                timezone=LA_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    return rows


def _extract_detail_urls(html: str, *, list_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for article in soup.select(".view-content article.node--type-event"):
        anchor = article.select_one("h2 a[href]")
        if anchor is None:
            continue
        url = urljoin(list_url, anchor["href"])
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _parse_listing_date(article) -> datetime | None:
    time_node = article.select_one(".field--name-field-date time[datetime]")
    if time_node is None:
        return None
    value = _normalize_text(time_node.get("datetime"))
    if not value:
        return None
    return _parse_iso_date(value)


def _parse_detail_page(html: str | None) -> dict[str, object] | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    article = soup.select_one("article.node--type-event.node--view-mode-full")
    if article is None:
        return None

    date_node = article.select_one(".field--name-field-date time[datetime]")
    if date_node is None:
        return None

    event_day = _parse_iso_date(_normalize_text(date_node.get("datetime")))
    if event_day is None:
        return None

    time_text = _normalize_text(
        article.select_one(".field--name-field-time .field__item").get_text(" ", strip=True)
        if article.select_one(".field--name-field-time .field__item")
        else None
    )
    admission_text = _normalize_text(
        article.select_one(".field--name-field-admission .field__item").get_text(" ", strip=True)
        if article.select_one(".field--name-field-admission .field__item")
        else None
    )
    body_text = _normalize_text(
        article.select_one(".field--name-body .field__item").get_text(" ", strip=True)
        if article.select_one(".field--name-body .field__item")
        else None
    )
    if body_text:
        body_text = re.split(r"\bSupport\b", body_text, maxsplit=1, flags=re.IGNORECASE)[0].strip() or None
    start_at, end_at = _parse_event_datetimes(event_day, time_text)

    return {
        "description": body_text,
        "admission": admission_text,
        "start_at": start_at,
        "end_at": end_at,
    }


def _parse_event_datetimes(day: datetime, time_text: str | None) -> tuple[datetime | None, datetime | None]:
    base_day = day.replace(hour=0, minute=0, second=0, microsecond=0)
    if not time_text:
        return base_day, None

    normalized = time_text.replace("\u2013", "-").replace("\u2014", "-").replace("\xa0", " ")
    range_match = TIME_RANGE_RE.search(normalized)
    if range_match:
        start_meridiem = range_match.group("start_meridiem") or range_match.group("end_meridiem")
        start_hour, start_minute = _to_24h(
            int(range_match.group("start_hour")),
            int(range_match.group("start_minute") or 0),
            start_meridiem,
        )
        end_hour, end_minute = _to_24h(
            int(range_match.group("end_hour")),
            int(range_match.group("end_minute") or 0),
            range_match.group("end_meridiem"),
        )
        return (
            base_day.replace(hour=start_hour, minute=start_minute),
            base_day.replace(hour=end_hour, minute=end_minute),
        )

    single_match = TIME_SINGLE_RE.search(normalized)
    if single_match:
        hour, minute = _to_24h(
            int(single_match.group("hour")),
            int(single_match.group("minute") or 0),
            single_match.group("meridiem"),
        )
        return base_day.replace(hour=hour, minute=minute), None

    return base_day, None


def _parse_iso_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d")
        except ValueError:
            return None


def _to_24h(hour: int, minute: int, meridiem: str | None) -> tuple[int, int]:
    normalized = (meridiem or "").replace(".", "").lower()
    if normalized == "pm" and hour != 12:
        hour += 12
    if normalized == "am" and hour == 12:
        hour = 0
    return hour, minute


def _should_exclude_event(text: str) -> bool:
    normalized = text.lower()
    return any(keyword in normalized for keyword in HARD_EXCLUDED_KEYWORDS + SOFT_EXCLUDED_KEYWORDS)


def _should_include_event(text: str) -> bool:
    normalized = text.lower()
    return any(keyword in normalized for keyword in INCLUDED_KEYWORDS)


def _is_registration_required(text: str) -> bool:
    normalized = text.lower()
    return any(keyword in normalized for keyword in ("register", "registration", "tickets", "ticket", "application"))


def _infer_activity_type(text: str) -> str:
    normalized = text.lower()
    if any(keyword in normalized for keyword in ("talk", "lecture", "conversation")):
        return "talk"
    if any(keyword in normalized for keyword in ("workshop", "class", "artmaking", "art 101", "lab")):
        return "workshop"
    return "activity"


def _parse_age_range(*, title: str, description: str | None) -> tuple[int | None, int | None]:
    text = " ".join(part for part in [title, description or ""] if part)
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))

    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def _join_non_empty(parts: list[str | None]) -> str | None:
    filtered = [part.strip() for part in parts if part and part.strip()]
    if not filtered:
        return None
    return " | ".join(filtered)


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.replace("\xa0", " ").split())
    return normalized or None
