import asyncio
import re
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

DANFORTH_EVENTS_URL = "https://danforth.framingham.edu/visit/event-calendar/"
DANFORTH_EVENT_URL_PREFIX = "https://danforth.framingham.edu/event/"

NY_TIMEZONE = "America/New_York"
DANFORTH_VENUE_NAME = "Danforth Art Museum and Art School"
DANFORTH_CITY = "Framingham"
DANFORTH_STATE = "MA"
DANFORTH_DEFAULT_LOCATION = "Danforth Art Museum and Art School, Framingham, MA"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": DANFORTH_EVENTS_URL,
}

INCLUDE_PATTERNS = (
    " activity ",
    " artist ",
    " artmaking ",
    " class ",
    " discussion ",
    " drop into art ",
    " family ",
    " lecture ",
    " lunchtime lecture ",
    " talk ",
    " workshop ",
)
STRICT_EXCLUDE_PATTERNS = (
    " annual meeting ",
    " concert ",
    " exhibition closing ",
    " film ",
    " fundraiser ",
    " fundraising ",
    " open house ",
    " party ",
    " performance ",
    " reception ",
    " storytime ",
    " tour ",
    " tours ",
)

DATE_LINE_RE = re.compile(
    r"^(?:[A-Za-z]+,\s+)?(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})(?:,\s*(?P<year>\d{4}))?(?:\s+at|,\s*)(?P<time>.+)$"
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*"
    r"(?P<start_meridiem>a\.?m\.?|p\.?m\.?|am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*"
    r"(?P<end_meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)


async def fetch_danforth_page(
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
        raise RuntimeError("Unable to fetch Danforth event pages") from last_exception
    raise RuntimeError("Unable to fetch Danforth event pages after retries")


async def load_danforth_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_danforth_page(DANFORTH_EVENTS_URL, client=client)
        detail_urls = _extract_detail_urls(listing_html)
        detail_pages = await asyncio.gather(*(fetch_danforth_page(url, client=client) for url in detail_urls))

    return {
        "listing_html": listing_html,
        "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
    }


def parse_danforth_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, html in payload.get("detail_pages", {}).items():
        row = _build_row_from_detail_page(source_url=source_url, html=html)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class DanforthEventsAdapter(BaseSourceAdapter):
    source_name = "danforth_events"

    async def fetch(self) -> list[str]:
        html = await fetch_danforth_page(DANFORTH_EVENTS_URL)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_danforth_payload/parse_danforth_payload from script runner.")


def _extract_detail_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = _normalize_space(anchor.get("href") or "")
        if not href or not href.startswith(DANFORTH_EVENT_URL_PREFIX):
            continue
        if href in seen:
            continue
        seen.add(href)
        urls.append(href)
    return urls


def _build_row_from_detail_page(*, source_url: str, html: str) -> ExtractedActivity | None:
    detail = _parse_detail_page(html)
    title = detail.get("title")
    date_line = detail.get("date_line")
    if not title or not date_line:
        return None

    datetimes = _parse_date_line(date_line)
    if datetimes is None:
        return None
    start_at, end_at = datetimes

    description = _join_non_empty([detail.get("subtitle"), detail.get("body")])
    include_blob = " ".join([title, description or ""]).lower()
    token_blob = f" {' '.join(include_blob.split())} "
    if any(pattern in token_blob for pattern in STRICT_EXCLUDE_PATTERNS):
        return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None
    if "cash bar" in include_blob and " lecture " not in token_blob and " workshop " not in token_blob:
        return None

    location_text = "Online" if " zoom " in token_blob or " virtual " in token_blob else DANFORTH_DEFAULT_LOCATION
    registration_required = (
        "register" in include_blob
        or "registration is required" in include_blob
        or "registration requested" in include_blob
    )
    price_kwargs = _price_kwargs_from_description(description)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=DANFORTH_VENUE_NAME,
        location_text=location_text,
        city=DANFORTH_CITY,
        state=DANFORTH_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=None,
        age_max=None,
        drop_in=("drop into art" in token_blob or "drop-in" in token_blob or "drop in" in token_blob),
        registration_required=registration_required,
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **price_kwargs,
    )


def _parse_detail_page(html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    title = None
    date_line = None
    subtitle = None
    body_lines: list[str] = []

    if soup.title:
        raw_title = _normalize_space(soup.title.get_text(" ", strip=True))
        if raw_title:
            title = raw_title.split("–", 1)[0].strip()

    start_index = 0
    if "View Full Calendar" in lines:
        start_index = lines.index("View Full Calendar") + 1

    for index in range(start_index, len(lines) - 1):
        if DATE_LINE_RE.match(lines[index]):
            candidate_title = lines[index + 1]
            if title is not None and candidate_title != title:
                title = candidate_title
            date_line = lines[index]
            next_index = index + 2
            if next_index < len(lines) and lines[next_index] not in {"Register Here", "d"}:
                subtitle = lines[next_index]
                next_index += 1
            for body_line in lines[next_index:]:
                if body_line in {"Register Here", "SUPPORT THE DANFORTH"}:
                    break
                if body_line == "d":
                    break
                body_lines.append(body_line)
            break

    return {
        "title": title,
        "date_line": date_line,
        "subtitle": subtitle,
        "body": " ".join(body_lines).strip() or None,
    }


def _parse_date_line(value: str) -> tuple[datetime, datetime | None] | None:
    match = DATE_LINE_RE.match(value)
    if match is None:
        return None

    month = match.group("month")
    day = int(match.group("day"))
    year = int(match.group("year") or datetime.now(ZoneInfo(NY_TIMEZONE)).year)
    time_text = match.group("time").strip()

    start_time = _parse_time(time_text, allow_range_start=True)
    if start_time is None:
        return None
    start_at = datetime(year, _month_number(month), day, start_time[0], start_time[1])

    end_match = TIME_RANGE_RE.search(time_text)
    end_at = None
    if end_match is not None:
        end_time = _parse_time(time_text, allow_range_start=False)
        if end_time is not None:
            end_at = datetime(year, _month_number(month), day, end_time[0], end_time[1])

    return start_at, end_at


def _parse_time(value: str, *, allow_range_start: bool) -> tuple[int, int] | None:
    if allow_range_start:
        range_match = TIME_RANGE_RE.search(value)
        if range_match is not None:
            meridiem = range_match.group("start_meridiem") or range_match.group("end_meridiem")
            return _hour_minute(
                range_match.group("start_hour"),
                range_match.group("start_minute"),
                meridiem,
            )

    single_match = TIME_SINGLE_RE.search(value)
    if single_match is not None:
        return _hour_minute(
            single_match.group("hour"),
            single_match.group("minute"),
            single_match.group("meridiem"),
        )

    range_match = TIME_RANGE_RE.search(value)
    if range_match is not None:
        return _hour_minute(
            range_match.group("end_hour"),
            range_match.group("end_minute"),
            range_match.group("end_meridiem"),
        )
    return None


def _hour_minute(hour_value: str, minute_value: str | None, meridiem_value: str | None) -> tuple[int, int] | None:
    if meridiem_value is None:
        return None
    hour = int(hour_value)
    minute = int(minute_value or "0")
    meridiem = meridiem_value.lower().replace(".", "")
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return hour, minute


def _month_number(name: str) -> int:
    return datetime.strptime(name, "%B").month


def _infer_activity_type(token_blob: str) -> str:
    if any(keyword in token_blob for keyword in (" lecture ", " talk ", " discussion ")):
        return "lecture"
    return "workshop"


def _price_kwargs_from_description(description: str | None) -> dict[str, bool | None | str]:
    normalized = " ".join((description or "").lower().split())
    if "paid museum admission" in normalized:
        return {"is_free": False, "free_verification_status": "confirmed"}
    if "with museum admission" in normalized:
        return {"is_free": None, "free_verification_status": "uncertain"}
    if any(
        phrase in normalized
        for phrase in (
            "admission is always free",
            "admission is free",
            "event is free",
            "this event is free",
            "event is free with paid museum admission",
        )
    ):
        return {"is_free": True, "free_verification_status": "confirmed"}

    is_free, free_verification_status = infer_price_classification(description)
    return {
        "is_free": is_free,
        "free_verification_status": free_verification_status,
    }


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    if not values:
        return None
    return " | ".join(values)


def _normalize_space(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split())
    return cleaned or None
