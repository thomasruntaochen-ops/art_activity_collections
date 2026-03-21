import asyncio
import re
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from html import unescape
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from bs4 import NavigableString

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

NEW_BEDFORD_EVENTS_URL = "https://newbedfordart.org/events/"
NEW_BEDFORD_WP_PAGES_URL = "https://newbedfordart.org/wp-json/wp/v2/pages"
NEW_BEDFORD_TIMEZONE = "America/New_York"
NEW_BEDFORD_VENUE_NAME = "New Bedford Art Museum"
NEW_BEDFORD_CITY = "New Bedford"
NEW_BEDFORD_STATE = "MA"
NEW_BEDFORD_DEFAULT_LOCATION = "New Bedford Art Museum, 608 Pleasant Street, New Bedford, MA 02740"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": NEW_BEDFORD_EVENTS_URL,
}

MONTH_NAMES = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
MONTH_PATTERN = "|".join(MONTH_NAMES)
TITLE_EXCLUDE_PATTERNS = (
    "artreads",
    "poetry",
    "yoga",
    "vacation art program",
)
TEXT_EXCLUDE_PATTERNS = (
    " poetry ",
    " book club ",
    " reading ",
    " yoga ",
    " meditation ",
    " mindfulness ",
    " camp ",
)
INCLUDE_PATTERNS = (
    " workshop ",
    " workshops ",
    " class ",
    " classes ",
    " printmaking ",
    " sketchbook ",
    " drawing ",
    " clay ",
    " ceramics ",
    " pottery ",
    " candle making ",
    " make art ",
    " artmaking ",
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>AM|PM|am|pm)?\s*"
    r"(?:-|–)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>AM|PM|am|pm)?"
)
MONTH_DAY_CHUNK_RE = re.compile(
    rf"(?P<month>{MONTH_PATTERN})\s+(?P<days>.*?)(?=(?:{MONTH_PATTERN})\s+\d|$)",
    re.IGNORECASE,
)
DATE_RANGE_RE = re.compile(
    rf"(?P<month1>{MONTH_PATTERN})\s+"
    r"(?P<day1>\d{1,2})(?:st|nd|rd|th)?"
    r"(?:,\s*(?P<year1>\d{4}))?"
    r"\s*(?:-|–)\s*"
    rf"(?:(?P<month2>{MONTH_PATTERN})\s+)?"
    r"(?P<day2>\d{1,2})(?:st|nd|rd|th)?"
    r"(?:,\s*(?P<year2>\d{4}))?",
    re.IGNORECASE,
)
FULL_DATE_RANGE_RE = re.compile(
    rf"(?P<weekday1>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    rf"(?P<month1>{MONTH_PATTERN})\s+"
    r"(?P<day1>\d{1,2})(?:st|nd|rd|th)?"
    r"(?:,\s*(?P<year1>\d{4}))?"
    r"\s*(?:-|–)\s*"
    rf"(?P<weekday2>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    rf"(?P<month2>{MONTH_PATTERN})\s+"
    r"(?P<day2>\d{1,2})(?:st|nd|rd|th)?"
    r"(?:,\s*(?P<year2>\d{4}))?",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(?P<min>\d{1,2})\s*(?:-|to)\s*(?P<max>\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\b(?P<age>\d{1,2})\s*\+")


async def fetch_new_bedford_page(
    slug: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> dict:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS)

    last_exception: Exception | None = None
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(NEW_BEDFORD_WP_PAGES_URL, params={"slug": slug})
            except httpx.HTTPError as exc:
                last_exception = exc
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            if response.status_code < 400:
                payload = response.json()
                if isinstance(payload, list) and payload:
                    return payload[0]
                raise RuntimeError(f"New Bedford page JSON for slug '{slug}' was empty.")

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError(f"Unable to fetch New Bedford Art Museum page '{slug}'") from last_exception
    raise RuntimeError(f"Unable to fetch New Bedford Art Museum page '{slug}' after retries")


async def load_new_bedford_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_page = await fetch_new_bedford_page("events", client=client)
        listing_entries = _extract_listing_entries(((listing_page.get("content") or {}).get("rendered") or "").strip())

        detail_entries = [entry for entry in listing_entries if _should_fetch_detail(entry["title"])]
        detail_pages = await asyncio.gather(
            *[fetch_new_bedford_page(_slug_from_url(entry["source_url"]), client=client) for entry in detail_entries]
        )

    details_by_url = {
        entry["source_url"]: detail_page
        for entry, detail_page in zip(detail_entries, detail_pages, strict=False)
    }
    return {
        "entries": detail_entries,
        "details_by_url": details_by_url,
    }


def parse_new_bedford_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NEW_BEDFORD_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, datetime]] = set()

    for entry in payload.get("entries") or []:
        detail_page = (payload.get("details_by_url") or {}).get(entry["source_url"])
        if not detail_page:
            continue

        for row in _build_rows(entry, detail_page):
            if row.start_at.date() < current_date:
                continue
            key = (row.source_url, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class NewBedfordEventsAdapter(BaseSourceAdapter):
    source_name = "new_bedford_events"

    async def fetch(self) -> list[str]:
        payload = await load_new_bedford_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_new_bedford_payload/parse_new_bedford_payload from script runner.")


def _extract_listing_entries(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    entries: list[dict[str, str]] = []
    current_section = ""

    for node in soup.children:
        if isinstance(node, NavigableString):
            continue
        if getattr(node, "name", None) != "h2":
            continue

        anchor = node.find("a")
        heading_text = _normalize_space(node.get_text(" ", strip=True))
        if not heading_text:
            continue

        if anchor is None:
            current_section = heading_text
            continue

        source_url = _normalize_space(anchor.get("href"))
        if not source_url:
            continue

        teaser = _extract_listing_teaser(node)
        entries.append(
            {
                "section": current_section,
                "title": heading_text,
                "source_url": source_url,
                "teaser": teaser,
            }
        )

    return entries


def _extract_listing_teaser(node) -> str:
    sibling = node.next_sibling
    while sibling is not None:
        if isinstance(sibling, NavigableString):
            sibling = sibling.next_sibling
            continue
        if getattr(sibling, "name", None) == "p":
            return _normalize_space(sibling.get_text(" ", strip=True))
        if getattr(sibling, "name", None) == "h2":
            break
        sibling = sibling.next_sibling
    return ""


def _should_fetch_detail(title: str) -> bool:
    normalized_title = title.lower()
    return not any(pattern in normalized_title for pattern in TITLE_EXCLUDE_PATTERNS)


def _build_rows(entry: dict[str, str], detail_page: dict) -> list[ExtractedActivity]:
    title = entry["title"] or _html_to_text((detail_page.get("title") or {}).get("rendered"))
    source_url = _normalize_space(detail_page.get("link")) or entry["source_url"]
    rendered_html = ((detail_page.get("content") or {}).get("rendered") or "").strip()
    if not rendered_html:
        return []

    lines = _extract_lines(rendered_html)
    detail_text = " ".join(lines)
    keyword_blob = _token_blob(" ".join([title, entry.get("section", ""), entry.get("teaser", ""), detail_text]))
    if not _should_include_event(title=title, token_blob=keyword_blob):
        return []

    time_range = _extract_time_range(lines)
    occurrence_starts, occurrence_end_override = _extract_occurrences(lines=lines, teaser=entry.get("teaser", ""))
    if not occurrence_starts:
        return []

    price_text = _extract_price_text(lines)
    is_free, free_status = infer_price_classification(price_text)
    age_min, age_max = _extract_age_range(detail_text)
    description = _build_description(lines)
    if entry.get("section"):
        description = _join_non_empty([description, f"Section: {entry['section']}"])

    rows: list[ExtractedActivity] = []
    for occurrence_date in occurrence_starts:
        start_at, end_at = _apply_time_range(occurrence_date, time_range=time_range)
        if occurrence_end_override is not None and time_range is None:
            end_at = datetime.combine(occurrence_end_override, time.min)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=NEW_BEDFORD_VENUE_NAME,
                location_text=NEW_BEDFORD_DEFAULT_LOCATION,
                city=NEW_BEDFORD_CITY,
                state=NEW_BEDFORD_STATE,
                activity_type=_infer_activity_type(keyword_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=(" drop in " in keyword_blob),
                registration_required=_registration_required(keyword_blob, price_text),
                start_at=start_at,
                end_at=end_at,
                timezone=NEW_BEDFORD_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    return rows


def _should_include_event(*, title: str, token_blob: str) -> bool:
    normalized_title = title.lower()
    if any(pattern in normalized_title for pattern in TITLE_EXCLUDE_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in TEXT_EXCLUDE_PATTERNS):
        return False
    return any(pattern in token_blob for pattern in INCLUDE_PATTERNS)


def _extract_occurrences(*, lines: list[str], teaser: str) -> tuple[list[date], date | None]:
    today = datetime.now(ZoneInfo(NEW_BEDFORD_TIMEZONE)).date()

    for line in lines:
        parsed_range = _parse_full_date_range(line, default_year=today.year)
        if parsed_range is not None:
            return [parsed_range[0]], parsed_range[1]

    explicit_dates: list[date] = []
    capture_explicit = False
    for line in lines:
        lowered = line.lower()
        if line.endswith(":") and "upcoming dates" in lowered:
            capture_explicit = True
            continue
        if line.startswith("Class Dates:"):
            explicit_dates.extend(_parse_month_day_line(line, default_year=today.year))
            continue
        if line.startswith("Upcoming:"):
            explicit_dates.extend(_parse_month_day_line(line, default_year=today.year))
            continue
        if capture_explicit:
            if line.startswith("The ") or line.startswith("ABOUT ") or line == "SIGN UP":
                capture_explicit = False
                continue
            explicit_dates.extend(_parse_month_day_line(line, default_year=today.year))

    explicit_dates = sorted({value for value in explicit_dates})
    if explicit_dates:
        return explicit_dates, None

    fallback_dates: list[date] = []
    for line in lines:
        if any(month in line for month in MONTH_NAMES):
            fallback_dates.extend(_parse_month_day_line(line, default_year=today.year))
    fallback_dates = sorted({value for value in fallback_dates})
    if fallback_dates:
        return fallback_dates, None

    joined_text = " | ".join(lines)
    parsed_weekly = _parse_weekly_range(joined_text, default_year=today.year)
    if parsed_weekly:
        return parsed_weekly, None

    teaser_dates = _parse_month_day_line(teaser, default_year=today.year)
    if teaser_dates:
        return teaser_dates, None

    return [], None


def _parse_full_date_range(value: str, *, default_year: int) -> tuple[date, date] | None:
    match = FULL_DATE_RANGE_RE.search(value)
    if not match:
        return None
    start = _build_date(match.group("month1"), match.group("day1"), match.group("year1"), default_year)
    end = _build_date(match.group("month2"), match.group("day2"), match.group("year2"), default_year)
    return (start, end)


def _parse_weekly_range(value: str, *, default_year: int) -> list[date]:
    match = DATE_RANGE_RE.search(value)
    if not match:
        return []

    start = _build_date(match.group("month1"), match.group("day1"), match.group("year1"), default_year)
    end = _build_date(match.group("month2") or match.group("month1"), match.group("day2"), match.group("year2"), default_year)
    if end < start:
        return []

    values: list[date] = []
    current = start
    while current <= end:
        values.append(current)
        current += timedelta(days=7)
    return values


def _parse_month_day_line(value: str, *, default_year: int) -> list[date]:
    cleaned = (
        value.replace("Class Dates:", "")
        .replace("Upcoming Dates:", "")
        .replace("Upcoming:", "")
        .replace("(", " ")
        .replace(")", " ")
    )
    dates: list[date] = []
    for match in MONTH_DAY_CHUNK_RE.finditer(cleaned):
        month_name = match.group("month")
        days_blob = match.group("days")
        year_match = re.search(r"\b(20\d{2})\b", days_blob)
        year = int(year_match.group(1)) if year_match else default_year
        for token in re.split(r"(?:,|&|and)", days_blob):
            day_match = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)?\b", token)
            if not day_match:
                continue
            dates.append(date(year, _month_number(month_name), int(day_match.group(1))))
    return dates


def _extract_time_range(lines: list[str]) -> tuple[time, time] | None:
    for line in lines:
        match = TIME_RANGE_RE.search(line)
        if not match:
            continue
        start_meridiem = match.group("start_meridiem") or match.group("end_meridiem")
        end_meridiem = match.group("end_meridiem") or start_meridiem
        if start_meridiem is None or end_meridiem is None:
            continue
        start = _build_time(match.group("start_hour"), match.group("start_minute"), start_meridiem)
        end = _build_time(match.group("end_hour"), match.group("end_minute"), end_meridiem)
        return start, end
    return None


def _apply_time_range(occurrence_date: date, *, time_range: tuple[time, time] | None) -> tuple[datetime, datetime | None]:
    if time_range is None:
        start_at = datetime.combine(occurrence_date, time.min)
        return start_at, None

    start_time, end_time = time_range
    start_at = datetime.combine(occurrence_date, start_time)
    end_at = datetime.combine(occurrence_date, end_time)
    return start_at, end_at


def _build_time(hour_text: str, minute_text: str | None, meridiem: str) -> time:
    hour = int(hour_text)
    minute = int(minute_text or "0")
    normalized_meridiem = meridiem.upper()
    if normalized_meridiem == "PM" and hour != 12:
        hour += 12
    if normalized_meridiem == "AM" and hour == 12:
        hour = 0
    return time(hour=hour, minute=minute)


def _extract_price_text(lines: list[str]) -> str | None:
    for line in lines:
        normalized = line.lower()
        if "$" in line or "tickets:" in normalized or "admission:" in normalized or "members:" in normalized:
            return line
    return None


def _extract_age_range(detail_text: str) -> tuple[int | None, int | None]:
    if match := AGE_RANGE_RE.search(detail_text):
        return int(match.group("min")), int(match.group("max"))
    if match := AGE_PLUS_RE.search(detail_text):
        return int(match.group("age")), None
    return None, None


def _build_description(lines: list[str]) -> str | None:
    description_lines: list[str] = []
    for line in lines:
        normalized = line.lower()
        if not line:
            continue
        if line == "SIGN UP":
            continue
        if line.startswith("Upcoming Dates"):
            continue
        if line.startswith("Class Dates:"):
            continue
        if line.startswith("Upcoming:"):
            continue
        if len(line.split()) <= 2 and re.fullmatch(r"[A-Za-z&]+", line):
            continue
        if line.startswith("The Classes") or line.startswith("The Instructor"):
            continue
        if line.isupper() and len(line.split()) <= 5:
            continue
        if "$" in line or "Tickets:" in line or "Admission:" in line or "Members:" in line:
            continue
        if TIME_RANGE_RE.search(line):
            continue
        if DATE_RANGE_RE.search(line):
            continue
        if re.fullmatch(rf"(?:{MONTH_PATTERN})\s+[\d,\s&]+", line):
            continue
        description_lines.append(line)
    if not description_lines:
        return None
    return _join_non_empty(description_lines[:8])


def _registration_required(keyword_blob: str, price_text: str | None) -> bool | None:
    if " sign up " in keyword_blob or " reserve " in keyword_blob or " register " in keyword_blob:
        return True
    if price_text:
        return True
    return None


def _infer_activity_type(keyword_blob: str) -> str:
    if " drawing " in keyword_blob or " sketchbook " in keyword_blob:
        return "workshop"
    if " workshop " in keyword_blob or " printmaking " in keyword_blob:
        return "workshop"
    return "class"


def _extract_lines(html: str) -> list[str]:
    soup = BeautifulSoup(unescape(html), "html.parser")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    text = soup.get_text("\n", strip=True)
    lines = [_normalize_space(line) for line in text.splitlines()]
    return [line for line in lines if line]


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    soup = BeautifulSoup(unescape(value), "html.parser")
    text = soup.get_text(" ", strip=True)
    return _normalize_space(text)


def _token_blob(value: str) -> str:
    normalized = re.sub(r"[\W_]+", " ", (value or "").lower())
    return f" {' '.join(normalized.split())} "


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    return path.split("/")[-1]


def _month_number(name: str) -> int:
    return MONTH_NAMES.index(name.capitalize()) + 1


def _build_date(month_name: str, day_text: str, year_text: str | None, default_year: int) -> date:
    year = int(year_text) if year_text else default_year
    return date(year, _month_number(month_name), int(day_text))


def _join_non_empty(parts: list[str]) -> str | None:
    values = [part for part in parts if part]
    if not values:
        return None
    return " | ".join(values)


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(unescape(value).split())
