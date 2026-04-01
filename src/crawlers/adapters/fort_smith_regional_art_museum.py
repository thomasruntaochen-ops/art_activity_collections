from __future__ import annotations

import asyncio
import re
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

AR_TIMEZONE = "America/Chicago"
FSRAM_EVENTS_URL = "https://fsram.org/education/"
FSRAM_VENUE_NAME = "Fort Smith Regional Art Museum"
FSRAM_CITY = "Fort Smith"
FSRAM_STATE = "AR"
FSRAM_LOCATION = "Fort Smith Regional Art Museum, Fort Smith, AR"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

ALWAYS_REJECT_PATTERNS = (
    " guided tours ",
    " group tours ",
    " homeschool ",
    " every saturday ",
    " every wednesday ",
    " once a month ",
    " every other tuesday ",
    " survey ",
    " tour ",
    " tours ",
)
STRONG_INCLUDE_PATTERNS = (
    " art start ",
    " class ",
    " classes ",
    " figure drawing workshop ",
    " junk journaling workshop ",
    " lecture ",
    " procreate ",
    " talk ",
    " workshop ",
    " workshops ",
)
DATE_RANGE_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*&\s*"
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"([A-Za-z]{3,9})\s+(\d{1,2})\s*&\s*(\d{1,2}),?\s*(?:from\s*)?"
    r"(\d{1,2})(?::(\d{2}))?\s*[–-]\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
    re.IGNORECASE,
)
SINGLE_DATE_RANGE_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s*(?:at|from)?\s*"
    r"(\d{1,2})(?::(\d{2}))?\s*[–-]\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
    re.IGNORECASE,
)
SINGLE_DATE_AT_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"([A-Za-z]{3,9})\s+(\d{1,2})\s+at\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
TITLE_DATE_SUFFIX_RE = re.compile(
    r"\s+(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+[A-Za-z]{3,9}\s+\d{1,2}.*$",
    re.IGNORECASE,
)
MONTH_LOOKUP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


async def fetch_fsram_events_page(
    url: str = FSRAM_EVENTS_URL,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
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

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Fort Smith Regional Art Museum education page") from last_exception
    raise RuntimeError("Unable to fetch Fort Smith Regional Art Museum education page after retries")


class FortSmithRegionalArtMuseumAdapter(BaseSourceAdapter):
    source_name = "fort_smith_regional_art_museum_events"

    def __init__(self, url: str = FSRAM_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_fsram_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_fsram_events_html(payload, list_url=self.url)


def parse_fsram_events_html(html: str, *, list_url: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    current_date = datetime.now(ZoneInfo(AR_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for inner in soup.select("div.et_pb_text_inner"):
        title_node = inner.find(["h3", "h2", "h1"])
        if title_node is None:
            continue
        title = _clean_title(title_node.get_text(" ", strip=True))
        if not title:
            continue

        token_blob = _searchable_blob(title)
        if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS):
            continue
        if not any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS):
            continue

        column = inner.find_parent(class_=re.compile(r"\bet_pb_column\b"))
        module = inner.find_parent(class_=re.compile(r"\bet_pb_module\b"))
        if column is None or module is None:
            continue

        raw_text = _normalize_space(inner.get_text(" ", strip=True))
        parsed = _parse_datetime_range(raw_text)
        if parsed is None:
            continue
        start_at, end_at = parsed
        if start_at.date() < current_date:
            continue

        description_parts = []
        for paragraph in inner.find_all("p"):
            text = _normalize_space(paragraph.get_text(" ", strip=True))
            if text:
                description_parts.append(text)
        description = " ".join(description_parts)
        full_blob = _searchable_blob(" ".join([title, description]))
        if any(pattern in full_blob for pattern in ALWAYS_REJECT_PATTERNS):
            continue

        button = column.select_one("a.et_pb_button[href]")
        source_url = button.get("href") if button is not None else list_url
        is_free, free_status = infer_price_classification(description, default_is_free=None)
        age_min, age_max = _parse_age_range(" ".join([title, description]))

        registration_required = (" register " in full_blob or " registration required " in full_blob)
        if button is not None and "qgiv.com" in (button.get("href") or ""):
            registration_required = True

        row = ExtractedActivity(
            source_url=source_url,
            title=title,
            description=description or None,
            venue_name=FSRAM_VENUE_NAME,
            location_text=FSRAM_LOCATION,
            city=FSRAM_CITY,
            state=FSRAM_STATE,
            activity_type=_infer_activity_type(full_blob),
            age_min=age_min,
            age_max=age_max,
            drop_in=(" no need to register " in full_blob or " just drop on by " in full_blob),
            registration_required=registration_required,
            start_at=start_at,
            end_at=end_at,
            timezone=AR_TIMEZONE,
            is_free=is_free,
            free_verification_status=free_status,
        )

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_datetime_range(text: str) -> tuple[datetime, datetime | None] | None:
    year = datetime.now(ZoneInfo(AR_TIMEZONE)).year

    match = DATE_RANGE_RE.search(text)
    if match is not None:
        month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
        if month is None:
            return None
        start_day = int(match.group(2))
        end_day = int(match.group(3))
        start_hour, start_minute = _parse_time_parts(match.group(4), match.group(5), match.group(8))
        end_hour, end_minute = _parse_time_parts(match.group(6), match.group(7), match.group(8))
        start_at = datetime(year, month, start_day, start_hour, start_minute)
        end_at = datetime(year, month, end_day, end_hour, end_minute)
        return start_at, end_at

    match = SINGLE_DATE_RANGE_RE.search(text)
    if match is not None:
        month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
        if month is None:
            return None
        day = int(match.group(2))
        start_hour, start_minute = _parse_time_parts(match.group(3), match.group(4), match.group(7))
        end_hour, end_minute = _parse_time_parts(match.group(5), match.group(6), match.group(7))
        start_at = datetime(year, month, day, start_hour, start_minute)
        end_at = datetime(year, month, day, end_hour, end_minute)
        return start_at, end_at

    match = SINGLE_DATE_AT_RE.search(text)
    if match is not None:
        month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
        if month is None:
            return None
        day = int(match.group(2))
        hour, minute = _parse_time_parts(match.group(3), match.group(4), match.group(5))
        return datetime(year, month, day, hour, minute), None

    return None


def _parse_time_parts(hour_text: str, minute_text: str | None, meridiem: str) -> tuple[int, int]:
    hour = int(hour_text)
    minute = int(minute_text or "0")
    normalized = meridiem.lower()
    if normalized == "pm" and hour != 12:
        hour += 12
    if normalized == "am" and hour == 12:
        hour = 0
    return hour, minute


def _infer_activity_type(token_blob: str) -> str:
    if any(pattern in token_blob for pattern in (" lecture ", " talk ", " talks ")):
        return "lecture"
    return "workshop"


def _clean_title(value: str) -> str:
    return _normalize_space(TITLE_DATE_SUFFIX_RE.sub("", value))


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())
