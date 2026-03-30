from __future__ import annotations

import asyncio
import re
from datetime import date
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

AR_TIMEZONE = "America/Chicago"
BRADBURY_EVENTS_URL = "https://bradburyartmuseum.org/event"
BRADBURY_VENUE_NAME = "Bradbury Art Museum"
BRADBURY_CITY = "Jonesboro"
BRADBURY_STATE = "AR"
BRADBURY_LOCATION = "Bradbury Art Museum, Jonesboro, AR"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " class ",
    " classes ",
    " lecture ",
    " lectures ",
    " photography workshop ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
ALWAYS_REJECT_PATTERNS = (
    " opening reception ",
    " reception ",
    " tour ",
    " tours ",
    " yoga ",
)
REGISTRATION_PATTERNS = (
    " contact ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)


async def fetch_bradbury_events_page(
    url: str = BRADBURY_EVENTS_URL,
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
        raise RuntimeError("Unable to fetch Bradbury events page") from last_exception
    raise RuntimeError("Unable to fetch Bradbury events page after retries")


class BradburyArtMuseumAdapter(BaseSourceAdapter):
    source_name = "bradbury_art_museum_events"

    def __init__(self, url: str = BRADBURY_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_bradbury_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_bradbury_events_html(payload, list_url=self.url)


def parse_bradbury_events_html(html: str, *, list_url: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    current_date = datetime.now(ZoneInfo(AR_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for article in soup.select("article.eventlist-event--upcoming"):
        title_node = article.select_one(".eventlist-title-link")
        if title_node is None:
            continue
        title = _normalize_space(title_node.get_text(" ", strip=True))
        if not title:
            continue

        description = " ".join(
            _normalize_space(node.get_text(" ", strip=True))
            for node in article.select(".eventlist-excerpt p")
            if _normalize_space(node.get_text(" ", strip=True))
        )
        token_blob = _searchable_blob(" ".join([title, description]))
        if not _should_keep_event(token_blob):
            continue

        source_url = httpx.URL(list_url).join(title_node.get("href") or "").human_repr()
        event_date = _parse_event_date(article)
        if event_date is None or event_date < current_date:
            continue
        start_at, end_at = _parse_event_times(article, event_date)
        if start_at is None:
            continue

        is_free, free_status = infer_price_classification(description, default_is_free=None)
        age_min, age_max = _parse_age_range(" ".join([title, description]))

        row = ExtractedActivity(
            source_url=source_url,
            title=title,
            description=description or None,
            venue_name=BRADBURY_VENUE_NAME,
            location_text=BRADBURY_LOCATION,
            city=BRADBURY_CITY,
            state=BRADBURY_STATE,
            activity_type=_infer_activity_type(token_blob),
            age_min=age_min,
            age_max=age_max,
            drop_in=False,
            registration_required=any(pattern in token_blob for pattern in REGISTRATION_PATTERNS),
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


def _parse_event_date(article) -> date | None:
    node = article.select_one("time.event-date")
    if node is None:
        return None
    value = (node.get("datetime") or "").strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_event_times(article, event_date: date) -> tuple[datetime | None, datetime | None]:
    start_node = article.select_one("time.event-time-12hr-start")
    if start_node is None:
        return datetime.combine(event_date, datetime.min.time()), None

    start_text = _normalize_space(start_node.get_text(" ", strip=True))
    start_parts = _parse_clock_time(start_text)
    if start_parts is None:
        return None, None
    start_at = datetime(event_date.year, event_date.month, event_date.day, *start_parts)

    end_node = article.select_one("time.event-time-12hr-end")
    if end_node is None:
        return start_at, None
    end_text = _normalize_space(end_node.get_text(" ", strip=True))
    end_parts = _parse_clock_time(end_text)
    if end_parts is None:
        return start_at, None
    end_at = datetime(event_date.year, event_date.month, event_date.day, *end_parts)
    return start_at, end_at


def _parse_clock_time(value: str) -> tuple[int, int] | None:
    match = re.search(r"(\d{1,2})(?::(\d{2}))?\s*(AM|PM)", value, re.IGNORECASE)
    if match is None:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    meridiem = match.group(3).lower()
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return hour, minute


def _should_keep_event(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    return any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)


def _infer_activity_type(token_blob: str) -> str:
    if any(pattern in token_blob for pattern in (" lecture ", " lectures ", " talk ", " talks ")):
        return "lecture"
    return "workshop"


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
