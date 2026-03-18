import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

GROUNDS_FOR_SCULPTURE_CALENDAR_URL = "https://www.groundsforsculpture.org/Calendar/"

NY_TIMEZONE = "America/New_York"
GROUNDS_FOR_SCULPTURE_VENUE_NAME = "Grounds For Sculpture"
GROUNDS_FOR_SCULPTURE_CITY = "Hamilton"
GROUNDS_FOR_SCULPTURE_STATE = "NJ"
GROUNDS_FOR_SCULPTURE_DEFAULT_LOCATION = "Hamilton, NJ"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": GROUNDS_FOR_SCULPTURE_CALENDAR_URL,
}

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

EXCLUDED_KEYWORDS = (
    "ticket",
    "tour",
    "registration",
    "camp",
    "yoga",
    "sports",
    "free night",
    "fundraising",
    "admission",
    "exhibition",
    "film",
    "tv",
    "reading",
    "writing",
    "open house",
    "performance",
    "music",
    "storytime",
    "dinner",
    "reception",
    "jazz",
    "orchestra",
    "band",
    "tai chi",
)
INCLUDED_KEYWORDS = (
    "talk",
    "class",
    "lecture",
    "activity",
    "workshop",
    "lab",
    "conversation",
    "family",
    "teen",
    "teens",
    "youth",
    "kids",
    "children",
    "child",
    "tots",
)


async def fetch_grounds_for_sculpture_page(
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
        raise RuntimeError("Unable to fetch Grounds For Sculpture calendar") from last_exception
    raise RuntimeError("Unable to fetch Grounds For Sculpture calendar after retries")


async def load_grounds_for_sculpture_calendar_payload() -> dict:
    html = await fetch_grounds_for_sculpture_page(GROUNDS_FOR_SCULPTURE_CALENDAR_URL)
    return {
        "listing_url": GROUNDS_FOR_SCULPTURE_CALENDAR_URL,
        "html": html,
    }


def parse_grounds_for_sculpture_calendar_payload(payload: dict) -> list[ExtractedActivity]:
    list_url = str(payload.get("listing_url") or GROUNDS_FOR_SCULPTURE_CALENDAR_URL)
    html = str(payload.get("html") or "")
    return parse_grounds_for_sculpture_events_html(html, list_url=list_url)


class GroundsForSculptureCalendarAdapter(BaseSourceAdapter):
    source_name = "grounds_for_sculpture_calendar"

    def __init__(self, url: str = GROUNDS_FOR_SCULPTURE_CALENDAR_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_grounds_for_sculpture_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_grounds_for_sculpture_events_html(payload, list_url=self.url)


def parse_grounds_for_sculpture_events_html(html: str, *, list_url: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in soup.select("div.list-teaser"):
        title_anchor = card.select_one("a.list-teaser__title[href]")
        if title_anchor is None:
            continue

        source_url = urljoin(list_url, title_anchor.get("href", "").strip())
        title = _normalize_text(title_anchor.get_text(" ", strip=True))
        if not source_url or not title:
            continue

        category = _normalize_text(
            card.select_one(".list-teaser__category").get_text(" ", strip=True)
            if card.select_one(".list-teaser__category")
            else None
        )
        date_line = _extract_date_line(card)
        price = _normalize_text(
            card.select_one(".list-teaser__price").get_text(" ", strip=True)
            if card.select_one(".list-teaser__price")
            else None
        )
        location = _normalize_text(
            card.select_one(".list-teaser__location").get_text(" ", strip=True)
            if card.select_one(".list-teaser__location")
            else None
        )

        if date_line is None:
            continue
        start_at, end_at = _parse_datetimes(date_line[0], date_line[1])
        if start_at is None:
            continue

        text_blob = " ".join(part for part in [title, category or "", price or "", location or ""] if part).lower()
        if _should_exclude_event(text_blob) and not _should_include_event(text_blob):
            continue
        if not _should_include_event(text_blob):
            continue

        description = " | ".join(
            part for part in [f"Category: {category}" if category else None, price, f"Location: {location}" if location else None] if part
        )
        age_min, age_max = _parse_age_range(" ".join(part for part in [title, description] if part))
        is_free, free_status = infer_price_classification(" ".join(part for part in [price, description] if part))
        registration_required = (
            "registration" in text_blob or "register" in text_blob or "ticket" in text_blob
        ) and "no registration" not in text_blob

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=GROUNDS_FOR_SCULPTURE_VENUE_NAME,
                location_text=GROUNDS_FOR_SCULPTURE_DEFAULT_LOCATION,
                city=GROUNDS_FOR_SCULPTURE_CITY,
                state=GROUNDS_FOR_SCULPTURE_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=registration_required,
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _extract_date_line(card) -> tuple[str, str | None] | None:
    date_container = card.select_one(".list-teaser__date")
    if date_container is None:
        return None
    parts = [
        _normalize_text(span.get_text(" ", strip=True))
        for span in date_container.select("span")
        if _normalize_text(span.get_text(" ", strip=True))
    ]
    if not parts:
        return None
    date_text = parts[0]
    time_text = parts[1] if len(parts) > 1 else None
    return date_text, time_text


def _parse_datetimes(date_text: str, time_text: str | None) -> tuple[datetime | None, datetime | None]:
    try:
        base = datetime.strptime(date_text, "%B %d, %Y")
    except ValueError:
        return None, None

    base = base.replace(hour=0, minute=0, second=0, microsecond=0)
    if not time_text:
        return base, None

    normalized = (
        time_text.replace("\xa0", " ")
        .replace("A.M.", "AM")
        .replace("P.M.", "PM")
        .replace("a.m.", "am")
        .replace("p.m.", "pm")
    )
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
            base.replace(hour=start_hour, minute=start_minute),
            base.replace(hour=end_hour, minute=end_minute),
        )

    single_match = TIME_SINGLE_RE.search(normalized)
    if single_match:
        hour, minute = _to_24h(
            int(single_match.group("hour")),
            int(single_match.group("minute") or 0),
            single_match.group("meridiem"),
        )
        return base.replace(hour=hour, minute=minute), None

    return base, None


def _to_24h(hour: int, minute: int, meridiem: str | None) -> tuple[int, int]:
    normalized = (meridiem or "").replace(".", "").lower()
    if normalized == "pm" and hour != 12:
        hour += 12
    if normalized == "am" and hour == 12:
        hour = 0
    return hour, minute


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _should_exclude_event(text: str) -> bool:
    return any(keyword in text for keyword in EXCLUDED_KEYWORDS)


def _should_include_event(text: str) -> bool:
    return any(keyword in text for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(text: str) -> str:
    if any(keyword in text for keyword in ("talk", "lecture", "conversation")):
        return "talk"
    if any(keyword in text for keyword in ("workshop", "class", "lab", "tots", "artmaking")):
        return "workshop"
    return "activity"


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.replace("\xa0", " ").split())
    return normalized or None
