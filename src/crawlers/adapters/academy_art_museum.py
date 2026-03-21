import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

ACADEMY_ART_MUSEUM_EVENTS_URL = "https://academyartmuseum.org/education/adult/events/"
ACADEMY_ART_MUSEUM_TIMEZONE = "America/New_York"
ACADEMY_ART_MUSEUM_VENUE_NAME = "Academy Art Museum"
ACADEMY_ART_MUSEUM_CITY = "Easton"
ACADEMY_ART_MUSEUM_STATE = "MD"
ACADEMY_ART_MUSEUM_DEFAULT_LOCATION = "Easton, MD"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://academyartmuseum.org/education/adult/",
}

TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>a\.?m\.?|p\.?m\.?|am|pm)?"
    r"\s*(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?|am|pm)\b",
    re.IGNORECASE,
)
EXCLUDED_PATTERNS = (
    "annual spring event",
    "camp",
    "camps",
    "exhibition",
    "member reception",
    "music",
    "opening reception",
    "member event",
    "reception",
)
INCLUDED_PATTERNS = (
    "class",
    "classes",
    "workshop",
    "workshops",
    "lecture",
    "lectures",
    "conversation",
    "demonstration",
    "art club",
    "art day",
    "critique",
    "studio",
    "plein air",
)
ART_ACTIVITY_PATTERNS = (
    "acrylic",
    "art history",
    "ceramic",
    "ceramics",
    "collage",
    "cyanotype",
    "drawing",
    "mixed media",
    "painting",
    "pastel",
    "photo",
    "photography",
    "plein air",
    "pottery",
    "printmaking",
    "watercolor",
)


async def fetch_academy_art_museum_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[academy-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[academy-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[academy-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            print(f"[academy-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[academy-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Academy Art Museum events page") from last_exception
    raise RuntimeError("Unable to fetch Academy Art Museum events page after retries")


class AcademyArtMuseumEventsAdapter(BaseSourceAdapter):
    source_name = "academy_art_museum_events"

    def __init__(self, url: str = ACADEMY_ART_MUSEUM_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_academy_art_museum_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_academy_art_museum_events_html(payload, list_url=self.url)


def parse_academy_art_museum_events_html(
    html: str,
    *,
    list_url: str = ACADEMY_ART_MUSEUM_EVENTS_URL,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in soup.select("div.single"):
        title_node = card.select_one("div.single-title h2.s-title")
        link_node = card.select_one("div.single-title a[href]")
        if title_node is None or link_node is None:
            continue

        title = _normalize_space(title_node.get_text(" ", strip=True))
        source_url = urljoin(list_url, link_node.get("href", "").strip())
        if not title or not source_url:
            continue

        categories = [
            _normalize_space(node.get_text(" ", strip=True))
            for node in card.select("div.single-categories span.single-category")
        ]
        categories = [value for value in categories if value]

        appears_text = _normalize_space(
            (card.select_one("div.single-dates span.appears-date") or {}).get_text(" ", strip=True)
            if card.select_one("div.single-dates span.appears-date")
            else ""
        )
        start_date_text = _normalize_space(
            (card.select_one("div.single-dates span.starting-date") or {}).get_text(" ", strip=True)
            if card.select_one("div.single-dates span.starting-date")
            else ""
        )
        end_date_text = _normalize_space(
            (card.select_one("div.single-dates span.ending-date") or {}).get_text(" ", strip=True)
            if card.select_one("div.single-dates span.ending-date")
            else ""
        )

        if not _should_include_event(title=title, categories=categories, appears_text=appears_text):
            continue

        start_date = _parse_hidden_date(start_date_text)
        if start_date is None:
            continue
        end_date = _parse_hidden_date(end_date_text) or start_date

        start_at, end_at = _build_datetimes(
            start_date=start_date,
            end_date=end_date,
            appears_text=appears_text,
        )
        if start_at is None:
            continue

        description_parts: list[str] = []
        if appears_text:
            description_parts.append(f"Schedule: {appears_text}")
        if categories:
            description_parts.append(f"Categories: {', '.join(categories)}")
        description = " | ".join(description_parts) if description_parts else None

        text_blob = " ".join([title, appears_text, description or ""]).lower()
        age_min, age_max = _parse_age_range(text_blob)

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=ACADEMY_ART_MUSEUM_VENUE_NAME,
                location_text=ACADEMY_ART_MUSEUM_DEFAULT_LOCATION,
                city=ACADEMY_ART_MUSEUM_CITY,
                state=ACADEMY_ART_MUSEUM_STATE,
                activity_type=_infer_activity_type(title=title, categories=categories),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=("registration" in text_blob or "application" in text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=ACADEMY_ART_MUSEUM_TIMEZONE,
                **price_classification_kwargs(
                    " ".join([title, " ".join(categories)]),
                    default_is_free=(
                        True
                        if ("free events" in text_blob or title.lower().startswith("free "))
                        else None
                    ),
                ),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _should_include_event(*, title: str, categories: list[str], appears_text: str) -> bool:
    blob = _searchable_blob(" ".join([title, appears_text, " ".join(categories)]))
    if any(pattern in blob for pattern in EXCLUDED_PATTERNS):
        return False
    if any(pattern in blob for pattern in INCLUDED_PATTERNS):
        return True
    return any(pattern in blob for pattern in ART_ACTIVITY_PATTERNS)


def _parse_hidden_date(value: str | None) -> datetime | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, "%b %d, %Y")
    except ValueError:
        return None


def _build_datetimes(
    *,
    start_date: datetime,
    end_date: datetime,
    appears_text: str,
) -> tuple[datetime | None, datetime | None]:
    time_range = TIME_RANGE_RE.search(appears_text)
    if time_range is not None:
        start_at = _combine_with_time(
            start_date=start_date,
            hour=time_range.group("start_hour"),
            minute=time_range.group("start_minute"),
            meridiem=time_range.group("start_meridiem"),
        )
        end_meridiem = time_range.group("end_meridiem")
        start_meridiem = time_range.group("start_meridiem")
        if end_meridiem is None and start_meridiem is not None:
            end_meridiem = start_meridiem
        end_at = _combine_with_time(
            start_date=end_date,
            hour=time_range.group("end_hour"),
            minute=time_range.group("end_minute"),
            meridiem=end_meridiem,
        )
        return start_at, end_at

    time_single = TIME_SINGLE_RE.search(appears_text)
    if time_single is not None:
        return (
            _combine_with_time(
                start_date=start_date,
                hour=time_single.group("hour"),
                minute=time_single.group("minute"),
                meridiem=time_single.group("meridiem"),
            ),
            None,
        )

    return start_date.replace(hour=0, minute=0), None


def _combine_with_time(
    *,
    start_date: datetime,
    hour: str | None,
    minute: str | None,
    meridiem: str | None,
) -> datetime | None:
    if hour is None or meridiem is None:
        return None
    hour_value = int(hour)
    minute_value = int(minute or "0")
    meridiem_value = meridiem.lower().replace(".", "")
    if meridiem_value == "pm" and hour_value != 12:
        hour_value += 12
    if meridiem_value == "am" and hour_value == 12:
        hour_value = 0
    return start_date.replace(hour=hour_value, minute=minute_value)


def _infer_activity_type(*, title: str, categories: list[str]) -> str:
    blob = _searchable_blob(" ".join([title, " ".join(categories)]))
    if "workshop" in blob or "class" in blob or "studio" in blob:
        return "workshop"
    if "lecture" in blob or "talk" in blob or "conversation" in blob or "demonstration" in blob:
        return "lecture"
    if "art club" in blob or "art day" in blob:
        return "activity"
    return "workshop"


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    range_match = re.search(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", text, re.IGNORECASE)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))
    plus_match = re.search(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", text, re.IGNORECASE)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _normalize_space(value: str | None) -> str:
    return " ".join((value or "").split())


def _searchable_blob(value: str) -> str:
    return f" {_normalize_space(value).lower()} "
