import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urlencode
from urllib.parse import urljoin

import httpx

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

NORTON_SIMON_FAMILY_URL = "https://www.nortonsimon.org/calendar/family-youth-and-teens"
NORTON_SIMON_LECTURES_URL = "https://www.nortonsimon.org/calendar/lectures"
NORTON_SIMON_CLASSES_URL = "https://www.nortonsimon.org/calendar/adult-art-classes"
NORTON_SIMON_CATEGORY_IDS = {
    NORTON_SIMON_FAMILY_URL: 5,
    NORTON_SIMON_LECTURES_URL: 4,
    NORTON_SIMON_CLASSES_URL: 6,
}

LA_TIMEZONE = "America/Los_Angeles"
NORTON_SIMON_VENUE_NAME = "Norton Simon Museum"
NORTON_SIMON_CITY = "Pasadena"
NORTON_SIMON_STATE = "CA"
NORTON_SIMON_DEFAULT_LOCATION = "Pasadena, CA"

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
    "tickets",
    "tour",
    "tours",
    "fundraiser",
    "fundraising",
    "performance",
    "music",
    "film",
    "screening",
    "admission",
    "membership",
)
INCLUDED_KEYWORDS = (
    "activity",
    "art class",
    "artmaking",
    "class",
    "classes",
    "family",
    "lecture",
    "teen",
    "teens",
    "workshop",
    "workshops",
    "youth",
)
AJAX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nortonsimon.org/calendar",
    "X-Requested-With": "XMLHttpRequest",
}


def resolve_nortonsimon_category_id(list_url: str) -> int:
    return NORTON_SIMON_CATEGORY_IDS.get(list_url.rstrip("/"), 0)


def build_nortonsimon_events_url(list_url: str, *, limit: int = 12, category: int | None = None) -> str:
    if category is None:
        category = resolve_nortonsimon_category_id(list_url)
    return f"{list_url.rstrip('/')}/getEvents?{urlencode({'limit': limit, 'category': category})}"


async def fetch_nortonsimon_events_json(
    list_url: str,
    *,
    limit: int = 12,
    category: int | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    url = build_nortonsimon_events_url(list_url, limit=limit, category=category)
    print(f"[nortonsimon-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=AJAX_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[nortonsimon-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[nortonsimon-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(
                        f"[nortonsimon-fetch] transient transport error, retrying after {wait_seconds:.1f}s"
                    )
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[nortonsimon-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[nortonsimon-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[nortonsimon-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Norton Simon events payload") from last_exception
    raise RuntimeError("Unable to fetch Norton Simon events payload after retries")


class NortonSimonEventsAdapter(BaseSourceAdapter):
    source_name = "nortonsimon_family"

    def __init__(self, url: str = NORTON_SIMON_FAMILY_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await fetch_nortonsimon_events_json(self.url)
        return [payload]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_nortonsimon_events_json(payload, list_url=self.url)


def parse_nortonsimon_events_json(
    payload: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    current_date = (now or datetime.now()).date()
    data = json.loads(payload)
    events = data.get("Events") or []
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in events:
        details = event_obj.get("EventDetails") or {}
        title = _normalize_text(details.get("Title"))
        if not title or is_irrelevant_item_text(title):
            continue

        summary = _normalize_text(details.get("LandSummary"))
        categories = _collect_categories(details)
        blob = " ".join(part for part in [title, summary or "", " ".join(categories)] if part)
        normalized_blob = blob.lower()
        if any(keyword in normalized_blob for keyword in EXCLUDED_KEYWORDS):
            continue
        if not any(keyword in normalized_blob for keyword in INCLUDED_KEYWORDS):
            continue

        source_url = urljoin(list_url, _normalize_text(details.get("Link")) or list_url)
        start_at, end_at = _parse_event_datetimes(event_obj=event_obj, details=details)
        if start_at is None or start_at.date() < current_date:
            continue

        age_min, age_max = _parse_age_range(title=title, description=summary)
        description = _join_non_empty(
            [
                summary,
                f"Category: {', '.join(categories)}" if categories else None,
            ]
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
                venue_name=NORTON_SIMON_VENUE_NAME,
                location_text=NORTON_SIMON_DEFAULT_LOCATION,
                city=NORTON_SIMON_CITY,
                state=NORTON_SIMON_STATE,
                activity_type=_infer_activity_type(normalized_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in normalized_blob or "drop in" in normalized_blob),
                registration_required=any(
                    keyword in normalized_blob for keyword in ("registration", "register", "reserve", "ticket")
                ),
                start_at=start_at,
                end_at=end_at,
                timezone=LA_TIMEZONE,
                free_verification_status=("confirmed" if "free" in normalized_blob else "inferred"),
            )
        )

    return rows


def _parse_event_datetimes(*, event_obj: dict, details: dict) -> tuple[datetime | None, datetime | None]:
    day = _parse_event_day(event_obj)
    if day is None:
        return None, None
    time_text = _normalize_text(details.get("StartTime"))
    end_text = _normalize_text(details.get("EndTime"))
    if time_text and end_text:
        combined = f"{time_text} - {end_text}"
    else:
        combined = time_text

    base_day = day.replace(hour=0, minute=0, second=0, microsecond=0)
    if not combined:
        return base_day, None

    normalized = combined.replace("\u2013", "-").replace("\u2014", "-")
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


def _parse_event_day(event_obj: dict) -> datetime | None:
    for candidate in (
        event_obj.get("DateFormat1"),
        event_obj.get("SortDate"),
        event_obj.get("Date"),
    ):
        parsed = _parse_event_day_value(candidate)
        if parsed is not None:
            return parsed
    return None


def _parse_event_day_value(value) -> datetime | None:
    normalized = _normalize_text(str(value)) if value is not None else None
    if not normalized:
        return None

    for fmt in ("%Y-%m-%d", "%Y%m%d", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue

    return None


def _collect_categories(details: dict) -> list[str]:
    values: list[str] = []
    for key in ("EventSubCategory", "EventCategory"):
        raw = details.get(key)
        if isinstance(raw, list):
            values.extend(_normalize_text(str(item)) for item in raw if _normalize_text(str(item)))
        elif raw:
            normalized = _normalize_text(str(raw))
            if normalized:
                values.append(normalized)
    return values


def _to_24h(hour: int, minute: int, meridiem: str | None) -> tuple[int, int]:
    normalized = (meridiem or "").replace(".", "").lower()
    if normalized == "pm" and hour != 12:
        hour += 12
    if normalized == "am" and hour == 12:
        hour = 0
    return hour, minute


def _infer_activity_type(text: str) -> str:
    if any(keyword in text for keyword in ("lecture", "talk", "conversation")):
        return "talk"
    if any(keyword in text for keyword in ("class", "workshop", "artmaking")):
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
