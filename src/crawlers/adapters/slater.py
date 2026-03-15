from __future__ import annotations

import asyncio
from datetime import date
from datetime import datetime
from datetime import time
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

SLATER_EVENTS_PAGE_URL = "https://www.slatermuseum.org/events"
SLATER_EVENTS_ICS_URL = "https://www.slatermuseum.org/fs/calendar-manager/events.ics?calendar_ids=2"

SLATER_TIMEZONE = "America/New_York"
SLATER_VENUE_NAME = "Slater Memorial Museum"
SLATER_CITY = "Norwich"
SLATER_STATE = "CT"
SLATER_DEFAULT_LOCATION = "Norwich, CT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/calendar,text/plain;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SLATER_EVENTS_PAGE_URL,
}

INCLUDED_KEYWORDS = (
    "talk",
    "lecture",
    "conversation",
    "discussion",
    "book signing",
    "workshop",
    "class",
    "art class",
    "activity",
    "lab",
)
EXCLUDED_KEYWORDS = (
    "closed",
    "storytime",
    "tour",
    "docent tour",
    "exhibition",
    "art exhibition",
    "show opening",
    "art show",
    "showcase",
    "festival",
    "celebration",
    "movie series",
    "movie",
    "film",
    "artisan craft show",
)
REGISTRATION_MARKERS = (
    "register",
    "registration",
    "rsvp",
    "reserve",
    "ticket required",
)
AGE_RANGE_REPLACEMENTS = (
    ("and up", "+"),
    ("–", "-"),
    ("—", "-"),
    (" to ", "-"),
)


async def fetch_slater_events_ics(
    url: str = SLATER_EVENTS_ICS_URL,
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
        raise RuntimeError("Unable to fetch Slater events ICS feed") from last_exception
    raise RuntimeError("Unable to fetch Slater events ICS feed after retries")


def parse_slater_events_ics(
    ics_text: str,
    *,
    start_date: str | date | datetime | None = None,
) -> list[ExtractedActivity]:
    start_day = _coerce_start_date(start_date)
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event in _parse_ics_events(ics_text):
        row = _build_row_from_event(event)
        if row is None:
            continue
        if row.start_at.date() < start_day:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class SlaterEventsAdapter(BaseSourceAdapter):
    source_name = "slater_events"

    async def fetch(self) -> list[str]:
        return [await fetch_slater_events_ics()]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_slater_events_ics(payload)


def _coerce_start_date(value: str | date | datetime | None) -> date:
    if value is None:
        return datetime.now(ZoneInfo(SLATER_TIMEZONE)).date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_ics_events(ics_text: str) -> list[dict[str, dict[str, object]]]:
    events: list[dict[str, dict[str, object]]] = []
    current_event: dict[str, dict[str, object]] | None = None

    for line in _unfold_ics_lines(ics_text):
        if line == "BEGIN:VEVENT":
            current_event = {}
            continue
        if line == "END:VEVENT":
            if current_event:
                events.append(current_event)
            current_event = None
            continue
        if current_event is None or ":" not in line:
            continue

        raw_key, raw_value = line.split(":", 1)
        key_parts = raw_key.split(";")
        key = key_parts[0].upper()
        params: dict[str, str] = {}
        for item in key_parts[1:]:
            if "=" not in item:
                continue
            param_key, param_value = item.split("=", 1)
            params[param_key.upper()] = param_value

        current_event[key] = {"value": raw_value, "params": params}

    return events


def _unfold_ics_lines(ics_text: str) -> list[str]:
    unfolded: list[str] = []
    for raw_line in ics_text.splitlines():
        line = raw_line.rstrip("\r")
        if line.startswith((" ", "\t")) and unfolded:
            unfolded[-1] += line[1:]
            continue
        unfolded.append(line)
    return unfolded


def _build_row_from_event(event: dict[str, dict[str, object]]) -> ExtractedActivity | None:
    title = _normalize_space(_ics_text_value(event.get("SUMMARY")))
    description = _normalize_space(_ics_text_value(event.get("DESCRIPTION")))
    location = _normalize_space(_ics_text_value(event.get("LOCATION"))) or SLATER_DEFAULT_LOCATION
    uid = _normalize_space(_ics_text_value(event.get("UID")))

    if not title:
        return None
    if not _should_include_event(title=title, description=description):
        return None

    start_at = _parse_ics_datetime(event.get("DTSTART"))
    if start_at is None:
        return None

    end_at = _parse_ics_datetime(event.get("DTEND"))
    if end_at is not None and end_at <= start_at:
        end_at = None

    text_blob = _normalize_space(" ".join(part for part in (title, description, location) if part))
    age_min, age_max = _parse_age_range(text_blob)
    is_free, free_status = infer_price_classification(text_blob)

    source_url = SLATER_EVENTS_PAGE_URL
    if uid:
        source_url = f"{SLATER_EVENTS_ICS_URL}#uid={quote(uid, safe='')}"

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description or None,
        venue_name=SLATER_VENUE_NAME,
        location_text=location,
        city=SLATER_CITY,
        state=SLATER_STATE,
        activity_type=_infer_activity_type(title=title, description=description),
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob.lower() or "drop in" in text_blob.lower()),
        registration_required=any(marker in text_blob.lower() for marker in REGISTRATION_MARKERS),
        start_at=start_at,
        end_at=end_at,
        timezone=SLATER_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _ics_text_value(field: dict[str, object] | None) -> str:
    if not field:
        return ""
    value = str(field.get("value") or "")
    return (
        value.replace("\\n", "\n")
        .replace("\\N", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
    )


def _parse_ics_datetime(field: dict[str, object] | None) -> datetime | None:
    if not field:
        return None

    value = str(field.get("value") or "").strip()
    params = field.get("params") or {}
    if not value:
        return None

    is_date_only = isinstance(params, dict) and params.get("VALUE") == "DATE"
    if is_date_only or len(value) == 8:
        parsed_day = datetime.strptime(value[:8], "%Y%m%d").date()
        return datetime.combine(parsed_day, time.min)

    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
        try:
            if value.endswith("Z"):
                return datetime.strptime(value, f"{fmt}Z")
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _should_include_event(*, title: str, description: str) -> bool:
    text_blob = _normalize_space(f"{title} {description}").lower()
    if not text_blob:
        return False

    if any(keyword in text_blob for keyword in EXCLUDED_KEYWORDS):
        return False

    return any(keyword in text_blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(*, title: str, description: str) -> str:
    text_blob = _normalize_space(f"{title} {description}").lower()
    if any(keyword in text_blob for keyword in ("talk", "lecture", "conversation", "discussion", "book signing")):
        return "talk"
    return "workshop"


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    lowered = text.lower()
    for old, new in AGE_RANGE_REPLACEMENTS:
        lowered = lowered.replace(old, new)

    if "ages " not in lowered and "age " not in lowered:
        return None, None

    fragments = lowered.replace("/", " ").replace("(", " ").replace(")", " ").split()
    for index, token in enumerate(fragments):
        if token not in {"ages", "age"} or index + 1 >= len(fragments):
            continue
        next_token = fragments[index + 1].strip(".,")
        if next_token.endswith("+") and next_token[:-1].isdigit():
            return int(next_token[:-1]), None
        if "-" in next_token:
            first, second = next_token.split("-", 1)
            if first.isdigit() and second.isdigit():
                return int(first), int(second)
        if next_token.isdigit():
            return int(next_token), None

    return None, None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())
