import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

HARVARD_CALENDAR_URL = "https://harvardartmuseums.org/calendar"
HARVARD_TIMEZONE = "America/New_York"
HARVARD_VENUE_NAME = "Harvard Art Museums"
HARVARD_CITY = "Cambridge"
HARVARD_STATE = "MA"
HARVARD_DEFAULT_LOCATION = "Harvard Art Museums, 32 Quincy Street, Cambridge, MA"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": HARVARD_CALENDAR_URL,
}

INITIAL_EVENTS_RE = re.compile(
    r"var\s+initialEvents\s*=\s*\[\]\.concat\((\[[\s\S]*?\])\);",
    re.IGNORECASE,
)
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|\u2013|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)

DIRECT_INCLUDE_TYPES = frozenset({"gallery talk", "lecture", "seminar", "workshop"})
DIRECT_EXCLUDE_TYPES = frozenset(
    {
        "film",
        "performance",
        "supporter event",
        "tour",
    }
)
INCLUDE_MARKERS = (
    " talk ",
    " talks ",
    " lecture ",
    " lectures ",
    " workshop ",
    " workshops ",
    " seminar ",
    " seminars ",
    " conversation ",
    " in conversation ",
    " discussion ",
    " materials lab ",
    " art study center seminar ",
)
EXCLUDE_MARKERS = (
    " tour ",
    " tours ",
    " performance ",
    " performances ",
    " recital ",
    " recital:",
    " film ",
    " films ",
    " story time ",
    " storytime ",
    " study session ",
    " reading period ",
    " at night ",
    " mingle ",
    " dj ",
    " drink ",
    " drinks ",
    " snack ",
    " snacks ",
    " supporter ",
    " friends, fellows, and partners ",
    " friends and partners ",
    " members only ",
    " member event ",
    " walking tour ",
)


async def fetch_harvard_calendar_page(
    url: str = HARVARD_CALENDAR_URL,
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
        raise RuntimeError("Unable to fetch Harvard Art Museums calendar page") from last_exception
    raise RuntimeError("Unable to fetch Harvard Art Museums calendar page after retries")


async def load_harvard_payload() -> str:
    return await fetch_harvard_calendar_page()


def parse_harvard_payload(payload: str) -> list[ExtractedActivity]:
    event_objects = _extract_initial_events(payload)
    current_date = datetime.now(ZoneInfo(HARVARD_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in event_objects:
        row = _build_row_from_event_obj(event_obj)
        if row is None:
            continue

        last_date = row.end_at.date() if row.end_at is not None else row.start_at.date()
        if last_date < current_date:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class HarvardCalendarAdapter(BaseSourceAdapter):
    source_name = "harvard_calendar_listings"

    async def fetch(self) -> list[str]:
        html = await fetch_harvard_calendar_page()
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_harvard_payload(payload)


def _extract_initial_events(html: str) -> list[dict]:
    match = INITIAL_EVENTS_RE.search(html)
    if match is None:
        return []
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


def _build_row_from_event_obj(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    if not title or is_irrelevant_item_text(title):
        return None

    event_type = _normalize_space(event_obj.get("event_type")).lower()
    summary_text = _html_to_text(event_obj.get("summary"))
    description_text = _html_to_text(
        event_obj.get("description")
        or ((event_obj.get("html_attributes") or {}).get("description") if isinstance(event_obj.get("html_attributes"), dict) else "")
    )
    description = _join_unique_parts([summary_text, description_text])
    keyword_blob = _matching_blob(" ".join([title, event_type, summary_text, description_text]))

    if not _should_include_event(event_type=event_type, keyword_blob=keyword_blob):
        return None

    source_url = _normalize_space(event_obj.get("event_link"))
    if not source_url:
        source_url = urljoin(HARVARD_CALENDAR_URL, _normalize_space(event_obj.get("slug")))
    if not source_url:
        return None

    start_at = _parse_event_datetime(event_obj.get("date"))
    if start_at is None:
        return None
    end_at = _parse_event_datetime(event_obj.get("end_date"))

    location_text = _build_location_text(event_obj)
    age_min, age_max = _parse_age_range(description or "")

    price_text = description or summary_text or title
    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=HARVARD_VENUE_NAME,
        location_text=location_text,
        city=HARVARD_CITY,
        state=HARVARD_STATE,
        activity_type=_infer_activity_type(event_type=event_type, keyword_blob=keyword_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=_infer_drop_in(keyword_blob),
        registration_required=_infer_registration_required(keyword_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=HARVARD_TIMEZONE,
        **price_classification_kwargs(price_text, default_is_free=None),
    )


def _should_include_event(*, event_type: str, keyword_blob: str) -> bool:
    if not event_type and not keyword_blob:
        return False

    if event_type in DIRECT_EXCLUDE_TYPES:
        return False

    if any(marker in keyword_blob for marker in EXCLUDE_MARKERS):
        return False

    if event_type in DIRECT_INCLUDE_TYPES:
        return True

    if any(marker in keyword_blob for marker in INCLUDE_MARKERS):
        return True

    return False


def _infer_activity_type(*, event_type: str, keyword_blob: str) -> str:
    if event_type in {"workshop", "seminar"}:
        return "workshop"
    if any(marker in keyword_blob for marker in (" workshop ", " workshops ", " seminar ", " seminars ", " materials lab ")):
        return "workshop"
    return "lecture"


def _build_location_text(event_obj: dict) -> str:
    institution = _normalize_space(event_obj.get("institution"))
    address = _normalize_space(event_obj.get("address"))

    parts: list[str] = []
    if institution:
        parts.append(institution)
    elif HARVARD_VENUE_NAME not in parts:
        parts.append(HARVARD_VENUE_NAME)
    if address:
        parts.append(address)
    parts.append(f"{HARVARD_CITY}, {HARVARD_STATE}")

    return ", ".join(parts) if parts else HARVARD_DEFAULT_LOCATION


def _parse_event_datetime(value: object) -> datetime | None:
    text = _normalize_space(value)
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(second=0, microsecond=0)
    local_dt = dt.astimezone(ZoneInfo(HARVARD_TIMEZONE))
    return local_dt.replace(tzinfo=None, second=0, microsecond=0)


def _parse_age_range(text: str | None) -> tuple[int | None, int | None]:
    normalized = _normalize_space(text)
    if not normalized:
        return None, None

    match = AGE_RANGE_RE.search(normalized)
    if match:
        return int(match.group(1)), int(match.group(2))

    match = AGE_PLUS_RE.search(normalized)
    if match:
        return int(match.group(1)), None

    return None, None


def _infer_drop_in(keyword_blob: str) -> bool | None:
    if (
        " drop in " in keyword_blob
        or " drop-in " in keyword_blob
        or " walk ins welcome " in keyword_blob
        or " first come first served " in keyword_blob
    ):
        return True
    if " at capacity " in keyword_blob:
        return False
    return None


def _infer_registration_required(keyword_blob: str) -> bool | None:
    if " does not require registration " in keyword_blob or " no registration required " in keyword_blob:
        return False
    if (
        " registration is required " in keyword_blob
        or " register here " in keyword_blob
        or " register now " in keyword_blob
        or " registration required " in keyword_blob
        or " at capacity " in keyword_blob
        or " invitations are forthcoming " in keyword_blob
    ):
        return True
    if (
        " drop in " in keyword_blob
        or " drop-in " in keyword_blob
        or " walk ins welcome " in keyword_blob
        or " first come first served " in keyword_blob
    ):
        return False
    return None


def _html_to_text(value: object) -> str:
    text = _normalize_space(str(value or ""))
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return _normalize_space(soup.get_text(" ", strip=True))


def _join_unique_parts(parts: list[str]) -> str | None:
    unique: list[str] = []
    seen: set[str] = set()
    for part in parts:
        normalized = _normalize_space(part)
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(normalized)
    if not unique:
        return None
    if len(unique) >= 2 and unique[1].lower().startswith(unique[0].lower()):
        return unique[1]
    return " | ".join(unique)


def _matching_blob(text: str | None) -> str:
    normalized = NON_ALNUM_RE.sub(" ", (text or "").lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    return " ".join(str(value or "").split())
