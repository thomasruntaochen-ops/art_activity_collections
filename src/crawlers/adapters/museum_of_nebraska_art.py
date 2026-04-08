import asyncio
import re
from datetime import datetime
from decimal import Decimal
from html import unescape

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

MONA_CLASSES_URL = "https://mona.unk.edu/events/category/classes/"
MONA_PROGRAMS_URL = "https://mona.unk.edu/events/category/programs/"
MONA_CLASSES_API_URL = "https://mona.unk.edu/wp-json/tribe/events/v1/events/?categories=classes&per_page=50"
MONA_PROGRAMS_API_URL = "https://mona.unk.edu/wp-json/tribe/events/v1/events/?categories=programs&per_page=50"

MONA_TIMEZONE = "America/Chicago"
MONA_VENUE_NAME = "Museum of Nebraska Art"
MONA_CITY = "Kearney"
MONA_STATE = "NE"
MONA_DEFAULT_LOCATION = "Museum of Nebraska Art, Kearney, NE"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": MONA_CLASSES_URL,
}

INCLUDE_MARKERS = (
    " activity ",
    " art activity ",
    " class ",
    " classes ",
    " drawing ",
    " figure drawing ",
    " workshop ",
    " workshops ",
    " open studio ",
    " mini ",
)
EXCLUDE_MARKERS = (
    " book club ",
    " exhibition ",
    " reading ",
    " tour ",
    " tours ",
    " yoga ",
    " storytime ",
)
AGE_RANGE_RE = r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b"
AGE_PLUS_RE = r"\b(?:ages?\s*)?(\d{1,2})(?:\+| and up)\b"


async def fetch_mona_events(
    url: str,
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
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            if response.status_code < 400:
                return response.json()

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch MONA events API") from last_exception
    raise RuntimeError("Unable to fetch MONA events API after retries")


async def load_mona_payload() -> dict:
    events: list[dict] = []
    seen_ids: set[int] = set()

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for url in (MONA_CLASSES_API_URL, MONA_PROGRAMS_API_URL):
            payload = await fetch_mona_events(url, client=client)
            for event in payload.get("events") or []:
                event_id = int(event.get("id") or 0)
                if event_id and event_id in seen_ids:
                    continue
                if event_id:
                    seen_ids.add(event_id)
                events.append(event)

    return {"events": events}


def parse_mona_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(MONA_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event in payload.get("events") or []:
        row = _build_row(event)
        if row is None or row.start_at.date() < current_date:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class MuseumOfNebraskaArtAdapter(BaseSourceAdapter):
    source_name = "museum_of_nebraska_art_events"

    async def fetch(self) -> list[str]:
        payload = await load_mona_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_mona_payload/parse_mona_payload from the script runner.")


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("end_date"))

    description = _join_non_empty(
        [
            _html_to_text(event_obj.get("excerpt")),
            _html_to_text(event_obj.get("description")),
        ]
    )
    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    if category_names:
        description = _join_non_empty([description, f"Categories: {', '.join(category_names)}"])

    blob = _search_blob(" ".join(filter(None, [title, description, " ".join(category_names)])))
    if any(marker in blob for marker in EXCLUDE_MARKERS):
        return None
    if not any(marker in blob for marker in INCLUDE_MARKERS):
        return None

    amount = _extract_amount(event_obj.get("cost_details"))
    is_free, free_status = infer_price_classification_from_amount(
        amount,
        text=description,
        default_is_free=None,
    )
    age_min, age_max = _parse_age_range(" ".join(filter(None, [title, description])))
    venue_name = _normalize_space((event_obj.get("venue") or {}).get("venue") if isinstance(event_obj.get("venue"), dict) else "")
    location_text = _extract_location(event_obj.get("venue")) or (f"{venue_name}, {MONA_CITY}, {MONA_STATE}" if venue_name else MONA_DEFAULT_LOCATION)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=venue_name or MONA_VENUE_NAME,
        location_text=location_text,
        city=MONA_CITY,
        state=MONA_STATE,
        activity_type="lecture" if " lecture " in blob or " talk " in blob else "workshop",
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop in" in blob or "drop-in" in blob),
        registration_required=(" register " in blob or " registration " in blob or bool(_normalize_space(event_obj.get("website")))),
        start_at=start_at,
        end_at=end_at,
        timezone=MONA_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_amount(cost_details: object) -> Decimal | None:
    if not isinstance(cost_details, dict):
        return None
    values = cost_details.get("values")
    if not isinstance(values, list):
        return None
    for value in values:
        try:
            return Decimal(str(value))
        except Exception:
            continue
    return None


def _extract_location(venue_obj: object) -> str | None:
    if not isinstance(venue_obj, dict):
        return None
    parts = [
        _normalize_space(venue_obj.get("venue")),
        _normalize_space(venue_obj.get("city")),
        _normalize_space(venue_obj.get("state")),
    ]
    parts = [part for part in parts if part]
    return ", ".join(parts) if parts else None


def _parse_datetime(value: object) -> datetime | None:
    text = _normalize_space(value)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = re.search(AGE_RANGE_RE, text, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus = re.search(AGE_PLUS_RE, text, re.IGNORECASE)
    if plus:
        return int(plus.group(1)), None
    return None, None


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    return " | ".join(values) if values else None


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(unescape(value).split())


def _search_blob(value: str) -> str:
    normalized = _normalize_space(value).lower()
    return f" {normalized} "
