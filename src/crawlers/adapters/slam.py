import asyncio
from datetime import datetime
from decimal import Decimal

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

SLAM_EVENTS_URL = "https://www.slam.org/events/"
SLAM_EVENTS_API_URL = "https://www.slam.org/wp-json/tribe/events/v1/events"

MO_TIMEZONE = "America/Chicago"
SLAM_VENUE_NAME = "Saint Louis Art Museum"
SLAM_CITY = "St. Louis"
SLAM_STATE = "MO"
SLAM_DEFAULT_LOCATION = "One Fine Arts Drive, Forest Park, St. Louis, MO 63110-1380"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SLAM_EVENTS_URL,
}

INCLUDE_PATTERNS = (
    " activity ",
    " activities ",
    " art making ",
    " conversation ",
    " discussion ",
    " drop in ",
    " drop-in ",
    " family ",
    " families ",
    " lecture ",
    " lectures ",
    " lets talk ",
    " open studio ",
    " scavenger hunt ",
    " studio ",
    " talk ",
    " workshop ",
    " workshops ",
    " young artists ",
)
NO_OVERRIDE_EXCLUDES = (
    " concert ",
    " fundraiser ",
    " fundraising ",
    " jazz ",
    " meditation ",
    " mindfulness ",
    " music ",
    " orchestra ",
    " performance ",
    " poetry ",
    " reading ",
    " reception ",
    " story time ",
    " storytime ",
    " tai chi ",
    " tea ",
    " tasting ",
    " tour ",
    " tours ",
    " wine ",
    " young friends ",
    " yoga ",
)
SOFT_EXCLUDES = (
    " admission ",
    " exhibition ",
    " exhibitions ",
    " free fridays ",
    " member hours ",
    " opening ",
    " party ",
)


async def fetch_slam_events_page(
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
        raise RuntimeError("Unable to fetch Saint Louis Art Museum events API") from last_exception
    raise RuntimeError("Unable to fetch Saint Louis Art Museum events API after retries")


async def load_slam_payload(*, page_limit: int | None = None, per_page: int = 50, start_date: str | None = None) -> dict:
    today = start_date or datetime.now(ZoneInfo(MO_TIMEZONE)).date().isoformat()
    events: list[dict] = []
    next_url = f"{SLAM_EVENTS_API_URL}?per_page={per_page}&page=1&start_date={today}"
    pages_seen = 0

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while next_url:
            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break
            payload = await fetch_slam_events_page(next_url, client=client)
            pages_seen += 1
            events.extend(payload.get("events") or [])
            next_url = payload.get("next_rest_url")

    return {"events": events}


def parse_slam_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(MO_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        row = _build_row(event_obj)
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


class SlamEventsAdapter(BaseSourceAdapter):
    source_name = "slam_events"

    def __init__(self, url: str = SLAM_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_slam_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_slam_payload/parse_slam_payload from the script runner.")


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(_html_to_text(event_obj.get("title")))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("end_date"))
    description = _html_to_text(event_obj.get("description"))
    excerpt = _html_to_text(event_obj.get("excerpt"))
    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    venue_name, location_text = _extract_location(event_obj.get("venue"))
    price_text = _normalize_space(event_obj.get("cost") or "")

    description_parts = [part for part in [excerpt, description] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description_text = " | ".join(description_parts) if description_parts else None

    token_blob = _searchable_blob(" ".join([title, description_text or "", " ".join(category_names)]))
    if not _should_include_event(token_blob=token_blob, title=title):
        return None

    amount = _extract_amount(event_obj.get("cost_details"))
    registration_required = any(
        marker in token_blob for marker in (" register ", " registration ", " reserve ", " ticket ", " tickets ")
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description_text,
        venue_name=venue_name or SLAM_VENUE_NAME,
        location_text=location_text or SLAM_DEFAULT_LOCATION,
        city=SLAM_CITY,
        state=SLAM_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=None,
        age_max=None,
        drop_in=(" drop in " in token_blob or " drop-in " in token_blob),
        registration_required=registration_required,
        start_at=start_at,
        end_at=end_at,
        timezone=MO_TIMEZONE,
        **price_classification_kwargs_from_amount(amount, text=price_text or description_text),
    )


def _should_include_event(*, token_blob: str, title: str) -> bool:
    if any(pattern in token_blob for pattern in NO_OVERRIDE_EXCLUDES):
        return False

    has_override_include = any(pattern in token_blob for pattern in INCLUDE_PATTERNS)
    if any(pattern in token_blob for pattern in SOFT_EXCLUDES) and not has_override_include:
        return False
    if not has_override_include:
        return False

    normalized_title = title.lower()
    if "camp" in normalized_title:
        return False
    if "exhibition opening" in normalized_title or "opening celebration" in normalized_title:
        return False
    if "free fridays" in normalized_title:
        return False
    if "member hours" in normalized_title:
        return False

    return True


def _extract_location(venue_obj: object) -> tuple[str | None, str | None]:
    if not isinstance(venue_obj, dict):
        return None, None
    venue_name = _normalize_space(venue_obj.get("venue"))
    address = _normalize_space(venue_obj.get("address"))
    city = _normalize_space(venue_obj.get("city"))
    state = _normalize_space(venue_obj.get("state") or venue_obj.get("province") or venue_obj.get("stateprovince"))
    postal_code = _normalize_space(venue_obj.get("zip"))
    parts = [part for part in [address, city, state, postal_code] if part]
    location_text = ", ".join(parts) if parts else None
    return venue_name or None, location_text


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


def _infer_activity_type(token_blob: str) -> str:
    if any(marker in token_blob for marker in (" lecture ", " lectures ", " talk ", " discussion ", " conversation ")):
        return "lecture"
    return "workshop"


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y-%m-%d":
                return datetime.combine(parsed.date(), datetime.min.time())
            return parsed
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None)
    except ValueError:
        return None


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _searchable_blob(value: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else " " for ch in value)
    return f" {' '.join(normalized.split())} "
