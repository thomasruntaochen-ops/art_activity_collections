import asyncio
from datetime import datetime
from decimal import Decimal

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

PARRISH_EVENTS_URL = "https://parrishart.org/education/childrens-workshops/"
PARRISH_EVENTS_API_URL = "https://parrishart.org/wp-json/tribe/events/v1/events"

NY_TIMEZONE = "America/New_York"
PARRISH_VENUE_NAME = "Parrish Art Museum"
PARRISH_CITY = "Water Mill"
PARRISH_STATE = "NY"
PARRISH_DEFAULT_LOCATION = "Water Mill, NY"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": PARRISH_EVENTS_URL,
}

INCLUDE_PATTERNS = (
    " activity ",
    " art ",
    " class ",
    " classes ",
    " conversation ",
    " discussion ",
    " lab ",
    " lecture ",
    " student ",
    " students ",
    " talk ",
    " workshop ",
    " workshops ",
)
HARD_EXCLUDE_PATTERNS = (
    " admission ",
    " camp ",
    " camps ",
    " closed ",
    " concert ",
    " dance ",
    " exhibition ",
    " film ",
    " fundraiser ",
    " fundraising ",
    " gallery closed ",
    " galleries closed ",
    " mindfulness ",
    " music ",
    " open house ",
    " orchestra ",
    " performance ",
    " poetry ",
    " reading ",
    " reception ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)


async def fetch_parrish_events_page(
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
        raise RuntimeError("Unable to fetch Parrish events API") from last_exception
    raise RuntimeError("Unable to fetch Parrish events API after retries")


async def load_parrish_payload(*, page_limit: int | None = None, per_page: int = 50) -> dict:
    events: list[dict] = []
    next_url = f"{PARRISH_EVENTS_API_URL}?per_page={per_page}"
    pages_seen = 0

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while next_url:
            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break
            payload = await fetch_parrish_events_page(next_url, client=client)
            pages_seen += 1
            events.extend(payload.get("events") or [])
            next_url = payload.get("next_rest_url")

    return {"events": events}


def parse_parrish_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
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


class ParrishEventsAdapter(BaseSourceAdapter):
    source_name = "parrish_events"

    def __init__(self, url: str = PARRISH_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_parrish_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_parrish_payload/parse_parrish_payload from script runner.")


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("end_date"))

    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    list_description = _html_to_text(event_obj.get("description"))
    excerpt = _html_to_text(event_obj.get("excerpt"))
    price_text = _normalize_space(event_obj.get("cost") or "")

    description_parts = [part for part in [excerpt, list_description] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = " | ".join(description_parts) if description_parts else None

    include_blob = " ".join([title, description or "", " ".join(category_names)]).lower()
    token_blob = f" {' '.join(include_blob.split())} "
    if (
        " dance " in token_blob
        or " tour " in token_blob
        or " tours " in token_blob
        or " galleries closed " in token_blob
        or " gallery closed " in token_blob
    ):
        return None
    if any(pattern in token_blob for pattern in HARD_EXCLUDE_PATTERNS):
        if (
            " workshop " not in token_blob
            and " class " not in token_blob
            and " classes " not in token_blob
            and " lecture " not in token_blob
            and " talk " not in token_blob
            and " conversation " not in token_blob
        ):
            return None
        if " camp " in token_blob or " camps " in token_blob:
            return None
        if " galleries closed " in token_blob or " gallery closed " in token_blob:
            return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    amount = _extract_amount(event_obj.get("cost_details"))
    is_free, free_status = infer_price_classification_from_amount(amount, text=price_text)
    location_name = _extract_location_name(event_obj.get("venue"))
    activity_type = _infer_activity_type(token_blob)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=PARRISH_VENUE_NAME,
        location_text=location_name or PARRISH_DEFAULT_LOCATION,
        city=PARRISH_CITY,
        state=PARRISH_STATE,
        activity_type=activity_type,
        age_min=None,
        age_max=None,
        drop_in=(" drop-in " in token_blob or " drop in " in token_blob),
        registration_required=(" register " in token_blob or " ticket " in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_amount(cost_details: object) -> Decimal | None:
    if not isinstance(cost_details, dict):
        return None
    values = cost_details.get("values")
    if isinstance(values, list):
        for value in values:
            try:
                return Decimal(str(value))
            except Exception:
                continue
    return None


def _extract_location_name(venue_obj: object) -> str | None:
    if not isinstance(venue_obj, dict):
        return None
    name = _normalize_space(venue_obj.get("venue"))
    city = _normalize_space(venue_obj.get("city"))
    state = _normalize_space(venue_obj.get("state"))
    parts = [part for part in [name, city, state] if part]
    if not parts:
        return None
    return ", ".join(parts)


def _infer_activity_type(token_blob: str) -> str:
    if (
        " talk " in token_blob
        or " lecture " in token_blob
        or " conversation " in token_blob
        or " discussion " in token_blob
    ):
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
    return None


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())
