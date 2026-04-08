import asyncio
from datetime import datetime
from decimal import Decimal
import re

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

UMFA_EVENTS_URL = "https://umfa.utah.edu/events/"
UMFA_EVENTS_API_URL = "https://umfa.utah.edu/wp-json/tribe/events/v1/events"

UMFA_TIMEZONE = "America/Denver"
UMFA_VENUE_NAME = "Utah Museum of Fine Arts"
UMFA_CITY = "Salt Lake City"
UMFA_STATE = "UT"
UMFA_DEFAULT_LOCATION = "Utah Museum of Fine Arts, Salt Lake City, UT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": UMFA_EVENTS_URL,
}

INCLUDE_PATTERNS = (
    " all abilities ",
    " art making ",
    " artist talk ",
    " creative aging ",
    " discussion ",
    " family ",
    " families ",
    " make a ",
    " panel ",
    " print ",
    " talk ",
    " third saturday ",
    " workshop ",
)
EXCLUDE_PATTERNS = (
    " admission ",
    " concert ",
    " exhibition ",
    " film ",
    " fundraiser ",
    " fundraising ",
    " music ",
    " open house ",
    " performance ",
    " poetry ",
    " reading ",
    " reception ",
    " storytime ",
    " tour ",
    " yoga ",
)


async def fetch_umfa_events_page(
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
        raise RuntimeError("Unable to fetch UMFA events API") from last_exception
    raise RuntimeError("Unable to fetch UMFA events API after retries")


async def load_umfa_payload(*, page_limit: int | None = None, per_page: int = 100) -> dict:
    events: list[dict] = []
    next_url = f"{UMFA_EVENTS_API_URL}?per_page={per_page}"
    pages_seen = 0

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while next_url:
            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break
            payload = await fetch_umfa_events_page(next_url, client=client)
            pages_seen += 1
            events.extend(payload.get("events") or [])
            next_url = payload.get("next_rest_url")

    return {"events": events}


def parse_umfa_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(UMFA_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        row = _build_row(event_obj)
        if row is None or row.start_at.date() < current_date:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class UmfaEventsAdapter(BaseSourceAdapter):
    source_name = "umfa_events"

    async def fetch(self) -> list[str]:
        payload = await load_umfa_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_umfa_payload/parse_umfa_payload from script runner.")


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("end_date"))

    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    description_text = _clean_event_text(_html_to_text(event_obj.get("description")))
    excerpt_text = _clean_event_text(_html_to_text(event_obj.get("excerpt")))
    price_text = _normalize_space(event_obj.get("cost"))

    description_parts: list[str] = []
    if excerpt_text:
        description_parts.append(excerpt_text)
    if description_text and description_text not in excerpt_text:
        description_parts.append(description_text)
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = " | ".join(description_parts) if description_parts else None

    blob = _searchable_blob(" ".join([title, description or "", " ".join(category_names)]))
    if not _should_include_event(blob):
        return None

    amount = _extract_amount(event_obj.get("cost_details"))
    is_free, free_status = infer_price_classification_from_amount(amount, text=price_text)
    location_text = _extract_location_name(event_obj.get("venue")) or UMFA_DEFAULT_LOCATION

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=UMFA_VENUE_NAME,
        location_text=location_text,
        city=UMFA_CITY,
        state=UMFA_STATE,
        activity_type=_infer_activity_type(blob),
        age_min=None,
        age_max=None,
        drop_in=(" drop-in " in blob or " drop in " in blob),
        registration_required=(
            (" register " in blob)
            or (" registration " in blob)
            or (" ticket " in blob)
            or (" tickets " in blob)
        ) and " registration is not required " not in blob,
        start_at=start_at,
        end_at=end_at,
        timezone=UMFA_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _should_include_event(blob: str) -> bool:
    if any(pattern in blob for pattern in EXCLUDE_PATTERNS):
        if (
            " artist talk " not in blob
            and " panel " not in blob
            and " workshop " not in blob
            and " third saturday " not in blob
        ):
            return False
        if " yoga " in blob or " storytime " in blob:
            return False
    return any(pattern in blob for pattern in INCLUDE_PATTERNS)


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
    venue_name = _normalize_space(venue_obj.get("venue"))
    city = _normalize_space(venue_obj.get("city"))
    state = _normalize_space(venue_obj.get("state"))
    if venue_name.lower() == "umfa":
        venue_name = UMFA_VENUE_NAME
    if state.lower() == "utah":
        state = "UT"
    parts = [venue_name, city, state]
    parts = [part for part in parts if part]
    if not parts:
        return None
    return ", ".join(parts)


def _infer_activity_type(blob: str) -> str:
    if any(keyword in blob for keyword in (" talk ", " panel ", " discussion ")):
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


def _clean_event_text(value: str) -> str:
    text = _normalize_space(value)
    if not text:
        return ""
    text = re.sub(r"When:\s+Thursday,\s+January\s+1,\s+1970,\s+12\s+am", "", text, flags=re.IGNORECASE)
    for marker in (
        " You May also Like ",
        " You May Also Like ",
        " You may Also Like ",
        " Related News ",
        " Learn More ",
    ):
        index = text.find(marker)
        if index != -1:
            text = text[:index]
    return _normalize_space(text)


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _searchable_blob(value: str) -> str:
    return f" {_normalize_space(value).lower()} "
