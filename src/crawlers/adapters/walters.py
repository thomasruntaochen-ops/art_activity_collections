import asyncio
import html
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

WALTERS_EVENTS_URL = "https://thewalters.org/events/"
WALTERS_EVENTS_API_URL = "https://thewalters.org/wp-json/tribe/events/v1/events"
WALTERS_TIMEZONE = "America/New_York"
WALTERS_VENUE_NAME = "Walters Art Museum"
WALTERS_CITY = "Baltimore"
WALTERS_STATE = "MD"
WALTERS_DEFAULT_LOCATION = "Baltimore, MD"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": WALTERS_EVENTS_URL,
}

INCLUDE_PATTERNS = (
    " art-making ",
    " art making ",
    " class ",
    " classes ",
    " conversation ",
    " discussion ",
    " drawing ",
    " families ",
    " family ",
    " kids ",
    " lecture ",
    " lectures ",
    " sketching ",
    " talk ",
    " workshop ",
    " workshops ",
)
HARD_EXCLUDE_PATTERNS = (
    " celebration ",
    " exhibition ",
    " exhibitions ",
    " fundraiser ",
    " fundraising ",
    " music ",
    " performance ",
    " reception ",
    " supporters ",
    " tour ",
    " tours ",
    " yoga ",
)


async def fetch_walters_events_page(
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
        raise RuntimeError("Unable to fetch Walters events endpoint") from last_exception
    raise RuntimeError("Unable to fetch Walters events endpoint after retries")


async def load_walters_events_payload(
    *,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, list[dict]]:
    events: list[dict] = []
    next_url = f"{WALTERS_EVENTS_API_URL}?per_page={per_page}"
    pages_seen = 0

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while next_url:
            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break
            payload = await fetch_walters_events_page(next_url, client=client)
            pages_seen += 1
            events.extend(payload.get("events") or [])
            next_url = payload.get("next_rest_url")

    return {"events": events}


class WaltersEventsAdapter(BaseSourceAdapter):
    source_name = "walters_events"

    async def fetch(self) -> list[str]:
        payload = await load_walters_events_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_walters_events_payload/parse_walters_events_payload from script runner.")


def parse_walters_events_payload(payload: dict) -> list[ExtractedActivity]:
    current_day = datetime.now().date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        row = _build_row(event_obj)
        if row is None or row.start_at.date() < current_day:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("end_date"))
    category_names = [_normalize_space(html.unescape(item.get("name") or "")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    description_text = _html_to_text(event_obj.get("description"))
    excerpt_text = _html_to_text(event_obj.get("excerpt"))
    price_text = _normalize_space(event_obj.get("cost"))

    description_parts = [part for part in [excerpt_text, description_text] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = " | ".join(description_parts) if description_parts else None

    if not _should_include_event(title=title, description=description, categories=category_names):
        return None

    blob = _searchable_blob(" ".join([title, description or "", " ".join(category_names)]))
    venue_name = _extract_venue_name(event_obj.get("venue"))
    location_text = venue_name or WALTERS_DEFAULT_LOCATION

    return ExtractedActivity(
        source_url=urljoin(WALTERS_EVENTS_URL, source_url),
        title=title,
        description=description,
        venue_name=WALTERS_VENUE_NAME,
        location_text=location_text,
        city=WALTERS_CITY,
        state=WALTERS_STATE,
        activity_type=_infer_activity_type(blob),
        age_min=None,
        age_max=None,
        drop_in=(" drop-in " in blob or " drop in " in blob),
        registration_required=(
            (" registration required " in blob)
            or (" register " in blob)
            or (" tickets required " in blob)
        ) and " not required " not in blob,
        start_at=start_at,
        end_at=end_at,
        timezone=WALTERS_TIMEZONE,
        **price_classification_kwargs(
            " ".join([price_text, description or ""]),
            default_is_free=True if " free " in blob else None,
        ),
    )


def _should_include_event(*, title: str, description: str | None, categories: list[str]) -> bool:
    blob = _searchable_blob(" ".join([title, description or "", " ".join(categories)]))
    has_include = any(pattern in blob for pattern in INCLUDE_PATTERNS)
    if not has_include:
        return False
    if " supporters " in blob or " tour " in blob or " tours " in blob:
        return False
    if any(pattern in blob for pattern in HARD_EXCLUDE_PATTERNS):
        if " art-making " not in blob and " art making " not in blob and " lectures " not in blob and " lecture " not in blob:
            return False
        if " performance " in blob:
            return False
    return True


def _infer_activity_type(blob: str) -> str:
    if " lecture " in blob or " lectures " in blob or " talk " in blob or " conversation " in blob:
        return "lecture"
    if " family " in blob or " families " in blob:
        return "activity"
    return "workshop"


def _extract_venue_name(value: object) -> str | None:
    if isinstance(value, dict):
        for key in ("venue", "venue_name", "name"):
            candidate = _normalize_space(value.get(key))
            if candidate:
                return candidate
    return None


def _parse_datetime(value: str | None) -> datetime | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _html_to_text(value: str | None) -> str | None:
    if not value:
        return None
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    normalized = _normalize_space(html.unescape(text))
    return normalized or None


def _normalize_space(value: str | None) -> str:
    return " ".join((value or "").split())


def _searchable_blob(value: str) -> str:
    return f" {_normalize_space(value).lower()} "
