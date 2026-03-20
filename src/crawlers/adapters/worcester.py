import asyncio
from datetime import datetime
from decimal import Decimal

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

WORCESTER_EVENTS_URL = "https://www.worcesterart.org/events/"
WORCESTER_EVENTS_API_URL = "https://www.worcesterart.org/wp-json/tribe/events/v1/events"

NY_TIMEZONE = "America/New_York"
WORCESTER_VENUE_NAME = "Worcester Art Museum"
WORCESTER_CITY = "Worcester"
WORCESTER_STATE = "MA"
WORCESTER_DEFAULT_LOCATION = "Worcester Art Museum, Worcester, MA"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": WORCESTER_EVENTS_URL,
}

INCLUDE_PATTERNS = (
    " activity ",
    " class ",
    " classes ",
    " conversation ",
    " demo ",
    " demonstration ",
    " discussion ",
    " family ",
    " hands-on ",
    " lab ",
    " lecture ",
    " panel ",
    " studio ",
    " student ",
    " talk ",
    " workshop ",
    " workshops ",
)
HARD_EXCLUDE_PATTERNS = (
    " after hours ",
    " concert ",
    " festival ",
    " film ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " music ",
    " party ",
    " performance ",
    " poetry ",
    " reading ",
    " reception ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)


async def fetch_worcester_events_page(
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
        raise RuntimeError("Unable to fetch Worcester Art Museum events API") from last_exception
    raise RuntimeError("Unable to fetch Worcester Art Museum events API after retries")


async def load_worcester_payload(*, page_limit: int | None = None, per_page: int = 50) -> dict:
    events: list[dict] = []
    next_url = f"{WORCESTER_EVENTS_API_URL}?per_page={per_page}"
    pages_seen = 0

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while next_url:
            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break
            payload = await fetch_worcester_events_page(next_url, client=client)
            pages_seen += 1
            events.extend(payload.get("events") or [])
            next_url = payload.get("next_rest_url")

    return {"events": events}


def parse_worcester_payload(payload: dict) -> list[ExtractedActivity]:
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


class WorcesterEventsAdapter(BaseSourceAdapter):
    source_name = "worcester_events"

    def __init__(self, url: str = WORCESTER_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_worcester_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_worcester_payload/parse_worcester_payload from script runner.")


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
    tag_names = [_normalize_space(item.get("name")) for item in (event_obj.get("tags") or [])]
    tag_names = [value for value in tag_names if value]
    if category_names:
        description = _join_non_empty([description, f"Categories: {', '.join(category_names)}"])
    if tag_names:
        description = _join_non_empty([description, f"Tags: {', '.join(tag_names)}"])

    include_blob = " ".join(
        [
            title,
            description or "",
            " ".join(category_names),
            " ".join(tag_names),
        ]
    ).lower()
    token_blob = f" {' '.join(include_blob.split())} "
    if any(pattern in token_blob for pattern in HARD_EXCLUDE_PATTERNS):
        return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    amount = _extract_amount(event_obj.get("cost"))
    is_free, free_status = _price_classification(description=description, amount=amount)
    location_text = _extract_location_name(event_obj.get("venue")) or WORCESTER_DEFAULT_LOCATION

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=WORCESTER_VENUE_NAME,
        location_text=location_text,
        city=WORCESTER_CITY,
        state=WORCESTER_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in token_blob or "drop in" in token_blob),
        registration_required=("register" in token_blob or "ticket" in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_amount(value: object) -> Decimal | None:
    if value is None:
        return None
    text = _normalize_space(value)
    if not text:
        return None
    cleaned = text.replace("$", "").replace(",", "").strip()
    try:
        return Decimal(cleaned)
    except Exception:
        return None


def _extract_location_name(venue_obj: object) -> str | None:
    if not isinstance(venue_obj, dict):
        return None
    parts = [
        _normalize_space(venue_obj.get("venue")),
        _normalize_space(venue_obj.get("city")),
        _normalize_space(venue_obj.get("state")),
    ]
    parts = [part for part in parts if part]
    if not parts:
        return None
    return ", ".join(parts)


def _infer_activity_type(token_blob: str) -> str:
    if any(keyword in token_blob for keyword in (" lecture ", " talk ", " conversation ", " discussion ", " panel ")):
        return "lecture"
    return "workshop"


def _price_classification(*, description: str | None, amount: Decimal | None) -> tuple[bool | None, str]:
    normalized = " ".join((description or "").lower().split())
    if "with museum admission" in normalized:
        return None, "uncertain"
    return infer_price_classification_from_amount(amount, text=description)


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _html_to_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    return _normalize_space(text)


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    if not values:
        return None
    return " | ".join(values)


def _normalize_space(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split())
    return cleaned or None
