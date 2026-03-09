import asyncio
import html
import json
from datetime import datetime
from decimal import Decimal
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.types import ExtractedActivity

SDMART_EVENTS_URL = "https://www.sdmart.org/events/"

LA_TIMEZONE = "America/Los_Angeles"
SDMART_VENUE_NAME = "The San Diego Museum of Art"
SDMART_CITY = "San Diego"
SDMART_STATE = "CA"
SDMART_DEFAULT_LOCATION = "San Diego, CA"

EXCLUDED_PRIMARY_KEYWORDS = (
    "ticket",
    "tickets",
    "tour",
    "tours",
    "registration",
    "camp",
    "free night",
    "fundraising",
    "admission",
    "exhibition",
    "film",
    "tv",
    "reading",
    "writing",
    "open house",
    "performance",
    "music",
    "concert",
    "member morning",
    "member mornings",
    "bus tour",
)
INCLUDED_KEYWORDS = (
    "talk",
    "lecture",
    "conversation",
    "class",
    "activity",
    "workshop",
    "lab",
    "family",
    "drawing",
    "speaker",
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.sdmart.org/events/",
}


async def fetch_sdmart_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[sdmart-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[sdmart-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[sdmart-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[sdmart-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[sdmart-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[sdmart-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[sdmart-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch SDMA events page") from last_exception
    raise RuntimeError("Unable to fetch SDMA events page after retries")


class SdmartEventsAdapter(BaseSourceAdapter):
    source_name = "sdmart_events"

    def __init__(self, url: str = SDMART_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_sdmart_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_sdmart_events_html(payload, list_url=self.url)


def parse_sdmart_events_html(
    html: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    current_date = (now or datetime.now()).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        data = _safe_json_loads(script.string or script.get_text() or "")
        if data is None:
            continue
        for event_obj in _iter_event_objects(data):
            row = _build_row_from_event_obj(event_obj=event_obj, list_url=list_url, current_date=current_date)
            if row is None:
                continue
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    return rows


def _build_row_from_event_obj(
    *,
    event_obj: dict,
    list_url: str,
    current_date,
) -> ExtractedActivity | None:
    title = _normalize_text(event_obj.get("name"))
    if not title:
        return None

    description = _normalize_text(event_obj.get("description"))
    source_url = urljoin(list_url, _normalize_text(event_obj.get("url")) or list_url)
    start_at = _parse_datetime(event_obj.get("startDate"))
    if start_at is None or start_at.date() < current_date:
        return None

    end_at = _parse_datetime(event_obj.get("endDate"))
    if end_at is not None and (end_at - start_at).total_seconds() > 8 * 3600:
        end_at = None

    text_blob = " ".join(part for part in [title, description or ""] if part).lower()
    if any(keyword in text_blob for keyword in EXCLUDED_PRIMARY_KEYWORDS):
        return None
    if not any(keyword in text_blob for keyword in INCLUDED_KEYWORDS):
        return None

    offer_price = _extract_offer_price(event_obj.get("offers"))
    if offer_price is not None and offer_price > 0:
        return None

    location_text = _extract_location_text(event_obj.get("location"))
    full_description = _join_non_empty(
        [
            description,
            f"Location: {location_text}" if location_text and location_text != SDMART_DEFAULT_LOCATION else None,
        ]
    )

    free_status = "confirmed" if offer_price == 0 or "free" in text_blob else "inferred"
    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=SDMART_VENUE_NAME,
        location_text=location_text,
        city=SDMART_CITY,
        state=SDMART_STATE,
        activity_type=_infer_activity_type(title=title, description=description),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=("registration" in text_blob and "not required" not in text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=LA_TIMEZONE,
        free_verification_status=free_status,
    )


def _iter_event_objects(data) -> list[dict]:
    stack = [data]
    events: list[dict] = []
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if current.get("@type") == "Event":
                events.append(current)
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            stack.extend(current)
    return events


def _extract_offer_price(offers) -> Decimal | None:
    if isinstance(offers, list):
        for offer in offers:
            price = _extract_offer_price(offer)
            if price is not None:
                return price
        return None
    if not isinstance(offers, dict):
        return None

    price = offers.get("price")
    if price in (None, ""):
        return None
    try:
        return Decimal(str(price))
    except Exception:
        return None


def _extract_location_text(location) -> str:
    if not isinstance(location, dict):
        return SDMART_DEFAULT_LOCATION

    parts: list[str] = []
    name = _normalize_text(location.get("name"))
    if name:
        parts.append(name)

    address = location.get("address")
    if isinstance(address, dict):
        for key in ("streetAddress", "addressLocality", "addressRegion", "postalCode"):
            value = _normalize_text(address.get(key))
            if value:
                parts.append(value)

    location_text = ", ".join(dict.fromkeys(parts))
    return location_text or SDMART_DEFAULT_LOCATION


def _infer_activity_type(*, title: str, description: str | None) -> str:
    text_blob = " ".join(part for part in [title, description or ""] if part).lower()
    if any(keyword in text_blob for keyword in ("talk", "lecture", "conversation", "speaker")):
        return "talk"
    return "workshop"


def _parse_datetime(value) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.replace(tzinfo=None)
    return parsed


def _safe_json_loads(value: str):
    text = value.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _normalize_text(value) -> str | None:
    if value is None:
        return None
    return _normalize_space(str(value))


def _normalize_space(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(html.unescape(value).replace("\xa0", " ").split())
    return normalized or None


def _join_non_empty(parts: list[str | None]) -> str | None:
    kept = [part for part in parts if part]
    return " | ".join(kept) if kept else None
