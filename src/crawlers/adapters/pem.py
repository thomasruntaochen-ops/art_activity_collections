import asyncio
import json
from datetime import datetime
from decimal import Decimal

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

PEM_EVENTS_URL = "https://www.pem.org/events"

NY_TIMEZONE = "America/New_York"
PEM_VENUE_NAME = "Peabody Essex Museum"
PEM_CITY = "Salem"
PEM_STATE = "MA"
PEM_DEFAULT_LOCATION = "Peabody Essex Museum, Salem, MA"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": PEM_EVENTS_URL,
}

INCLUDE_PATTERNS = (
    " art ",
    " class ",
    " classes ",
    " collage ",
    " conversation ",
    " creative ",
    " cyanotype ",
    " discussion ",
    " drop-in ",
    " drop in ",
    " family ",
    " foundations ",
    " handbuilding ",
    " hands-on ",
    " kids ",
    " lecture ",
    " make your own ",
    " making your own ",
    " panel discussion ",
    " scratchboard ",
    " studio ",
    " talk ",
    " workshop ",
    " workshops ",
)
STRICT_EXCLUDE_PATTERNS = (
    " camp ",
    " camps ",
    " concert ",
    " fashion show ",
    " film ",
    " gala ",
    " meditation ",
    " mindfulness ",
    " music ",
    " performance ",
    " poetry ",
    " reception ",
    " sound bath ",
    " storytime ",
    " tour ",
    " tours ",
)


async def fetch_pem_page(
    url: str,
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
        raise RuntimeError("Unable to fetch Peabody Essex Museum events page") from last_exception
    raise RuntimeError("Unable to fetch Peabody Essex Museum events page after retries")


async def load_pem_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_pem_page(PEM_EVENTS_URL, client=client)
        event_objects = _extract_event_objects(listing_html)
        detail_urls = sorted(
            {
                _normalize_space(str(event_obj.get("url") or event_obj.get("@id") or ""))
                for event_obj in event_objects
                if _normalize_space(str(event_obj.get("url") or event_obj.get("@id") or ""))
            }
        )
        detail_pages = await asyncio.gather(*(fetch_pem_page(url, client=client) for url in detail_urls))

    return {
        "listing_html": listing_html,
        "events": event_objects,
        "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
    }


def parse_pem_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        source_url = _normalize_space(str(event_obj.get("url") or event_obj.get("@id") or ""))
        row = _build_row_from_event_obj(
            event_obj=event_obj,
            detail_html=payload.get("detail_pages", {}).get(source_url),
        )
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


class PemEventsAdapter(BaseSourceAdapter):
    source_name = "pem_events"

    async def fetch(self) -> list[str]:
        html = await fetch_pem_page(PEM_EVENTS_URL)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_pem_payload/parse_pem_payload from script runner.")


def _extract_event_objects(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    event_objects: list[dict] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        event_objects.extend(_iter_event_objects(data))
    return event_objects


def _iter_event_objects(value: object) -> list[dict]:
    if isinstance(value, dict):
        result: list[dict] = []
        event_type = value.get("@type")
        types = event_type if isinstance(event_type, list) else [event_type]
        if any(str(item).lower() == "event" for item in types if item):
            result.append(value)
        for item in value.values():
            result.extend(_iter_event_objects(item))
        return result
    if isinstance(value, list):
        result: list[dict] = []
        for item in value:
            result.extend(_iter_event_objects(item))
        return result
    return []


def _build_row_from_event_obj(*, event_obj: dict, detail_html: str | None) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("name") or event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url") or event_obj.get("@id"))
    start_at = _parse_datetime(event_obj.get("startDate"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("endDate"))
    if end_at is not None and "creative kids" in title.lower() and end_at.date() > start_at.date():
        return None
    detail = _parse_detail_page(detail_html) if detail_html else {}

    description = _join_non_empty(
        [
            detail.get("summary"),
            _normalize_space(event_obj.get("description")),
        ]
    )
    include_blob = " ".join([title, description or ""]).lower()
    token_blob = f" {' '.join(include_blob.split())} "
    if any(pattern in token_blob for pattern in STRICT_EXCLUDE_PATTERNS):
        return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    offer_amount = _extract_offer_amount(event_obj.get("offers"))
    location_text = _build_location_text(event_obj.get("location")) or PEM_DEFAULT_LOCATION
    if " virtual " in token_blob or " online " in token_blob:
        location_text = "Online"

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=PEM_VENUE_NAME,
        location_text=location_text or PEM_DEFAULT_LOCATION,
        city=PEM_CITY,
        state=PEM_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in token_blob or "drop in" in token_blob),
        registration_required=("register" in token_blob or "ticket" in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **price_classification_kwargs_from_amount(offer_amount, text=description),
    )


def _parse_detail_page(html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    meta_description = None
    meta_tag = soup.find("meta", attrs={"name": "description"})
    if meta_tag is not None:
        meta_description = _normalize_space(meta_tag.get("content"))

    location_text = None
    lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    for index, line in enumerate(lines):
        if line == "Museum Hours:" and index + 1 < len(lines):
            break
        if line == "Plan your visit" and index > 20:
            break
        if line.lower().startswith("from"):
            continue
        if "Salem, MA" in line or "Essex Street" in line:
            location_text = _normalize_space(line)
            break

    return {
        "summary": meta_description,
        "location_text": location_text,
    }


def _extract_offer_amount(value: object) -> Decimal | None:
    if not isinstance(value, dict):
        return None
    price = value.get("price")
    if price is None:
        return None
    try:
        return Decimal(str(price))
    except Exception:
        return None


def _build_location_text(location_obj: object) -> str | None:
    if not isinstance(location_obj, dict):
        return None
    name = _normalize_space(location_obj.get("name"))
    address = location_obj.get("address")
    street = None
    city = None
    state = None
    if isinstance(address, dict):
        street = _normalize_space(address.get("streetAddress"))
        city = _normalize_space(address.get("addressLocality"))
        state = _normalize_space(address.get("addressRegion"))
    parts = [part for part in [name, street, city, state] if part]
    if not parts:
        return None
    return ", ".join(parts)


def _infer_activity_type(token_blob: str) -> str:
    if any(keyword in token_blob for keyword in (" lecture ", " talk ", " conversation ", " discussion ", " panel ")):
        return "lecture"
    return "workshop"


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
