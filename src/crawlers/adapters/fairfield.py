import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

FAIRFIELD_CALENDAR_URL = "https://events.fairfield.edu/department/fairfield_university_art_museum/calendar"

NY_TIMEZONE = "America/New_York"
FAIRFIELD_VENUE_NAME = "Fairfield University Art Museum"
FAIRFIELD_CITY = "Fairfield"
FAIRFIELD_STATE = "CT"
FAIRFIELD_DEFAULT_LOCATION = "Fairfield, CT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": FAIRFIELD_CALENDAR_URL,
}

PAGE_NUMBER_RE = re.compile(r"/calendar/(\d+)(?:[/?#]|$)")
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

INCLUDED_KEYWORDS = (
    "talk",
    "lecture",
    "conversation",
    "workshop",
    "class",
    "lab",
    "activity",
    "gallery talk",
    "art in focus",
    "family day",
    "family days",
)
EXCLUDED_KEYWORDS = (
    "virtual",
    "tour",
    "tours",
    "camp",
    "yoga",
    "sports",
    "free night",
    "fundraising",
    "fundraiser",
    "admission",
    "film",
    "tv",
    "reading",
    "writing",
    "open house",
    "performance",
    "music",
    "poem",
    "poetry",
    "prose",
    "meditation",
    "mindfulness",
    "storytime",
    "dinner",
    "reception",
    "jazz",
    "orchestra",
    "band",
)


async def fetch_fairfield_page(
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
        raise RuntimeError("Unable to fetch Fairfield University Art Museum calendar page") from last_exception
    raise RuntimeError("Unable to fetch Fairfield University Art Museum calendar page after retries")


def build_fairfield_calendar_url(*, page_number: int) -> str:
    if page_number <= 1:
        return FAIRFIELD_CALENDAR_URL
    return f"{FAIRFIELD_CALENDAR_URL}/{page_number}"


async def load_fairfield_calendar_payload(*, page_limit: int | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        first_html = await fetch_fairfield_page(FAIRFIELD_CALENDAR_URL, client=client)
        page_total = _parse_page_total(first_html)
        if page_limit is not None:
            page_total = min(page_total, max(page_limit, 1))

        page_payloads: list[tuple[str, str]] = [(FAIRFIELD_CALENDAR_URL, first_html)]
        additional_urls = [
            build_fairfield_calendar_url(page_number=page_number)
            for page_number in range(2, page_total + 1)
        ]
        if additional_urls:
            fetched = await asyncio.gather(*(fetch_fairfield_page(url, client=client) for url in additional_urls))
            page_payloads.extend(zip(additional_urls, fetched, strict=True))

    return {"pages": page_payloads}


def parse_fairfield_calendar_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for list_url, html in payload["pages"]:
        for event_obj in _extract_event_objects(html):
            row = _build_row_from_event_obj(event_obj=event_obj, list_url=list_url)
            if row is None:
                continue
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class FairfieldCalendarAdapter(BaseSourceAdapter):
    source_name = "fairfield_calendar"

    async def fetch(self) -> list[str]:
        html = await fetch_fairfield_page(FAIRFIELD_CALENDAR_URL)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_fairfield_calendar_payload({"pages": [(FAIRFIELD_CALENDAR_URL, payload)]})


def _parse_page_total(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    page_numbers = [1]
    for anchor in soup.select("nav[aria-label='pagination'] a[href]"):
        href = (anchor.get("href") or "").strip()
        match = PAGE_NUMBER_RE.search(href)
        if match is None:
            continue
        page_numbers.append(int(match.group(1)))
    return max(page_numbers)


def _extract_event_objects(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    event_objects: list[dict] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = (script.get_text() or "").strip()
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue

        for event_obj in _iter_event_objects(data):
            if isinstance(event_obj, dict) and event_obj.get("@type") == "Event":
                event_objects.append(event_obj)
    return event_objects


def _iter_event_objects(value: object) -> list[dict]:
    if isinstance(value, dict):
        result: list[dict] = []
        if value.get("@type") == "Event":
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


def _build_row_from_event_obj(*, event_obj: dict, list_url: str) -> ExtractedActivity | None:
    title = _normalize_space(str(event_obj.get("name") or event_obj.get("title") or ""))
    description = _normalize_space(str(event_obj.get("description") or ""))
    if not title:
        return None
    if not _should_include_event(title=title, description=description):
        return None

    source_url = urljoin(
        list_url,
        str(event_obj.get("url") or event_obj.get("@id") or "").strip(),
    )
    if not source_url:
        return None

    start_at = _parse_datetime(event_obj.get("startDate") or event_obj.get("start_date"))
    if start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("endDate") or event_obj.get("end_date"))

    location_text = _build_location_text(event_obj.get("location"))
    age_min, age_max = _parse_age_range(description)
    text_blob = " ".join([title, description]).lower()
    is_free, free_status = infer_price_classification(_extract_offer_price_text(event_obj.get("offers")))

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description or None,
        venue_name=FAIRFIELD_VENUE_NAME,
        location_text=location_text,
        city=FAIRFIELD_CITY,
        state=FAIRFIELD_STATE,
        activity_type=_infer_activity_type(title=title, description=description),
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=_has_registration_offer(event_obj.get("offers"), text_blob=text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _should_include_event(*, title: str, description: str) -> bool:
    blob = _normalize_space(f"{title} {description}").lower()
    if not blob:
        return False

    if any(keyword in blob for keyword in EXCLUDED_KEYWORDS):
        return False

    if "for adults" in blob or "adults," in blob or "adults " in blob:
        return False

    return any(keyword in blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(*, title: str, description: str) -> str:
    blob = _normalize_space(f"{title} {description}").lower()
    if any(keyword in blob for keyword in ("lecture", "talk", "conversation", "art in focus", "gallery talk")):
        return "talk"
    return "workshop"


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _parse_age_range(description: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(description)
    if match is not None:
        return int(match.group(1)), int(match.group(2))

    match = AGE_PLUS_RE.search(description)
    if match is not None:
        return int(match.group(1)), None

    return None, None


def _extract_offer_price_text(offers: object) -> str | None:
    prices: list[str] = []
    for offer in _iter_offer_objects(offers):
        price = _normalize_space(str(offer.get("price") or ""))
        if price:
            prices.append(price)
    if not prices:
        return None
    return " | ".join(prices)


def _has_registration_offer(offers: object, *, text_blob: str) -> bool | None:
    for offer in _iter_offer_objects(offers):
        url = _normalize_space(str(offer.get("url") or ""))
        if url:
            return True
    if "register" in text_blob or "registration" in text_blob:
        return True
    return None


def _iter_offer_objects(value: object) -> list[dict]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _build_location_text(location: object) -> str:
    if isinstance(location, dict):
        name = _normalize_space(str(location.get("name") or ""))
        address = _normalize_space(str(location.get("address") or ""))
        if name and address:
            if name in address:
                return address
            return f"{name}, {address}"
        if name:
            return name
        if address:
            return address
    return FAIRFIELD_DEFAULT_LOCATION


def _normalize_space(value: str) -> str:
    return " ".join(value.split()).strip()
