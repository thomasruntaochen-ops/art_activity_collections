import asyncio
import html
import json
from datetime import datetime
from decimal import Decimal
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

ELMUSEO_EVENTS_LIST_URL = "https://elmuseo.org/events/list/"

NY_TIMEZONE = "America/New_York"
ELMUSEO_VENUE_NAME = "El Museo del Barrio"
ELMUSEO_CITY = "New York"
ELMUSEO_STATE = "NY"
ELMUSEO_DEFAULT_LOCATION = "New York, NY"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": ELMUSEO_EVENTS_LIST_URL,
}

INCLUDE_KEYWORDS = (
    "activity",
    "art-making",
    "art making",
    "artists panel",
    "conversation",
    "cultural celebration",
    "discussion",
    "family",
    "hands-on",
    "lab",
    "lecture",
    "panel",
    "participatory",
    "symposium",
    "talk",
    "workshop",
)
TITLE_EXCLUDE_KEYWORDS = (
    "dance performance",
    "festival",
    "music",
    "orchestra",
    "performance",
)
BODY_EXCLUDE_KEYWORDS = (
    "after-hours",
    "band",
    "concert",
    "dance lesson",
    "dj",
    "dinner",
    "film",
    "fundraiser",
    "music",
    "open house",
    "orchestra",
    "performance",
    "poetry",
    "reading",
    "storytime",
    "tour",
)


async def fetch_elmuseo_page(
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
        raise RuntimeError("Unable to fetch El Museo del Barrio event pages") from last_exception
    raise RuntimeError("Unable to fetch El Museo del Barrio event pages after retries")


async def load_elmuseo_payload(*, page_limit: int | None = None) -> dict:
    page_urls: list[str] = []
    page_html_by_url: dict[str, str] = {}
    current_url = ELMUSEO_EVENTS_LIST_URL

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while current_url and current_url not in page_html_by_url:
            if page_limit is not None and len(page_urls) >= max(page_limit, 1):
                break

            current_html = await fetch_elmuseo_page(current_url, client=client)
            page_urls.append(current_url)
            page_html_by_url[current_url] = current_html
            current_url = _extract_next_page_url(current_html, list_url=page_urls[-1])

        page_events: list[dict] = []
        detail_urls: set[str] = set()
        for page_url in page_urls:
            for event_obj in _extract_event_objects(page_html_by_url[page_url]):
                event_obj["_list_url"] = page_url
                page_events.append(event_obj)
                source_url = _normalize_space(str(event_obj.get("url") or event_obj.get("@id") or ""))
                if source_url:
                    detail_urls.add(source_url)

        detail_pages = await asyncio.gather(*(fetch_elmuseo_page(url, client=client) for url in sorted(detail_urls)))

    return {
        "pages": [(page_url, page_html_by_url[page_url]) for page_url in page_urls],
        "events": page_events,
        "detail_pages": {url: html for url, html in zip(sorted(detail_urls), detail_pages, strict=True)},
    }


def parse_elmuseo_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload["events"]:
        row = _build_row_from_event_obj(
            event_obj=event_obj,
            detail_html=payload["detail_pages"].get(_normalize_space(str(event_obj.get("url") or event_obj.get("@id") or ""))),
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


class ElMuseoEventsAdapter(BaseSourceAdapter):
    source_name = "elmuseo_events"

    def __init__(self, url: str = ELMUSEO_EVENTS_LIST_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_elmuseo_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        events = _extract_event_objects(payload)
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
            detail_urls = sorted(
                {
                    _normalize_space(str(event_obj.get("url") or event_obj.get("@id") or ""))
                    for event_obj in events
                    if _normalize_space(str(event_obj.get("url") or event_obj.get("@id") or ""))
                }
            )
            detail_pages = await asyncio.gather(*(fetch_elmuseo_page(url, client=client) for url in detail_urls))

        return parse_elmuseo_payload(
            {
                "pages": [(self.url, payload)],
                "events": events,
                "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
            }
        )


def _extract_event_objects(html_text: str) -> list[dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    event_objects: list[dict] = []
    for script in soup.select('script[type="application/ld+json"]'):
        script_text = (script.get_text() or "").strip()
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        for event_obj in _iter_event_objects(data):
            if isinstance(event_obj, dict):
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


def _build_row_from_event_obj(*, event_obj: dict, detail_html: str | None) -> ExtractedActivity | None:
    title = _clean_html_text(event_obj.get("name") or event_obj.get("title"))
    if not title:
        return None

    source_url = _normalize_space(str(event_obj.get("url") or event_obj.get("@id") or ""))
    if not source_url:
        return None

    start_at = _parse_datetime(event_obj.get("startDate") or event_obj.get("start_date"))
    if start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("endDate") or event_obj.get("end_date"))

    detail = _parse_detail_page(detail_html) if detail_html else {}
    list_description = _clean_html_text(event_obj.get("description"))
    description = detail.get("description") or list_description
    location_text = detail.get("location_text") or _build_location_text(event_obj.get("location"))
    price_amount = _extract_offer_amount(event_obj.get("offers"))
    price_text = detail.get("price_text") or _extract_offer_price_text(event_obj.get("offers"))

    if not _should_include_event(title=title, description=description):
        return None

    blob = " ".join(filter(None, [title, description, list_description, location_text, price_text])).lower()
    is_free, free_status = infer_price_classification_from_amount(price_amount, text=price_text)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description or None,
        venue_name=ELMUSEO_VENUE_NAME,
        location_text=location_text or ELMUSEO_DEFAULT_LOCATION,
        city=ELMUSEO_CITY,
        state=ELMUSEO_STATE,
        activity_type=_infer_activity_type(title=title, description=description),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in blob or "drop in" in blob),
        registration_required=("registration" in blob and "no registration" not in blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _parse_detail_page(html_text: str) -> dict:
    soup = BeautifulSoup(html_text, "html.parser")
    paragraphs = [
        _normalize_space(paragraph.get_text(" ", strip=True))
        for paragraph in soup.select("main p")
        if _normalize_space(paragraph.get_text(" ", strip=True))
    ]

    description_parts: list[str] = []
    price_text: str | None = None
    for text in paragraphs:
        lowered = text.lower()
        if lowered == "+ google map":
            continue
        if "registration here >" in lowered and len(lowered) < 90:
            continue
        if lowered.startswith("registration here"):
            continue
        if lowered.startswith("ticket information"):
            continue
        if "register here for the concert" in lowered:
            continue
        if "$" in text or "free" in lowered or "ticket" in lowered or "admission" in lowered:
            price_text = text
            continue
        description_parts.append(text)

    return {
        "description": " ".join(description_parts).strip() or None,
        "price_text": price_text,
    }


def _should_include_event(*, title: str, description: str | None) -> bool:
    title_blob = _normalize_space(title).lower()
    description_blob = _normalize_space(description).lower()
    blob = f"{title_blob} {description_blob}".strip()

    if any(keyword in title_blob for keyword in TITLE_EXCLUDE_KEYWORDS):
        return False

    has_include = any(keyword in blob for keyword in INCLUDE_KEYWORDS)
    if not has_include:
        return False

    if any(keyword in blob for keyword in BODY_EXCLUDE_KEYWORDS):
        strong_include = any(keyword in blob for keyword in ("panel", "symposium", "talk", "conversation", "discussion"))
        if not strong_include:
            return False

    return True


def _infer_activity_type(*, title: str, description: str | None) -> str:
    blob = f"{_normalize_space(title)} {_normalize_space(description)}".lower()
    if any(keyword in blob for keyword in ("panel", "symposium", "talk", "conversation", "discussion", "lecture")):
        return "talk"
    return "activity"


def _build_location_text(location: object) -> str | None:
    if isinstance(location, dict):
        name = _clean_html_text(location.get("name"))
        address = location.get("address")
        address_bits: list[str] = []
        if isinstance(address, dict):
            for key in ("streetAddress", "addressLocality", "addressRegion"):
                value = _clean_html_text(address.get(key))
                if value:
                    address_bits.append(value)
        joined_address = ", ".join(dict.fromkeys(address_bits))
        if name and joined_address:
            return f"{name}, {joined_address}"
        if name:
            return name
        if joined_address:
            return joined_address
    return None


def _extract_offer_amount(offers: object) -> Decimal | None:
    if isinstance(offers, dict):
        offers = [offers]
    if not isinstance(offers, list):
        return None
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        raw_price = offer.get("price")
        if raw_price in (None, ""):
            continue
        try:
            return Decimal(str(raw_price))
        except Exception:
            continue
    return None


def _extract_offer_price_text(offers: object) -> str | None:
    if isinstance(offers, dict):
        offers = [offers]
    if not isinstance(offers, list):
        return None

    prices: list[str] = []
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        price = _normalize_space(str(offer.get("price") or ""))
        currency = _normalize_space(str(offer.get("priceCurrency") or ""))
        if not price:
            continue
        if currency:
            prices.append(f"{currency} {price}")
        else:
            prices.append(price)
    return ", ".join(prices) or None


def _extract_next_page_url(html_text: str, *, list_url: str) -> str | None:
    soup = BeautifulSoup(html_text, "html.parser")
    link = soup.select_one('.tribe-events-c-nav__next a[href], link[rel="next"]')
    if link is None:
        return None
    href = (link.get("href") or "").strip()
    return urljoin(list_url, href) if href else None


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    text = _normalize_space(str(value))
    if not text:
        return None
    try:
        return parse_iso_datetime(text, timezone_name=NY_TIMEZONE)
    except ValueError:
        return None


def _clean_html_text(value: object) -> str:
    raw = _normalize_space(str(value or ""))
    if not raw:
        return ""
    unescaped = html.unescape(raw)
    return _normalize_space(BeautifulSoup(unescaped, "html.parser").get_text(" ", strip=True))


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())
