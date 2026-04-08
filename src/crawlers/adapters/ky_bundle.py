from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from html import unescape
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

JSON_HEADERS = {
    **DEFAULT_HEADERS,
    "Accept": "application/json,text/plain,*/*",
}

MONTH_LOOKUP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

WEEKDAY_LOOKUP = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

DATE_WITH_YEAR_AND_TIME_RE = re.compile(
    r"(?P<weekday>[A-Za-z]{3,9}),?\s+"
    r"(?P<month>[A-Za-z]{3,9})\s+"
    r"(?P<day>\d{1,2})\s+"
    r"(?P<year>\d{4}),?\s*"
    r"(?P<time>.+)$",
    re.IGNORECASE,
)
DATE_WITHOUT_YEAR_RE = re.compile(
    r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"(?P<month>[A-Za-z]{3,9})\s+"
    r"(?P<day>\d{1,2})(?:st|nd|rd|th)?"
    r"(?:,\s*(?P<time>[^.]+))?",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*"
    r"(?P<start_meridiem>am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*"
    r"(?P<end_meridiem>am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>am|pm)",
    re.IGNORECASE,
)

INCLUDE_PATTERNS = (
    " activity ",
    " activities ",
    " artist talk ",
    " artist talks ",
    " chat ",
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " craft ",
    " crafts ",
    " discussion ",
    " discussions ",
    " family day ",
    " family fun ",
    " hands-on art-making ",
    " lecture ",
    " lectures ",
    " paper weaving ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)

REJECT_PATTERNS = (
    " admission day ",
    " camp ",
    " camps ",
    " cinema ",
    " concert ",
    " concerts ",
    " dinner ",
    " exhibition ",
    " exhibitions ",
    " film ",
    " films ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " members only ",
    " mindfulness ",
    " movie ",
    " music ",
    " open house ",
    " orchestra ",
    " performance ",
    " performances ",
    " poetry ",
    " reading ",
    " reception ",
    " storytime ",
    " tai chi ",
    " tour ",
    " tours ",
    " writing ",
    " yoga ",
)

TITLE_REJECT_PATTERNS = (
    " camp ",
    " camps ",
    " cinema ",
    " concert ",
    " concerts ",
    " exhibition ",
    " exhibitions ",
    " film ",
    " films ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " music ",
    " performance ",
    " performances ",
    " poetry ",
    " reception ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)


@dataclass(frozen=True, slots=True)
class KyVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    timezone: str
    list_urls: tuple[str, ...]
    default_location: str


KY_VENUES: tuple[KyVenueConfig, ...] = (
    KyVenueConfig(
        slug="speed",
        source_name="ky_speed_events",
        venue_name="Speed Art Museum",
        city="Louisville",
        state="KY",
        timezone="America/New_York",
        list_urls=("https://www.speedmuseum.org/events/",),
        default_location="Speed Art Museum, Louisville, KY",
    ),
    KyVenueConfig(
        slug="kmac",
        source_name="ky_kmac_events",
        venue_name="KMAC Museum",
        city="Louisville",
        state="KY",
        timezone="America/New_York",
        list_urls=(
            "https://www.kmacmuseum.org/events",
            "https://www.kmacmuseum.org/familyfunday",
        ),
        default_location="KMAC Museum, Louisville, KY",
    ),
    KyVenueConfig(
        slug="uk_art_museum",
        source_name="ky_uk_art_museum_events",
        venue_name="University of Kentucky Art Museum",
        city="Lexington",
        state="KY",
        timezone="America/New_York",
        list_urls=("https://finearts.uky.edu/art-museum/events",),
        default_location="UK Art Museum, Lexington, KY",
    ),
    KyVenueConfig(
        slug="artcenter",
        source_name="ky_artcenter_events",
        venue_name="Art Center of the Bluegrass",
        city="Danville",
        state="KY",
        timezone="America/New_York",
        list_urls=(
            "https://artcenterky.org/events/",
            "https://artcenterky.org/events/family-day/",
        ),
        default_location="Art Center of the Bluegrass, Danville, KY",
    ),
)

KY_VENUES_BY_SLUG = {venue.slug: venue for venue in KY_VENUES}


async def fetch_html(
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
        raise RuntimeError(f"Unable to fetch HTML: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch HTML after retries: {url}")


async def fetch_json(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
) -> object:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=JSON_HEADERS)

    try:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()
    finally:
        if owns_client:
            await client.aclose()


async def load_ky_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    max_speed_detail_pages: int = 60,
) -> dict[str, dict]:
    selected = [KY_VENUES_BY_SLUG[slug] for slug in venue_slugs] if venue_slugs else list(KY_VENUES)
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as html_client:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=JSON_HEADERS) as json_client:
            for venue in selected:
                if venue.slug == "speed":
                    payload[venue.slug] = await _load_speed_payload(
                        venue=venue,
                        client=html_client,
                        max_detail_pages=max_speed_detail_pages,
                    )
                elif venue.slug == "uk_art_museum":
                    payload[venue.slug] = await _load_uk_art_museum_payload(venue=venue, client=html_client)
                elif venue.slug == "kmac":
                    payload[venue.slug] = await _load_kmac_payload(venue=venue, client=html_client)
                elif venue.slug == "artcenter":
                    payload[venue.slug] = await _load_artcenter_payload(venue=venue, client=json_client)
                else:
                    payload[venue.slug] = {}

    return payload


def parse_ky_events(payload: dict, *, venue: KyVenueConfig) -> list[ExtractedActivity]:
    if venue.slug == "speed":
        return _parse_speed_events(payload, venue=venue)
    if venue.slug == "uk_art_museum":
        return _parse_uk_art_museum_events(payload, venue=venue)
    if venue.slug == "kmac":
        return _parse_kmac_events(payload, venue=venue)
    if venue.slug == "artcenter":
        return _parse_artcenter_events(payload, venue=venue)
    return []


class KyBundleAdapter(BaseSourceAdapter):
    source_name = "ky_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_ky_bundle_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_ky_bundle_payload/parse_ky_events from the script runner.")


def get_ky_source_prefixes() -> tuple[str, ...]:
    return (
        "https://www.speedmuseum.org/event/",
        "https://finearts.uky.edu/art-museum/events/",
        "https://www.kmacmuseum.org/familyfunday",
        "https://artcenterky.org/events/family-day/",
    )


async def _load_speed_payload(
    *,
    venue: KyVenueConfig,
    client: httpx.AsyncClient,
    max_detail_pages: int,
) -> dict:
    list_html = await fetch_html(venue.list_urls[0], client=client)
    soup = BeautifulSoup(list_html, "html.parser")

    detail_urls: list[str] = []
    seen_urls: set[str] = set()
    for selector in (
        "a.tribe-events-calendar-month-mobile-events__mobile-event-title-link[href]",
        "a.tribe-events-calendar-month__calendar-event-title-link[href]",
    ):
        for anchor in soup.select(selector):
            url = urljoin(venue.list_urls[0], anchor.get("href") or "")
            title = _normalize_space(anchor.get_text(" ", strip=True)) or ""
            if not url or "/event/" not in url or url in seen_urls:
                continue
            if not _speed_title_looks_relevant(title):
                continue
            seen_urls.add(url)
            detail_urls.append(url)
            if len(detail_urls) >= max_detail_pages:
                break
        if len(detail_urls) >= max_detail_pages:
            break

    semaphore = asyncio.Semaphore(8)

    async def _fetch_detail(url: str) -> tuple[str, str]:
        async with semaphore:
            return url, await fetch_html(url, client=client)

    detail_pairs = await asyncio.gather(*[_fetch_detail(url) for url in detail_urls])
    return {
        "list_html": list_html,
        "detail_pages": {url: html for url, html in detail_pairs},
    }


async def _load_uk_art_museum_payload(*, venue: KyVenueConfig, client: httpx.AsyncClient) -> dict:
    list_html = await fetch_html(venue.list_urls[0], client=client)
    soup = BeautifulSoup(list_html, "html.parser")

    detail_urls: list[str] = []
    for anchor in soup.select(".views-field-title a[href]"):
        url = urljoin(venue.list_urls[0], anchor.get("href") or "")
        if url and url not in detail_urls:
            detail_urls.append(url)

    semaphore = asyncio.Semaphore(6)

    async def _fetch_detail(url: str) -> tuple[str, str]:
        async with semaphore:
            return url, await fetch_html(url, client=client)

    detail_pairs = await asyncio.gather(*[_fetch_detail(url) for url in detail_urls])
    return {
        "list_html": list_html,
        "detail_pages": {url: html for url, html in detail_pairs},
    }


async def _load_kmac_payload(*, venue: KyVenueConfig, client: httpx.AsyncClient) -> dict:
    family_html = await fetch_html(venue.list_urls[1], client=client)
    return {"family_html": family_html}


async def _load_artcenter_payload(*, venue: KyVenueConfig, client: httpx.AsyncClient) -> dict:
    page_payload = await fetch_json("https://artcenterky.org/wp-json/wp/v2/pages?slug=family-day", client=client)
    if not isinstance(page_payload, list):
        return {}
    return {"page_payload": page_payload}


def _parse_speed_events(payload: dict, *, venue: KyVenueConfig) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(venue.timezone)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, html in (payload.get("detail_pages") or {}).items():
        event_obj = _extract_event_schema(html)
        if event_obj is None:
            continue

        title = _normalize_space(event_obj.get("name"))
        description = _html_to_text(event_obj.get("description"))
        if not title or not _is_qualifying_event(title=title, description=description):
            continue

        start_at = _parse_iso_or_none(event_obj.get("startDate"), timezone_name=venue.timezone)
        if start_at is None or start_at.date() < current_date:
            continue

        end_at = _parse_iso_or_none(event_obj.get("endDate"), timezone_name=venue.timezone)
        location_name = _extract_location_name(event_obj.get("location")) or venue.default_location
        offer_text, is_ticketed = _extract_offer_text(event_obj.get("offers"))
        description_parts = [part for part in [description, f"Location: {location_name}" if location_name else None, offer_text] if part]
        full_description = " | ".join(description_parts) if description_parts else None
        text_blob = _searchable_text(" ".join([title, full_description or ""]))
        price_kwargs = _price_kwargs(full_description)

        row = ExtractedActivity(
            source_url=source_url,
            title=title,
            description=full_description,
            venue_name=venue.venue_name,
            location_text=location_name,
            city=venue.city,
            state=venue.state,
            activity_type=_infer_activity_type(text_blob),
            age_min=None,
            age_max=None,
            drop_in=(" drop-in " in text_blob or " drop in " in text_blob),
            registration_required=(
                " registration " in text_blob
                or " reserve " in text_blob
                or " sold out " in text_blob
                or is_ticketed
            ),
            start_at=start_at,
            end_at=end_at,
            timezone=venue.timezone,
            **price_kwargs,
        )

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    return rows


def _parse_uk_art_museum_events(payload: dict, *, venue: KyVenueConfig) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(venue.timezone)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _normalize_space((soup.select_one("h1") or soup.title).get_text(" ", strip=True) if (soup.select_one("h1") or soup.title) else "")
        if not title:
            continue

        date_text = _normalize_space(_first_text(soup.select(".field--name-field-eventdates")))
        price_text = _strip_field_label(_normalize_space(_first_text(soup.select(".field--name-field-ticket-prices"))), "Ticket Prices")
        location_text = _normalize_space(
            _strip_field_label(_first_text(soup.select(".field--name-field-college-facility")), "Event Location")
            or _strip_field_label(_first_text(soup.select(".field--name-field-location")), "Event Location")
            or venue.default_location
        )
        description = _longest_text(soup.select(".editorial"))
        if not _is_qualifying_event(title=title, description=description):
            continue

        start_at, end_at = _parse_uk_event_datetime(date_text)
        if start_at is None or start_at.date() < current_date:
            continue

        description_parts = [part for part in [description, f"Price: {price_text}" if price_text else None] if part]
        full_description = " | ".join(description_parts) if description_parts else None
        text_blob = _searchable_text(" ".join([title, full_description or "", price_text or ""]))
        price_kwargs = _price_kwargs(price_text or full_description)

        row = ExtractedActivity(
            source_url=source_url,
            title=title,
            description=full_description,
            venue_name=venue.venue_name,
            location_text=location_text,
            city=venue.city,
            state=venue.state,
            activity_type=_infer_activity_type(text_blob),
            age_min=None,
            age_max=None,
            drop_in=(" walk-ins are also welcome " in text_blob or " walk ins are also welcome " in text_blob),
            registration_required=(" registration encouraged " in text_blob or " reservation " in text_blob),
            start_at=start_at,
            end_at=end_at,
            timezone=venue.timezone,
            **price_kwargs,
        )

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    return rows


def _parse_kmac_events(payload: dict, *, venue: KyVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("family_html")
    if not html:
        return []

    text = _page_text(html)
    title = "Family Fun Day"
    if not _is_qualifying_event(title=title, description=text):
        return []

    match = re.search(r"scheduled for (?P<date>Saturday,\s+[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?)", text, re.IGNORECASE)
    if match is None:
        return []

    event_date = _parse_yearless_weekday_date(match.group("date"), timezone_name=venue.timezone)
    if event_date is None:
        return []

    description = _extract_kmac_description(text)
    return [
        ExtractedActivity(
            source_url=venue.list_urls[1],
            title=title,
            description=description,
            venue_name=venue.venue_name,
            location_text=venue.default_location,
            city=venue.city,
            state=venue.state,
            activity_type="activity",
            age_min=None,
            age_max=None,
            drop_in=False,
            registration_required=False,
            start_at=datetime.combine(event_date, time.min),
            end_at=None,
            timezone=venue.timezone,
            is_free=True,
            free_verification_status="confirmed",
        )
    ]


def _parse_artcenter_events(payload: dict, *, venue: KyVenueConfig) -> list[ExtractedActivity]:
    page_payload = payload.get("page_payload") or []
    if not page_payload:
        return []

    item = page_payload[0]
    content = item.get("content", {}).get("rendered")
    text = _html_to_text(content)
    if not text:
        return []

    title = _normalize_space(item.get("title", {}).get("rendered")) or "Family Day"
    if not _is_qualifying_event(title=title, description=text):
        return []

    match = re.search(
        r"(?P<date>Saturday,\s+[A-Za-z]+\s+\d{1,2})\s+(?P<time>\d{1,2}(?::\d{2})?\s*(?:-|–|—|to)\s*\d{1,2}(?::\d{2})?\s*(?:am|pm))",
        text,
        re.IGNORECASE,
    )
    if match is None:
        return []

    event_date = _parse_yearless_weekday_date(match.group("date"), timezone_name=venue.timezone)
    if event_date is None:
        return []
    start_at, end_at = _parse_time_range_for_date(event_date, match.group("time"))

    return [
        ExtractedActivity(
            source_url=venue.list_urls[1],
            title=title,
            description=text,
            venue_name=venue.venue_name,
            location_text=venue.default_location,
            city=venue.city,
            state=venue.state,
            activity_type="activity",
            age_min=None,
            age_max=None,
            drop_in=False,
            registration_required=False,
            start_at=start_at or datetime.combine(event_date, time.min),
            end_at=end_at,
            timezone=venue.timezone,
            is_free=True,
            free_verification_status="confirmed",
        )
    ]


def _extract_event_schema(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        event_obj = _find_event_object(data)
        if event_obj is not None:
            return event_obj
    return None


def _find_event_object(value: object) -> dict | None:
    if isinstance(value, dict):
        obj_type = value.get("@type")
        if obj_type == "Event" or (isinstance(obj_type, list) and "Event" in obj_type):
            return value
        for nested in value.values():
            found = _find_event_object(nested)
            if found is not None:
                return found
        return None
    if isinstance(value, list):
        for item in value:
            found = _find_event_object(item)
            if found is not None:
                return found
    return None


def _extract_location_name(value: object) -> str | None:
    if isinstance(value, dict):
        return _normalize_space(value.get("name")) or _normalize_space(value.get("address"))
    if isinstance(value, list):
        for item in value:
            found = _extract_location_name(item)
            if found:
                return found
    return _normalize_space(value) if isinstance(value, str) else None


def _extract_offer_text(value: object) -> tuple[str | None, bool]:
    offers = value if isinstance(value, list) else [value] if value else []
    fragments: list[str] = []
    has_ticket_link = False

    for offer in offers:
        if isinstance(offer, dict):
            price = _normalize_space(offer.get("price"))
            currency = _normalize_space(offer.get("priceCurrency"))
            availability = _normalize_space(offer.get("availability"))
            url = _normalize_space(offer.get("url"))
            if price:
                fragments.append(f"Price: {price}{f' {currency}' if currency else ''}".strip())
            if availability:
                fragments.append(f"Availability: {availability.rsplit('/', 1)[-1]}")
            if url:
                has_ticket_link = True
                fragments.append("Tickets available")
        elif isinstance(offer, str):
            fragments.append(_normalize_space(offer) or "")

    text = " | ".join(fragment for fragment in fragments if fragment)
    return text or None, has_ticket_link


def _parse_iso_or_none(value: object, *, timezone_name: str) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return parse_iso_datetime(value, timezone_name=timezone_name)
    except ValueError:
        return None


def _parse_uk_event_datetime(text: str | None) -> tuple[datetime | None, datetime | None]:
    normalized = _normalize_space(text)
    if not normalized:
        return None, None
    match = DATE_WITH_YEAR_AND_TIME_RE.search(normalized)
    if match is None:
        return None, None

    month = MONTH_LOOKUP.get(match.group("month")[:3].lower())
    if month is None:
        return None, None

    try:
        event_date = date(int(match.group("year")), month, int(match.group("day")))
    except ValueError:
        return None, None
    return _parse_time_range_for_date(event_date, match.group("time"))


def _parse_yearless_weekday_date(text: str | None, *, timezone_name: str) -> date | None:
    normalized = _normalize_space(text)
    if not normalized:
        return None
    match = DATE_WITHOUT_YEAR_RE.search(normalized)
    if match is None:
        return None

    month = MONTH_LOOKUP.get(match.group("month")[:3].lower())
    weekday = WEEKDAY_LOOKUP.get(match.group("weekday").lower())
    if month is None or weekday is None:
        return None

    today = datetime.now(ZoneInfo(timezone_name)).date()
    day = int(match.group("day"))
    for year in range(today.year, today.year + 3):
        try:
            candidate = date(year, month, day)
        except ValueError:
            return None
        if candidate.weekday() != weekday:
            continue
        if candidate >= today:
            return candidate
    return None


def _parse_time_range_for_date(event_date: date, time_text: str | None) -> tuple[datetime | None, datetime | None]:
    normalized = _normalize_space(time_text)
    if not normalized:
        return datetime.combine(event_date, time.min), None

    match = TIME_RANGE_RE.search(normalized.lower().replace("\u2013", "-").replace("\u2014", "-"))
    if match is not None:
        start_at = _build_datetime(
            event_date=event_date,
            hour_text=match.group("start_hour"),
            minute_text=match.group("start_minute"),
            meridiem=match.group("start_meridiem") or match.group("end_meridiem"),
        )
        end_at = _build_datetime(
            event_date=event_date,
            hour_text=match.group("end_hour"),
            minute_text=match.group("end_minute"),
            meridiem=match.group("end_meridiem"),
        )
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(normalized.lower())
    if single_match is not None:
        return (
            _build_datetime(
                event_date=event_date,
                hour_text=single_match.group("hour"),
                minute_text=single_match.group("minute"),
                meridiem=single_match.group("meridiem"),
            ),
            None,
        )

    return datetime.combine(event_date, time.min), None


def _build_datetime(
    *,
    event_date: date,
    hour_text: str,
    minute_text: str | None,
    meridiem: str | None,
) -> datetime | None:
    if meridiem is None:
        return None
    hour = int(hour_text)
    minute = int(minute_text or "0")
    meridiem = meridiem.lower()
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return datetime.combine(event_date, time(hour=hour, minute=minute))


def _extract_kmac_description(text: str) -> str:
    match = re.search(
        r"Spring Family Fun Day is scheduled for .*? More information coming soon\. (?P<body>.*?)(?: Can't make it to Family Fun Day\?| STREET ADDRESS )",
        text,
        re.IGNORECASE,
    )
    if match is not None:
        return _normalize_space(match.group("body")) or text
    return text


def _price_kwargs(text: str | None) -> dict[str, bool | None | str]:
    blob = _searchable_text(text or "")
    if " with admission " in blob or " included with admission " in blob:
        return {"is_free": False, "free_verification_status": "confirmed"}
    if re.search(r"\bfree\b", text or "", re.IGNORECASE):
        return {"is_free": True, "free_verification_status": "confirmed"}
    return price_classification_kwargs(text)


def _is_qualifying_event(*, title: str, description: str | None) -> bool:
    title_blob = _searchable_text(title)
    if any(pattern in title_blob for pattern in TITLE_REJECT_PATTERNS):
        return False
    blob = _searchable_text(" ".join([title, description or ""]))
    if not any(pattern in blob for pattern in INCLUDE_PATTERNS):
        return False
    if " opening day " in blob or " museum closed " in blob:
        return False
    if any(pattern in blob for pattern in REJECT_PATTERNS):
        return True
    return True


def _infer_activity_type(text: str) -> str:
    if any(pattern in text for pattern in (" lecture ", " lectures ", " talk ", " talks ", " conversation ", " discussions ", " discussion ", " chat ")):
        return "talk"
    if any(pattern in text for pattern in (" workshop ", " workshops ", " class ", " classes ", " paper weaving ", " watercolor ")):
        return "workshop"
    return "activity"


def _page_text(html: str) -> str:
    return _normalize_space(BeautifulSoup(html, "html.parser").get_text("\n", strip=True))


def _html_to_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = BeautifulSoup(unescape(value), "html.parser").get_text(" ", strip=True)
    return _normalize_space(text)


def _normalize_space(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(unescape(value).split())
    return normalized or None


def _searchable_text(value: str) -> str:
    return f" {' '.join((value or '').lower().split())} "


def _first_text(nodes: list[BeautifulSoup]) -> str | None:
    for node in nodes:
        text = _normalize_space(node.get_text(" ", strip=True))
        if text:
            return text
    return None


def _longest_text(nodes: list[BeautifulSoup]) -> str | None:
    texts = [_normalize_space(node.get_text(" ", strip=True)) for node in nodes]
    texts = [text for text in texts if text]
    if not texts:
        return None
    return max(texts, key=len)


def _strip_field_label(value: str | None, label: str) -> str | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None
    prefix = f"{label} "
    if normalized.startswith(prefix):
        return normalized[len(prefix) :].strip() or None
    return normalized


def _speed_title_looks_relevant(title: str) -> bool:
    blob = _searchable_text(title)
    if any(pattern in blob for pattern in TITLE_REJECT_PATTERNS):
        return False
    lower_title = title.lower()
    return any(keyword in lower_title for keyword in ("activity", "chat", "class", "family", "lecture", "talk", "workshop"))
