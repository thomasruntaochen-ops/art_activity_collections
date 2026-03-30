from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from urllib.parse import urljoin
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

AR_TIMEZONE = "America/Chicago"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " art fair ",
    " art making ",
    " art-making ",
    " class ",
    " classes ",
    " conversation ",
    " discussion ",
    " hands on ",
    " hands-on ",
    " lecture ",
    " lectures ",
    " mystery at the museum ",
    " open studios conversation ",
    " reading beyond words ",
    " second saturday ",
    " studio social ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
WEAK_INCLUDE_PATTERNS = (
    " art ",
    " family ",
    " families ",
    " kid ",
    " kids ",
    " teen ",
    " teens ",
    " tween ",
    " youth ",
)
ALWAYS_REJECT_PATTERNS = (
    " admission ",
    " concert ",
    " culinary ",
    " dinner ",
    " exhibition ",
    " exhibitions ",
    " festival ",
    " film ",
    " films ",
    " food drink ",
    " forest bathing ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " member event ",
    " music ",
    " party ",
    " performance ",
    " performances ",
    " tour ",
    " tours ",
    " volunteer orientation ",
    " yoga ",
)
HARD_REJECT_PATTERNS = (
    " camp ",
    " camps ",
    " community meet up ",
    " community meetup ",
    " dog days ",
    " forest bathing ",
    " sound bath ",
    " volunteer orientation ",
)
REGISTRATION_PATTERNS = (
    " book ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
    " ticket ",
    " tickets ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
    " open studio ",
    " open studios ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)\s*(?:-|–|—|to)\s*(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
SINGLE_TIME_RE = re.compile(r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)", re.IGNORECASE)
DATE_RE = re.compile(r"([A-Za-z]{3,9})\s+(\d{1,2})(?:,\s*(\d{4}))?", re.IGNORECASE)
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


@dataclass(frozen=True, slots=True)
class ArCustomCalendarVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_urls: tuple[str, ...]


AR_CUSTOM_CALENDAR_VENUES: tuple[ArCustomCalendarVenueConfig, ...] = (
    ArCustomCalendarVenueConfig(
        slug="crystal_bridges",
        source_name="ar_crystal_bridges_events",
        venue_name="Crystal Bridges Museum of American Art",
        city="Bentonville",
        state="AR",
        list_urls=(
            "https://crystalbridges.org/calendar/",
            "https://crystalbridges.org/families/",
        ),
    ),
    ArCustomCalendarVenueConfig(
        slug="momentary",
        source_name="ar_momentary_events",
        venue_name="The Momentary",
        city="Bentonville",
        state="AR",
        list_urls=("https://themomentary.org/calendar/",),
    ),
)

AR_CUSTOM_CALENDAR_VENUES_BY_SLUG = {venue.slug: venue for venue in AR_CUSTOM_CALENDAR_VENUES}


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


async def load_ar_custom_calendar_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    max_detail_pages: int = 200,
) -> dict[str, dict]:
    selected = (
        [AR_CUSTOM_CALENDAR_VENUES_BY_SLUG[slug] for slug in venue_slugs]
        if venue_slugs
        else list(AR_CUSTOM_CALENDAR_VENUES)
    )
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            list_htmls: dict[str, str] = {}
            detail_pages: dict[str, str] = {}

            if venue.slug == "momentary":
                first_url = venue.list_urls[0]
                first_html = await fetch_html(first_url, client=client)
                list_htmls[first_url] = first_html
                page_urls = _extract_momentary_page_urls(first_html, first_url)
                for page_url in page_urls[1:]:
                    list_htmls[page_url] = await fetch_html(page_url, client=client)
            else:
                for list_url in venue.list_urls:
                    list_htmls[list_url] = await fetch_html(list_url, client=client)

            detail_urls = _extract_detail_urls(venue.slug, list_htmls)
            for detail_url in detail_urls[:max_detail_pages]:
                try:
                    detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                except Exception as exc:
                    print(f"[ar-custom-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")

            payload[venue.slug] = {
                "list_htmls": list_htmls,
                "detail_pages": detail_pages,
            }

    return payload


def parse_ar_custom_calendar_events(
    payload: dict,
    *,
    venue: ArCustomCalendarVenueConfig,
) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(AR_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}

    for source_url, html in (payload.get("detail_pages") or {}).items():
        row = _parse_detail_page(source_url=source_url, html=html, venue=venue)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    rows = list(deduped.values())
    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class ArCustomCalendarBundleAdapter(BaseSourceAdapter):
    source_name = "ar_custom_calendar_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_ar_custom_calendar_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError(
            "Use load_ar_custom_calendar_bundle_payload/parse_ar_custom_calendar_events from script runner."
        )


def _extract_momentary_page_urls(first_html: str, first_url: str) -> list[str]:
    soup = BeautifulSoup(first_html, "html.parser")
    urls = [first_url]
    seen = {first_url}
    for anchor in soup.select("a.page-numbers[href]"):
        absolute = urljoin(first_url, anchor.get("href") or "")
        if absolute in seen:
            continue
        seen.add(absolute)
        urls.append(absolute)
    return urls


def _extract_detail_urls(venue_slug: str, list_htmls: dict[str, str]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    selectors = {
        "crystal_bridges": ("a.c-event-card__cover-link[href]",),
        "momentary": (
            ".c-event-card > a[href]",
            ".c-event-card__title a[href]",
            ".c-event-card__btn[href]",
        ),
    }
    for list_url, html in list_htmls.items():
        soup = BeautifulSoup(html, "html.parser")
        for selector in selectors.get(venue_slug, ("a.c-event-card__cover-link[href]",)):
            for anchor in soup.select(selector):
                absolute = urljoin(list_url, anchor.get("href") or "")
                if not absolute:
                    continue
                if venue_slug == "crystal_bridges" and not absolute.startswith("https://crystalbridges.org/calendar/"):
                    continue
                if venue_slug == "momentary" and not absolute.startswith("https://themomentary.org/calendar/"):
                    continue
                if absolute in seen:
                    continue
                seen.add(absolute)
                urls.append(absolute)
    return urls


def _parse_detail_page(
    *,
    source_url: str,
    html: str,
    venue: ArCustomCalendarVenueConfig,
) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    event_obj = _extract_json_ld_event(soup)

    title = _normalize_space(
        _coalesce(
            _string_or_none(event_obj.get("name") if event_obj else None),
            soup.select_one("h1.c-masthead__heading").get_text(" ", strip=True) if soup.select_one("h1.c-masthead__heading") else None,
        )
    )
    if not title:
        return None

    description = _extract_description_text(soup)
    category_text = _extract_category_text(soup)
    location_text = _extract_location_text(soup) or _extract_location_from_event_obj(event_obj)
    if not location_text:
        location_text = f"{venue.city}, {venue.state}"
    date_text = _extract_date_text(soup)
    price_text = _extract_price_text(soup, event_obj)
    if _is_sold_out(soup):
        return None

    start_at = _parse_event_start(event_obj, date_text=date_text)
    if start_at is None:
        return None
    end_at = _parse_event_end(event_obj, date_text=date_text, start_at=start_at)

    token_blob = _searchable_blob(" ".join([title, category_text or "", description or "", price_text or ""]))
    category_blob = _searchable_blob(category_text or "")
    if not _should_keep_event(token_blob=token_blob, category_blob=category_blob):
        return None

    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))
    amount = _extract_offer_amount(event_obj)
    if amount is not None:
        is_free, free_status = infer_price_classification_from_amount(amount, text=price_text, default_is_free=None)
    else:
        is_free, free_status = infer_price_classification(price_text or description, default_is_free=None)

    description_parts = [part for part in [description, f"Category: {category_text}" if category_text else None] if part]
    if price_text:
        description_parts.append(f"Price: {price_text}")
    full_description = " | ".join(dict.fromkeys(description_parts)) or None

    registration_required = any(pattern in token_blob for pattern in REGISTRATION_PATTERNS)
    if any(phrase in token_blob for phrase in (" no registration required ", " no tickets required ")):
        registration_required = False

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(token_blob, category_blob=category_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=registration_required,
        start_at=start_at,
        end_at=end_at,
        timezone=AR_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_json_ld_event(soup: BeautifulSoup) -> dict | None:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = (script.string or script.get_text() or "").strip()
        if not script_text:
            continue
        try:
            payload = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        event_obj = _find_event_obj(payload)
        if event_obj is not None:
            return event_obj
    return None


def _find_event_obj(payload: object) -> dict | None:
    if isinstance(payload, dict):
        if payload.get("@type") == "Event":
            return payload
        for value in payload.values():
            nested = _find_event_obj(value)
            if nested is not None:
                return nested
    if isinstance(payload, list):
        for item in payload:
            nested = _find_event_obj(item)
            if nested is not None:
                return nested
    return None


def _extract_description_text(soup: BeautifulSoup) -> str:
    paragraphs: list[str] = []
    for selector in (
        ".c-col-text-area p",
        ".c-container__items p",
        ".c-content-wrap p",
        ".c-page_body p",
    ):
        for node in soup.select(selector):
            text = _normalize_space(node.get_text(" ", strip=True))
            if not text:
                continue
            paragraphs.append(text)
    return " ".join(dict.fromkeys(paragraphs))


def _extract_category_text(soup: BeautifulSoup) -> str | None:
    values: list[str] = []
    for selector in (
        ".c-event-details__category",
        ".c-masthead__eventtype",
        ".c-event-card__category",
    ):
        for node in soup.select(selector):
            text = _normalize_space(node.get_text(" ", strip=True))
            if text:
                values.append(text)
    return ", ".join(dict.fromkeys(values)) or None


def _extract_location_text(soup: BeautifulSoup) -> str | None:
    for selector in (
        ".c-event-details__location",
        ".c-masthead__location",
    ):
        node = soup.select_one(selector)
        if node is None:
            continue
        text = _normalize_space(node.get_text(" ", strip=True))
        if text:
            return text
    return None


def _extract_location_from_event_obj(event_obj: dict | None) -> str | None:
    if not isinstance(event_obj, dict):
        return None
    location = event_obj.get("location")
    if isinstance(location, dict):
        name = _normalize_space(location.get("name"))
        address = location.get("address")
        if isinstance(address, dict):
            locality = _normalize_space(address.get("addressLocality"))
            region = _normalize_space(address.get("addressRegion"))
            parts = [part for part in [name, locality, region] if part]
            return ", ".join(parts) if parts else None
        return name or None
    return None


def _extract_date_text(soup: BeautifulSoup) -> str:
    for selector in (
        ".c-event-details__datetime",
        ".c-masthead__datetime time",
        ".c-masthead__datetime",
    ):
        node = soup.select_one(selector)
        if node is None:
            continue
        text = _normalize_space(node.get_text(" ", strip=True))
        if text:
            return text
    return ""


def _extract_price_text(soup: BeautifulSoup, event_obj: dict | None) -> str | None:
    for selector in (
        ".c-event-details__price-info",
        ".c-masthead__price-and-book",
    ):
        node = soup.select_one(selector)
        if node is None:
            continue
        text = _normalize_space(node.get_text(" ", strip=True))
        if text:
            return text

    if isinstance(event_obj, dict):
        offers = event_obj.get("offers")
        if isinstance(offers, dict):
            amount = _coalesce(_string_or_none(offers.get("price")), _string_or_none(offers.get("lowPrice")))
            if amount:
                return amount

    return None


def _extract_offer_amount(event_obj: dict | None) -> Decimal | None:
    if not isinstance(event_obj, dict):
        return None
    offers = event_obj.get("offers")
    if isinstance(offers, dict):
        for key in ("price", "lowPrice"):
            value = offers.get(key)
            if value is None:
                continue
            try:
                return Decimal(str(value))
            except Exception:
                continue
    return None


def _parse_event_start(event_obj: dict | None, *, date_text: str) -> datetime | None:
    if isinstance(event_obj, dict):
        start_value = event_obj.get("startDate")
        parsed = _parse_iso_datetime(start_value)
        if parsed is not None:
            return parsed
    return _parse_date_text(date_text)


def _parse_event_end(event_obj: dict | None, *, date_text: str, start_at: datetime) -> datetime | None:
    if isinstance(event_obj, dict):
        end_value = event_obj.get("endDate")
        parsed = _parse_iso_datetime(end_value)
        if parsed is not None:
            return parsed

    time_match = TIME_RANGE_RE.search(date_text)
    if time_match is None:
        return None
    return datetime(
        start_at.year,
        start_at.month,
        start_at.day,
        *_parse_time_parts(time_match.group(4), time_match.group(5), time_match.group(6)),
    )


def _parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(ZoneInfo(AR_TIMEZONE)).replace(tzinfo=None)


def _parse_date_text(value: str) -> datetime | None:
    if not value:
        return None
    date_match = DATE_RE.search(value)
    if date_match is None:
        return None
    month = MONTH_LOOKUP.get(date_match.group(1)[:3].lower())
    if month is None:
        return None
    day = int(date_match.group(2))
    year = int(date_match.group(3) or datetime.now(ZoneInfo(AR_TIMEZONE)).year)

    time_match = TIME_RANGE_RE.search(value)
    if time_match is not None:
        hour, minute = _parse_time_parts(time_match.group(1), time_match.group(2), time_match.group(3))
        return datetime(year, month, day, hour, minute)

    single_time_match = SINGLE_TIME_RE.search(value)
    if single_time_match is not None:
        hour, minute = _parse_time_parts(
            single_time_match.group(1),
            single_time_match.group(2),
            single_time_match.group(3),
        )
        return datetime(year, month, day, hour, minute)

    return datetime(year, month, day)


def _parse_time_parts(hour_text: str, minute_text: str | None, meridiem: str) -> tuple[int, int]:
    hour = int(hour_text)
    minute = int(minute_text or "0")
    normalized = meridiem.lower().replace(".", "")
    if normalized == "pm" and hour != 12:
        hour += 12
    if normalized == "am" and hour == 12:
        hour = 0
    return hour, minute


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _should_keep_event(*, token_blob: str, category_blob: str) -> bool:
    if " sold out " in token_blob or " cancelled " in token_blob or " canceled " in token_blob:
        return False
    if any(pattern in token_blob for pattern in HARD_REJECT_PATTERNS):
        return False
    if any(pattern in category_blob for pattern in HARD_REJECT_PATTERNS):
        return False

    strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    weak_include = strong_include or any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS)
    if not weak_include:
        return False

    if any(pattern in category_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS) and not strong_include:
        return False

    return True


def _infer_activity_type(token_blob: str, *, category_blob: str) -> str:
    if " talk lecture " in category_blob:
        return "lecture"
    if any(pattern in token_blob for pattern in (" lecture ", " lectures ", " talk ", " talks ", " conversation ", " discussion ")):
        return "lecture"
    return "workshop"


def _is_sold_out(soup: BeautifulSoup) -> bool:
    text = _searchable_blob(soup.get_text(" ", strip=True))
    return " sold out " in text


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _string_or_none(value: object) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _coalesce(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None


def get_ar_custom_calendar_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in AR_CUSTOM_CALENDAR_VENUES:
        for list_url in venue.list_urls:
            parsed = urlparse(list_url)
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(dict.fromkeys(prefixes))
