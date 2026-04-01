from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from decimal import Decimal
from urllib.parse import urljoin
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

MI_TIMEZONE = "America/Detroit"
MI_ZONEINFO = ZoneInfo(MI_TIMEZONE)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

PLAYWRIGHT_LAUNCH_ARGS = (
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
)

GRAM_RANGE_SEP_RE = re.compile(r"\s*(?:-|–|—)\s*")
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?)"
    r"\s*(?:-|–|—)\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})(?:\+|\s*(?:and\s+up|up))\b", re.IGNORECASE)
MATCH_NORMALIZE_RE = re.compile(r"[\W_]+", re.UNICODE)


@dataclass(frozen=True, slots=True)
class MiVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str


MI_VENUES: tuple[MiVenueConfig, ...] = (
    MiVenueConfig(
        slug="dia",
        source_name="mi_dia_events",
        venue_name="Detroit Institute of Arts",
        city="Detroit",
        state="MI",
        list_url="https://dia.org/events",
    ),
    MiVenueConfig(
        slug="grand_rapids",
        source_name="mi_grand_rapids_events",
        venue_name="Grand Rapids Art Museum",
        city="Grand Rapids",
        state="MI",
        list_url="https://www.artmuseumgr.org/events",
    ),
    MiVenueConfig(
        slug="kalamazoo",
        source_name="mi_kalamazoo_events",
        venue_name="Kalamazoo Institute of Arts",
        city="Kalamazoo",
        state="MI",
        list_url="https://www.kiarts.org/events/",
    ),
    MiVenueConfig(
        slug="msu_broad",
        source_name="mi_msu_broad_events",
        venue_name="MSU Broad Art Museum",
        city="East Lansing",
        state="MI",
        list_url="https://broadmuseum.msu.edu/events/",
    ),
    MiVenueConfig(
        slug="muskegon",
        source_name="mi_muskegon_events",
        venue_name="Muskegon Museum of Art",
        city="Muskegon",
        state="MI",
        list_url="https://muskegonartmuseum.org/events/",
    ),
    MiVenueConfig(
        slug="umma",
        source_name="mi_umma_events",
        venue_name="University of Michigan Museum of Art",
        city="Ann Arbor",
        state="MI",
        list_url="https://umma.umich.edu/visit/events/",
    ),
)

MI_VENUES_BY_SLUG = {venue.slug: venue for venue in MI_VENUES}


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

            if response.status_code in (403, 429) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            if response.status_code in (500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError(f"Unable to fetch HTML: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch HTML after retries: {url}")


async def fetch_html_playwright(url: str, *, timeout_ms: int = 90000) -> str:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, args=list(PLAYWRIGHT_LAUNCH_ARGS))
        page = await browser.new_page(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            locale="en-US",
            timezone_id=MI_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def _fetch_detail_pages(
    *,
    urls: set[str],
    client: httpx.AsyncClient,
    label: str,
    concurrency: int = 8,
) -> dict[str, str]:
    semaphore = asyncio.Semaphore(max(concurrency, 1))

    async def _fetch_one(url: str) -> tuple[str, str | None]:
        async with semaphore:
            try:
                return url, await fetch_html(url, client=client)
            except Exception as exc:
                print(f"[mi-bundle] {label} detail fetch failed url={url}: {exc}")
                return url, None

    pages: dict[str, str] = {}
    for url, html in await asyncio.gather(*(_fetch_one(url) for url in sorted(urls))):
        if html:
            pages[url] = html
    return pages


async def load_mi_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    gram_month_count: int = 6,
    broad_max_pages: int = 8,
) -> dict[str, dict]:
    selected = [MI_VENUES_BY_SLUG[slug] for slug in venue_slugs] if venue_slugs else list(MI_VENUES)
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            if venue.slug == "dia":
                payload[venue.slug] = await _load_dia_payload(venue=venue)
            elif venue.slug == "kalamazoo":
                payload[venue.slug] = await _load_kalamazoo_payload(client=client)
            elif venue.slug == "grand_rapids":
                payload[venue.slug] = await _load_grand_rapids_payload(
                    venue=venue,
                    client=client,
                    month_count=gram_month_count,
                )
            elif venue.slug == "msu_broad":
                payload[venue.slug] = await _load_msu_broad_payload(
                    venue=venue,
                    client=client,
                    max_pages=broad_max_pages,
                )
            elif venue.slug == "muskegon":
                payload[venue.slug] = {
                    "list_html": await fetch_html(venue.list_url, client=client),
                }
            elif venue.slug == "umma":
                payload[venue.slug] = await _load_umma_payload(venue=venue, client=client)
            else:
                payload[venue.slug] = {}

    return payload


def parse_mi_events(payload: dict, *, venue: MiVenueConfig) -> list[ExtractedActivity]:
    if venue.slug == "dia":
        rows = _parse_dia_events(payload, venue=venue)
    elif venue.slug == "kalamazoo":
        rows = _parse_kalamazoo_events(payload, venue=venue)
    elif venue.slug == "grand_rapids":
        rows = _parse_grand_rapids_events(payload, venue=venue)
    elif venue.slug == "msu_broad":
        rows = _parse_msu_broad_events(payload, venue=venue)
    elif venue.slug == "muskegon":
        rows = _parse_muskegon_events(payload, venue=venue)
    elif venue.slug == "umma":
        rows = _parse_umma_events(payload, venue=venue)
    else:
        rows = []

    today = datetime.now(MI_ZONEINFO).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < today:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


class MiBundleAdapter(BaseSourceAdapter):
    source_name = "mi_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_mi_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_mi_bundle_payload/parse_mi_events from the script runner.")


async def _load_kalamazoo_payload(*, client: httpx.AsyncClient) -> dict:
    events: list[dict] = []
    next_url = "https://www.kiarts.org/wp-json/tribe/events/v1/events?per_page=50"

    while next_url:
        response = await client.get(next_url)
        response.raise_for_status()
        page_payload = response.json()
        events.extend(page_payload.get("events") or [])
        next_url = page_payload.get("next_rest_url")

    return {"events": events}


async def _load_dia_payload(*, venue: MiVenueConfig) -> dict:
    return {
        "list_html": await fetch_html_playwright(venue.list_url),
    }


async def _load_grand_rapids_payload(
    *,
    venue: MiVenueConfig,
    client: httpx.AsyncClient,
    month_count: int,
) -> dict:
    month_pages: dict[str, str] = {}
    detail_pages: dict[str, str] = {}
    detail_urls: set[str] = set()
    empty_months = 0

    for month_start in _iter_month_starts(count=month_count):
        month_url = f"{venue.list_url}?rangeStart={month_start.isoformat()}"
        html = await fetch_html(month_url, client=client)
        month_pages[month_url] = html

        links = _extract_grand_rapids_event_links(html)
        if links:
            empty_months = 0
            detail_urls.update(links)
        else:
            empty_months += 1
            if empty_months >= 2:
                break

    if detail_urls:
        detail_pages = await _fetch_detail_pages(urls=detail_urls, client=client, label="grand_rapids")

    return {
        "month_pages": month_pages,
        "detail_pages": detail_pages,
    }


async def _load_msu_broad_payload(
    *,
    venue: MiVenueConfig,
    client: httpx.AsyncClient,
    max_pages: int,
) -> dict:
    list_pages: dict[str, str] = {}
    detail_pages: dict[str, str] = {}
    detail_urls: set[str] = set()

    for page_number in range(1, max_pages + 1):
        page_url = venue.list_url if page_number == 1 else f"{venue.list_url}?pno={page_number}"
        html = await fetch_html(page_url, client=client)
        list_pages[page_url] = html
        page_links = _extract_msu_broad_event_links(html)
        if not page_links:
            break
        detail_urls.update(page_links)
        if not _msu_broad_has_next_page(html, current_page=page_number):
            break

    if detail_urls:
        detail_pages = await _fetch_detail_pages(urls=detail_urls, client=client, label="msu_broad")

    return {
        "list_pages": list_pages,
        "detail_pages": detail_pages,
    }


async def _load_umma_payload(*, venue: MiVenueConfig, client: httpx.AsyncClient) -> dict:
    list_html = await fetch_html(venue.list_url, client=client)
    detail_pages: dict[str, str] = {}

    detail_urls = _extract_umma_detail_links(list_html)
    if detail_urls:
        detail_pages = await _fetch_detail_pages(urls=detail_urls, client=client, label="umma")

    return {
        "list_html": list_html,
        "detail_pages": detail_pages,
    }


def _parse_kalamazoo_events(payload: dict, *, venue: MiVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []

    for event_obj in payload.get("events") or []:
        title = _normalize_space(event_obj.get("title"))
        source_url = _normalize_space(event_obj.get("url"))
        start_at = _parse_iso_or_local_datetime(event_obj.get("start_date"), timezone_name=MI_TIMEZONE)
        if not title or not source_url or start_at is None:
            continue

        description = _clean_html(event_obj.get("description"))
        category_text = ", ".join(
            _normalize_space(category.get("name"))
            for category in event_obj.get("categories") or []
            if _normalize_space(category.get("name"))
        )
        custom_text = " | ".join(
            f"{_normalize_space(field.get('label'))}: {_normalize_space(field.get('value'))}"
            for field in (event_obj.get("custom_fields") or {}).values()
            if isinstance(field, dict)
            and (_normalize_space(field.get("label")) or _normalize_space(field.get("value")))
        )
        combined_description = " | ".join(
            part for part in (description, f"Categories: {category_text}" if category_text else None, custom_text or None) if part
        ) or None
        if not _should_keep_kalamazoo_event(title=title, description=combined_description, category_text=category_text):
            continue

        venue_info = event_obj.get("venue") or {}
        if isinstance(venue_info, list):
            venue_info = venue_info[0] if venue_info else {}
        if not isinstance(venue_info, dict):
            venue_info = {}
        location_parts = [
            _normalize_space(venue_info.get("venue")),
            _normalize_space(venue_info.get("address")),
            _normalize_space(venue_info.get("city")),
            _normalize_space(venue_info.get("state")),
        ]
        location_text = ", ".join(part for part in location_parts if part) or f"{venue.city}, {venue.state}"

        cost_amount = _extract_first_decimal((event_obj.get("cost_details") or {}).get("values"))
        is_free, free_status = infer_price_classification_from_amount(
            cost_amount,
            text=combined_description,
            default_is_free=None,
        )
        age_min, age_max = _parse_age_range(combined_description)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=combined_description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_activity_type_for_text(title=title, description=combined_description),
                age_min=age_min,
                age_max=age_max,
                drop_in=_contains_any(_normalize_for_match(f"{title} {combined_description or ''}"), (" drop in ", " drop-in ")),
                registration_required=_registration_required_for_text(title=title, description=combined_description),
                start_at=start_at,
                end_at=_parse_iso_or_local_datetime(event_obj.get("end_date"), timezone_name=MI_TIMEZONE),
                timezone=MI_TIMEZONE,
                free_verification_status=free_status,
                is_free=is_free,
            )
        )

    return rows


def _parse_dia_events(payload: dict, *, venue: MiVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    rows: list[ExtractedActivity] = []

    for card in soup.select("article.calendar-card.node__event"):
        link = card.select_one("a.overlay[href]")
        type_node = card.select_one(".left-side .event-type")
        right_side = card.select_one(".right-side .data")
        if link is None or type_node is None:
            continue

        title = _normalize_space(link.get_text(" ", strip=True))
        source_url = urljoin(venue.list_url, link.get("href") or "")
        event_type = _normalize_space(type_node.get_text(" ", strip=True))
        description = _normalize_space(right_side.get_text(" ", strip=True) if right_side else None)
        combined_description = " | ".join(part for part in (description, f"Type: {event_type}") if part) or None
        if not _should_keep_dia_event(title=title, description=combined_description, event_type=event_type):
            continue

        age_min, age_max = _parse_age_range(description)
        is_free, free_status = infer_price_classification(combined_description, default_is_free=None)

        for start_at, end_at in _extract_dia_occurrences(card):
            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=combined_description,
                    venue_name=venue.venue_name,
                    location_text=f"{venue.venue_name}, {venue.city}, {venue.state}",
                    city=venue.city,
                    state=venue.state,
                    activity_type=_activity_type_for_text(title=event_type, description=combined_description),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=_contains_any(_normalize_for_match(f"{event_type} {title}"), (" drop in ", " drop in workshop ")),
                    registration_required=False,
                    start_at=start_at,
                    end_at=end_at,
                    timezone=MI_TIMEZONE,
                    free_verification_status=free_status,
                    is_free=is_free,
                )
            )

    return rows


def _parse_grand_rapids_events(payload: dict, *, venue: MiVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    detail_pages = payload.get("detail_pages") or {}

    for html in (payload.get("month_pages") or {}).values():
        soup = BeautifulSoup(html, "html.parser")
        for anchor in _iter_gram_cards(soup):
            source_url = urljoin(venue.list_url, anchor.get("href") or "")
            if not source_url:
                continue

            card_info = _parse_grand_rapids_card(anchor)
            if card_info is None:
                continue

            title = card_info["title"]
            category_text = card_info["category_text"]
            detail_description, location_text, cost_text = _parse_grand_rapids_detail(detail_pages.get(source_url) or "")
            combined_description = " | ".join(
                part
                for part in (
                    detail_description,
                    f"Categories: {category_text}" if category_text else None,
                    f"Cost: {cost_text}" if cost_text else None,
                )
                if part
            ) or None
            if not _should_keep_grand_rapids_event(
                title=title,
                description=combined_description,
                category_text=category_text,
            ):
                continue

            key = (source_url, title, card_info["start_at"])
            if key in seen:
                continue
            seen.add(key)

            price_text = " | ".join(part for part in (category_text, cost_text, combined_description or "") if part)
            is_free, free_status = infer_price_classification(price_text, default_is_free=None)

            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=combined_description,
                    venue_name=venue.venue_name,
                    location_text=location_text or f"{venue.city}, {venue.state}",
                    city=venue.city,
                    state=venue.state,
                    activity_type=_activity_type_for_text(title=title, description=combined_description),
                    age_min=None,
                    age_max=None,
                    drop_in=_contains_any(_normalize_for_match(f"{title} {combined_description or ''}"), (" drop in ", " drop-in ")),
                    registration_required=_registration_required_for_text(title=title, description=combined_description),
                    start_at=card_info["start_at"],
                    end_at=card_info["end_at"],
                    timezone=MI_TIMEZONE,
                    free_verification_status=free_status,
                    is_free=is_free,
                )
            )

    return rows


def _parse_msu_broad_events(payload: dict, *, venue: MiVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    detail_pages = payload.get("detail_pages") or {}

    for html in (payload.get("list_pages") or {}).values():
        soup = BeautifulSoup(html, "html.parser")
        for event_node in soup.select(".em-events-list-grouped .em-event.em-item"):
            link = event_node.select_one("h3.event-title a")
            if link is None:
                continue
            source_url = urljoin(venue.list_url, link.get("href") or "")
            title = _normalize_space(link.get_text(" ", strip=True))
            if not title or not source_url:
                continue

            date_text = _normalize_space(event_node.select_one(".em-event-date") and event_node.select_one(".em-event-date").get_text(" ", strip=True))
            time_text = _normalize_space(event_node.select_one(".em-event-time") and event_node.select_one(".em-event-time").get_text(" ", strip=True))
            location_text = _normalize_space(event_node.select_one(".em-event-location") and event_node.select_one(".em-event-location").get_text(" ", strip=True))
            start_at, end_at = _parse_human_start_end(date_text, time_text)
            if start_at is None:
                continue

            categories = [
                _normalize_space(node.get_text(" ", strip=True))
                for node in event_node.select(".em-event-categories a")
                if _normalize_space(node.get_text(" ", strip=True))
            ]
            category_text = ", ".join(categories)
            detail_description, detail_category_text = _parse_msu_broad_detail(detail_pages.get(source_url) or "")
            if detail_category_text and not category_text:
                category_text = detail_category_text
            combined_description = " | ".join(
                part for part in (detail_description, f"Categories: {category_text}" if category_text else None) if part
            ) or None
            if not _should_keep_msu_broad_event(
                title=title,
                description=combined_description,
                category_text=category_text,
            ):
                continue

            key = (source_url, title, start_at)
            if key in seen:
                continue
            seen.add(key)

            price_text = " | ".join(part for part in (combined_description or "", category_text) if part)
            is_free, free_status = infer_price_classification(price_text, default_is_free=None)

            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=combined_description,
                    venue_name=venue.venue_name,
                    location_text=location_text or f"{venue.city}, {venue.state}",
                    city=venue.city,
                    state=venue.state,
                    activity_type=_activity_type_for_text(title=title, description=combined_description),
                    age_min=None,
                    age_max=None,
                    drop_in=_contains_any(_normalize_for_match(f"{title} {combined_description or ''}"), (" drop in ", " drop-in ")),
                    registration_required=_registration_required_for_text(title=title, description=combined_description),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=MI_TIMEZONE,
                    free_verification_status=free_status,
                    is_free=is_free,
                )
            )

    return rows


def _parse_muskegon_events(payload: dict, *, venue: MiVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for script in soup.select('script[type="application/ld+json"]'):
        data = _safe_json_loads(script.string or script.get_text() or "")
        if data is None:
            continue
        for event_obj in _iter_event_objects(data):
            if _normalize_space(event_obj.get("@type")) not in {"Event", ""} and "Event" not in str(event_obj.get("@type")):
                continue

            title = _normalize_space(event_obj.get("name"))
            source_url = _normalize_space(event_obj.get("url"))
            start_at = _parse_iso_or_local_datetime(event_obj.get("startDate"), timezone_name=MI_TIMEZONE)
            if not title or not source_url or start_at is None:
                continue

            description = _clean_html(event_obj.get("description"))
            location_name = _extract_location_name(event_obj.get("location"))
            offers = event_obj.get("offers")
            offer_text = _normalize_space(json.dumps(offers, sort_keys=True)) if offers else None
            combined_description = " | ".join(
                part
                for part in (
                    description,
                    f"Location: {location_name}" if location_name else None,
                    f"Offers: {offer_text}" if offer_text else None,
                )
                if part
            ) or None
            if not _should_keep_muskegon_event(title=title, description=combined_description):
                continue

            end_at = _parse_iso_or_local_datetime(event_obj.get("endDate"), timezone_name=MI_TIMEZONE)
            start_at, end_at = _maybe_override_suspicious_schema_times(
                start_at=start_at,
                end_at=end_at,
                description=description,
            )

            key = (source_url, title, start_at)
            if key in seen:
                continue
            seen.add(key)

            amount = _extract_offer_amount(offers)
            is_free, free_status = infer_price_classification_from_amount(
                amount,
                text=combined_description,
                default_is_free=None,
            )

            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=combined_description,
                    venue_name=venue.venue_name,
                    location_text=location_name or f"{venue.city}, {venue.state}",
                    city=venue.city,
                    state=venue.state,
                    activity_type=_activity_type_for_text(title=title, description=combined_description),
                    age_min=None,
                    age_max=None,
                    drop_in=_contains_any(_normalize_for_match(f"{title} {combined_description or ''}"), (" drop in ", " drop-in ")),
                    registration_required=_registration_required_for_text(title=title, description=combined_description),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=MI_TIMEZONE,
                    free_verification_status=free_status,
                    is_free=is_free,
                )
            )

    return rows


def _parse_umma_events(payload: dict, *, venue: MiVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    featured_row = _parse_umma_featured_event(soup)
    if featured_row is not None:
        source_url = featured_row["source_url"]
        detail_description, detail_registration_required = _parse_umma_detail(detail_pages.get(source_url) or "")
        combined_description = detail_description or featured_row["description"]
        if _should_keep_umma_event(title=featured_row["title"], description=combined_description):
            key = (source_url, featured_row["title"], featured_row["start_at"])
            seen.add(key)
            is_free, free_status = infer_price_classification(combined_description, default_is_free=None)
            registration_required = detail_registration_required or _registration_required_for_text(
                title=featured_row["title"],
                description=combined_description,
            )
            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=featured_row["title"],
                    description=combined_description,
                    venue_name=venue.venue_name,
                    location_text=featured_row["location_text"],
                    city=venue.city,
                    state=venue.state,
                    activity_type=_activity_type_for_text(title=featured_row["title"], description=combined_description),
                    age_min=None,
                    age_max=None,
                    drop_in=False,
                    registration_required=registration_required,
                    start_at=featured_row["start_at"],
                    end_at=featured_row["end_at"],
                    timezone=MI_TIMEZONE,
                    free_verification_status=free_status,
                    is_free=is_free,
                )
            )

    for card in soup.select(".block-eventslist .card.card-event"):
        anchor = card.select_one("a.th7.treg")
        if anchor is None:
            continue
        source_url = urljoin(venue.list_url, anchor.get("href") or "")
        title = _normalize_space(anchor.get_text(" ", strip=True))
        if not title or not source_url:
            continue

        datetime_node = card.select_one(".datetime")
        spans = datetime_node.find_all("span") if datetime_node else []
        if len(spans) < 2:
            continue
        date_text = _normalize_space(spans[0].get_text(" ", strip=True))
        time_text = _normalize_space(spans[1].get_text(" ", strip=True))
        start_at, end_at = _parse_human_start_end(date_text, time_text)
        if start_at is None:
            continue

        location_text = None
        for child in card.find_all("div", recursive=False):
            classes = set(child.get("class") or [])
            if "tp2" in classes and "datetime" not in classes:
                location_text = _normalize_space(child.get_text(" ", strip=True))
                break

        detail_description, detail_registration_required = _parse_umma_detail(detail_pages.get(source_url) or "")
        related = ", ".join(
            _normalize_space(node.get_text(" ", strip=True))
            for node in card.select(".related a")
            if _normalize_space(node.get_text(" ", strip=True))
        )
        combined_description = " | ".join(
            part
            for part in (
                detail_description,
                f"Related exhibition: {related}" if related else None,
            )
            if part
        ) or None
        if not _should_keep_umma_event(title=title, description=combined_description):
            continue

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        is_free, free_status = infer_price_classification(combined_description, default_is_free=None)
        registration_required = detail_registration_required or _registration_required_for_text(
            title=title,
            description=combined_description,
        )
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=combined_description,
                venue_name=venue.venue_name,
                location_text=location_text or f"{venue.city}, {venue.state}",
                city=venue.city,
                state=venue.state,
                activity_type=_activity_type_for_text(title=title, description=combined_description),
                age_min=None,
                age_max=None,
                drop_in=False,
                registration_required=registration_required,
                start_at=start_at,
                end_at=end_at,
                timezone=MI_TIMEZONE,
                free_verification_status=free_status,
                is_free=is_free,
            )
        )

    return rows


def _extract_grand_rapids_event_links(html: str) -> set[str]:
    soup = BeautifulSoup(html, "html.parser")
    return {
        urljoin("https://www.artmuseumgr.org/events", anchor.get("href") or "")
        for anchor in _iter_gram_cards(soup)
        if anchor.get("href")
    }


def _iter_gram_cards(soup: BeautifulSoup):
    section = soup.find("section", attrs={"x-data": "eventList()"})
    if section is None:
        return []
    cards: list = []
    for anchor in section.select("a.flex.thumb[href]"):
        href = _normalize_space(anchor.get("href"))
        if not href or "/events/" not in href:
            continue
        if anchor.select_one(".thumb__title") is None:
            continue
        cards.append(anchor)
    return cards


def _parse_grand_rapids_card(anchor) -> dict | None:
    title_node = anchor.select_one(".thumb__title")
    title = _normalize_space(title_node.get_text(" ", strip=True) if title_node else None)
    if not title:
        return None

    body_columns = anchor.find_all("div", recursive=False)
    if len(body_columns) < 2:
        return None
    body = body_columns[1]
    body_divs = [node for node in body.find_all("div", recursive=False)]
    if len(body_divs) < 4:
        return None

    category_text = _normalize_space(body_divs[0].get_text(" ", strip=True))
    date_text = _normalize_space(body_divs[2].get_text(" ", strip=True))
    time_text = _normalize_space(body_divs[3].get_text(" ", strip=True))
    start_at, end_at = _parse_human_start_end(date_text, time_text)
    if start_at is None:
        return None

    return {
        "title": title,
        "category_text": category_text,
        "start_at": start_at,
        "end_at": end_at,
    }


def _parse_grand_rapids_detail(html: str) -> tuple[str | None, str | None, str | None]:
    if not html:
        return None, None, None

    soup = BeautifulSoup(html, "html.parser")
    description = None
    location_text = None
    cost_text = None

    prose = soup.select_one(".pb-20 .prose")
    if prose is not None:
        description = _normalize_space(prose.get_text(" ", strip=True))

    blocks = soup.select(".pb-20 > div")
    for block in blocks:
        heading = block.select_one(".type-lg")
        if heading is None:
            continue
        heading_text = _normalize_space(heading.get_text(" ", strip=True)).lower()
        value_node = block.select_one(".prose")
        value_text = _normalize_space(value_node.get_text(" ", strip=True) if value_node else None)
        if heading_text == "location":
            location_text = value_text
        elif heading_text == "cost":
            cost_text = value_text

    return description, location_text, cost_text


def _extract_msu_broad_event_links(html: str) -> set[str]:
    soup = BeautifulSoup(html, "html.parser")
    return {
        urljoin("https://broadmuseum.msu.edu/events/", anchor.get("href") or "")
        for anchor in soup.select(".em-events-list-grouped .em-event .event-title a[href]")
    }


def _msu_broad_has_next_page(html: str, *, current_page: int) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    pagination = soup.select_one(".em-pagination")
    if pagination is None:
        return False
    for anchor in pagination.select("a.page-numbers[href]"):
        title = _normalize_space(anchor.get("title"))
        if title and title.isdigit() and int(title) > current_page:
            return True
        href = _normalize_space(anchor.get("href"))
        if href and f"pno={current_page + 1}" in href:
            return True
    return False


def _parse_msu_broad_detail(html: str) -> tuple[str | None, str | None]:
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    article = soup.select_one("article.event-item")
    if article is None:
        return None, None

    paragraphs = article.select(".event-description p")
    if not paragraphs:
        paragraphs = article.find_all("p")
    description = " ".join(_normalize_space(p.get_text(" ", strip=True)) for p in paragraphs if _normalize_space(p.get_text(" ", strip=True)))
    category_text = ", ".join(
        _normalize_space(node.get_text(" ", strip=True))
        for node in article.select(".event-categories a")
        if _normalize_space(node.get_text(" ", strip=True))
    )
    return description or None, category_text or None


def _extract_umma_detail_links(html: str) -> set[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = {
        urljoin("https://umma.umich.edu/visit/events/", anchor.get("href") or "")
        for anchor in soup.select(".block-eventslist .card.card-event a.th7.treg[href]")
    }
    featured = soup.select_one('.introblock-5050 a.btn.cta[href]')
    if featured is not None and featured.get("href"):
        links.add(urljoin("https://umma.umich.edu/visit/events/", featured.get("href") or ""))
    return links


def _parse_umma_featured_event(soup: BeautifulSoup) -> dict | None:
    block = soup.select_one(".introblock-5050")
    if block is None:
        return None

    title_node = block.select_one("h1.th5")
    title = _normalize_space(title_node.get_text(" ", strip=True) if title_node else None)
    cta = block.select_one("a.btn.cta[href]")
    if not title or cta is None:
        return None

    meta_line = _normalize_space(block.select_one(".col1 .wys p[style]") and block.select_one(".col1 .wys p[style]").get_text(" ", strip=True))
    if not meta_line:
        return None
    meta_match = re.search(
        r"(?P<date>(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})"
        r"\s*,?\s*(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?\s*(?:-|–|—)\s*"
        r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
        meta_line,
        re.IGNORECASE,
    )
    if meta_match is None:
        return None
    date_text = _normalize_space(meta_match.group("date"))
    time_text = _normalize_space(meta_match.group("time"))
    start_at, end_at = _parse_human_start_end(date_text, time_text)
    if start_at is None:
        return None

    location_text = None
    location_paragraphs = block.select(".col1 .wys p")
    if location_paragraphs:
        location_text = _normalize_space(location_paragraphs[-1].get_text(" ", strip=True))

    description = _normalize_space(block.select_one(".col2 .wys") and block.select_one(".col2 .wys").get_text(" ", strip=True))

    return {
        "source_url": urljoin("https://umma.umich.edu/visit/events/", cta.get("href") or ""),
        "title": title,
        "description": description,
        "location_text": location_text or "Ann Arbor, MI",
        "start_at": start_at,
        "end_at": end_at,
    }


def _parse_umma_detail(html: str) -> tuple[str | None, bool]:
    if not html:
        return None, False

    soup = BeautifulSoup(html, "html.parser")
    description = _normalize_space(
        " ".join(
            _normalize_space(node.get_text(" ", strip=True))
            for node in soup.select(".single-event-main p")
            if _normalize_space(node.get_text(" ", strip=True))
        )
    )
    registration_required = bool(soup.select_one('a[href*="#register"], .bgf-form, form#gform_'))
    return description or None, registration_required


def _should_keep_kalamazoo_event(*, title: str, description: str | None, category_text: str | None) -> bool:
    blob = _normalize_for_match(" ".join(part for part in (title, description or "", category_text or "") if part))
    if _contains_any(blob, (" free thursdays ", " storytime ", " reception ", " opening reception ", " poetry ", " poem ", " art talks back ")):
        return False
    if _contains_any(blob, (" wonder walks ", " gallery gathering ", " demonstration ", " demonstrations ", " workshop ", " class ", " lecture ", " talk ", " activity ", " family ")):
        return True
    if _contains_any(blob, (" artstream ",)):
        return True
    return False


def _should_keep_dia_event(*, title: str, description: str | None, event_type: str | None) -> bool:
    blob = _normalize_for_match(" ".join(part for part in (event_type or "", title, description or "") if part))
    if _contains_any(blob, (" detroit film theatre ", " film ", " movie ", " guided tour ", " tour ")):
        return False
    return _contains_any(blob, (" drawing in the galleries ", " drop in workshop ", " guest demo "))


def _should_keep_grand_rapids_event(*, title: str, description: str | None, category_text: str | None) -> bool:
    blob = _normalize_for_match(" ".join(part for part in (title, description or "", category_text or "") if part))
    if _contains_any(blob, (" membership exchange ", " gala ", " meditation ", " fundraising events ", " exhibition opening ", " wellness ", " yoga ", " vinyasa ")):
        return False
    if _contains_any(blob, (" adult workshop ", " artist talk ", " gallery chat ", " drop-in studio ", " creativity lab ", " art journaling ")):
        return True
    return _contains_any(blob, (" workshop ", " talk ", " lecture ", " class ", " chat ", " activity "))


def _should_keep_msu_broad_event(*, title: str, description: str | None, category_text: str | None) -> bool:
    blob = _normalize_for_match(" ".join(part for part in (title, description or "", category_text or "") if part))
    if _contains_any(blob, (" museum tour ", " screening ", " watch + talk ", " watch talk ", " wellness ", " welcome back ", " tour ")):
        return False
    if _contains_any(blob, (" family day ", " artist talks ", " start with art ", " chill out with art ", " take your child to work day ")):
        return True
    if _contains_any(blob, (" families ", " make ", " talk + listen ", " educator workshop ", " activity ", " workshop ", " talk ")):
        return True
    return False


def _should_keep_muskegon_event(*, title: str, description: str | None) -> bool:
    blob = _normalize_for_match(" ".join(part for part in (title, description or "") if part))
    if _contains_any(blob, (" storytime ", " poem ", " poetry ", " reception ", " competition ", " movie ", " film ", " tour ")):
        return False
    if _contains_any(blob, (" family night ", " workshop ", " lecture ", " talk ", " class ", " activity ", " art-filled activities ", " art making ", " artmaking ")):
        return True
    return False


def _should_keep_umma_event(*, title: str, description: str | None) -> bool:
    blob = _normalize_for_match(" ".join(part for part in (title, description or "") if part))
    if _contains_any(blob, (" film sampler ", " screening ", " film ", " movie ")):
        return False
    if _contains_any(blob, (" public talk ", " lecture ", " talk ", " conversation ", " discussion ", " demonstration ", " workshop ", " print demonstration ", " artist ")):
        return True
    if _contains_any(blob, (" an evening with ",)):
        return True
    return False


def _activity_type_for_text(*, title: str, description: str | None) -> str:
    blob = _normalize_for_match(" ".join(part for part in (title, description or "") if part))
    if _contains_any(blob, (" talk ", " talks ", " lecture ", " lectures ", " conversation ", " discussion ", " gallery chat ", " public talk ")):
        return "talk"
    if _contains_any(blob, (" workshop ", " workshops ", " class ", " classes ", " studio ", " drop-in ", " drop in ", " make ", " artmaking ", " demonstration ", " activity ")):
        return "workshop"
    return "activity"


def _iter_month_starts(*, count: int) -> list[date]:
    today = datetime.now(MI_ZONEINFO).date()
    first_of_month = today.replace(day=1)
    values: list[date] = []
    current = first_of_month
    for _ in range(max(count, 1)):
        values.append(current)
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        current = next_month
    return values


def _parse_human_start_end(date_text: str | None, time_text: str | None) -> tuple[datetime | None, datetime | None]:
    event_date = _parse_human_date(date_text)
    if event_date is None:
        return None, None

    normalized_time_text = _normalize_space(time_text)
    if not normalized_time_text or normalized_time_text.lower() == "all day":
        return datetime.combine(event_date, time.min), None

    match = TIME_RANGE_RE.search(normalized_time_text)
    if match:
        start_text = _normalize_space(match.group("start"))
        end_text = _normalize_space(match.group("end"))
        if not _contains_meridiem(start_text):
            end_meridiem_match = re.search(r"(am|pm)$", end_text.lower())
            if end_meridiem_match:
                start_text = f"{start_text} {end_meridiem_match.group(1)}"
        start_time = _parse_time_component(start_text)
        end_time = _parse_time_component(end_text)
        if start_time is None:
            return None, None
        start_at = datetime.combine(event_date, start_time)
        end_at = datetime.combine(event_date, end_time) if end_time is not None else None
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(normalized_time_text)
    if single_match:
        parsed_time = _parse_time_component(single_match.group("time"))
        if parsed_time is not None:
            return datetime.combine(event_date, parsed_time), None

    return datetime.combine(event_date, time.min), None


def _parse_human_date(value: str | None) -> date | None:
    text = _normalize_space(value)
    if not text:
        return None

    text = text.replace("\u2019", "'")
    if " — " in text:
        text = text.split(" — ", 1)[0].strip()
    elif " – " in text:
        text = text.split(" – ", 1)[0].strip()
    elif " - " in text:
        text = text.split(" - ", 1)[0].strip()

    formats = (
        "%A, %b %d, %Y",
        "%a, %b %d, %Y",
        "%A, %B %d, %Y",
        "%a, %B %d, %Y",
        "%A, %b %d %Y",
        "%a, %b %d %Y",
        "%A, %B %d %Y",
        "%a, %B %d %Y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%b %d %Y",
        "%B %d %Y",
    )
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _extract_dia_occurrences(card) -> list[tuple[datetime, datetime | None]]:
    tokens: list[str] = []
    additional_dates = card.select_one(".left-side .additional-dates")
    if additional_dates is not None:
        tokens = [_normalize_space(text) for text in additional_dates.stripped_strings if _normalize_space(text)]
        if tokens and tokens[0].lower() == "additional dates":
            tokens = tokens[1:]
    else:
        first_date = card.select_one(".left-side .first-date")
        if first_date is not None:
            tokens = [_normalize_space(text) for text in first_date.stripped_strings if _normalize_space(text)]

    normalized_tokens: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if index + 1 < len(tokens) and tokens[index + 1].startswith(","):
            token = f"{token}{tokens[index + 1]}"
            index += 1
        normalized_tokens.append(token.lstrip(", ").strip())
        index += 1

    occurrences: list[tuple[datetime, datetime | None]] = []
    index = 0
    while index + 1 < len(normalized_tokens):
        date_text = normalized_tokens[index]
        time_text = normalized_tokens[index + 1]
        start_at, end_at = _parse_dia_date_time(date_text, time_text)
        if start_at is not None:
            occurrences.append((start_at, end_at))
            index += 2
            continue
        index += 1

    return occurrences


def _parse_dia_date_time(date_text: str, time_text: str) -> tuple[datetime | None, datetime | None]:
    if not date_text or not time_text:
        return None, None

    today = datetime.now(MI_ZONEINFO).date()
    date_with_year = date_text if re.search(r"\b\d{4}\b", date_text) else f"{date_text}, {today.year}"
    start_at, end_at = _parse_human_start_end(date_with_year, time_text)
    if start_at is None:
        return None, None

    if start_at.date() < today - timedelta(days=180):
        future_date_text = re.sub(r",\s*\d{4}\b", f", {today.year + 1}", date_with_year)
        future_start_at, future_end_at = _parse_human_start_end(future_date_text, time_text)
        if future_start_at is not None:
            return future_start_at, future_end_at

    return start_at, end_at


def _parse_time_component(value: str | None) -> time | None:
    text = _normalize_space(value)
    if not text:
        return None
    text = text.lower().replace("a.m.", "am").replace("p.m.", "pm").replace("a.m", "am").replace("p.m", "pm")
    text = re.sub(r"(?<=\d)(am|pm)\b", r" \1", text)
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            return datetime.strptime(text.upper(), fmt).time()
        except ValueError:
            continue
    return None


def _parse_iso_or_local_datetime(value: str | None, *, timezone_name: str) -> datetime | None:
    text = _normalize_space(value)
    if not text:
        return None

    if "T" in text or re.search(r"[+-]\d{2}:\d{2}$", text):
        try:
            return parse_iso_datetime(text, timezone_name=timezone_name)
        except ValueError:
            pass

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            return datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _parse_age_range(description: str | None) -> tuple[int | None, int | None]:
    text = _normalize_space(description)
    if not text:
        return None, None

    range_match = AGE_RANGE_RE.search(text)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))

    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def _extract_first_decimal(values) -> Decimal | None:
    if isinstance(values, list):
        for value in values:
            try:
                return Decimal(str(value))
            except Exception:
                continue
    return None


def _extract_offer_amount(offers) -> Decimal | None:
    if isinstance(offers, dict):
        price = offers.get("price")
        if price is not None:
            try:
                return Decimal(str(price))
            except Exception:
                return None
    return None


def _extract_location_name(location) -> str | None:
    if isinstance(location, dict):
        return _normalize_space(location.get("name") or location.get("address"))
    return _normalize_space(str(location)) if location is not None else None


def _iter_event_objects(data) -> list[dict]:
    if isinstance(data, dict):
        event_type = data.get("@type")
        if event_type == "Event" or (isinstance(event_type, list) and "Event" in event_type):
            return [data]
        objects: list[dict] = []
        for value in data.values():
            objects.extend(_iter_event_objects(value))
        return objects
    if isinstance(data, list):
        objects: list[dict] = []
        for item in data:
            objects.extend(_iter_event_objects(item))
        return objects
    return []


def _safe_json_loads(value: str) -> dict | list | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _clean_html(value) -> str | None:
    if value is None:
        return None
    text = BeautifulSoup(str(value), "html.parser").get_text(" ", strip=True)
    return _normalize_space(text)


def _normalize_space(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def _normalize_for_match(value: str) -> str:
    normalized = MATCH_NORMALIZE_RE.sub(" ", _normalize_space(value).lower())
    return f" {' '.join(normalized.split())} "


def _contains_any(value: str, needles: tuple[str, ...]) -> bool:
    return any(needle in value for needle in needles)


def _contains_meridiem(value: str) -> bool:
    return bool(re.search(r"(?:am|pm|a\.?m\.?|p\.?m\.?)$", _normalize_space(value), re.IGNORECASE))


def _registration_required_for_text(*, title: str, description: str | None) -> bool:
    blob = _normalize_for_match(" ".join(part for part in (title, description or "") if part))
    if _contains_any(
        blob,
        (
            " registration is not required ",
            " no registration required ",
            " registration not required ",
            " no need to register ",
            " register is not required ",
        ),
    ):
        return False
    return _contains_any(
        blob,
        (
            " register ",
            " registration ",
            " preregistration ",
            " pre registration ",
            " reserve ",
            " rsvp ",
            " book now ",
            " ticket ",
            " tickets ",
        ),
    )


def _maybe_override_suspicious_schema_times(
    *,
    start_at: datetime,
    end_at: datetime | None,
    description: str | None,
) -> tuple[datetime, datetime | None]:
    if not description:
        return start_at, end_at

    if start_at.hour > 8:
        return start_at, end_at

    match = TIME_RANGE_RE.search(description)
    if match is None:
        return start_at, end_at

    override_start = _parse_time_component(match.group("start"))
    override_end = _parse_time_component(match.group("end"))
    if override_start is None:
        return start_at, end_at

    corrected_start = datetime.combine(start_at.date(), override_start)
    corrected_end = datetime.combine(start_at.date(), override_end) if override_end is not None else end_at
    return corrected_start, corrected_end


def get_mi_source_prefixes() -> tuple[str, ...]:
    return (
        "https://dia.org/events/",
        "https://www.artmuseumgr.org/events/",
        "https://www.kiarts.org/event/",
        "https://broadmuseum.msu.edu/events/",
        "https://muskegonartmuseum.org/event/",
        "https://umma.umich.edu/events/",
    )
