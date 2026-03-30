from __future__ import annotations

import asyncio
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal
from urllib.parse import urlencode
from urllib.parse import urljoin
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

CO_TIMEZONE = "America/Denver"

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

PLAYWRIGHT_LAUNCH_ARGS = (
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
)

BOT_CHALLENGE_MARKERS = (
    "just a moment",
    "__cf_chl_",
    "challenge-platform",
    "enable javascript and cookies to continue",
)

UNAVAILABLE_PATTERNS = (
    " cancelled ",
    " canceled ",
    " no session ",
    " sold out ",
    " waitlist only ",
)

GENERAL_REJECT_PATTERNS = (
    " admission ",
    " concert ",
    " concerts ",
    " exhibition ",
    " exhibitions ",
    " gala ",
    " market ",
    " meditation ",
    " mindfulness ",
    " music ",
    " opening reception ",
    " performance ",
    " performances ",
    " pop up shop ",
    " pop-up shop ",
    " reception ",
    " salsa ",
    " shopping ",
    " tour ",
    " tours ",
    " yoga ",
)

GENERAL_LECTURE_PATTERNS = (
    " conversation ",
    " discussion ",
    " gallery talk ",
    " lecture ",
    " lectures ",
    " one painting at a time ",
    " panel ",
    " presentation ",
    " presentations ",
    " public talk ",
    " talk ",
    " talks ",
)

DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
)

REGISTRATION_PATTERNS = (
    " preregister ",
    " pre-register ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
    " tickets required ",
    " ticket required ",
)

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
MONTH_DAY_YEAR_RE = re.compile(
    r"(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?\.?,?\s+)?"
    r"([A-Za-z]{3,9}\s+\d{1,2}(?:st|nd|rd|th)?(?:,\s*\d{4})?)",
    re.IGNORECASE,
)
GREGORY_DATETIME_RE = re.compile(
    r"Date\(s\)\s*-\s*(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\s+"
    r"(?P<times>\d{1,2}(?::\d{2})?\s*[ap]m(?:\s*(?:-|–|—|to)\s*\d{1,2}(?::\d{2})?\s*[ap]m)?)",
    re.IGNORECASE,
)
CU_PRIMARY_DATETIME_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"(?P<date>[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4})\s+from\s+(?P<times>[^|]+?)(?:\s+at\s+|\s*$)",
    re.IGNORECASE,
)
CU_NEXT_DATE_RE = re.compile(
    r"Next Date:\s*(?P<date>[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?),?\s*from\s*(?P<times>[^|]+)",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?)\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class CoVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    discovery_mode: str
    api_url: str | None = None


CO_VENUES: tuple[CoVenueConfig, ...] = (
    CoVenueConfig(
        slug="aspen",
        source_name="co_aspen_art_museum_events",
        venue_name="Aspen Art Museum",
        city="Aspen",
        state="CO",
        list_url="https://aspenartmuseum.org/calendar/",
        default_location="Aspen Art Museum, Aspen, CO",
        discovery_mode="aspen_html",
    ),
    CoVenueConfig(
        slug="bmoca",
        source_name="co_bmoca_events",
        venue_name="Boulder Museum of Contemporary Art",
        city="Boulder",
        state="CO",
        list_url="https://www.bmoca.org/events/",
        default_location="Boulder Museum of Contemporary Art, Boulder, CO",
        discovery_mode="bmoca_html",
    ),
    CoVenueConfig(
        slug="clyfford_still",
        source_name="co_clyfford_still_museum_events",
        venue_name="Clyfford Still Museum",
        city="Denver",
        state="CO",
        list_url="https://clyffordstillmuseum.org/events/",
        default_location="Clyfford Still Museum, Denver, CO",
        discovery_mode="clyfford_html",
    ),
    CoVenueConfig(
        slug="fac",
        source_name="co_fine_arts_center_events",
        venue_name="Colorado Springs Fine Arts Center",
        city="Colorado Springs",
        state="CO",
        list_url="https://fac.coloradocollege.edu/events/",
        default_location="Colorado Springs Fine Arts Center, Colorado Springs, CO",
        discovery_mode="tribe_api",
        api_url="https://fac.coloradocollege.edu/wp-json/tribe/events/v1/events",
    ),
    CoVenueConfig(
        slug="cu_art_museum",
        source_name="co_cu_art_museum_events",
        venue_name="CU Art Museum",
        city="Boulder",
        state="CO",
        list_url="https://www.colorado.edu/cuartmuseum/programs-virtual-activities",
        default_location="CU Art Museum, Boulder, CO",
        discovery_mode="cu_html",
    ),
    CoVenueConfig(
        slug="gregory_allicar",
        source_name="co_gregory_allicar_events",
        venue_name="Gregory Allicar Museum of Art",
        city="Fort Collins",
        state="CO",
        list_url="https://artmuseum.colostate.edu/events/",
        default_location="Gregory Allicar Museum of Art, Fort Collins, CO",
        discovery_mode="gregory_html",
    ),
    CoVenueConfig(
        slug="museo",
        source_name="co_museo_de_las_americas_events",
        venue_name="Museo de las Americas",
        city="Denver",
        state="CO",
        list_url="https://museo.org/museo-calendar/",
        default_location="Museo de las Americas, Denver, CO",
        discovery_mode="tribe_api",
        api_url="https://museo.org/wp-json/tribe/events/v1/events",
    ),
)

CO_VENUES_BY_SLUG = {venue.slug: venue for venue in CO_VENUES}


class CoBundleAdapter(BaseSourceAdapter):
    source_name = "co_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_co_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_co_bundle_payload/parse_co_events from the script runner.")


async def fetch_html(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    use_playwright_fallback: bool = False,
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
                text = response.text
                if use_playwright_fallback and _looks_like_bot_challenge(text):
                    return await fetch_html_playwright(url)
                return text

            if use_playwright_fallback and response.status_code in (403, 429):
                return await fetch_html_playwright(url)

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if use_playwright_fallback and async_playwright is not None:
        return await fetch_html_playwright(url)

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
            timezone_id=CO_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def fetch_tribe_events_page(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> dict:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=JSON_HEADERS)

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
                try:
                    payload = response.json()
                except json.JSONDecodeError as exc:
                    snippet = _normalize_space(response.text[:400])
                    raise RuntimeError(
                        "CO tribe endpoint returned non-JSON content: "
                        f"url={response.url} status={response.status_code} snippet={snippet!r}"
                    ) from exc
                if isinstance(payload, dict):
                    return payload
                raise RuntimeError(
                    "CO tribe endpoint returned unexpected payload type: "
                    f"{type(payload).__name__}"
                )

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch CO tribe endpoint") from last_exception
    raise RuntimeError("Unable to fetch CO tribe endpoint after retries")


async def load_co_bundle_payload(
    *,
    venues: list[CoVenueConfig] | tuple[CoVenueConfig, ...] | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, dict]:
    selected = list(venues) if venues is not None else list(CO_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}
    today = datetime.now(ZoneInfo(CO_TIMEZONE)).date().isoformat()

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.discovery_mode == "tribe_api":
                    if venue.api_url is None:
                        raise RuntimeError("Venue is missing api_url")
                    events: list[dict] = []
                    next_url = _with_query(venue.api_url, per_page=per_page, page=1, start_date=today)
                    pages_seen = 0
                    async with httpx.AsyncClient(
                        timeout=30.0,
                        follow_redirects=True,
                        headers=JSON_HEADERS,
                    ) as json_client:
                        while next_url:
                            if page_limit is not None and pages_seen >= max(page_limit, 1):
                                break
                            page_payload = await fetch_tribe_events_page(next_url, client=json_client)
                            pages_seen += 1
                            events.extend(page_payload.get("events") or [])
                            next_url = page_payload.get("next_rest_url")
                    payload_by_slug[venue.slug] = {"events": events}
                    continue

                if venue.discovery_mode == "aspen_html":
                    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
                    payload_by_slug[venue.slug] = {"html": html}
                    continue

                if venue.discovery_mode == "cu_html":
                    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
                    payload_by_slug[venue.slug] = {"html": html}
                    continue

                if venue.discovery_mode == "gregory_html":
                    list_html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
                    detail_items = _extract_gregory_listing_items(list_html, venue.list_url)
                    for item in detail_items:
                        item["html"] = await fetch_html(item["url"], client=client, use_playwright_fallback=True)
                    payload_by_slug[venue.slug] = {"detail_items": detail_items}
                    continue

                if venue.discovery_mode == "clyfford_html":
                    list_html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
                    detail_items = _extract_clyfford_listing_items(list_html, venue.list_url)
                    for item in detail_items:
                        item["html"] = await fetch_html(item["url"], client=client, use_playwright_fallback=True)
                    payload_by_slug[venue.slug] = {"detail_items": detail_items}
                    continue

                if venue.discovery_mode == "bmoca_html":
                    list_html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
                    detail_items = _extract_bmoca_listing_items(list_html, venue.list_url)
                    for item in detail_items:
                        item["html"] = await fetch_html(item["url"], client=client, use_playwright_fallback=True)
                    payload_by_slug[venue.slug] = {"detail_items": detail_items}
                    continue

                raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[co-bundle] venue={venue.slug} failed: {exc}")

    return {"payload_by_slug": payload_by_slug, "errors_by_slug": errors_by_slug}


def parse_co_events(payload: dict, *, venue: CoVenueConfig) -> list[ExtractedActivity]:
    if venue.discovery_mode == "tribe_api":
        return _parse_tribe_events(payload.get("events") or [], venue=venue)
    if venue.discovery_mode == "aspen_html":
        return _parse_aspen_events(payload.get("html") or "", venue=venue)
    if venue.discovery_mode == "cu_html":
        return _parse_cu_events(payload.get("html") or "", venue=venue)
    if venue.discovery_mode == "gregory_html":
        return _parse_gregory_events(payload.get("detail_items") or [], venue=venue)
    if venue.discovery_mode == "clyfford_html":
        return _parse_clyfford_events(payload.get("detail_items") or [], venue=venue)
    if venue.discovery_mode == "bmoca_html":
        return _parse_bmoca_events(payload.get("detail_items") or [], venue=venue)
    raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")


def get_co_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in CO_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
        if venue.api_url:
            parsed_api = urlparse(venue.api_url)
            prefixes.append(f"{parsed_api.scheme}://{parsed_api.netloc}/")
    return tuple(sorted(set(prefixes)))


def _parse_tribe_events(events: list[dict], *, venue: CoVenueConfig) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(CO_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in events:
        row = _build_tribe_row(event_obj, venue=venue)
        if row is None or row.start_at.date() < current_date:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _build_tribe_row(event_obj: dict, *, venue: CoVenueConfig) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("end_date"))
    description_text = _html_to_text(event_obj.get("description"))
    excerpt_text = _html_to_text(event_obj.get("excerpt"))
    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    price_text = _normalize_space(event_obj.get("cost") or "")
    location_text = _extract_location_name(event_obj.get("venue")) or venue.default_location

    description_parts = [part for part in [excerpt_text, description_text] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = " | ".join(description_parts) if description_parts else None

    title_blob = _searchable_blob(" ".join([title, " ".join(category_names)]))
    token_blob = _searchable_blob(" ".join([title, description or "", " ".join(category_names)]))
    if venue.slug == "fac":
        if not _keep_fac_event(title_blob=title_blob, token_blob=token_blob):
            return None
    elif venue.slug == "museo":
        if not _keep_museo_event(title_blob=title_blob, token_blob=token_blob):
            return None
    else:
        return None

    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))
    amount = _extract_amount(event_obj.get("cost_details"))

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(token_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=_registration_required(token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=CO_TIMEZONE,
        **price_classification_kwargs_from_amount(amount, text=price_text or description),
    )


def _parse_aspen_events(html: str, *, venue: CoVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = datetime.now(ZoneInfo(CO_TIMEZONE)).date()

    for block in soup.select(".default-post-item-alt"):
        title_el = block.select_one(".title-value")
        info_el = block.select_one(".general-info .leading-tight")
        if title_el is None or info_el is None:
            continue

        title = _normalize_space(title_el.get_text(" ", strip=True))
        if not title:
            continue

        info_lines = [
            _normalize_space(part)
            for part in info_el.get_text("|", strip=True).split("|")
            if _normalize_space(part)
        ]
        if not info_lines:
            continue
        date_text = _normalize_space(info_lines[0])
        time_text = _normalize_space(info_lines[1]) if len(info_lines) > 1 else ""
        start_at, end_at = _parse_date_and_times(date_text, time_text)
        if start_at is None or start_at.date() < current_date:
            continue

        hidden_text = _normalize_space(
            " ".join(el.get_text(" ", strip=True) for el in block.select(".hidden-content .rich-text"))
        )
        if block.name == "a" and block.get("href"):
            source_url = urljoin(venue.list_url, block.get("href") or "")
        else:
            source_anchor = block.select_one(".hidden-content a[href]") or block.select_one("a[href]")
            source_url = urljoin(venue.list_url, source_anchor.get("href") or "") if source_anchor else venue.list_url
        description = hidden_text or None

        token_blob = _searchable_blob(" ".join([title, description or ""]))
        if not _keep_aspen_event(title_blob=_searchable_blob(title), token_blob=token_blob):
            continue

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type="workshop",
                age_min=age_min,
                age_max=age_max,
                drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
                registration_required=_registration_required(token_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=CO_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_clyfford_events(detail_items: list[dict], *, venue: CoVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = datetime.now(ZoneInfo(CO_TIMEZONE)).date()

    for item in detail_items:
        title = _normalize_space(item.get("title"))
        source_url = _normalize_space(item.get("url"))
        date_text = _normalize_space(item.get("date_text"))
        if not title or not source_url or not date_text:
            continue

        parsed = _parse_clyfford_datetime(date_text)
        if parsed is None:
            continue
        start_at, end_at = parsed
        if start_at.date() < current_date:
            continue

        detail_text = _extract_main_text(item.get("html") or "")
        description = detail_text or None
        token_blob = _searchable_blob(" ".join([title, description or ""]))
        if not _keep_clyfford_event(title_blob=_searchable_blob(title), token_blob=token_blob):
            continue

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(token_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
                registration_required=_registration_required(token_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=CO_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_cu_events(html: str, *, venue: CoVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = datetime.now(ZoneInfo(CO_TIMEZONE)).date()

    for card in soup.select(".grid-card"):
        title_el = card.select_one(".grid-text-container strong.h3")
        link_el = card.select_one(".grid-text-container a[href]")
        if title_el is None or link_el is None:
            continue

        title = _normalize_space(title_el.get_text(" ", strip=True))
        source_url = urljoin(venue.list_url, link_el.get("href") or "")
        body_containers = card.select(".grid-text-container")
        description = None
        if len(body_containers) > 1:
            description = _normalize_space(body_containers[1].get_text(" ", strip=True)) or None
        token_blob = _searchable_blob(" ".join([title, description or ""]))
        if not _keep_cu_event(title_blob=_searchable_blob(title), token_blob=token_blob):
            continue

        parsed = _parse_cu_datetime(title, description or "")
        if parsed is None:
            continue
        start_at, end_at = parsed
        if start_at.date() < current_date:
            continue

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(token_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
                registration_required=_registration_required(token_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=CO_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_gregory_events(detail_items: list[dict], *, venue: CoVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = datetime.now(ZoneInfo(CO_TIMEZONE)).date()

    for item in detail_items:
        title = _normalize_space(item.get("title"))
        source_url = _normalize_space(item.get("url"))
        html = item.get("html") or ""
        if not title or not source_url or not html:
            continue

        detail_text = _extract_main_text(html)
        parsed = _parse_gregory_datetime(detail_text)
        if parsed is None:
            continue
        start_at, end_at = parsed
        if start_at.date() < current_date:
            continue

        description = detail_text or None
        token_blob = _searchable_blob(" ".join([title, description or ""]))
        if not _keep_gregory_event(title_blob=_searchable_blob(title), token_blob=token_blob):
            continue

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(token_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
                registration_required=_registration_required(token_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=CO_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_bmoca_events(detail_items: list[dict], *, venue: CoVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = datetime.now(ZoneInfo(CO_TIMEZONE)).date()

    for item in detail_items:
        title = _normalize_space(item.get("title"))
        source_url = _normalize_space(item.get("url"))
        date_text = _normalize_space(item.get("date_text"))
        time_text = _normalize_space(item.get("time_text"))
        if not title or not source_url or not date_text:
            continue

        start_at, end_at = _parse_date_and_times(date_text, time_text)
        if start_at is None or start_at.date() < current_date:
            continue

        detail_text = _extract_meta_description(item.get("html") or "") or _extract_main_text(item.get("html") or "")
        excerpt = _normalize_space(item.get("excerpt"))
        description = " | ".join(part for part in [excerpt, detail_text] if part) or None
        token_blob = _searchable_blob(" ".join([title, description or ""]))
        if not _keep_bmoca_event(title_blob=_searchable_blob(title), token_blob=token_blob):
            continue

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(token_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
                registration_required=_registration_required(token_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=CO_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _extract_gregory_listing_items(html: str, list_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str]] = []
    for li in soup.select("li.event-item"):
        link = li.select_one(".event-title a[href]")
        if link is None:
            continue
        items.append(
            {
                "title": _normalize_space(link.get_text(" ", strip=True)),
                "url": urljoin(list_url, link.get("href") or ""),
            }
        )
    return items


def _extract_clyfford_listing_items(html: str, list_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str]] = []
    for box in soup.select(".recent-news-box"):
        link = box.select_one("h4 a[href]")
        date_el = box.select_one("small")
        if link is None or date_el is None:
            continue
        items.append(
            {
                "title": _normalize_space(link.get_text(" ", strip=True)),
                "url": urljoin(list_url, link.get("href") or ""),
                "date_text": _normalize_space(date_el.get_text(" ", strip=True)),
            }
        )
    return items


def _extract_bmoca_listing_items(html: str, list_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str]] = []
    for article in soup.select("article.eventlist-event--upcoming"):
        title_link = article.select_one(".eventlist-title-link[href]")
        date_el = article.select_one("li.eventlist-meta-date time.event-date[datetime]")
        if title_link is None or date_el is None:
            continue
        start_time_el = article.select_one("time.event-time-localized-start") or article.select_one("time.event-time-localized")
        end_time_el = article.select_one("time.event-time-localized-end")
        excerpt = _normalize_space(
            (article.select_one(".eventlist-excerpt") or article).get_text(" ", strip=True)
        )
        time_parts = [
            _normalize_space(start_time_el.get_text(" ", strip=True)) if start_time_el is not None else "",
            _normalize_space(end_time_el.get_text(" ", strip=True)) if end_time_el is not None else "",
        ]
        time_text = " - ".join(part for part in time_parts if part)
        items.append(
            {
                "title": _normalize_space(title_link.get_text(" ", strip=True)),
                "url": urljoin(list_url, title_link.get("href") or ""),
                "date_text": _normalize_space(date_el.get_text(" ", strip=True)),
                "time_text": time_text,
                "excerpt": excerpt,
            }
        )
    return items


def _keep_aspen_event(*, title_blob: str, token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in UNAVAILABLE_PATTERNS):
        return False
    return any(
        pattern in title_blob
        for pattern in (
            " active art ",
            " art inc ",
            " drop in art school ",
            " drop-in art school ",
            " family workshop ",
            " story art ",
        )
    )


def _keep_clyfford_event(*, title_blob: str, token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in UNAVAILABLE_PATTERNS):
        return False
    if any(pattern in title_blob for pattern in (" pay what you wish ", " free day ", " stillness ", " music in the galleries ")):
        return False
    if " beyond the canvas tour " in title_blob or " still uncovered tour " in title_blob or " last look tour " in title_blob:
        return False
    return any(
        pattern in title_blob
        for pattern in (
            " art crawl ",
            " dia del nino ",
            " member art making ",
            " member art-making ",
            " one painting at a time ",
        )
    )


def _keep_cu_event(*, title_blob: str, token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in (" meditation ", " chamber music ", " performance ")):
        return False
    return any(
        pattern in title_blob
        for pattern in (
            " fairy tale dungeons dragons ",
            " fairy tale dungeons d dragons ",
            " second saturdays ",
        )
    )


def _keep_gregory_event(*, title_blob: str, token_blob: str) -> bool:
    if " family day " in title_blob:
        return True
    if any(pattern in token_blob for pattern in GENERAL_REJECT_PATTERNS) and " gallery talk " not in token_blob:
        return False
    if " gallery talk " in token_blob and " art " in token_blob:
        return True
    return False


def _keep_bmoca_event(*, title_blob: str, token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in UNAVAILABLE_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in (" art camp ", " camps ", " fundraiser ", " fundraising party ")):
        return False
    if any(pattern in token_blob for pattern in GENERAL_REJECT_PATTERNS):
        return any(
            pattern in token_blob
            for pattern in (
                " creative coding ",
                " dia del nino ",
                " diy ",
                " drawing club ",
                " eco collage ",
                " eco-collage ",
                " family day ",
                " spark ",
                " workshop ",
                " murals ",
            )
        )
    return any(
        pattern in title_blob
        for pattern in (
            " art game lab ",
            " creative coding ",
            " dia del nino ",
            " diy ",
            " drawing club ",
            " eco collage ",
            " eco-collage ",
            " free family day ",
            " murals ",
            " spark ",
            " workshop ",
        )
    )


def _keep_fac_event(*, title_blob: str, token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in UNAVAILABLE_PATTERNS):
        return False
    if any(
        pattern in token_blob
        for pattern in (
            " gala ",
            " jagged little pill ",
            " theatre ",
            " theater ",
            " yoga ",
        )
    ):
        return False
    return any(
        pattern in token_blob
        for pattern in (
            " art school ",
            " backpacks bites ",
            " backpacks bites ",
            " beading ",
            " bemis community printshop ",
            " craft lab ",
            " creative outing ",
            " demo ",
            " demos ",
            " get hands on ",
            " get hands-on ",
            " hands on ",
            " hands-on ",
            " lecture ",
            " lectures ",
            " member moments ",
            " printshop ",
            " workshop ",
            " workshops ",
        )
    )


def _keep_museo_event(*, title_blob: str, token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in UNAVAILABLE_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in (" gala ", " concert ", " concerts ", " dinner ", " auction ")):
        return False
    if any(pattern in title_blob for pattern in (" conversation club ",)):
        return False
    if " celebration " in title_blob and not any(
        pattern in token_blob for pattern in (" art making ", " interactive workshop ", " workshop ", " collective art ")
    ):
        return False
    return any(
        pattern in token_blob
        for pattern in (
            " art making ",
            " collective art ",
            " community art ",
            " creative ",
            " hands on ",
            " hands-on ",
            " interactive workshop ",
            " workshop ",
            " workshops ",
            " youth ",
        )
    )


def _infer_activity_type(token_blob: str) -> str:
    if any(pattern in token_blob for pattern in GENERAL_LECTURE_PATTERNS):
        return "lecture"
    return "workshop"


def _registration_required(token_blob: str) -> bool | None:
    if " no registration required " in token_blob or " no registration " in token_blob:
        return False
    if any(pattern in token_blob for pattern in REGISTRATION_PATTERNS):
        return True
    return None


def _parse_clyfford_datetime(value: str) -> tuple[datetime, datetime | None] | None:
    text = _normalize_space(value)
    match = re.match(
        r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s*(?P<date>[A-Za-z]{3}\s+\d{2},\s+\d{4})\s+(?P<times>.+?)(?:\s+[A-Z]{2,4})?$",
        text,
    )
    if match is None:
        return None
    return _parse_date_and_times(match.group("date"), match.group("times"))


def _parse_gregory_datetime(text: str) -> tuple[datetime, datetime | None] | None:
    match = GREGORY_DATETIME_RE.search(text)
    if match is None:
        return None
    return _parse_date_and_times(match.group("date"), match.group("times"))


def _parse_cu_datetime(title: str, description: str) -> tuple[datetime, datetime | None] | None:
    combined = " ".join([title, description])
    match = CU_PRIMARY_DATETIME_RE.search(combined)
    if match is not None:
        return _parse_date_and_times(match.group("date"), match.group("times"))
    next_date_match = CU_NEXT_DATE_RE.search(combined)
    if next_date_match is not None:
        return _parse_date_and_times(
            f"{next_date_match.group('date')}, {datetime.now(ZoneInfo(CO_TIMEZONE)).year}",
            next_date_match.group("times"),
        )
    return None


def _parse_date_and_times(date_text: str, time_text: str) -> tuple[datetime | None, datetime | None]:
    event_date = _parse_month_day_year(date_text)
    if event_date is None:
        return None, None
    normalized_time_text = _normalize_space(time_text.replace("MDT", "").replace("MST", ""))
    if not normalized_time_text:
        return datetime.combine(event_date, datetime.min.time()), None

    range_parts = re.split(r"\s*(?:-|–|—|to)\s*", normalized_time_text, maxsplit=1)
    if len(range_parts) == 2:
        start_value, end_value = range_parts
        end_period = _extract_period(end_value)
        start_time = _parse_meridiem_time_text(start_value, default_period=end_period)
        end_time = _parse_meridiem_time_text(end_value)
        if start_time is None:
            return None, None
        start_at = datetime.combine(event_date, start_time)
        end_at = datetime.combine(event_date, end_time) if end_time is not None else None
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(normalized_time_text)
    if single_match is not None:
        start_time = _parse_meridiem_time_text(single_match.group("time"))
        if start_time is not None:
            return datetime.combine(event_date, start_time), None

    return datetime.combine(event_date, datetime.min.time()), None


def _parse_meridiem_time_text(value: str | None, *, default_period: str | None = None) -> time | None:
    if not value:
        return None
    text = _normalize_space(value).lower().replace(".", "").replace(" ", "")
    if text.endswith("am") or text.endswith("pm"):
        pass
    elif default_period:
        text = f"{text}{default_period.lower().replace('.', '')}"
    else:
        return None

    for fmt in ("%I:%M%p", "%I%p"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def _extract_period(value: str) -> str | None:
    lowered = value.lower().replace(".", "")
    if lowered.endswith("am"):
        return "am"
    if lowered.endswith("pm"):
        return "pm"
    return None


def _parse_month_day_year(value: str) -> date | None:
    text = _normalize_space(value)
    text = re.sub(
        r"^(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)\.?,?\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", text, flags=re.IGNORECASE)
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


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

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None)
    except ValueError:
        return None


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
    name = _normalize_space(venue_obj.get("venue"))
    city = _normalize_space(venue_obj.get("city"))
    state = _normalize_space(venue_obj.get("state") or venue_obj.get("province"))
    parts = [part for part in [name, city, state] if part]
    return ", ".join(parts) if parts else None


def _extract_main_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for selector in ("main", "article", ".entry-content", ".eventitem-column-content", ".sqs-block-content"):
        node = soup.select_one(selector)
        if node is not None:
            text = _normalize_space(node.get_text(" ", strip=True))
            if text:
                return text
    return _normalize_space(soup.get_text(" ", strip=True))


def _extract_meta_description(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for selector in ("meta[property='og:description']", "meta[name='description']"):
        node = soup.select_one(selector)
        if node is not None:
            content = _normalize_space(node.get("content"))
            if content:
                return content
    return ""


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    without_shortcodes = re.sub(r"\[[^\]]+\]", " ", value)
    return _normalize_space(BeautifulSoup(without_shortcodes, "html.parser").get_text(" ", strip=True))


def _searchable_blob(value: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-z0-9]+", " ", ascii_text.lower())
    return f" {' '.join(normalized.split())} "


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _looks_like_bot_challenge(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in BOT_CHALLENGE_MARKERS)


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _with_query(url: str, **params: object) -> str:
    query = urlencode({key: value for key, value in params.items() if value is not None})
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{query}" if query else url
