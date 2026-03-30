from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from datetime import time
from decimal import Decimal
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

AL_TIMEZONE = "America/Chicago"

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

BOT_CHALLENGE_MARKERS = (
    "Just a moment...",
    "__cf_chl_",
    "challenge-platform",
    "Enable JavaScript and cookies to continue",
)

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " alcohol ink collage workshop ",
    " adult class ",
    " art crawl ",
    " art ed central ",
    " art class ",
    " art classes ",
    " artbreak ",
    " art talk ",
    " arttalk ",
    " artist talk ",
    " artists in action ",
    " artsy tots ",
    " card making ",
    " classes ",
    " collage ",
    " create ",
    " creative torso ",
    " draw ",
    " drawing ",
    " family art ",
    " family festival ",
    " family studio ",
    " figurative sculpture ",
    " iconographic drawing ",
    " kids art classes ",
    " lab ",
    " lecture ",
    " lectures ",
    " make take ",
    " make your own ",
    " maker ",
    " makers ",
    " master artist workshop ",
    " mini makers ",
    " painting ",
    " papermaking ",
    " plein air workshop ",
    " pottery ",
    " presentation ",
    " presentations ",
    " science night ",
    " school of the arts ",
    " sketching ",
    " sit with me ",
    " studio ",
    " talk ",
    " teen wheel throwing ",
    " teens ",
    " text as texture ",
    " knitting ",
    " wheel throwing ",
    " workshop ",
    " workshops ",
    " world build ",
    " wreath class ",
    " youth ",
)

WEAK_INCLUDE_PATTERNS = (
    " art ",
    " artist ",
    " artists ",
    " create ",
    " creative ",
    " family ",
    " kid ",
    " kids ",
    " teen ",
    " youth ",
)

ALWAYS_REJECT_PATTERNS = (
    " admission ",
    " brunch ",
    " camp ",
    " camps ",
    " closed ",
    " closing reception ",
    " coming soon ",
    " concert ",
    " concerts ",
    " dance ",
    " dinner ",
    " film ",
    " films ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " members only opening reception ",
    " meditation ",
    " mindfulness ",
    " music ",
    " opening reception ",
    " performance ",
    " performances ",
    " private collection ",
    " production ",
    " productions ",
    " reading ",
    " reception ",
    " sold out ",
    " storytime ",
    " study day ",
    " support group events ",
    " theater ",
    " theatre ",
    " tour ",
    " tours ",
    " volunteer meeting ",
    " yoga ",
)

CONTEXTUAL_REJECT_PATTERNS = (
    " exhibition ",
    " exhibitions ",
    " closing day ",
    " holiday closures ",
    " museum closed ",
    " gallery open ",
    " open galleries ",
)

HARD_REJECT_PATTERNS = (
    " closing day ",
    " holiday closures ",
    " mindfulness ",
    " open galleries ",
    " smartguide ",
)

UNAVAILABLE_PATTERNS = (
    " cancelled ",
    " canceled ",
    " sold out ",
    " waitlist only ",
)

DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
)

REGISTRATION_PATTERNS = (
    " preregistration ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
    " ticket required ",
    " tickets required ",
)

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(?:-|–|—|to)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.IGNORECASE)
AEIVA_DATE_TIME_RE = re.compile(
    r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?\.?,?\s+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s+"
    r"(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+to\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm)))?",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class AlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    discovery_mode: str
    api_url: str | None = None
    calendar_url: str | None = None


AL_VENUES: tuple[AlVenueConfig, ...] = (
    AlVenueConfig(
        slug="aeiva",
        source_name="al_aeiva_events",
        venue_name="Abroms-Engel Institute for the Visual Arts",
        city="Birmingham",
        state="AL",
        list_url="https://www.uab.edu/aeiva/events",
        default_location="Abroms-Engel Institute for the Visual Arts, Birmingham, AL",
        discovery_mode="uab_calendar",
        calendar_url="https://calendar.uab.edu/arts",
    ),
    AlVenueConfig(
        slug="birmingham",
        source_name="al_birmingham_museum_of_art_events",
        venue_name="Birmingham Museum of Art",
        city="Birmingham",
        state="AL",
        list_url="https://www.artsbma.org/events/list/",
        default_location="Birmingham Museum of Art, Birmingham, AL",
        discovery_mode="tribe_html",
    ),
    AlVenueConfig(
        slug="huntsville",
        source_name="al_huntsville_museum_of_art_events",
        venue_name="Huntsville Museum of Art",
        city="Huntsville",
        state="AL",
        list_url="https://hsvmuseum.org/eventscalendar/",
        default_location="Huntsville Museum of Art, Huntsville, AL",
        discovery_mode="tribe_api",
        api_url="https://hsvmuseum.org/wp-json/tribe/events/v1/events",
    ),
    AlVenueConfig(
        slug="jcsm",
        source_name="al_jcsm_events",
        venue_name="Jule Collins Smith Museum of Fine Art",
        city="Auburn",
        state="AL",
        list_url="https://jcsm.auburn.edu/events/",
        default_location="Jule Collins Smith Museum of Fine Art, Auburn, AL",
        discovery_mode="tribe_api",
        api_url="https://jcsm.auburn.edu/wp-json/tribe/events/v1/events",
    ),
    AlVenueConfig(
        slug="mobile",
        source_name="al_mobile_museum_of_art_events",
        venue_name="Mobile Museum of Art",
        city="Mobile",
        state="AL",
        list_url="https://www.mobilemuseumofart.com/event/",
        default_location="Mobile Museum of Art, Mobile, AL",
        discovery_mode="tribe_api",
        api_url="https://www.mobilemuseumofart.com/wp-json/tribe/events/v1/events",
    ),
    AlVenueConfig(
        slug="mmfa",
        source_name="al_montgomery_museum_of_fine_arts_events",
        venue_name="Montgomery Museum of Fine Arts",
        city="Montgomery",
        state="AL",
        list_url="https://mmfa.org/events/calendar/",
        default_location="Montgomery Museum of Fine Arts, Montgomery, AL",
        discovery_mode="tribe_api",
        api_url="https://mmfa.org/wp-json/tribe/events/v1/events",
    ),
    AlVenueConfig(
        slug="paul_r_jones",
        source_name="al_paul_r_jones_events",
        venue_name="Paul R. Jones Museum",
        city="Tuscaloosa",
        state="AL",
        list_url="https://paulrjonescollection.ua.edu/category/events-exhibitions/",
        default_location="Paul R. Jones Museum, Tuscaloosa, AL",
        discovery_mode="tribe_api",
        api_url="https://paulrjonescollection.ua.edu/wp-json/tribe/events/v1/events",
    ),
    AlVenueConfig(
        slug="tennessee_valley",
        source_name="al_tennessee_valley_museum_of_art_events",
        venue_name="Tennessee Valley Museum of Art",
        city="Tuscumbia",
        state="AL",
        list_url="https://tennesseevalleyarts.org/events/",
        default_location="Tennessee Valley Museum of Art, Tuscumbia, AL",
        discovery_mode="tribe_api",
        api_url="https://tennesseevalleyarts.org/wp-json/tribe/events/v1/events",
    ),
    AlVenueConfig(
        slug="wiregrass",
        source_name="al_wiregrass_museum_of_art_events",
        venue_name="Wiregrass Museum of Art",
        city="Dothan",
        state="AL",
        list_url="https://www.wiregrassmuseum.org/calendar/",
        default_location="Wiregrass Museum of Art, Dothan, AL",
        discovery_mode="tribe_api",
        api_url="https://www.wiregrassmuseum.org/wp-json/tribe/events/v1/events",
    ),
)

AL_VENUES_BY_SLUG = {venue.slug: venue for venue in AL_VENUES}


class AlBundleAdapter(BaseSourceAdapter):
    source_name = "al_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_al_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_al_bundle_payload/parse_al_events from the script runner.")


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
            timezone_id=AL_TIMEZONE,
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
                try:
                    payload = response.json()
                except json.JSONDecodeError as exc:
                    snippet = _debug_snippet(response.text)
                    raise RuntimeError(
                        "AL tribe endpoint returned non-JSON content: "
                        f"url={response.url} status={response.status_code} snippet={snippet!r}"
                    ) from exc
                if isinstance(payload, dict):
                    return payload
                raise RuntimeError(
                    "AL tribe endpoint returned unexpected payload type: "
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
        raise RuntimeError("Unable to fetch AL tribe endpoint") from last_exception
    raise RuntimeError("Unable to fetch AL tribe endpoint after retries")


async def load_al_bundle_payload(
    *,
    venues: list[AlVenueConfig] | tuple[AlVenueConfig, ...] | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, dict]:
    selected = list(venues) if venues is not None else list(AL_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}
    today = datetime.now(ZoneInfo(AL_TIMEZONE)).date().isoformat()

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.discovery_mode == "tribe_api":
                    if venue.api_url is None:
                        raise RuntimeError("Venue is missing api_url")
                    next_url = _with_query(venue.api_url, per_page=per_page, page=1, start_date=today)
                    pages_seen = 0
                    events: list[dict] = []
                    while next_url:
                        if page_limit is not None and pages_seen >= max(page_limit, 1):
                            break
                        page_payload = await fetch_tribe_events_page(next_url, client=client)
                        pages_seen += 1
                        events.extend(page_payload.get("events") or [])
                        next_url = page_payload.get("next_rest_url")
                    payload_by_slug[venue.slug] = {"events": events}
                    continue

                if venue.discovery_mode == "tribe_html":
                    pages = await _load_birmingham_pages(venue.list_url, page_limit=page_limit)
                    payload_by_slug[venue.slug] = {"pages": pages}
                    continue

                if venue.discovery_mode == "uab_calendar":
                    calendar_url = venue.calendar_url or venue.list_url
                    html = await fetch_html(calendar_url, client=client)
                    payload_by_slug[venue.slug] = {"html": html}
                    continue

                raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[al-bundle] venue={venue.slug} failed: {exc}")

    return {"payload_by_slug": payload_by_slug, "errors_by_slug": errors_by_slug}


def parse_al_events(payload: dict, *, venue: AlVenueConfig) -> list[ExtractedActivity]:
    if venue.discovery_mode == "tribe_api":
        return _parse_tribe_events(payload.get("events") or [], venue=venue)
    if venue.discovery_mode == "tribe_html":
        return _parse_birmingham_events(payload.get("pages") or [], venue=venue)
    if venue.discovery_mode == "uab_calendar":
        return _parse_aeiva_events(payload.get("html") or "", venue=venue)
    raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")


def get_al_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in AL_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
        if venue.calendar_url:
            calendar_parsed = urlparse(venue.calendar_url)
            prefixes.append(f"{calendar_parsed.scheme}://{calendar_parsed.netloc}/")
    return tuple(prefixes)


def _parse_tribe_events(events: list[dict], *, venue: AlVenueConfig) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(AL_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in events:
        row = _build_tribe_row(event_obj, venue=venue)
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


def _build_tribe_row(event_obj: dict, *, venue: AlVenueConfig) -> ExtractedActivity | None:
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
    if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
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
        timezone=AL_TIMEZONE,
        **price_classification_kwargs_from_amount(amount, text=price_text or description),
    )


def _parse_birmingham_events(pages: list[str], *, venue: AlVenueConfig) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(AL_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for html in pages:
        soup = BeautifulSoup(html, "html.parser")
        for article in soup.select("article.tribe-events-calendar-list__event"):
            title_link = article.select_one(".tribe-events-calendar-list__event-title a[href]")
            if title_link is None:
                continue
            source_url = urljoin(venue.list_url, title_link.get("href") or "")
            title = _normalize_space(title_link.get_text(" ", strip=True))
            if not title or not source_url:
                continue

            time_el = article.select_one("time")
            start_at, end_at = _parse_birmingham_time(
                date_value=time_el.get("datetime") if time_el else None,
                time_text=_normalize_space(time_el.get_text(" ", strip=True) if time_el else ""),
            )
            if start_at is None or start_at.date() < current_date:
                continue

            description = _normalize_space(
                (article.select_one(".tribe-events-calendar-list__event-description") or article).get_text(" ", strip=True)
            )
            if description == title:
                description = None

            token_blob = _searchable_blob(" ".join([title, description or ""]))
            title_blob = _searchable_blob(title)
            if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
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
                    timezone=AL_TIMEZONE,
                    **price_classification_kwargs(description),
                )
            )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_aeiva_events(html: str, *, venue: AlVenueConfig) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(AL_TIMEZONE)).date()
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for heading in soup.find_all(["h3", "h4"]):
        link = heading.find("a", href=True)
        if link is None:
            continue
        source_url = urljoin(venue.calendar_url or venue.list_url, link.get("href") or "")
        if "/event/" not in source_url:
            continue

        title = _normalize_space(link.get_text(" ", strip=True))
        if not title:
            continue

        block = heading.parent
        if block is None:
            continue
        block_text = _normalize_space(block.get_text(" ", strip=True))
        if venue.venue_name not in block_text:
            continue

        parsed_times = _parse_aeiva_block_datetime(block_text)
        if parsed_times is None:
            continue
        start_at, end_at = parsed_times
        if start_at.date() < current_date:
            continue

        token_blob = _searchable_blob(" ".join([title, block_text]))
        title_blob = _searchable_blob(title)
        if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
            continue

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        age_min, age_max = _parse_age_range(" ".join([title, block_text]))

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=block_text,
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
                timezone=AL_TIMEZONE,
                **price_classification_kwargs(block_text),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


async def _load_birmingham_pages(list_url: str, *, page_limit: int | None = None) -> list[str]:
    pages: list[str] = []
    seen_urls: set[str] = set()
    next_url: str | None = list_url

    while next_url and next_url not in seen_urls:
        if page_limit is not None and len(pages) >= max(page_limit, 1):
            break
        seen_urls.add(next_url)
        html = await fetch_html_playwright(next_url)
        pages.append(html)
        next_url = _extract_next_page_url(html, current_url=next_url)

    return pages


def _extract_next_page_url(html: str, *, current_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for selector in (
        ".tribe-events-c-nav__next a[href]",
        "a.tribe-events-c-nav__next[href]",
        "a[rel='next'][href]",
    ):
        anchor = soup.select_one(selector)
        if anchor is not None:
            return urljoin(current_url, anchor.get("href") or "")
    return None


def _parse_birmingham_time(*, date_value: str | None, time_text: str) -> tuple[datetime | None, datetime | None]:
    if not date_value:
        return None, None
    try:
        event_date = datetime.strptime(date_value.strip(), "%Y-%m-%d").date()
    except ValueError:
        return None, None

    range_match = TIME_RANGE_RE.search(time_text)
    if range_match:
        start_time, end_time = _parse_time_range_match(range_match)
        if start_time is None:
            return None, None
        start_at = datetime.combine(event_date, start_time)
        end_at = datetime.combine(event_date, end_time) if end_time is not None else None
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(time_text)
    if single_match:
        start_time = _parse_clock(
            single_match.group(1),
            single_match.group(2),
            single_match.group(3),
        )
        return datetime.combine(event_date, start_time), None

    return datetime.combine(event_date, datetime.min.time()), None


def _parse_time_range_match(match: re.Match[str]) -> tuple[time | None, time | None]:
    start_period = (match.group(3) or match.group(6) or "").lower()
    end_period = (match.group(6) or "").lower()
    if not end_period:
        return None, None
    start_time = _parse_clock(match.group(1), match.group(2), start_period)
    end_time = _parse_clock(match.group(4), match.group(5), end_period)
    return start_time, end_time


def _parse_clock(hour_text: str, minute_text: str | None, period_text: str) -> time:
    hour = int(hour_text)
    minute = int(minute_text or "0")
    normalized_period = period_text.lower().replace(".", "")
    if normalized_period == "pm" and hour != 12:
        hour += 12
    if normalized_period == "am" and hour == 12:
        hour = 0
    return datetime.strptime(f"{hour:02d}:{minute:02d}", "%H:%M").time()


def _parse_aeiva_block_datetime(text: str) -> tuple[datetime, datetime | None] | None:
    match = AEIVA_DATE_TIME_RE.search(text)
    if match is None:
        return None
    event_date = _parse_month_day_year(match.group(1))
    if event_date is None:
        return None
    start_time = _parse_meridiem_time(match.group(2))
    end_time = _parse_meridiem_time(match.group(3)) if match.group(3) else None
    if start_time is None:
        return None
    start_at = datetime.combine(event_date, start_time)
    end_at = datetime.combine(event_date, end_time) if end_time is not None else None
    return start_at, end_at


def _parse_month_day_year(value: str) -> datetime.date | None:
    text = _normalize_space(value)
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_meridiem_time(value: str | None) -> time | None:
    if not value:
        return None
    text = _normalize_space(value).lower().replace(".", "")
    for fmt in ("%I:%M%p", "%I%p"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def _should_keep_event(*, title_blob: str, token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in UNAVAILABLE_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in HARD_REJECT_PATTERNS):
        return False

    strong_title_include = any(pattern in title_blob for pattern in STRONG_INCLUDE_PATTERNS)
    strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    if not strong_include:
        return False

    if any(pattern in title_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS) and not strong_include:
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_title_include:
        return False

    return True


def _infer_activity_type(token_blob: str) -> str:
    if any(
        pattern in token_blob
        for pattern in (
            " art talk ",
            " arttalk ",
            " artist talk ",
            " book talk ",
            " conversation ",
            " lecture ",
            " lectures ",
            " presentation ",
            " presentations ",
            " recognition day ",
            " science night ",
            " talk ",
            " talks ",
        )
    ):
        return "lecture"
    return "workshop"


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


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


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    without_shortcodes = re.sub(r"\[[^\]]+\]", " ", value)
    return _normalize_space(BeautifulSoup(without_shortcodes, "html.parser").get_text(" ", strip=True))


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _registration_required(token_blob: str) -> bool:
    if any(
        phrase in token_blob
        for phrase in (
            " no reservation necessary ",
            " no reservations necessary ",
            " registration is not required ",
            " registration not required ",
        )
    ):
        return False
    return any(pattern in token_blob for pattern in REGISTRATION_PATTERNS)


def _looks_like_bot_challenge(text: str) -> bool:
    return any(marker in text for marker in BOT_CHALLENGE_MARKERS)


def _debug_snippet(text: str, *, limit: int = 180) -> str:
    collapsed = " ".join(text.split())
    return collapsed[:limit]


def _with_query(url: str, **params: object) -> str:
    query_parts = []
    for key, value in params.items():
        if value is None:
            continue
        query_parts.append(f"{key}={value}")
    delimiter = "&" if "?" in url else "?"
    return f"{url}{delimiter}{'&'.join(query_parts)}"
