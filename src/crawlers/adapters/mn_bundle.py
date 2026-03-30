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
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

MN_TIMEZONE = "America/Chicago"

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

MMAA_EVENTS_API_URL = "https://mmaa.org/wp-json/tribe/events/v1/events"
ROURKE_CALENDAR_URL = "https://tockify.com/rourkeart?view=agendaMonth"

INCLUDE_PATTERNS = (
    " activity ",
    " activities ",
    " artist talk ",
    " class ",
    " classes ",
    " critique ",
    " curator talk ",
    " demo ",
    " demonstration ",
    " discussion ",
    " educator workshop ",
    " family day ",
    " hands-on ",
    " lab ",
    " lecture ",
    " live printmaking ",
    " open studio ",
    " panel ",
    " printmaking ",
    " roundtable ",
    " sketching ",
    " speaker ",
    " studio session ",
    " studio sessions ",
    " talk ",
    " toddler ",
    " webinar ",
    " workshop ",
    " workshops ",
)
HARD_REJECT_PATTERNS = (
    " admission ",
    " band ",
    " book launch ",
    " camp ",
    " camps ",
    " closing ",
    " concert ",
    " dinner ",
    " dining ",
    " exhibition ",
    " exhibitions ",
    " field trip ",
    " free night ",
    " fundraiser ",
    " fundraising ",
    " meditation ",
    " member days ",
    " member preview ",
    " mindfulness ",
    " music ",
    " open house ",
    " opening ",
    " openings ",
    " orchestra ",
    " performance ",
    " performances ",
    " poem ",
    " poems ",
    " poetry ",
    " preview ",
    " reading ",
    " reception ",
    " screening ",
    " storytime ",
    " story time ",
    " tai chi ",
    " tour ",
    " tours ",
    " wine ",
    " yoga ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
)
REGISTRATION_PATTERNS = (
    " link to register ",
    " pre-register ",
    " purchase tickets ",
    " register ",
    " registration ",
    " rsvp ",
    " reserve ",
    " ticket ",
    " tickets ",
    " tuition ",
)
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
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_RANGE_ALT_RE = re.compile(r"\bage\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
DATE_WITH_YEAR_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\.?\s+(?P<day>\d{1,2})(?:,)?\s+(?P<year>\d{4})",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*[ap]\.?m\.?)\s*(?:-|–|—|to)\s*(?P<end>\d{1,2}(?::\d{2})?\s*[ap]\.?m\.?)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*([ap]\.?m\.?)\b", re.IGNORECASE)
ARTSMIA_LISTING_RE = re.compile(
    r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2}),\s+(?P<year>\d{4})\s+"
    r"(?:from\s+)?(?P<start>\d{1,2}:\d{2}\s*[ap]m)\s*(?:to|-)\s*(?P<end>\d{1,2}:\d{2}\s*[ap]m)",
    re.IGNORECASE,
)
ARTSMIA_BODY_DATETIME_RE = re.compile(
    r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"([A-Za-z]{3,9})\s+(\d{1,2}),\s+(\d{4})\s+"
    r"(\d{1,2}:\d{2}\s*[ap]m)\s+to\s+(\d{1,2}:\d{2}\s*[ap]m)",
    re.IGNORECASE,
)
WAM_RANGE_WITH_END_DATE_RE = re.compile(
    r"(?P<start_month>[A-Za-z]{3,9})\s+(?P<start_day>\d{1,2})\s+(?P<start_year>\d{4})\s*\|\s*"
    r"(?P<start_time>\d{1,2}(?::\d{2})?\s*[ap]m)\s*-\s*"
    r"(?P<end_month>[A-Za-z]{3,9})\s+(?P<end_day>\d{1,2})\s+(?P<end_year>\d{4})\s*\|\s*"
    r"(?P<end_time>\d{1,2}(?::\d{2})?\s*[ap]m)",
    re.IGNORECASE,
)
WAM_SINGLE_DAY_RANGE_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2})\s+(?P<year>\d{4})\s*\|\s*"
    r"(?P<start_time>\d{1,2}(?::\d{2})?(?:\s*[ap]m)?)\s*-\s*(?P<end_time>\d{1,2}(?::\d{2})?\s*[ap]m)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class MnVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    source_prefixes: tuple[str, ...]


MN_VENUES: tuple[MnVenueConfig, ...] = (
    MnVenueConfig(
        slug="artsmia",
        source_name="mn_artsmia_events",
        venue_name="Minneapolis Institute of Art",
        city="Minneapolis",
        state="MN",
        list_url="https://new.artsmia.org/visit/calendar",
        source_prefixes=("https://new.artsmia.org/",),
    ),
    MnVenueConfig(
        slug="mmam",
        source_name="mn_mmam_events",
        venue_name="Minnesota Marine Art Museum",
        city="Winona",
        state="MN",
        list_url="https://mmam.org/calendar2",
        source_prefixes=("https://mmam.org/",),
    ),
    MnVenueConfig(
        slug="mmaa",
        source_name="mn_mmaa_events",
        venue_name="Minnesota Museum of American Art",
        city="St. Paul",
        state="MN",
        list_url="https://mmaa.org/calendar/",
        source_prefixes=("https://mmaa.org/",),
    ),
    MnVenueConfig(
        slug="rourke",
        source_name="mn_rourke_events",
        venue_name="The Rourke Art Gallery + Museum",
        city="Moorhead",
        state="MN",
        list_url="https://www.therourke.org/calendar.html",
        source_prefixes=("https://tockify.com/rourkeart", "https://www.therourke.org/"),
    ),
    MnVenueConfig(
        slug="wam",
        source_name="mn_wam_events",
        venue_name="Weisman Art Museum",
        city="Minneapolis",
        state="MN",
        list_url="https://wam.umn.edu/create-learn",
        source_prefixes=("https://wam.umn.edu/",),
    ),
)

MN_VENUES_BY_SLUG = {venue.slug: venue for venue in MN_VENUES}


class MnBundleAdapter(BaseSourceAdapter):
    source_name = "mn_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_mn_bundle_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_mn_bundle_payload/parse_mn_events from the script runner.")


def get_mn_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in MN_VENUES:
        prefixes.extend(venue.source_prefixes)
    return tuple(prefixes)


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
            timezone_id=MN_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(3000)
        html = await page.content()
        await browser.close()
        return html


async def fetch_pages_playwright(urls: list[str], *, timeout_ms: int = 90000) -> dict[str, str]:
    if not urls:
        return {}
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    pages: dict[str, str] = {}
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, args=list(PLAYWRIGHT_LAUNCH_ARGS))
        context = await browser.new_context(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            locale="en-US",
            timezone_id=MN_TIMEZONE,
        )
        for url in urls:
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(3000)
            pages[url] = await page.content()
            await page.close()
        await context.close()
        await browser.close()
    return pages


async def load_mn_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    max_links_per_venue: int = 40,
) -> dict[str, dict]:
    selected = [MN_VENUES_BY_SLUG[slug] for slug in venue_slugs] if venue_slugs else list(MN_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.slug == "artsmia":
                    payload_by_slug[venue.slug] = await _load_artsmia_payload(venue, max_links=max_links_per_venue)
                elif venue.slug == "mmam":
                    payload_by_slug[venue.slug] = await _load_mmam_payload(venue, client=client, max_links=max_links_per_venue)
                elif venue.slug == "mmaa":
                    payload_by_slug[venue.slug] = await _load_mmaa_payload(venue)
                elif venue.slug == "rourke":
                    payload_by_slug[venue.slug] = await _load_rourke_payload(venue, client=client)
                elif venue.slug == "wam":
                    payload_by_slug[venue.slug] = await _load_wam_payload(venue, client=client, max_links=max_links_per_venue)
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)

    return {
        "payload_by_slug": payload_by_slug,
        "errors_by_slug": errors_by_slug,
    }


def parse_mn_events(payload: dict, *, venue: MnVenueConfig) -> list[ExtractedActivity]:
    if venue.slug == "artsmia":
        rows = _parse_artsmia_events(payload, venue=venue)
    elif venue.slug == "mmam":
        rows = _parse_mmam_events(payload, venue=venue)
    elif venue.slug == "mmaa":
        rows = _parse_mmaa_events(payload, venue=venue)
    elif venue.slug == "rourke":
        rows = _parse_rourke_events(payload, venue=venue)
    elif venue.slug == "wam":
        rows = _parse_wam_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(MN_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.end_at is not None:
            if row.end_at.date() < current_date:
                continue
        elif row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


async def _load_artsmia_payload(venue: MnVenueConfig, *, max_links: int) -> dict:
    list_html = await fetch_html_playwright(venue.list_url)
    featured_items = _extract_artsmia_featured_items(list_html)
    listing_items = _extract_artsmia_listing_items(list_html)

    detail_urls: list[str] = []
    seen_urls: set[str] = set()
    for url in [item["url"] for item in featured_items] + [item["url"] for item in listing_items]:
        if url in seen_urls:
            continue
        seen_urls.add(url)
        detail_urls.append(url)
        if len(detail_urls) >= max(max_links, 1):
            break

    detail_pages = await fetch_pages_playwright(detail_urls)
    return {
        "list_html": list_html,
        "featured_items": featured_items,
        "listing_items": listing_items,
        "detail_pages": detail_pages,
    }


async def _load_mmam_payload(
    venue: MnVenueConfig,
    *,
    client: httpx.AsyncClient,
    max_links: int,
) -> dict:
    list_html = await fetch_html(venue.list_url, client=client)
    queue = list(_extract_mmam_urls(list_html))
    seen: set[str] = set()
    detail_pages: dict[str, str] = {}

    while queue and len(seen) < max(max_links, 1):
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)
        html = await fetch_html(url, client=client)
        detail_pages[url] = html
        for next_url in _extract_mmam_urls(html):
            if next_url not in seen and next_url not in queue and len(seen) + len(queue) < max(max_links, 1):
                queue.append(next_url)

    return {
        "list_html": list_html,
        "detail_pages": detail_pages,
    }


async def _load_mmaa_payload(venue: MnVenueConfig) -> dict:
    current_date = datetime.now(ZoneInfo(MN_TIMEZONE)).date().isoformat()
    next_url: str | None = None
    next_params: dict[str, str | int] | None = {
        "per_page": 50,
        "start_date": current_date,
    }
    pages: list[dict] = []

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=JSON_HEADERS) as client:
        while next_url or next_params:
            if next_url:
                response = await client.get(next_url)
            else:
                response = await client.get(MMAA_EVENTS_API_URL, params=next_params)
            response.raise_for_status()
            payload = response.json()
            pages.append(payload)
            next_url = payload.get("next_rest_url")
            next_params = None

    return {
        "list_url": venue.list_url,
        "pages": pages,
    }


async def _load_rourke_payload(venue: MnVenueConfig, *, client: httpx.AsyncClient) -> dict:
    html = await fetch_html(ROURKE_CALENDAR_URL, client=client)
    return {
        "list_html": html,
    }


async def _load_wam_payload(
    venue: MnVenueConfig,
    *,
    client: httpx.AsyncClient,
    max_links: int,
) -> dict:
    list_html = await fetch_html(venue.list_url, client=client)
    detail_urls: list[str] = []
    for url in _extract_wam_urls(list_html, venue.list_url):
        detail_urls.append(url)
        if len(detail_urls) >= max(max_links, 1):
            break

    detail_pages: dict[str, str] = {}
    for url in detail_urls:
        detail_pages[url] = await fetch_html(url, client=client)

    return {
        "list_html": list_html,
        "detail_pages": detail_pages,
    }


def _parse_artsmia_events(payload: dict, *, venue: MnVenueConfig) -> list[ExtractedActivity]:
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []

    for item in payload.get("featured_items") or []:
        detail_text = _extract_main_text(detail_pages.get(item["url"]))
        if not _should_include_event(item["title"], detail_text):
            continue
        location_text = _extract_label_value(detail_text, "Location") or f"{venue.venue_name}, {venue.city}, {venue.state}"
        description = _build_description(detail_text, extras=[f"Source section: featured events"])
        age_min, age_max = _extract_age_range(detail_text)
        is_free, free_status = infer_price_classification(detail_text, default_is_free=True if " free " in _token_blob(detail_text) else None)
        for instance in item.get("instances") or []:
            start_at = _parse_naive_datetime(instance.get("from"))
            end_at = _parse_naive_datetime(instance.get("to"))
            if start_at is None:
                continue
            rows.append(
                ExtractedActivity(
                    source_url=item["url"],
                    title=item["title"],
                    description=description,
                    venue_name=venue.venue_name,
                    location_text=location_text,
                    city=venue.city,
                    state=venue.state,
                    activity_type=_infer_activity_type(item["title"], detail_text),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=_has_any_pattern(detail_text, DROP_IN_PATTERNS),
                    registration_required=_has_registration_signal(detail_text),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=MN_TIMEZONE,
                    free_verification_status=free_status,
                    is_free=is_free,
                )
            )

    featured_urls = {item["url"] for item in (payload.get("featured_items") or [])}
    for item in payload.get("listing_items") or []:
        if item["url"] in featured_urls:
            continue
        detail_text = _extract_main_text(detail_pages.get(item["url"]))
        combined_text = " ".join(part for part in [item.get("date_text"), detail_text] if part)
        if not _should_include_event(item["title"], combined_text):
            continue
        start_at, end_at = _parse_artsmia_listing_datetime(item.get("date_text") or "")
        if start_at is None:
            datetimes = _extract_artsmia_detail_datetimes(detail_text)
            if datetimes:
                start_at, end_at = datetimes[0]
        if start_at is None:
            continue
        location_text = _extract_label_value(detail_text, "Location") or f"{venue.venue_name}, {venue.city}, {venue.state}"
        age_min, age_max = _extract_age_range(detail_text)
        is_free, free_status = infer_price_classification(detail_text, default_is_free=True if " free " in _token_blob(detail_text) else None)
        rows.append(
            ExtractedActivity(
                source_url=item["url"],
                title=item["title"],
                description=_build_description(detail_text, extras=[f"Calendar listing: {item.get('date_text') or ''}".strip()]),
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(item["title"], detail_text),
                age_min=age_min,
                age_max=age_max,
                drop_in=_has_any_pattern(detail_text, DROP_IN_PATTERNS),
                registration_required=_has_registration_signal(detail_text),
                start_at=start_at,
                end_at=end_at,
                timezone=MN_TIMEZONE,
                free_verification_status=free_status,
                is_free=is_free,
            )
        )

    return rows


def _parse_mmam_events(payload: dict, *, venue: MnVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for url, html in (payload.get("detail_pages") or {}).items():
        row = _build_mmam_row(url, html, venue=venue)
        if row is not None:
            rows.append(row)
    return rows


def _parse_mmaa_events(payload: dict, *, venue: MnVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for page in payload.get("pages") or []:
        for event in page.get("events") or []:
            row = _build_mmaa_row(event, venue=venue)
            if row is not None:
                rows.append(row)
    return rows


def _parse_rourke_events(payload: dict, *, venue: MnVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("list_html") or ""
    match = re.search(r"window\.tkf\s*=\s*(\{.*?\})\s*;\s*</script>", html, re.DOTALL)
    if not match:
        return []

    data = json.loads(match.group(1))
    rows: list[ExtractedActivity] = []
    for event in data.get("bootdata", {}).get("query", {}).get("pinboard", {}).get("events", []):
        row = _build_rourke_row(event, venue=venue)
        if row is not None:
            rows.append(row)
    return rows


def _parse_wam_events(payload: dict, *, venue: MnVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    list_text = _extract_main_text(payload.get("list_html"))
    for url, html in (payload.get("detail_pages") or {}).items():
        row = _build_wam_row(url, html, list_text=list_text, venue=venue)
        if row is not None:
            rows.append(row)
    return rows


def _build_mmam_row(url: str, html: str, *, venue: MnVenueConfig) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = _normalize_space(_first_text(soup.select_one("h1.eventitem-title")) or _meta_content(soup, "og:title"))
    title = title.replace("— Minnesota Marine Art Museum", "").strip()
    if not title:
        return None

    article = soup.select_one("article.eventitem")
    content_text = _normalize_space(article.get_text(" ", strip=True) if article else soup.get_text(" ", strip=True))
    if not _should_include_event(title, content_text):
        return None

    start_at, end_at = _extract_event_json_ld_datetimes(soup)
    if start_at is None:
        start_at, end_at = _extract_mmam_meta_datetimes(soup)
    if start_at is None:
        return None

    location_text = _normalize_space(
        " ".join(
            _first_text(node) or ""
            for node in soup.select(".eventitem-meta-address-line")
        )
    ) or f"{venue.venue_name}, {venue.city}, {venue.state}"
    age_min, age_max = _extract_age_range(content_text)
    is_free, free_status = infer_price_classification(
        content_text,
        default_is_free=True if " free " in _token_blob(content_text) else None,
    )

    return ExtractedActivity(
        source_url=url,
        title=title,
        description=_build_description(content_text, extras=_extract_tag_strings(soup.select(".eventitem-meta-tags a"))),
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(title, content_text),
        age_min=age_min,
        age_max=age_max,
        drop_in=_has_any_pattern(content_text, DROP_IN_PATTERNS),
        registration_required=_has_registration_signal(content_text),
        start_at=start_at,
        end_at=end_at,
        timezone=MN_TIMEZONE,
        free_verification_status=free_status,
        is_free=is_free,
    )


def _build_mmaa_row(event: dict, *, venue: MnVenueConfig) -> ExtractedActivity | None:
    title = _html_to_text(event.get("title"))
    source_url = _normalize_space(event.get("url"))
    start_at = _parse_naive_datetime(event.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_naive_datetime(event.get("end_date"))
    description = _html_to_text(event.get("description"))
    excerpt = _html_to_text(event.get("excerpt"))
    category_names = [_html_to_text(item.get("name")) for item in (event.get("categories") or [])]
    category_names = [value for value in category_names if value]
    blob = " ".join([title, description or "", excerpt or "", " ".join(category_names), _normalize_space(event.get("cost"))])
    if not _should_include_event(title, blob):
        return None

    description_parts = [part for part in [excerpt, description] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if event.get("cost"):
        description_parts.append(f"Price: {_normalize_space(event.get('cost'))}")
    description_text = " | ".join(description_parts) if description_parts else None
    age_min, age_max = _extract_age_range(blob)
    is_free, free_status = infer_price_classification(
        " ".join([description_text or "", _normalize_space(event.get("cost"))]),
        default_is_free=True if " free " in _token_blob(blob) else None,
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description_text,
        venue_name=venue.venue_name,
        location_text=f"{venue.venue_name}, {venue.city}, {venue.state}",
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(title, blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=_has_any_pattern(blob, DROP_IN_PATTERNS),
        registration_required=_has_registration_signal(blob),
        start_at=start_at,
        end_at=end_at,
        timezone=MN_TIMEZONE,
        free_verification_status=free_status,
        is_free=is_free,
    )


def _build_rourke_row(event: dict, *, venue: MnVenueConfig) -> ExtractedActivity | None:
    content = event.get("content") or {}
    title = _normalize_space(((content.get("summary") or {}).get("text")))
    if not title:
        return None

    description = _normalize_space(((content.get("description") or {}).get("text")))
    tags = ((content.get("tagset") or {}).get("tags") or {}).get("default") or []
    blob = " ".join([title, description, " ".join(_normalize_space(tag) for tag in tags)])
    if not _should_include_event(title, blob):
        return None

    start_at = _millis_to_datetime(((event.get("when") or {}).get("start") or {}).get("millis"))
    end_at = _millis_to_datetime(((event.get("when") or {}).get("end") or {}).get("millis"))
    if start_at is None:
        return None

    eid = event.get("eid") or {}
    uid = eid.get("uid") or "event"
    tid = eid.get("tid") or "0"
    source_url = f"{ROURKE_CALENDAR_URL}&uid={uid}&tid={tid}"
    location_text = _normalize_space(" ".join(part for part in [content.get("place"), content.get("address")] if part))
    age_min, age_max = _extract_age_range(blob)
    is_free, free_status = infer_price_classification(
        description,
        default_is_free=True if " free " in _token_blob(blob) else None,
    )
    extras = [f"Tags: {', '.join(_normalize_space(tag) for tag in tags if _normalize_space(tag))}"] if tags else []

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=_build_description(description, extras=extras),
        venue_name=venue.venue_name,
        location_text=location_text or f"{venue.venue_name}, {venue.city}, {venue.state}",
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(title, blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=_has_any_pattern(blob, DROP_IN_PATTERNS),
        registration_required=_has_registration_signal(blob),
        start_at=start_at,
        end_at=end_at,
        timezone=MN_TIMEZONE,
        free_verification_status=free_status,
        is_free=is_free,
    )


def _build_wam_row(url: str, html: str, *, list_text: str, venue: MnVenueConfig) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = _normalize_space(soup.title.get_text(" ", strip=True) if soup.title else "")
    title = title.replace("| Weisman Art Museum", "").strip()
    if not title:
        return None

    body_text = _extract_main_text(html)
    if not _should_include_event(title, body_text):
        return None

    start_at, end_at = _parse_wam_event_datetime(body_text)
    if start_at is None:
        return None

    age_min, age_max = _extract_age_range(body_text)
    combined_price_text = " ".join([body_text, list_text])
    is_free, free_status = infer_price_classification(
        combined_price_text,
        default_is_free=True if " free " in _token_blob(combined_price_text) else None,
    )

    return ExtractedActivity(
        source_url=url,
        title=title,
        description=_build_description(body_text),
        venue_name=venue.venue_name,
        location_text=f"{venue.venue_name}, {venue.city}, {venue.state}",
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(title, body_text),
        age_min=age_min,
        age_max=age_max,
        drop_in=_has_any_pattern(body_text, DROP_IN_PATTERNS) or " open studio " in _token_blob(title),
        registration_required=_has_registration_signal(body_text),
        start_at=start_at,
        end_at=end_at,
        timezone=MN_TIMEZONE,
        free_verification_status=free_status,
        is_free=is_free,
    )


def _extract_artsmia_featured_items(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.select_one("#__NEXT_DATA__")
    if script is None:
        return []
    data = json.loads(script.get_text())
    featured = data.get("props", {}).get("pageProps", {}).get("featuredEvents") or []
    items: list[dict] = []
    for item in featured:
        title = _normalize_space(item.get("title"))
        link = _normalize_space(item.get("link"))
        if not title or not link:
            continue
        instances = item.get("acf", {}).get("ev_instance") or []
        if not instances and item.get("nextInstance"):
            instances = [item["nextInstance"]]
        items.append(
            {
                "title": title,
                "url": urljoin("https://new.artsmia.org/visit/calendar", link).rstrip("/"),
                "instances": instances,
            }
        )
    return items


def _extract_artsmia_listing_items(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set[tuple[str, str, str]] = set()
    items: list[dict] = []
    for anchor in soup.select("a[href]"):
        href = _normalize_space(anchor.get("href"))
        if "/event/" not in href:
            continue
        url = urljoin("https://new.artsmia.org/visit/calendar", href).rstrip("/")
        lines = [line.strip() for line in anchor.get_text("\n").splitlines() if line.strip()]
        if not lines:
            continue
        title = lines[0]
        if title.upper() == "EVENT" and len(lines) > 1:
            title = lines[1]
            remainder = lines[2:]
        else:
            remainder = lines[1:]
        date_text = " ".join(remainder)
        key = (url, title, date_text)
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "title": _normalize_space(title),
                "url": url,
                "date_text": _normalize_space(date_text),
            }
        )
    return items


def _extract_mmam_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    selectors = (
        "h1 a[href^='/calendar2/']",
        "a.summary-title-link[href^='/calendar2/']",
        "a.eventitem-pager-link[href^='/calendar2/']",
    )
    for selector in selectors:
        for anchor in soup.select(selector):
            href = _normalize_space(anchor.get("href"))
            if not href or "?" in href:
                continue
            url = urljoin("https://mmam.org/calendar2", href)
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
    return urls


def _extract_wam_urls(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    stop_texts = {
        "about",
        "calendar",
        "contact",
        "create + learn",
        "create-learn",
        "donate",
        "exhibitions",
        "home",
        "plan your visit",
        "search",
        "support",
        "visit",
    }
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = _normalize_space(anchor.get("href"))
        text = _normalize_space(anchor.get_text(" ", strip=True))
        if not href or href.startswith("#") or href.startswith("mailto:"):
            continue
        if href.startswith("http"):
            parsed = urlparse(href)
            if parsed.netloc != "wam.umn.edu":
                continue
            url = href
        else:
            if not href.startswith("/"):
                continue
            url = urljoin(base_url, href)
        if url.rstrip("/") == base_url.rstrip("/"):
            continue
        if text.lower() in stop_texts:
            continue
        if len(text) < 8:
            continue
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _extract_event_json_ld_datetimes(soup: BeautifulSoup) -> tuple[datetime | None, datetime | None]:
    for script in soup.select('script[type="application/ld+json"]'):
        text = script.get_text(strip=True)
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for item in candidates:
            if not isinstance(item, dict):
                continue
            if item.get("@type") != "Event":
                continue
            start_at = _parse_iso_like_datetime(item.get("startDate"))
            end_at = _parse_iso_like_datetime(item.get("endDate"))
            if start_at is not None:
                return start_at, end_at
    return None, None


def _extract_mmam_meta_datetimes(soup: BeautifulSoup) -> tuple[datetime | None, datetime | None]:
    dates = [node.get("datetime") for node in soup.select("time.event-date[datetime]")]
    times = [_normalize_space(node.get_text(" ", strip=True)) for node in soup.select("time.event-time-12hr")]
    if not dates:
        return None, None
    start_at = _combine_date_and_time_text(dates[0], times[0] if times else None)
    end_at: datetime | None = None
    if len(dates) >= 2:
        end_text = times[1] if len(times) >= 2 else None
        end_at = _combine_date_and_time_text(dates[1], end_text)
    elif len(times) >= 2:
        end_at = _combine_date_and_time_text(dates[0], times[1])
    return start_at, end_at


def _extract_artsmia_detail_datetimes(text: str) -> list[tuple[datetime, datetime | None]]:
    matches: list[tuple[datetime, datetime | None]] = []
    for match in ARTSMIA_BODY_DATETIME_RE.finditer(text):
        start_date = _build_date(match.group(2), match.group(3), match.group(4))
        if start_date is None:
            continue
        start_time = _parse_time(match.group(5))
        end_time = _parse_time(match.group(6))
        matches.append(
            (
                datetime.combine(start_date, start_time or time.min),
                datetime.combine(start_date, end_time) if end_time is not None else None,
            )
        )
    return matches


def _extract_label_value(text: str, label: str) -> str | None:
    if not text:
        return None
    pattern = re.compile(
        rf"{re.escape(label)}\s+(?P<value>.+?)(?=\s+(?:Cost|Upcoming Dates and Time|Upcoming Date and Time|Share This Event|Questions\?|Minneapolis Institute of Art)\b)",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(text)
    if not match:
        return None
    return _normalize_space(match.group("value"))


def _parse_artsmia_listing_datetime(text: str) -> tuple[datetime | None, datetime | None]:
    match = ARTSMIA_LISTING_RE.search(text)
    if not match:
        return None, None
    start_date = _build_date(match.group("month"), match.group("day"), match.group("year"))
    if start_date is None:
        return None, None
    start_time = _parse_time(match.group("start"))
    end_time = _parse_time(match.group("end"))
    return (
        datetime.combine(start_date, start_time or time.min),
        datetime.combine(start_date, end_time) if end_time is not None else None,
    )


def _parse_wam_event_datetime(text: str) -> tuple[datetime | None, datetime | None]:
    text = _normalize_space(text)
    match = WAM_RANGE_WITH_END_DATE_RE.search(text)
    if match:
        start_date = _build_date(match.group("start_month"), match.group("start_day"), match.group("start_year"))
        end_date = _build_date(match.group("end_month"), match.group("end_day"), match.group("end_year"))
        if start_date is None or end_date is None:
            return None, None
        start_time = _parse_time(match.group("start_time"))
        end_time = _parse_time(match.group("end_time"))
        return (
            datetime.combine(start_date, start_time or time.min),
            datetime.combine(end_date, end_time or time.min) if end_time is not None else None,
        )

    match = WAM_SINGLE_DAY_RANGE_RE.search(text)
    if match:
        event_date = _build_date(match.group("month"), match.group("day"), match.group("year"))
        if event_date is None:
            return None, None
        end_time_text = match.group("end_time")
        start_time = _parse_time(_coerce_meridiem(match.group("start_time"), end_time_text))
        end_time = _parse_time(end_time_text)
        return (
            datetime.combine(event_date, start_time or time.min),
            datetime.combine(event_date, end_time) if end_time is not None else None,
        )
    return None, None


def _extract_main_text(html: str | None) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main")
    root = main or soup.body or soup
    return _normalize_space(root.get_text(" ", strip=True))


def _meta_content(soup: BeautifulSoup, prop: str) -> str | None:
    node = soup.select_one(f'meta[property="{prop}"]')
    if node is None:
        return None
    return _normalize_space(node.get("content"))


def _build_description(text: str | None, *, extras: list[str] | None = None) -> str | None:
    parts: list[str] = []
    normalized = _normalize_space(text)
    if normalized:
        parts.append(normalized)
    if extras:
        parts.extend(extra for extra in extras if extra)
    return " | ".join(parts) if parts else None


def _extract_tag_strings(nodes: list) -> list[str]:
    tags = [_normalize_space(node.get_text(" ", strip=True)) for node in nodes]
    tags = [tag for tag in tags if tag]
    return [f"Tags: {', '.join(tags)}"] if tags else []


def _extract_age_range(text: str | None) -> tuple[int | None, int | None]:
    normalized = _normalize_space(text)
    if not normalized:
        return None, None
    match = AGE_RANGE_RE.search(normalized) or AGE_RANGE_ALT_RE.search(normalized)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = AGE_PLUS_RE.search(normalized)
    if match:
        return int(match.group(1)), None
    return None, None


def _infer_activity_type(title: str, text: str | None) -> str:
    blob = _token_blob(" ".join([title, text or ""]))
    if any(token in blob for token in (" lecture ", " talk ", " webinar ", " roundtable ", " discussion ", " speaker ", " critique ")):
        return "lecture"
    if any(token in blob for token in (" workshop ", " class ", " classes ", " studio ", " sketching ", " drawing ", " printmaking ")):
        return "workshop"
    return "activity"


def _should_include_event(title: str, text: str | None) -> bool:
    blob = _token_blob(" ".join([title, text or ""]))
    title_blob = _token_blob(title)
    title_lower = _normalize_space(title).lower()
    has_include = any(pattern in blob for pattern in INCLUDE_PATTERNS)
    if not has_include:
        return False
    if "cocktail" in title_lower:
        return False

    title_reject_patterns = (
        " book launch ",
        " camp ",
        " camps ",
        " closing ",
        " concert ",
        " dining ",
        " dinner ",
        " exhibition ",
        " exhibitions ",
        " fundraiser ",
        " fundraising ",
        " member days ",
        " member preview ",
        " opening ",
        " openings ",
        " poem ",
        " poems ",
        " poetry ",
        " preview ",
        " reception ",
        " screening ",
        " storytime ",
        " story time ",
        " tour ",
        " tours ",
        " wine ",
        " yoga ",
    )
    if any(pattern in title_blob for pattern in title_reject_patterns):
        return False

    override_safe = (
        " demo ",
        " educator workshop ",
        " family day ",
        " live printmaking ",
        " open studio ",
        " roundtable ",
        " studio session ",
        " studio sessions ",
        " toddler ",
        " webinar ",
        " workshop ",
        " workshops ",
    )
    if any(pattern in blob for pattern in HARD_REJECT_PATTERNS):
        if not any(pattern in blob for pattern in override_safe):
            return False
        blocking_even_with_override = (
            " book launch ",
            " concert ",
            " fundraiser ",
            " fundraising ",
            " meditation ",
            " mindfulness ",
            " music ",
            " orchestra ",
            " poem ",
            " poems ",
            " poetry ",
            " reading ",
            " reception ",
            " screening ",
            " storytime ",
            " story time ",
            " tai chi ",
            " tour ",
            " tours ",
            " wine ",
            " yoga ",
        )
        if any(pattern in blob for pattern in blocking_even_with_override):
            return False
    return True


def _has_registration_signal(text: str | None) -> bool:
    blob = _token_blob(text)
    return any(pattern in blob for pattern in REGISTRATION_PATTERNS) and " not required " not in blob


def _has_any_pattern(text: str | None, patterns: tuple[str, ...]) -> bool:
    blob = _token_blob(text)
    return any(pattern in blob for pattern in patterns)


def _looks_like_bot_challenge(text: str) -> bool:
    snippet = _token_blob(text[:2000])
    return (
        " vercel security checkpoint " in snippet
        or " verifying your browser " in snippet
        or " attention required " in snippet
    )


def _first_text(node) -> str | None:
    if node is None:
        return None
    return _normalize_space(node.get_text(" ", strip=True))


def _html_to_text(value: str | None) -> str | None:
    if not value:
        return None
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    text = unescape(str(value))
    for dash in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212"):
        text = text.replace(dash, " ")
    return " ".join(text.replace("\xa0", " ").split())


def _token_blob(value: str | None) -> str:
    return f" {_normalize_space(value).lower()} "


def _parse_naive_datetime(value: str | None) -> datetime | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _parse_iso_like_datetime(value: str | None) -> datetime | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None
    normalized = normalized.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(ZoneInfo(MN_TIMEZONE)).replace(tzinfo=None)
    return dt


def _millis_to_datetime(value: int | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=ZoneInfo(MN_TIMEZONE)).replace(tzinfo=None)


def _combine_date_and_time_text(date_value: str, time_value: str | None) -> datetime | None:
    try:
        parsed_date = datetime.strptime(date_value, "%Y-%m-%d").date()
    except ValueError:
        return None
    parsed_time = _parse_time(time_value) if time_value else None
    return datetime.combine(parsed_date, parsed_time or time.min)


def _parse_time(value: str | None) -> time | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None
    match = TIME_SINGLE_RE.search(normalized)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or 0)
    meridiem = match.group(3).lower().replace(".", "")
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return time(hour=hour, minute=minute)


def _coerce_meridiem(start_text: str | None, end_text: str | None) -> str | None:
    start_value = _normalize_space(start_text)
    end_value = _normalize_space(end_text)
    if not start_value or not end_value:
        return start_value or None
    if TIME_SINGLE_RE.search(start_value):
        return start_value
    end_match = TIME_SINGLE_RE.search(end_value)
    if end_match is None:
        return start_value
    return f"{start_value} {end_match.group(3)}"


def _build_date(month_text: str, day_text: str, year_text: str | int) -> date | None:
    month = MONTH_LOOKUP.get(_normalize_space(month_text)[:3].lower())
    if month is None:
        return None
    try:
        return date(int(year_text), month, int(day_text))
    except ValueError:
        return None
