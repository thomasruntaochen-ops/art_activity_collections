from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from urllib.parse import parse_qs
from urllib.parse import unquote
from urllib.parse import urljoin
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None


HI_TIMEZONE = "Pacific/Honolulu"
CURRENT_DATE = datetime.now(ZoneInfo(HI_TIMEZONE)).date()
PLAYWRIGHT_LAUNCH_ARGS = (
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
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

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>[AaPp]\.?\s*[Mm]\.?)?"
    r"\s*(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>[AaPp]\.?\s*[Mm]\.?)",
    re.IGNORECASE,
)
FULL_DATE_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})",
    re.IGNORECASE,
)
MONTH_DAY_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2})",
    re.IGNORECASE,
)
WEEKDAY_NAME_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

EHCC_MONTH_PAGE_RE = re.compile(r"/programs/calendar(?:/(\d{4}-\d{2}))?$", re.IGNORECASE)
EHCC_CONTENT_RE = re.compile(r"content=\"(?P<value>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}-\d{2}:\d{2})\"")
DONKEY_MOVEMENT_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4}),\s*"
    r"(?P<start>\d{1,2}:\d{2})\s*-\s*(?P<end>\d{1,2}:\d{2}\s*[AaPp]\.?[Mm]\.?)",
    re.IGNORECASE,
)
DONKEY_PIT_FIRE_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day_one>\d{1,2})\s*&\s*(?P<day_two>\d{1,2})\s+"
    r"from\s+(?P<start>\d{1,2}:\d{2}\s*[AaPp]\.?[Mm]\.?)\s*[–-]\s*(?P<end>\d{1,2}:\d{2}\s*[AaPp]\.?[Mm]\.?)",
    re.IGNORECASE,
)
DONKEY_OPEN_STUDIO_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day_start>\d{1,2})\s*[–-]\s*(?P<day_end>\d{1,2})\s*:\s*"
    r"(?P<weekday_start>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)s?\s*[–-]\s*"
    r"(?P<weekday_end>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)s?\s*:\s*"
    r"(?P<first_start>\d{1,2}:\d{2}\s*[AaPp]\.?[Mm]\.?)\s*[–-]\s*(?P<first_end>\d{1,2}:\d{2}\s*[AaPp]\.?[Mm]\.?)"
    r"\s*and\s*"
    r"(?P<second_start>\d{1,2}:\d{2}\s*[AaPp]\.?[Mm]\.?)\s*[–-]\s*(?P<second_end>\d{1,2}:\d{2}\s*[AaPp]\.?[Mm]\.?)",
    re.IGNORECASE,
)
HOMA_SUBSHOW_RE = re.compile(
    r"(?P<weekday>[A-Za-z]{3})\s+(?P<month>[A-Za-z]{3})\s+(?P<day>\d{1,2})\s+"
    r"(?P<start_hour>\d{1,2}):(?P<start_minute>\d{2})(?P<start_meridiem>[AP]M)\s*-\s*"
    r"(?P<end_hour>\d{1,2}):(?P<end_minute>\d{2})(?P<end_meridiem>[AP]M)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class HiHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_urls: tuple[str, ...]
    discovery_mode: str


HI_HTML_VENUES: tuple[HiHtmlVenueConfig, ...] = (
    HiHtmlVenueConfig(
        slug="capitol_modern",
        source_name="hi_capitol_modern_events",
        venue_name="Capitol Modern",
        city="Honolulu",
        state="HI",
        list_urls=("https://www.capitolmodern.org/events",),
        discovery_mode="capitol_html",
    ),
    HiHtmlVenueConfig(
        slug="donkey_mill_art_center",
        source_name="hi_donkey_mill_events",
        venue_name="Donkey Mill Art Center",
        city="Hōlualoa",
        state="HI",
        list_urls=("https://donkeymillartcenter.org/event/",),
        discovery_mode="donkey_html",
    ),
    HiHtmlVenueConfig(
        slug="east_hawaii_cultural_center",
        source_name="hi_ehcc_events",
        venue_name="East Hawaiʻi Cultural Center",
        city="Hilo",
        state="HI",
        list_urls=("https://www.ehcc.org/programs/calendar",),
        discovery_mode="ehcc_html",
    ),
    HiHtmlVenueConfig(
        slug="honolulu_museum_of_art",
        source_name="hi_homa_events",
        venue_name="Honolulu Museum of Art",
        city="Honolulu",
        state="HI",
        list_urls=("https://honolulumuseum.org/events",),
        discovery_mode="homa_html",
    ),
    HiHtmlVenueConfig(
        slug="hui_noeau_visual_arts_center",
        source_name="hi_hui_events",
        venue_name="Hui No‘eau Visual Arts Center",
        city="Makawao",
        state="HI",
        list_urls=("https://www.huinoeau.com/art-events",),
        discovery_mode="hui_html",
    ),
)

HI_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in HI_HTML_VENUES}


class HiHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "hi_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_hi_html_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_hi_html_bundle_payload/parse_hi_html_events from the script runner.")


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
            timezone_id=HI_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def load_hi_html_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    months_ahead: int = 9,
) -> dict[str, dict]:
    selected = [HI_HTML_VENUES_BY_SLUG[slug] for slug in venue_slugs] if venue_slugs else list(HI_HTML_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.discovery_mode == "capitol_html":
                    list_url = venue.list_urls[0]
                    list_html = await fetch_html(list_url, client=client, use_playwright_fallback=True)
                    detail_urls = _extract_capitol_detail_urls(list_html, list_url)
                    detail_pages = {
                        detail_url: await fetch_html(detail_url, client=client, use_playwright_fallback=True)
                        for detail_url in detail_urls
                    }
                    payload_by_slug[venue.slug] = {"list_html": list_html, "detail_pages": detail_pages}
                    continue

                if venue.discovery_mode == "donkey_html":
                    list_url = venue.list_urls[0]
                    list_html = await fetch_html(list_url, client=client, use_playwright_fallback=True)
                    detail_items = _extract_donkey_listing_items(list_html, list_url)
                    for item in detail_items:
                        item["html"] = await fetch_html(item["url"], client=client, use_playwright_fallback=True)
                    payload_by_slug[venue.slug] = {"detail_items": detail_items}
                    continue

                if venue.discovery_mode == "ehcc_html":
                    calendar_pages: dict[str, str] = {}
                    detail_pages: dict[str, str] = {}
                    for page_url in _build_ehcc_month_urls(venue.list_urls[0], months_ahead=months_ahead):
                        calendar_pages[page_url] = await fetch_html(page_url, client=client, use_playwright_fallback=True)
                    detail_urls = {
                        "https://www.ehcc.org/content/art-fridays",
                        "https://www.ehcc.org/youth-arts-saturdays",
                    }
                    for entry in _extract_ehcc_month_entries(calendar_pages):
                        start_at = entry.get("start_at")
                        title = _normalize_space(entry.get("title"))
                        if not isinstance(start_at, datetime) or start_at.date() < CURRENT_DATE:
                            continue
                        if _keep_ehcc_event(title, _searchable_blob(title)):
                            detail_urls.add(entry["url"])
                    for detail_url in sorted(detail_urls):
                        detail_pages[detail_url] = await fetch_html(detail_url, client=client, use_playwright_fallback=True)
                    payload_by_slug[venue.slug] = {"calendar_pages": calendar_pages, "detail_pages": detail_pages}
                    continue

                if venue.discovery_mode == "homa_html":
                    list_url = venue.list_urls[0]
                    list_html = await fetch_html(list_url, client=client, use_playwright_fallback=True)
                    detail_urls = _extract_homa_detail_urls(list_html, list_url)
                    detail_pages = {
                        detail_url: await fetch_html(detail_url, client=client, use_playwright_fallback=True)
                        for detail_url in detail_urls
                    }
                    payload_by_slug[venue.slug] = {"list_html": list_html, "detail_pages": detail_pages}
                    continue

                if venue.discovery_mode == "hui_html":
                    list_url = venue.list_urls[0]
                    list_html = await fetch_html(list_url, client=client, use_playwright_fallback=True)
                    payload_by_slug[venue.slug] = {"list_html": list_html}
                    continue

                raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[hi-html-bundle] venue={venue.slug} failed: {exc}")

    return {"payload_by_slug": payload_by_slug, "errors_by_slug": errors_by_slug}


def parse_hi_html_events(payload: dict, *, venue: HiHtmlVenueConfig) -> list[ExtractedActivity]:
    if venue.discovery_mode == "capitol_html":
        return _parse_capitol_events(payload, venue=venue)
    if venue.discovery_mode == "donkey_html":
        return _parse_donkey_events(payload, venue=venue)
    if venue.discovery_mode == "ehcc_html":
        return _parse_ehcc_events(payload, venue=venue)
    if venue.discovery_mode == "homa_html":
        return _parse_homa_events(payload, venue=venue)
    if venue.discovery_mode == "hui_html":
        return _parse_hui_events(payload, venue=venue)
    raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")


def _parse_capitol_events(payload: dict, *, venue: HiHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    detail_pages = payload.get("detail_pages") or {}
    list_soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    cards_by_url: dict[str, str] = {}
    for anchor in list_soup.select('a[href^="/events/"]'):
        absolute = urljoin(venue.list_urls[0], anchor.get("href") or "")
        card_text = _normalize_space(anchor.get_text(" ", strip=True))
        if card_text:
            cards_by_url[absolute] = card_text

    for source_url, html in detail_pages.items():
        soup = BeautifulSoup(html, "html.parser")
        title = _normalize_space(_node_text(soup.select_one("h1")))
        if not title:
            continue
        card_text = cards_by_url.get(source_url, "")
        description = _extract_meta_description(html) or _normalize_space(card_text)
        token_blob = _searchable_blob(" ".join(part for part in (title, description, card_text) if part))
        if not _keep_capitol_event(token_blob):
            continue
        start_at, end_at = _extract_capitol_start_end(html=html, fallback_text=card_text)
        if start_at is None or start_at.date() < CURRENT_DATE:
            continue
        deduped[(source_url, title, start_at)] = ExtractedActivity(
            source_url=source_url,
            title=title,
            description=description or None,
            venue_name=venue.venue_name,
            location_text=f"{venue.city}, {venue.state}",
            city=venue.city,
            state=venue.state,
            activity_type=_infer_activity_type(token_blob),
            age_min=_parse_age_range(token_blob)[0],
            age_max=_parse_age_range(token_blob)[1],
            drop_in=("drop in" in token_blob or "drop-in" in token_blob),
            registration_required=("registration required" in token_blob),
            start_at=start_at,
            end_at=end_at,
            timezone=HI_TIMEZONE,
            **price_classification_kwargs(description or card_text, default_is_free=None),
        )
    rows = list(deduped.values())
    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_donkey_events(payload: dict, *, venue: HiHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}

    for item in payload.get("detail_items") or []:
        source_url = _normalize_space(item.get("url"))
        html = item.get("html") or ""
        soup = BeautifulSoup(html, "html.parser")
        title = _normalize_space(_node_text(soup.select_one("h1"))) or _normalize_space(item.get("title"))
        if not title or not source_url:
            continue

        description = _extract_donkey_description(soup)
        token_blob = _searchable_blob(" ".join(part for part in (title, description or "") if part))
        sessions: list[tuple[datetime, datetime | None]] = []
        if "open studio" in token_blob:
            sessions = _extract_donkey_open_studio_sessions(title=title, body_text=description or "")
        elif "pit fire" in token_blob:
            sessions = _extract_donkey_pit_fire_sessions(description or "")
        else:
            sessions = _extract_donkey_full_date_sessions(description or "")

        for start_at, end_at in sessions:
            if start_at.date() < CURRENT_DATE:
                continue
            age_min, age_max = _parse_age_range(token_blob)
            deduped[(source_url, title, start_at)] = ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=venue.venue_name,
                location_text=f"{venue.city}, {venue.state}",
                city=venue.city,
                state=venue.state,
                activity_type=("activity" if "open studio" in token_blob else "workshop"),
                age_min=age_min,
                age_max=age_max,
                drop_in=("open studio" in token_blob or "drop in" in token_blob or "drop-in" in token_blob),
                registration_required=_registration_required(token_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=HI_TIMEZONE,
                **price_classification_kwargs(description or "", default_is_free=None),
            )

    rows = list(deduped.values())
    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_ehcc_events(payload: dict, *, venue: HiHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    detail_pages = payload.get("detail_pages") or {}

    recurring_handlers = {
        "https://www.ehcc.org/content/art-fridays": _build_ehcc_art_party_rows,
        "https://www.ehcc.org/youth-arts-saturdays": _build_ehcc_youth_art_rows,
    }

    for detail_url, builder in recurring_handlers.items():
        html = detail_pages.get(detail_url)
        if not html:
            continue
        for row in builder(html=html, source_url=detail_url, venue=venue):
            if row.start_at.date() < CURRENT_DATE:
                continue
            deduped[(row.source_url, row.title, row.start_at)] = row

    month_entries = _extract_ehcc_month_entries(payload.get("calendar_pages") or {})
    for item in month_entries:
        if item["url"] in recurring_handlers:
            continue
        start_at = item["start_at"]
        if start_at is None or start_at.date() < CURRENT_DATE:
            continue
        detail_html = detail_pages.get(item["url"], "")
        description = _extract_ehcc_description(detail_html) if detail_html else None
        token_blob = _searchable_blob(" ".join(part for part in (item["title"], description or "") if part))
        if not _keep_ehcc_event(item["title"], token_blob):
            continue
        age_min, age_max = _parse_age_range(token_blob)
        deduped[(item["url"], item["title"], start_at)] = ExtractedActivity(
            source_url=item["url"],
            title=item["title"],
            description=description,
            venue_name=venue.venue_name,
            location_text=f"{venue.city}, {venue.state}",
            city=venue.city,
            state=venue.state,
            activity_type=_infer_activity_type(token_blob),
            age_min=age_min,
            age_max=age_max,
            drop_in=("drop in" in token_blob or "drop-in" in token_blob),
            registration_required=_registration_required(token_blob),
            start_at=start_at,
            end_at=item["end_at"],
            timezone=HI_TIMEZONE,
            **price_classification_kwargs(description or "", default_is_free=None),
        )

    rows = list(deduped.values())
    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_homa_events(payload: dict, *, venue: HiHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}

    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _extract_homa_title(soup)
        if not title:
            continue
        description = _extract_meta_description(html) or _extract_homa_description(soup)
        category_text = _extract_homa_category_text(soup)
        token_blob = _searchable_blob(" ".join(part for part in (title, description or "", category_text or "") if part))
        if not _keep_homa_event(token_blob):
            continue

        occurrences = _extract_homa_occurrences(soup, html=html)
        for start_at, end_at, location_text in occurrences:
            if start_at.date() < CURRENT_DATE:
                continue
            age_min, age_max = _parse_age_range(token_blob)
            deduped[(source_url, title, start_at)] = ExtractedActivity(
                source_url=source_url,
                title=title,
                description=" | ".join(
                    part for part in (
                        description,
                        f"Category: {category_text}" if category_text else None,
                    )
                    if part
                )
                or None,
                venue_name=venue.venue_name,
                location_text=location_text or f"{venue.city}, {venue.state}",
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(token_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=False,
                registration_required=_registration_required(token_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=HI_TIMEZONE,
                **price_classification_kwargs(description or category_text or "", default_is_free=None),
            )

    rows = list(deduped.values())
    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_hui_events(payload: dict, *, venue: HiHtmlVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    rows: list[ExtractedActivity] = []
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}

    for article in soup.select("article.eventlist-event"):
        title = None
        detail_url = None
        for anchor in article.select('a[href*="/art-events/"]'):
            href = urljoin(venue.list_urls[0], anchor.get("href") or "")
            text = _normalize_space(anchor.get_text(" ", strip=True))
            if not href.startswith("https://www.huinoeau.com/art-events/"):
                continue
            if detail_url is None:
                detail_url = href
            if text and "view event" not in text.lower():
                title = text
                detail_url = href
                break

        if not detail_url:
            continue
        if not title:
            title = _normalize_space(_node_text(article.select_one("h1, h2, h3")))
        if not title:
            continue

        description = _normalize_space(_node_text(article.select_one(".eventlist-column-info"))) or _normalize_space(article.get_text(" ", strip=True))
        token_blob = _searchable_blob(" ".join(part for part in (title, description or "") if part))
        if not _keep_hui_event(token_blob):
            continue

        start_at, end_at = _extract_hui_article_datetimes(article)
        if start_at is None or start_at.date() < CURRENT_DATE:
            continue

        age_min, age_max = _parse_age_range(token_blob)
        deduped[(detail_url, title, start_at)] = ExtractedActivity(
            source_url=detail_url,
            title=title,
            description=description or None,
            venue_name=venue.venue_name,
            location_text=f"{venue.city}, {venue.state}",
            city=venue.city,
            state=venue.state,
            activity_type=_infer_activity_type(token_blob),
            age_min=age_min,
            age_max=age_max,
            drop_in=False,
            registration_required=_registration_required(token_blob),
            start_at=start_at,
            end_at=end_at,
            timezone=HI_TIMEZONE,
            **price_classification_kwargs(description or "", default_is_free=None),
        )

    rows = list(deduped.values())
    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _build_ehcc_art_party_rows(
    *,
    html: str,
    source_url: str,
    venue: HiHtmlVenueConfig,
) -> list[ExtractedActivity]:
    title, description, start_at, end_at = _extract_ehcc_detail_core(html)
    if not title or start_at is None or end_at is None:
        return []

    rows: list[ExtractedActivity] = []
    token_blob = _searchable_blob(" ".join(part for part in (title, description or "") if part))
    age_min, age_max = _parse_age_range(token_blob)
    current = start_at.date()
    while current <= end_at.date():
        if current.weekday() == WEEKDAY_NAME_TO_INDEX["friday"]:
            occurrence_start = datetime.combine(current, start_at.timetz().replace(tzinfo=None))
            occurrence_end = datetime.combine(current, end_at.timetz().replace(tzinfo=None))
            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=description,
                    venue_name=venue.venue_name,
                    location_text="EHCC Annex, Hilo, HI",
                    city=venue.city,
                    state=venue.state,
                    activity_type="workshop",
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=True,
                    registration_required=False,
                    start_at=occurrence_start,
                    end_at=occurrence_end,
                    timezone=HI_TIMEZONE,
                    **price_classification_kwargs(description or "", default_is_free=None),
                )
            )
        current += timedelta(days=1)
    return rows


def _build_ehcc_youth_art_rows(
    *,
    html: str,
    source_url: str,
    venue: HiHtmlVenueConfig,
) -> list[ExtractedActivity]:
    title, description, start_at, end_at = _extract_ehcc_detail_core(html)
    if not title or start_at is None or end_at is None:
        return []

    rows: list[ExtractedActivity] = []
    token_blob = _searchable_blob(" ".join(part for part in (title, description or "") if part))
    age_min, age_max = _parse_age_range(token_blob)
    month_cursor = date(start_at.year, start_at.month, 1)
    month_end = date(end_at.year, end_at.month, 1)
    skip_february = "no yas" in token_blob and "february 2026" in token_blob

    while month_cursor <= month_end:
        if skip_february and month_cursor.year == 2026 and month_cursor.month == 2:
            month_cursor = _next_month(month_cursor)
            continue
        occurrence_date = _nth_weekday_of_month(
            year=month_cursor.year,
            month=month_cursor.month,
            weekday=WEEKDAY_NAME_TO_INDEX["saturday"],
            occurrence=2,
        )
        if occurrence_date is not None and start_at.date() <= occurrence_date <= end_at.date():
            occurrence_start = datetime.combine(occurrence_date, start_at.timetz().replace(tzinfo=None))
            occurrence_end = datetime.combine(occurrence_date, end_at.timetz().replace(tzinfo=None))
            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=description,
                    venue_name=venue.venue_name,
                    location_text="East Hawaiʻi Cultural Center, Hilo, HI",
                    city=venue.city,
                    state=venue.state,
                    activity_type="workshop",
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=False,
                    registration_required=False,
                    start_at=occurrence_start,
                    end_at=occurrence_end,
                    timezone=HI_TIMEZONE,
                    **price_classification_kwargs(description or "", default_is_free=None),
                )
            )
        month_cursor = _next_month(month_cursor)
    return rows


def _extract_capitol_detail_urls(html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select('a[href^="/events/"]'):
        absolute = urljoin(list_url, anchor.get("href") or "")
        parsed = urlparse(absolute)
        if not parsed.path.startswith("/events/") or parsed.path == "/events":
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        urls.append(absolute)
    return urls


def _extract_donkey_listing_items(html: str, list_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str]] = []
    for node in soup.select(".neon-event-list .event"):
        link = node.select_one('a[href*="/event/?event="]')
        if link is None:
            continue
        title_node = node.select_one("h2")
        title = _normalize_space(_node_text(title_node)) or _normalize_space(node.get_text(" ", strip=True))
        items.append(
            {
                "title": title,
                "url": urljoin(list_url, link.get("href") or ""),
            }
        )
    return items


def _build_ehcc_month_urls(list_url: str, *, months_ahead: int) -> list[str]:
    urls = [list_url]
    cursor = date(CURRENT_DATE.year, CURRENT_DATE.month, 1)
    for _ in range(max(months_ahead, 0)):
        cursor = _next_month(cursor)
        urls.append(f"https://www.ehcc.org/programs/calendar/{cursor:%Y-%m}")
    return urls


def _extract_ehcc_detail_urls(calendar_pages: dict[str, str]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for page_url, html in calendar_pages.items():
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.select(".calendar.monthview a[href]"):
            absolute = urljoin(page_url, anchor.get("href") or "")
            parsed = urlparse(absolute)
            if not parsed.netloc.endswith("ehcc.org"):
                continue
            if parsed.path.startswith("/calendar/day/"):
                continue
            if absolute in seen:
                continue
            seen.add(absolute)
            urls.append(absolute)
    return urls


def _extract_homa_detail_urls(html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select('a[href^="/events/"]'):
        absolute = urljoin(list_url, anchor.get("href") or "")
        parsed = urlparse(absolute)
        if not parsed.path.startswith("/events/"):
            continue
        if parsed.fragment or parsed.path == "/events":
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        urls.append(absolute)
    return urls


def _extract_donkey_description(soup: BeautifulSoup) -> str:
    parts: list[str] = []
    for selector in (
        ".post-content p",
        ".post-content li",
        ".fusion-text p",
        ".fusion-text li",
    ):
        for node in soup.select(selector):
            text = _normalize_space(node.get_text(" ", strip=True))
            if text:
                parts.append(text)
    if parts:
        return " ".join(dict.fromkeys(parts))
    return _normalize_space(soup.get_text(" ", strip=True))


def _extract_donkey_open_studio_sessions(*, title: str, body_text: str) -> list[tuple[datetime, datetime | None]]:
    match = DONKEY_OPEN_STUDIO_RE.search(body_text)
    if match is None:
        return []
    month = _month_number(match.group("month"))
    if month is None:
        return []
    year = _infer_year_for_month(month)
    date_start = date(year, month, int(match.group("day_start")))
    date_end = date(year, month, int(match.group("day_end")))
    weekday_start = WEEKDAY_NAME_TO_INDEX[match.group("weekday_start").lower()]
    weekday_end = WEEKDAY_NAME_TO_INDEX[match.group("weekday_end").lower()]
    weekdays = _weekday_range(weekday_start, weekday_end)
    sessions: list[tuple[datetime, datetime | None]] = []
    first_start = _combine_local(date_start, match.group("first_start"))
    first_end = _combine_local(date_start, match.group("first_end"))
    second_start = _combine_local(date_start, match.group("second_start"))
    second_end = _combine_local(date_start, match.group("second_end"))
    if first_start is None or first_end is None or second_start is None or second_end is None:
        return []
    current = date_start
    while current <= date_end:
        if current.weekday() in weekdays:
            sessions.append((datetime.combine(current, first_start.timetz().replace(tzinfo=None)), datetime.combine(current, first_end.timetz().replace(tzinfo=None))))
            sessions.append((datetime.combine(current, second_start.timetz().replace(tzinfo=None)), datetime.combine(current, second_end.timetz().replace(tzinfo=None))))
        current += timedelta(days=1)
    return sessions


def _extract_donkey_pit_fire_sessions(body_text: str) -> list[tuple[datetime, datetime | None]]:
    sessions: list[tuple[datetime, datetime | None]] = []
    for match in DONKEY_PIT_FIRE_RE.finditer(body_text):
        month = _month_number(match.group("month"))
        if month is None:
            continue
        year = _infer_year_for_month(month)
        start_one = _combine_local(date(year, month, int(match.group("day_one"))), match.group("start"))
        end_one = _combine_local(date(year, month, int(match.group("day_one"))), match.group("end"))
        start_two = _combine_local(date(year, month, int(match.group("day_two"))), match.group("start"))
        end_two = _combine_local(date(year, month, int(match.group("day_two"))), match.group("end"))
        if start_one and end_one:
            sessions.append((start_one, end_one))
        if start_two and end_two:
            sessions.append((start_two, end_two))
    return sessions


def _extract_donkey_full_date_sessions(body_text: str) -> list[tuple[datetime, datetime | None]]:
    sessions: list[tuple[datetime, datetime | None]] = []
    for match in DONKEY_MOVEMENT_RE.finditer(body_text):
        month = _month_number(match.group("month"))
        if month is None:
            continue
        year = int(match.group("year"))
        day_value = int(match.group("day"))
        start_text = _fill_missing_meridiem(match.group("start"), match.group("end"))
        start_at = _combine_local(date(year, month, day_value), start_text)
        end_at = _combine_local(date(year, month, day_value), match.group("end"))
        if start_at is not None:
            sessions.append((start_at, end_at))
    return sessions


def _extract_ehcc_month_entries(calendar_pages: dict[str, str]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    seen: set[tuple[str, str, datetime | None]] = set()
    for page_url, html in calendar_pages.items():
        soup = BeautifulSoup(html, "html.parser")
        for block in soup.select(".calendar.monthview"):
            title_link = block.select_one("a[href]")
            if title_link is None:
                continue
            title = _normalize_space(title_link.get_text(" ", strip=True))
            source_url = urljoin(page_url, title_link.get("href") or "")
            contents = str(block)
            matches = EHCC_CONTENT_RE.findall(contents)
            start_at = _parse_iso_datetime(matches[0]) if matches else None
            end_at = _parse_iso_datetime(matches[1]) if len(matches) > 1 else None
            key = (source_url, title, start_at)
            if key in seen:
                continue
            seen.add(key)
            items.append(
                {
                    "url": source_url,
                    "title": title,
                    "start_at": start_at,
                    "end_at": end_at,
                }
            )
    return items


def _extract_ehcc_detail_core(html: str) -> tuple[str | None, str | None, datetime | None, datetime | None]:
    soup = BeautifulSoup(html, "html.parser")
    title = _normalize_space(
        _node_text(soup.select_one("#page-title"))
        or _node_text(soup.select_one("h1.page__title"))
        or _node_text(soup.select_one("h1"))
    )
    description = _extract_ehcc_description(html)
    date_nodes = soup.select(".field-name-field-dates [property='dc:date']")
    date_values = [node.get("content") for node in date_nodes if node.get("content")]
    start_at = _parse_iso_datetime(date_values[0]) if date_values else None
    end_at = _parse_iso_datetime(date_values[1]) if len(date_values) > 1 else None
    return title, description, start_at, end_at


def _extract_ehcc_description(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    body_field = soup.select_one(".field-name-body .field-item")
    if body_field is not None:
        text = _normalize_space(body_field.get_text(" ", strip=True))
        if text:
            return text
    parts: list[str] = []
    for node in soup.select(".field-name-body p, .field-name-body li, .node-program p, .node-program li"):
        text = _normalize_space(node.get_text(" ", strip=True))
        if text:
            parts.append(text)
    if parts:
        return " ".join(dict.fromkeys(parts))
    meta = _extract_meta_description(html)
    return meta or None


def _extract_homa_title(soup: BeautifulSoup) -> str | None:
    title = _normalize_space(_node_text(soup.select_one("h1")))
    if title:
        return title
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        payload = _safe_json_loads(script.string or script.get_text())
        event_obj = _find_event_obj(payload)
        if isinstance(event_obj, dict):
            value = _normalize_space(event_obj.get("name"))
            if value:
                return value
    return None


def _extract_homa_description(soup: BeautifulSoup) -> str | None:
    parts: list[str] = []
    for selector in (
        ".showBody p",
        ".description p",
        ".body p",
        ".desc p",
    ):
        for node in soup.select(selector):
            text = _normalize_space(node.get_text(" ", strip=True))
            if text:
                parts.append(text)
    return " ".join(dict.fromkeys(parts)) or None


def _extract_homa_category_text(soup: BeautifulSoup) -> str | None:
    parts: list[str] = []
    for selector in (".meta", ".label", ".showHeader .meta", ".genres"):
        for node in soup.select(selector):
            text = _normalize_space(node.get_text(" ", strip=True))
            if text:
                parts.append(text)
    unique = list(dict.fromkeys(parts))
    return " | ".join(unique) or None


def _extract_homa_occurrences(soup: BeautifulSoup, *, html: str) -> list[tuple[datetime, datetime | None, str | None]]:
    rows: list[tuple[datetime, datetime | None, str | None]] = []
    for item in soup.select("li.subshow"):
        text = _normalize_space(item.get_text(" ", strip=True))
        match = HOMA_SUBSHOW_RE.search(text)
        if match is None:
            continue
        month = _month_number(match.group("month"))
        if month is None:
            continue
        year = _infer_year_for_month(month)
        start_at = _build_datetime(
            year=year,
            month=month,
            day=int(match.group("day")),
            hour=int(match.group("start_hour")),
            minute=int(match.group("start_minute")),
            meridiem=match.group("start_meridiem"),
        )
        end_at = _build_datetime(
            year=year,
            month=month,
            day=int(match.group("day")),
            hour=int(match.group("end_hour")),
            minute=int(match.group("end_minute")),
            meridiem=match.group("end_meridiem"),
        )
        location_text = None
        location_node = item.select_one(".location")
        if location_node is not None:
            location_text = _normalize_space(location_node.get_text(" ", strip=True))
        if location_text is None:
            location_text = _extract_homa_location_from_text(text)
        if start_at is not None:
            rows.append((start_at, end_at, location_text))
    if rows:
        return rows

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        payload = _safe_json_loads(script.string or script.get_text())
        event_obj = _find_event_obj(payload)
        if not isinstance(event_obj, dict):
            continue
        start_at = _parse_iso_datetime(event_obj.get("startDate"))
        if start_at is None:
            continue
        end_at = _parse_iso_datetime(event_obj.get("endDate"))
        location = event_obj.get("location")
        location_text = None
        if isinstance(location, dict):
            location_text = _normalize_space(location.get("name"))
        rows.append((start_at, end_at, location_text))
        break
    return rows


def _extract_homa_location_from_text(text: str) -> str | None:
    for marker in ("Honolulu Museum of Art", "Main entrance"):
        if marker in text:
            return f"Honolulu Museum of Art, {marker}" if marker != "Honolulu Museum of Art" else "Honolulu Museum of Art"
    return None


def _extract_hui_article_datetimes(article: BeautifulSoup) -> tuple[datetime | None, datetime | None]:
    gcal_link = article.find("a", href=re.compile(r"google\.com/calendar/event", re.IGNORECASE))
    if gcal_link is not None:
        dates = parse_qs(urlparse(gcal_link.get("href") or "").query).get("dates")
        if dates:
            raw = unquote(dates[0])
            parts = raw.split("/")
            if parts:
                start_at = _parse_gcal_datetime(parts[0])
                end_at = _parse_gcal_datetime(parts[1]) if len(parts) > 1 else None
                return start_at, end_at

    text = _normalize_space(article.get_text(" ", strip=True))
    date_match = re.search(
        r"([A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4})\s+(\d{1,2}:\d{2}\s+[AP]M)\s+(\d{1,2}:\d{2}\s+[AP]M)",
        text,
        re.IGNORECASE,
    )
    if date_match is None:
        return None, None
    base_date = datetime.strptime(date_match.group(1), "%A, %B %d, %Y").date()
    start_at = _combine_local(base_date, date_match.group(2))
    end_at = _combine_local(base_date, date_match.group(3))
    return start_at, end_at


def _extract_capitol_start_end(*, html: str, fallback_text: str) -> tuple[datetime | None, datetime | None]:
    text = _normalize_space(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
    date_match = re.search(
        r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})",
        text,
        re.IGNORECASE,
    )
    if date_match is None:
        date_match = re.search(
            r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})",
            fallback_text,
            re.IGNORECASE,
        )
        if date_match is None:
            return None, None
        month = _month_number(date_match.group(1))
        if month is None:
            return None, None
        base_date = date(int(date_match.group(3)), month, int(date_match.group(2)))
    else:
        month = _month_number(date_match.group(2))
        if month is None:
            return None, None
        base_date = date(int(date_match.group(4)), month, int(date_match.group(3)))

    time_match = TIME_RANGE_RE.search(text)
    if time_match is None:
        start_at = datetime.combine(base_date, time.min)
        return start_at, None

    start_text = _format_time_match(
        hour=time_match.group("start_hour"),
        minute=time_match.group("start_minute"),
        meridiem=time_match.group("start_meridiem") or time_match.group("end_meridiem"),
    )
    end_text = _format_time_match(
        hour=time_match.group("end_hour"),
        minute=time_match.group("end_minute"),
        meridiem=time_match.group("end_meridiem"),
    )
    return _combine_local(base_date, start_text), _combine_local(base_date, end_text)


def _keep_capitol_event(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in (" fundraiser ", " gala ", " music ", " reggae ", " performance ", " market ", " listening session ")):
        return False
    return any(pattern in token_blob for pattern in (" workshop ", " talk ", " lecture ", " class ", " conversation ", " activity "))


def _keep_ehcc_event(title: str, token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in (" exhibition ", " closed ", " film ", " concert ", " performance ")):
        return False
    if any(pattern in token_blob for pattern in (" workshop ", " art party ", " youth arts ", " class ", " classes ")):
        return True
    return False


def _keep_homa_event(token_blob: str) -> bool:
    strong_patterns = (
        " artist talk ",
        " lecture ",
        " conversation ",
        " discussion ",
        " panel ",
        " collection in context ",
        " green room series ",
        " workshop ",
        " class ",
    )
    reject_patterns = (
        " film ",
        " tour ",
        " tours ",
        " performance ",
        " music ",
        " nights ",
        " gala ",
        " reception ",
        " admission ",
    )
    if any(pattern in token_blob for pattern in strong_patterns):
        return True
    if any(pattern in token_blob for pattern in reject_patterns):
        return False
    return False


def _keep_hui_event(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in (" fundraiser ", " gala ", " reception ", " opening reception ", " art affair ", " holidays ")):
        return False
    if any(pattern in token_blob for pattern in (" lecture ", " workshop ", " panel discussion ", " talk story ", " class ", " discussion ")):
        return True
    return False


def _looks_like_bot_challenge(text: str) -> bool:
    normalized = text.lower()
    return any(
        snippet in normalized
        for snippet in (
            "cf-browser-verification",
            "cloudflare",
            "attention required!",
            "just a moment",
            "captcha",
        )
    )


def _safe_json_loads(value: str | None) -> object | None:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _find_event_obj(payload: object) -> dict | None:
    if isinstance(payload, dict):
        payload_type = payload.get("@type")
        if payload_type == "Event":
            return payload
        if isinstance(payload_type, list) and "Event" in payload_type:
            return payload
        for nested in payload.values():
            found = _find_event_obj(nested)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for nested in payload:
            found = _find_event_obj(nested)
            if found is not None:
                return found
    return None


def _extract_meta_description(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for attrs in (
        {"name": "description"},
        {"property": "og:description"},
        {"name": "twitter:description"},
    ):
        tag = soup.find("meta", attrs=attrs)
        if tag is None:
            continue
        content = _normalize_space(tag.get("content"))
        if content:
            return content
    return None


def _parse_gcal_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    try:
        if len(value) == 16 and value.endswith("Z"):
            return datetime.strptime(value, "%Y%m%dT%H%M%SZ")
        if len(value) == 15:
            return datetime.strptime(value, "%Y%m%dT%H%M%S")
        if len(value) == 8:
            parsed_date = datetime.strptime(value, "%Y%m%d").date()
            return datetime.combine(parsed_date, time.min)
    except ValueError:
        return None
    return None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def _node_text(node) -> str:
    if node is None:
        return ""
    return _normalize_space(node.get_text(" ", strip=True))


def _searchable_blob(value: str | None) -> str:
    return f" {_normalize_space(value).lower()} "


def _registration_required(token_blob: str) -> bool:
    if "registration is not required" in token_blob:
        return False
    return any(pattern in token_blob for pattern in (" register ", " registration ", " tickets ", " ticket ", " enroll "))


def _infer_activity_type(token_blob: str) -> str:
    if " lecture " in token_blob or " talk " in token_blob or " discussion " in token_blob or " conversation " in token_blob or " panel " in token_blob:
        return "talk"
    if " class " in token_blob or " workshop " in token_blob or " studio " in token_blob:
        return "workshop"
    return "activity"


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match is not None:
        return int(match.group(1)), int(match.group(2))
    match = AGE_PLUS_RE.search(text)
    if match is not None:
        return int(match.group(1)), None
    return None, None


def _month_number(value: str | None) -> int | None:
    if not value:
        return None
    return MONTH_LOOKUP.get(value.strip()[:3].lower())


def _infer_year_for_month(month: int) -> int:
    return CURRENT_DATE.year + 1 if month < CURRENT_DATE.month - 1 else CURRENT_DATE.year


def _fill_missing_meridiem(start_text: str, end_text: str) -> str:
    if re.search(r"[AaPp]\.?[Mm]\.?", start_text):
        return start_text
    meridiem_match = re.search(r"([AaPp]\.?[Mm]\.?)", end_text)
    if meridiem_match:
        return f"{start_text} {meridiem_match.group(1)}"
    return start_text


def _combine_local(base_date: date, time_text: str) -> datetime | None:
    parsed_time = _parse_time_value(time_text)
    if parsed_time is None:
        return None
    return datetime.combine(base_date, parsed_time)


def _parse_time_value(value: str | None) -> time | None:
    if not value:
        return None
    normalized = value.strip().lower().replace(".", "").replace(" ", "")
    match = re.match(r"(\d{1,2})(?::(\d{2}))?(am|pm)$", normalized)
    if match is None:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    meridiem = match.group(3)
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return time(hour=hour, minute=minute)


def _build_datetime(
    *,
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    meridiem: str,
) -> datetime | None:
    if meridiem.upper() == "PM" and hour != 12:
        hour += 12
    if meridiem.upper() == "AM" and hour == 12:
        hour = 0
    try:
        return datetime(year, month, day, hour, minute)
    except ValueError:
        return None


def _format_time_match(*, hour: str | None, minute: str | None, meridiem: str | None) -> str:
    minute_value = minute or "00"
    meridiem_value = (meridiem or "").replace(" ", "")
    return f"{hour}:{minute_value} {meridiem_value}".strip()


def _weekday_range(start_weekday: int, end_weekday: int) -> set[int]:
    values = {start_weekday}
    current = start_weekday
    while current != end_weekday:
        current = (current + 1) % 7
        values.add(current)
    return values


def _next_month(current: date) -> date:
    if current.month == 12:
        return date(current.year + 1, 1, 1)
    return date(current.year, current.month + 1, 1)


def _nth_weekday_of_month(*, year: int, month: int, weekday: int, occurrence: int) -> date | None:
    current = date(year, month, 1)
    seen = 0
    while current.month == month:
        if current.weekday() == weekday:
            seen += 1
            if seen == occurrence:
                return current
        current += timedelta(days=1)
    return None


def get_hi_html_source_prefixes() -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                f"{urlparse(url).scheme}://{urlparse(url).netloc}{urlparse(url).path.rstrip('/')}"
                for venue in HI_HTML_VENUES
                for url in venue.list_urls
            }
        )
    )
