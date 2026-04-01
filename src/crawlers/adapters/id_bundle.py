from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.parse import urlunparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

ID_TIMEZONE = "America/Boise"

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
    " afternoon art ",
    " art club ",
    " art lab ",
    " artmaking ",
    " beginning drawing ",
    " class ",
    " classes ",
    " clay studio ",
    " conversation ",
    " create ",
    " drawing ",
    " family art ",
    " figure drawing ",
    " lecture ",
    " lectures ",
    " makers night ",
    " museum passport ",
    " open studio ",
    " pottery ",
    " talk ",
    " talks ",
    " tweens ",
    " watercolor ",
    " wheel ",
    " workshop ",
    " workshops ",
    " youth ",
)

WEAK_INCLUDE_PATTERNS = (
    " art ",
    " artist ",
    " drawing ",
    " family ",
    " kid ",
    " kids ",
    " lecture ",
    " talk ",
    " teen ",
    " teens ",
    " tween ",
    " youth ",
)

ALWAYS_REJECT_PATTERNS = (
    " admission ",
    " camp ",
    " camps ",
    " closed ",
    " exhibition ",
    " exhibitions ",
    " film ",
    " gala ",
    " membership ",
    " members only ",
    " movie ",
    " music ",
    " opening reception ",
    " reception ",
    " spring break camp ",
    " storytime ",
    " story time ",
    " tour ",
    " tours ",
    " writing workshop ",
    " yoga ",
)

DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
    " no registration needed ",
)

REGISTRATION_PATTERNS = (
    " preregistration ",
    " pre-registration ",
    " pre registration ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
    " sign up ",
    " ticket ",
    " tickets ",
)

TIME_RANGE_RE = re.compile(
    r"(?<!\d)(?P<start>noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?)\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end>noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))(?!\d)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?<!\d)(noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))(?!\d)",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
YEARS_OLD_RE = re.compile(r"\bages?\s*(\d{1,2})\s*-\s*(\d{1,2})\b", re.IGNORECASE)
URL_DATE_RE = re.compile(r"/(20\d{2}-\d{2}-\d{2})/")
FULL_DATE_TIME_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"([A-Za-z]{3,9})\s+(\d{1,2}),\s+(20\d{2})\s*-\s*"
    r"(noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))"
    r"(?:\s*(?:-|–|—|to)\s*"
    r"(noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)))?",
    re.IGNORECASE,
)
CLASS_DATES_RE = re.compile(
    r"Class Dates:\s*(\d{1,2})/(\d{1,2})/(\d{4})\s+"
    r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*-\s*(\d{1,2}):(\d{2})\s*(AM|PM)",
    re.IGNORECASE,
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


@dataclass(frozen=True, slots=True)
class IdVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    discovery_mode: str
    default_location: str
    discovery_url: str | None = None


ID_VENUES: tuple[IdVenueConfig, ...] = (
    IdVenueConfig(
        slug="boise_art_museum",
        source_name="id_boise_art_museum_events",
        venue_name="Boise Art Museum",
        city="Boise",
        state="ID",
        list_url="https://boiseartmuseum.org/programs-events/",
        discovery_mode="boise_calendar_html",
        default_location="Boise Art Museum, Boise, ID",
    ),
    IdVenueConfig(
        slug="sun_valley_museum_of_art",
        source_name="id_sun_valley_museum_of_art_events",
        venue_name="Sun Valley Museum of Art",
        city="Ketchum",
        state="ID",
        list_url="https://svmoa.org/events",
        discovery_mode="svmoa_html",
        discovery_url=(
            "https://svmoa.org/events?"
            "type%5B16%5D=16&type%5B32%5D=32&type%5B19%5D=19&type%5B3%5D=3"
        ),
        default_location="Sun Valley Museum of Art, Ketchum, ID",
    ),
    IdVenueConfig(
        slug="the_art_museum_of_eastern_idaho",
        source_name="id_art_museum_of_eastern_idaho_events",
        venue_name="The Art Museum of Eastern Idaho",
        city="Idaho Falls",
        state="ID",
        list_url="https://www.theartmuseum.org/Calendar",
        discovery_mode="tam_calendar_html",
        default_location="The Art Museum of Eastern Idaho, Idaho Falls, ID",
    ),
)

ID_VENUES_BY_SLUG = {venue.slug: venue for venue in ID_VENUES}


class IdBundleAdapter(BaseSourceAdapter):
    source_name = "id_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_id_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_id_bundle_payload/parse_id_events from the script runner.")


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
                html = response.text
                if use_playwright_fallback and _looks_like_bot_challenge(html):
                    return await fetch_html_playwright(url)
                return html

            if response.status_code in (403, 429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if use_playwright_fallback:
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
            timezone_id=ID_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def load_id_bundle_payload(
    *,
    venues: list[IdVenueConfig] | tuple[IdVenueConfig, ...] | None = None,
) -> dict[str, dict]:
    selected = list(venues) if venues is not None else list(ID_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.discovery_mode == "boise_calendar_html":
                    payload_by_slug[venue.slug] = await _load_boise_payload(venue, client=client)
                elif venue.discovery_mode == "svmoa_html":
                    payload_by_slug[venue.slug] = await _load_svmoa_payload(venue, client=client)
                elif venue.discovery_mode == "tam_calendar_html":
                    payload_by_slug[venue.slug] = await _load_tam_payload(venue, client=client)
                else:
                    raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[id-bundle-fetch] venue={venue.slug} failed: {exc}")

    return {"payload_by_slug": payload_by_slug, "errors_by_slug": errors_by_slug}


def parse_id_events(payload: dict, *, venue: IdVenueConfig) -> list[ExtractedActivity]:
    if venue.discovery_mode == "boise_calendar_html":
        rows = _parse_boise_events(payload, venue=venue)
    elif venue.discovery_mode == "svmoa_html":
        rows = _parse_svmoa_events(payload, venue=venue)
    elif venue.discovery_mode == "tam_calendar_html":
        rows = _parse_tam_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(ID_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


def get_id_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in ID_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
        if venue.discovery_url:
            discovery_parsed = urlparse(venue.discovery_url)
            prefixes.append(f"{discovery_parsed.scheme}://{discovery_parsed.netloc}/")
    return tuple(dict.fromkeys(prefixes))


async def _load_boise_payload(
    venue: IdVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    events: list[dict[str, str | None]] = []
    seen: set[tuple[str, str]] = set()

    for node in soup.select("div.thumb-event-div"):
        link = node.select_one("a[href*='/bam_class/']")
        if link is None:
            continue
        href = _normalize_url(link.get("href"), venue.list_url)
        day_id = _normalize_space(node.get("id"))
        if not href or not day_id:
            continue
        time_text = _normalize_space(_node_text(link.select_one(".ftcalendar-event-time")))
        title_text = _extract_link_title(link, time_text=time_text)
        if not title_text:
            continue
        key = (href, day_id)
        if key in seen:
            continue
        seen.add(key)
        events.append(
            {
                "source_url": href,
                "date": day_id,
                "title": title_text,
                "time_text": time_text,
            }
        )

    detail_urls = sorted({event["source_url"] for event in events if event.get("source_url")})
    detail_html_by_url = await _fetch_many_html(detail_urls, client=client)
    return {"events": events, "detail_html_by_url": detail_html_by_url}


async def _load_svmoa_payload(
    venue: IdVenueConfig,
    *,
    client: httpx.AsyncClient,
    max_pages: int = 10,
) -> dict:
    start_url = venue.discovery_url or venue.list_url
    queue = [start_url]
    seen_pages: set[str] = set()
    items: list[dict[str, str | None]] = []
    detail_urls: set[str] = set()

    while queue and len(seen_pages) < max_pages:
        page_url = queue.pop(0)
        if page_url in seen_pages:
            continue
        seen_pages.add(page_url)
        html = await fetch_html(page_url, client=client, use_playwright_fallback=True)
        soup = BeautifulSoup(html, "html.parser")

        for article in soup.select("article.event.event-list-mode"):
            link = article.select_one("a.event-link[href]")
            href = _normalize_url(link.get("href") if link else None, venue.list_url)
            if not href:
                continue
            title = _normalize_space(_node_text(article.select_one(".field--name-field-node-title")))
            subtitle = _normalize_space(_node_text(article.select_one(".field--name-field-subtitle")))
            category = _normalize_space(_node_text(article.select_one(".category-title")))
            date_time_text = _normalize_space(_node_text(article.select_one(".field--name-field-date-time")))
            location = _normalize_space(_node_text(article.select_one(".location--title")))
            items.append(
                {
                    "source_url": href,
                    "title": title,
                    "subtitle": subtitle,
                    "category": category,
                    "date_time_text": date_time_text,
                    "location": location,
                }
            )
            detail_urls.add(href)

        next_link = soup.select_one("li.pager__item--next a[href]")
        next_url = _normalize_url(next_link.get("href") if next_link else None, venue.list_url)
        if next_url and next_url not in seen_pages and next_url not in queue:
            queue.append(next_url)

    detail_html_by_url = await _fetch_many_html(sorted(detail_urls), client=client)
    return {"items": items, "detail_html_by_url": detail_html_by_url}


async def _load_tam_payload(
    venue: IdVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    detail_urls: set[str] = set()
    for link in soup.select("a[href]"):
        href = _normalize_url(link.get("href"), venue.list_url)
        if href and "/Products/" in href:
            detail_urls.add(href)
    detail_html_by_url = await _fetch_many_html(sorted(detail_urls), client=client)
    return {"detail_urls": sorted(detail_urls), "detail_html_by_url": detail_html_by_url}


async def _fetch_many_html(
    urls: list[str],
    *,
    client: httpx.AsyncClient,
) -> dict[str, str]:
    async def _fetch_one(url: str) -> tuple[str, str]:
        html = await fetch_html(url, client=client, use_playwright_fallback=True)
        return url, html

    if not urls:
        return {}

    results = await asyncio.gather(*[_fetch_one(url) for url in urls])
    return {url: html for url, html in results}


def _parse_boise_events(payload: dict, *, venue: IdVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for event in payload.get("events") or []:
        source_url = _normalize_space(event.get("source_url"))
        title = _normalize_space(event.get("title"))
        event_date = _parse_iso_date(event.get("date"))
        if not source_url or not title or event_date is None:
            continue

        detail_html = detail_html_by_url.get(source_url) or ""
        detail_soup = BeautifulSoup(detail_html, "html.parser") if detail_html else BeautifulSoup("", "html.parser")
        detail_title = _extract_boise_detail_title(detail_soup)
        if detail_title and not _titles_match(title, detail_title):
            continue

        description = _first_non_empty(
            _extract_meta_content(detail_soup, "og:description"),
            _extract_meta_content(detail_soup, "description"),
            _normalize_space(_node_text(detail_soup.select_one(".entry-content"))),
        )
        time_text = _normalize_space(event.get("time_text"))
        parsed_range = _parse_time_range_on_date(" | ".join(part for part in [description or "", time_text or ""] if part), event_date)
        if parsed_range is None:
            continue
        start_at, end_at = parsed_range

        full_text = " ".join(part for part in [title, description or ""] if part)
        if not _should_keep_event(full_text):
            continue

        age_min, age_max = _parse_age_range(full_text)
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=detail_title or title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(full_text),
                age_min=age_min,
                age_max=age_max,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=ID_TIMEZONE,
                **price_classification_kwargs(full_text),
            )
        )

    return rows


def _parse_svmoa_events(payload: dict, *, venue: IdVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for item in payload.get("items") or []:
        source_url = _normalize_space(item.get("source_url"))
        title = _normalize_space(item.get("title"))
        subtitle = _normalize_space(item.get("subtitle"))
        category = _normalize_space(item.get("category"))
        if not source_url or not title:
            continue

        detail_html = detail_html_by_url.get(source_url) or ""
        detail_soup = BeautifulSoup(detail_html, "html.parser") if detail_html else BeautifulSoup("", "html.parser")
        detail_body = _normalize_space(_node_text(detail_soup.select_one(".field--name-field-wysiwyg")))
        detail_category = _normalize_space(_node_text(detail_soup.select_one(".category-title"))) or category
        detail_dt_text = _normalize_space(_node_text(detail_soup.select_one(".field--name-field-date-time")))
        detail_location = _normalize_space(_node_text(detail_soup.select_one(".field--name-field-location")))
        free_badge = _normalize_space(_node_text(detail_soup.select_one(".free-event")))

        full_text = " ".join(
            part for part in [title, subtitle or "", detail_category or "", detail_dt_text or "", detail_body or ""] if part
        )
        if not _should_keep_event(full_text):
            continue

        parsed_range = _parse_svmoa_datetime(source_url, detail_dt_text or item.get("date_time_text"))
        if parsed_range is None:
            parsed_range = _parse_time_range_from_url_date(
                source_url,
                " | ".join(part for part in [detail_body or "", item.get("date_time_text") or ""] if part),
            )
        if parsed_range is None:
            continue
        start_at, end_at = parsed_range

        description_parts = [subtitle, detail_body]
        if detail_category:
            description_parts.append(f"Category: {detail_category}")
        if free_badge:
            description_parts.append(f"Pricing: {free_badge}")
        description = " | ".join(part for part in description_parts if part) or None
        age_min, age_max = _parse_age_range(" ".join(part for part in [title, subtitle or "", detail_body or ""] if part))

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=detail_location or item.get("location") or venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(" ".join(part for part in [title, detail_category or "", detail_body or ""] if part)),
                age_min=age_min,
                age_max=age_max,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=ID_TIMEZONE,
                **price_classification_kwargs(" ".join(part for part in [description or "", detail_location or "", free_badge or ""] if part)),
            )
        )

    return rows


def _parse_tam_events(payload: dict, *, venue: IdVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for source_url in payload.get("detail_urls") or []:
        detail_html = detail_html_by_url.get(source_url) or ""
        if not detail_html:
            continue
        detail_soup = BeautifulSoup(detail_html, "html.parser")
        title = _first_non_empty(
            _normalize_space(_node_text(detail_soup.select_one("h1"))),
            _extract_meta_content(detail_soup, "og:title"),
            _normalize_space(detail_soup.title.get_text(" ", strip=True) if detail_soup.title else None),
        )
        if not title:
            continue

        text = _normalize_space(detail_soup.get_text(" ", strip=True))
        description = _first_non_empty(
            _extract_meta_content(detail_soup, "og:description"),
            _extract_meta_content(detail_soup, "description"),
            text,
        )
        full_text = " ".join(part for part in [title, description or ""] if part)
        if not _should_keep_event(full_text):
            continue

        parsed_range = _parse_tam_class_dates(text)
        if parsed_range is None:
            parsed_range = _parse_time_range_from_url_date(source_url, description or text)
        if parsed_range is None:
            continue
        start_at, end_at = parsed_range

        age_min, age_max = _parse_age_range(full_text)
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(full_text),
                age_min=age_min,
                age_max=age_max,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=ID_TIMEZONE,
                **price_classification_kwargs(full_text),
            )
        )

    return rows


def _parse_svmoa_datetime(source_url: str, text: str | None) -> tuple[datetime, datetime | None] | None:
    normalized = _normalize_space(text)
    if not normalized:
        return None
    match = FULL_DATE_TIME_RE.search(normalized)
    if match is None:
        return None
    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    if month is None:
        return None
    parsed_date = date(int(match.group(3)), month, int(match.group(2)))
    start_at = _clock_datetime(parsed_date, match.group(4), default_period=_extract_period(match.group(5)))
    end_label = match.group(5)
    end_at = _clock_datetime(parsed_date, end_label) if end_label else None
    return start_at, end_at


def _parse_tam_class_dates(text: str | None) -> tuple[datetime, datetime | None] | None:
    if not text:
        return None
    match = CLASS_DATES_RE.search(text)
    if match is None:
        return None
    event_date = date(int(match.group(3)), int(match.group(1)), int(match.group(2)))
    start_at = datetime.combine(
        event_date,
        _parse_clock_time(f"{match.group(4)}:{match.group(5)} {match.group(6)}"),
        tzinfo=ZoneInfo(ID_TIMEZONE),
    )
    end_at = datetime.combine(
        event_date,
        _parse_clock_time(f"{match.group(7)}:{match.group(8)} {match.group(9)}"),
        tzinfo=ZoneInfo(ID_TIMEZONE),
    )
    return start_at, end_at


def _parse_time_range_from_url_date(source_url: str, text: str | None) -> tuple[datetime, datetime | None] | None:
    event_date = _extract_url_date(source_url)
    if event_date is None:
        return None
    return _parse_time_range_on_date(text, event_date)


def _parse_time_range_on_date(text: str | None, event_date: date) -> tuple[datetime, datetime | None] | None:
    normalized = _normalize_space(text)
    if not normalized:
        return None

    range_match = TIME_RANGE_RE.search(normalized)
    if range_match is not None:
        start_label = range_match.group("start")
        end_label = range_match.group("end")
        end_at = _clock_datetime(event_date, end_label)
        start_at = _clock_datetime(event_date, start_label, default_period=_extract_period(end_label))
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(normalized)
    if single_match is not None:
        return _clock_datetime(event_date, single_match.group(1)), None

    return None


def _clock_datetime(
    event_date: date,
    label: str,
    *,
    default_period: str | None = None,
) -> datetime:
    return datetime.combine(
        event_date,
        _parse_clock_time(label, default_period=default_period),
        tzinfo=ZoneInfo(ID_TIMEZONE),
    )


def _parse_clock_time(label: str, *, default_period: str | None = None) -> time:
    value = _normalize_space(label).lower().replace(".", "")
    if value == "noon":
        return time(12, 0)

    match = re.match(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", value)
    if match is None:
        raise ValueError(f"Unsupported time label: {label}")

    hour = int(match.group(1))
    minute = int(match.group(2) or 0)
    period = match.group(3) or default_period
    if period is None:
        period = "pm" if 7 <= hour <= 11 else "am"

    if period == "am":
        if hour == 12:
            hour = 0
    elif period == "pm":
        if hour != 12:
            hour += 12

    return time(hour, minute)


def _extract_period(label: str | None) -> str | None:
    if not label:
        return None
    match = re.search(r"\b(am|pm)\b", label.lower().replace(".", ""))
    return match.group(1) if match else None


def _extract_link_title(link, *, time_text: str | None = None) -> str | None:
    full_text = _normalize_space(link.get_text(" ", strip=True))
    if not full_text:
        return None
    if time_text:
        time_prefix = _normalize_space(time_text)
        if full_text.startswith(time_prefix):
            full_text = _normalize_space(full_text[len(time_prefix):])
    return full_text


def _extract_boise_detail_title(soup: BeautifulSoup) -> str | None:
    title = _first_non_empty(
        _normalize_space(_node_text(soup.select_one("h1"))),
        _extract_meta_content(soup, "og:title"),
        _normalize_space(soup.title.get_text(" ", strip=True) if soup.title else None),
    )
    if not title:
        return None
    return re.sub(r"\s*-\s*Boise Art Museum\s*$", "", title, flags=re.IGNORECASE).strip()


def _titles_match(left: str, right: str) -> bool:
    left_blob = _searchable_blob(left)
    right_blob = _searchable_blob(right)
    if not left_blob or not right_blob:
        return False
    if left_blob == right_blob or left_blob in right_blob or right_blob in left_blob:
        return True
    left_tokens = set(left_blob.split())
    right_tokens = set(right_blob.split())
    if not left_tokens or not right_tokens:
        return False
    overlap = len(left_tokens & right_tokens)
    return overlap >= max(2, min(len(left_tokens), len(right_tokens)) - 1)


def _should_keep_event(text: str | None) -> bool:
    blob = _searchable_blob(text)
    if not blob:
        return False
    if _contains_any(blob, ALWAYS_REJECT_PATTERNS):
        return False
    if _contains_any(blob, STRONG_INCLUDE_PATTERNS):
        return True
    return _contains_any(blob, WEAK_INCLUDE_PATTERNS)


def _infer_activity_type(text: str | None) -> str | None:
    blob = _searchable_blob(text)
    if not blob:
        return None
    if any(token in blob for token in (" lecture ", " lectures ", " talk ", " talks ", " conversation ")):
        return "lecture"
    return "workshop"


def _registration_required(text: str | None) -> bool | None:
    blob = _searchable_blob(text)
    if not blob:
        return None
    if _contains_any(blob, DROP_IN_PATTERNS):
        return False
    if _contains_any(blob, REGISTRATION_PATTERNS):
        return True
    return None


def _parse_age_range(text: str | None) -> tuple[int | None, int | None]:
    normalized = _normalize_space(text)
    if not normalized:
        return None, None

    for pattern in (AGE_RANGE_RE, YEARS_OLD_RE):
        match = pattern.search(normalized)
        if match is not None:
            return int(match.group(1)), int(match.group(2))

    match = AGE_PLUS_RE.search(normalized)
    if match is not None:
        return int(match.group(1)), None

    if "all ages" in normalized.lower():
        return None, None
    return None, None


def _extract_url_date(source_url: str | None) -> date | None:
    if not source_url:
        return None
    match = URL_DATE_RE.search(source_url)
    if match is None:
        return None
    return _parse_iso_date(match.group(1))


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _looks_like_bot_challenge(html: str | None) -> bool:
    if not html:
        return True
    return any(marker in html for marker in BOT_CHALLENGE_MARKERS)


def _normalize_url(href: str | None, base_url: str) -> str | None:
    normalized = _normalize_space(href)
    if not normalized:
        return None
    absolute = urljoin(base_url, normalized)
    parsed = urlparse(absolute)
    query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", query, ""))


def _extract_meta_content(soup: BeautifulSoup, property_name: str) -> str | None:
    node = soup.select_one(f'meta[property="{property_name}"], meta[name="{property_name}"]')
    if node is None:
        return None
    return _normalize_space(node.get("content"))


def _contains_any(text: str | None, patterns: tuple[str, ...]) -> bool:
    blob = _searchable_blob(text)
    return any(pattern in blob for pattern in patterns)


def _searchable_blob(text: str | None) -> str:
    if not text:
        return ""
    normalized = re.sub(r"[^a-z0-9]+", " ", text.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(text: str | None) -> str | None:
    if text is None:
        return None
    normalized = " ".join(str(text).split())
    return normalized or None


def _node_text(node) -> str | None:
    if node is None:
        return None
    return node.get_text(" ", strip=True)


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        normalized = _normalize_space(value)
        if normalized:
            return normalized
    return None
