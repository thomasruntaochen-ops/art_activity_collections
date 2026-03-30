from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from urllib.parse import parse_qsl
from urllib.parse import quote
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

IL_TIMEZONE = "America/Chicago"

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
    " artmaking ",
    " artist talk ",
    " class ",
    " classes ",
    " conversation ",
    " create ",
    " discussion ",
    " drop in ",
    " family day ",
    " family program ",
    " family workshop ",
    " hands on ",
    " kids ",
    " lecture ",
    " open studio ",
    " panel ",
    " poetry workshop ",
    " talk ",
    " teen ",
    " workshop ",
    " youth ",
)

ABSOLUTE_REJECT_PATTERNS = (
    " application ",
    " cinema ",
    " deadline ",
    " film ",
    " gala ",
    " late night ",
    " movie ",
    " opening celebration ",
    " opening reception ",
    " parade ",
    " party ",
    " performance ",
    " screening ",
    " series schedule ",
    " trip ",
)

WEAK_INCLUDE_PATTERNS = (
    " art ",
    " family ",
    " kid ",
    " lecture ",
    " maker ",
    " student ",
    " talk ",
    " teen ",
    " youth ",
)

HARD_REJECT_PATTERNS = (
    " exhibition ",
    " exhibitions ",
    " fundraiser ",
)

SOFT_REJECT_PATTERNS = (
    " after dark ",
    " art adventure ",
    " festival ",
    " member preview ",
    " members only ",
    " reception ",
    " tour ",
    " tours ",
)

DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
    " no registration required ",
    " no registration needed ",
)

REGISTRATION_PATTERNS = (
    " purchase tickets ",
    " register ",
    " registration ",
    " reservations required ",
    " reserve ",
    " rsvp ",
    " sign up ",
    " ticket ",
    " tickets ",
)

TIME_RANGE_RE = re.compile(
    r"(?<![\d:])(?P<start>noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?)\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end>noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))(?![\d:])",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?<![\d:])(noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))(?![\d:])",
    re.IGNORECASE,
)
DATE_IN_TEXT_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|Mon\.?|Tue\.?|Tues\.?|"
    r"Wed\.?|Thu\.?|Thurs\.?|Fri\.?|Sat\.?|Sun\.?)?,?\s*"
    r"([A-Za-z]{3,9})\s+(\d{1,2}),\s*(20\d{2})(.*)",
    re.IGNORECASE,
)
DATE_NO_YEAR_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|Mon\.?|Tue\.?|Tues\.?|"
    r"Wed\.?|Thu\.?|Thurs\.?|Fri\.?|Sat\.?|Sun\.?)?,?\s*"
    r"([A-Za-z]{3,9})\s+(\d{1,2})(.*)",
    re.IGNORECASE,
)
MDY_TIME_RANGE_RE = re.compile(
    r"(\d{2})\.(\d{2})\.(20\d{2})\s+(\d{1,2}:\d{2}\s*[ap]\.?m\.?)\s*-\s*"
    r"(\d{2})\.(\d{2})\.(20\d{2})\s+(\d{1,2}:\d{2}\s*[ap]\.?m\.?)",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
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
class IlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    discovery_mode: str
    default_location: str
    discovery_url: str | None = None


IL_VENUES: tuple[IlVenueConfig, ...] = (
    IlVenueConfig(
        slug="art_institute_of_chicago",
        source_name="il_art_institute_of_chicago_events",
        venue_name="Art Institute of Chicago",
        city="Chicago",
        state="IL",
        list_url="https://www.artic.edu/events",
        discovery_mode="artic_html",
        discovery_url="https://www.artic.edu/events?audience=1",
        default_location="Art Institute of Chicago, Chicago, IL",
    ),
    IlVenueConfig(
        slug="block_museum",
        source_name="il_block_museum_events",
        venue_name="Mary and Leigh Block Museum of Art",
        city="Evanston",
        state="IL",
        list_url="https://www.blockmuseum.northwestern.edu/events/",
        discovery_mode="block_html",
        default_location="Mary and Leigh Block Museum of Art, Evanston, IL",
    ),
    IlVenueConfig(
        slug="elmhurst_art_museum",
        source_name="il_elmhurst_art_museum_events",
        venue_name="Elmhurst Art Museum",
        city="Elmhurst",
        state="IL",
        list_url="https://elmhurstartmuseum.org/events",
        discovery_mode="elmhurst_next",
        default_location="Elmhurst Art Museum, Elmhurst, IL",
    ),
    IlVenueConfig(
        slug="krannert_art_museum",
        source_name="il_krannert_art_museum_events",
        venue_name="Krannert Art Museum",
        city="Champaign",
        state="IL",
        list_url="https://kam.illinois.edu/exhibitions-events/events",
        discovery_mode="krannert_html",
        default_location="Krannert Art Museum, Champaign, IL",
    ),
    IlVenueConfig(
        slug="lizzadro_museum_of_lapidary_art",
        source_name="il_lizzadro_museum_events",
        venue_name="Lizzadro Museum of Lapidary Art",
        city="Oak Brook",
        state="IL",
        list_url="https://lizzadromuseum.org/event/",
        discovery_mode="lizzadro_html",
        default_location="Lizzadro Museum of Lapidary Art, Oak Brook, IL",
    ),
    IlVenueConfig(
        slug="mca_chicago",
        source_name="il_mca_chicago_events",
        venue_name="Museum of Contemporary Art Chicago",
        city="Chicago",
        state="IL",
        list_url="https://visit.mcachicago.org/events/",
        discovery_mode="mca_html",
        default_location="Museum of Contemporary Art Chicago, Chicago, IL",
    ),
    IlVenueConfig(
        slug="mocp",
        source_name="il_mocp_events",
        venue_name="Museum of Contemporary Photography",
        city="Chicago",
        state="IL",
        list_url="https://www.mocp.org/events/",
        discovery_mode="mocp_html",
        default_location="Museum of Contemporary Photography, Chicago, IL",
    ),
    IlVenueConfig(
        slug="national_museum_of_mexican_art",
        source_name="il_nmma_events",
        venue_name="National Museum of Mexican Art",
        city="Chicago",
        state="IL",
        list_url="https://nationalmuseumofmexicanart.org/events",
        discovery_mode="nmma_html",
        discovery_url="https://nationalmuseumofmexicanart.org/events/upcoming",
        default_location="National Museum of Mexican Art, Chicago, IL",
    ),
    IlVenueConfig(
        slug="smart_museum_of_art",
        source_name="il_smart_museum_events",
        venue_name="Smart Museum of Art",
        city="Chicago",
        state="IL",
        list_url="https://smartmuseum.uchicago.edu/public-practice/calendar-of-events/",
        discovery_mode="smart_html",
        default_location="Smart Museum of Art, Chicago, IL",
    ),
)

IL_VENUES_BY_SLUG = {venue.slug: venue for venue in IL_VENUES}


class IlBundleAdapter(BaseSourceAdapter):
    source_name = "il_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_il_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_il_bundle_payload/parse_il_events from the script runner.")


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
            timezone_id=IL_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def load_il_bundle_payload(
    *,
    venues: list[IlVenueConfig] | tuple[IlVenueConfig, ...] | None = None,
) -> dict[str, dict]:
    selected = list(venues) if venues is not None else list(IL_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.discovery_mode == "artic_html":
                    payload_by_slug[venue.slug] = await _load_artic_payload(venue, client=client)
                elif venue.discovery_mode == "block_html":
                    payload_by_slug[venue.slug] = await _load_block_payload(venue, client=client)
                elif venue.discovery_mode == "elmhurst_next":
                    payload_by_slug[venue.slug] = await _load_elmhurst_payload(venue, client=client)
                elif venue.discovery_mode == "krannert_html":
                    payload_by_slug[venue.slug] = await _load_krannert_payload(venue, client=client)
                elif venue.discovery_mode == "lizzadro_html":
                    payload_by_slug[venue.slug] = await _load_lizzadro_payload(venue, client=client)
                elif venue.discovery_mode == "mca_html":
                    payload_by_slug[venue.slug] = await _load_mca_payload(venue, client=client)
                elif venue.discovery_mode == "mocp_html":
                    payload_by_slug[venue.slug] = await _load_mocp_payload(venue, client=client)
                elif venue.discovery_mode == "nmma_html":
                    payload_by_slug[venue.slug] = await _load_nmma_payload(venue, client=client)
                elif venue.discovery_mode == "smart_html":
                    payload_by_slug[venue.slug] = await _load_smart_payload(venue, client=client)
                else:
                    raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[il-bundle-fetch] venue={venue.slug} failed: {exc}")

    return {"payload_by_slug": payload_by_slug, "errors_by_slug": errors_by_slug}


def parse_il_events(payload: dict, *, venue: IlVenueConfig) -> list[ExtractedActivity]:
    if venue.discovery_mode == "artic_html":
        rows = _parse_artic_events(payload, venue=venue)
    elif venue.discovery_mode == "block_html":
        rows = _parse_block_events(payload, venue=venue)
    elif venue.discovery_mode == "elmhurst_next":
        rows = _parse_elmhurst_events(payload, venue=venue)
    elif venue.discovery_mode == "krannert_html":
        rows = _parse_krannert_events(payload, venue=venue)
    elif venue.discovery_mode == "lizzadro_html":
        rows = _parse_lizzadro_events(payload, venue=venue)
    elif venue.discovery_mode == "mca_html":
        rows = _parse_mca_events(payload, venue=venue)
    elif venue.discovery_mode == "mocp_html":
        rows = _parse_mocp_events(payload, venue=venue)
    elif venue.discovery_mode == "nmma_html":
        rows = _parse_nmma_events(payload, venue=venue)
    elif venue.discovery_mode == "smart_html":
        rows = _parse_smart_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(IL_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


def get_il_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in IL_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
        if venue.discovery_url:
            discovery_parsed = urlparse(venue.discovery_url)
            prefixes.append(f"{discovery_parsed.scheme}://{discovery_parsed.netloc}/")
    return tuple(dict.fromkeys(prefixes))


async def _load_artic_payload(
    venue: IlVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.discovery_url or venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str | None]] = []
    seen: set[str] = set()

    for card in soup.select("a.m-listing__link[href]"):
        href = _normalize_url(card.get("href"), venue.list_url)
        if not href or href in seen:
            continue
        seen.add(href)
        items.append(
            {
                "source_url": href,
                "card_text": _normalize_space(card.get_text(" ", strip=True)),
            }
        )

    detail_html_by_url = await _fetch_many_html([item["source_url"] for item in items if item.get("source_url")], client=client)
    return {"items": items, "detail_html_by_url": detail_html_by_url}


async def _load_block_payload(
    venue: IlVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str | None]] = []
    seen: set[str] = set()

    for card in soup.select("#event-results a.event.small[href]"):
        href = _normalize_url(card.get("href"), venue.list_url)
        if not href or href in seen:
            continue
        seen.add(href)
        items.append(
            {
                "source_url": href,
                "card_text": _normalize_space(card.get_text(" ", strip=True)),
            }
        )

    detail_html_by_url = await _fetch_many_html([item["source_url"] for item in items if item.get("source_url")], client=client)
    return {"items": items, "detail_html_by_url": detail_html_by_url}


async def _load_elmhurst_payload(
    venue: IlVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    next_data = soup.select_one("#__NEXT_DATA__")
    if next_data is None:
        raise RuntimeError("Elmhurst page missing __NEXT_DATA__ payload")

    data = json.loads(next_data.get_text())
    entries: list[dict[str, str | None]] = []
    seen: set[tuple[str | None, str | None]] = set()

    def walk(obj: object) -> None:
        if isinstance(obj, dict):
            if obj.get("title") and obj.get("startDate"):
                key = (obj.get("title"), obj.get("startDate"))
                if key not in seen:
                    seen.add(key)
                    entries.append(obj)
            for value in obj.values():
                walk(value)
        elif isinstance(obj, list):
            for value in obj:
                walk(value)

    walk(data)
    return {"entries": entries}


async def _load_krannert_payload(
    venue: IlVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str | None]] = []
    seen: set[str] = set()

    for row in soup.select("div.views-row"):
        link = row.select_one("a[href]")
        href = _normalize_url(link.get("href") if link else None, venue.list_url)
        if not href or href in seen:
            continue
        seen.add(href)
        items.append(
            {
                "source_url": href,
                "card_text": _normalize_space(row.get_text(" ", strip=True)),
            }
        )

    detail_html_by_url = await _fetch_many_html([item["source_url"] for item in items if item.get("source_url")], client=client)
    return {"items": items, "detail_html_by_url": detail_html_by_url}


async def _load_lizzadro_payload(
    venue: IlVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str | None]] = []
    seen: set[str] = set()

    for article in soup.select("article.ecwd_event"):
        link = article.select_one("h2.entry-title a[href]")
        href = _normalize_url(link.get("href") if link else None, venue.list_url)
        if not href or href in seen:
            continue
        seen.add(href)
        items.append(
            {
                "source_url": href,
                "card_text": _normalize_space(article.get_text(" ", strip=True)),
            }
        )

    detail_html_by_url = await _fetch_many_html([item["source_url"] for item in items if item.get("source_url")], client=client)
    return {"items": items, "detail_html_by_url": detail_html_by_url}


async def _load_mca_payload(
    venue: IlVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str | None]] = []
    seen: set[str] = set()

    for card in soup.select("a.event-preview[href]"):
        href = _normalize_url(card.get("href"), venue.list_url)
        if not href or href in seen:
            continue
        seen.add(href)
        items.append(
            {
                "source_url": href,
                "program": _normalize_space(card.get("data-program")),
                "card_text": _normalize_space(card.get_text(" ", strip=True)),
            }
        )

    detail_html_by_url = await _fetch_many_html([item["source_url"] for item in items if item.get("source_url")], client=client)
    return {"items": items, "detail_html_by_url": detail_html_by_url}


async def _load_mocp_payload(
    venue: IlVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str | None]] = []
    detail_urls: list[str] = []

    for card in soup.select("div.connections-link"):
        type_text = _normalize_space(_node_text(card.select_one(".connections-link__type")))
        link = card.select_one("a.connections-link__link--horizontal[href]")
        href = _normalize_url(link.get("href") if link else None, venue.list_url)
        if not href or "upcoming" not in (type_text or "").lower():
            continue
        items.append(
            {
                "source_url": href,
                "card_text": _normalize_space(card.get_text(" ", strip=True)),
            }
        )
        detail_urls.append(href)

    detail_html_by_url = await _fetch_many_html(detail_urls, client=client)
    return {"items": items, "detail_html_by_url": detail_html_by_url}


async def _load_nmma_payload(
    venue: IlVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.discovery_url or venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str | None]] = []
    seen: set[str] = set()

    for link in soup.select("a[href*='/events/']"):
        href = _normalize_url(link.get("href"), venue.list_url)
        if not href or href in seen:
            continue
        if href.rstrip("/") in {
            venue.list_url.rstrip("/"),
            f"{venue.list_url.rstrip('/')}/upcoming",
            f"{venue.list_url.rstrip('/')}/archive",
        }:
            continue
        if "?" in href:
            continue
        seen.add(href)
        items.append(
            {
                "source_url": href,
                "card_text": _normalize_space(link.get_text(" ", strip=True)),
            }
        )

    detail_html_by_url = await _fetch_many_html([item["source_url"] for item in items if item.get("source_url")], client=client)
    return {"items": items, "detail_html_by_url": detail_html_by_url}


async def _load_smart_payload(
    venue: IlVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict:
    html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str | None]] = []
    seen: set[str] = set()

    for card in soup.select("li.calendar-event"):
        link = card.select_one("h2 a[href]")
        href = _normalize_url(link.get("href") if link else None, venue.list_url)
        if not href or href in seen:
            continue
        seen.add(href)
        items.append(
            {
                "source_url": href,
                "card_text": _normalize_space(card.get_text(" ", strip=True)),
            }
        )

    detail_html_by_url = await _fetch_many_html([item["source_url"] for item in items if item.get("source_url")], client=client)
    return {"items": items, "detail_html_by_url": detail_html_by_url}


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


def _parse_artic_events(payload: dict, *, venue: IlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for item in payload.get("items") or []:
        source_url = _normalize_space(item.get("source_url"))
        detail_html = detail_html_by_url.get(source_url or "") or ""
        if not source_url or not detail_html:
            continue

        soup = BeautifulSoup(detail_html, "html.parser")
        title = _clean_artic_title(
            _first_non_empty(
                _normalize_space(_node_text(soup.select_one("h1"))),
                _extract_meta_content(soup, "og:title"),
                _normalize_space(soup.title.get_text(" ", strip=True) if soup.title else None),
            )
        )
        if not title:
            continue

        meta = _parse_json_object(_node_text(soup.select_one("#page-meta-data"))) or {}
        event_date = _parse_iso_date(meta.get("date"))
        parsed_range = _parse_time_range_on_date(meta.get("time"), event_date) if event_date else None
        if parsed_range is None:
            parsed_range = _parse_explicit_datetime_range(_normalize_space(_node_text(soup.select_one("p.date"))))
        if parsed_range is None:
            continue
        start_at, end_at = parsed_range

        description = _first_non_empty(
            _normalize_space(_node_text(soup.select_one(".o-article__body"))),
            _extract_meta_content(soup, "description"),
            _extract_meta_content(soup, "og:description"),
        )
        type_text = _normalize_space(_node_text(soup.select_one("p.type")))
        location_text = _first_non_empty(
            _normalize_space(meta.get("location")),
            _normalize_space(_node_text(soup.select_one("[itemprop='location']"))),
            venue.default_location,
        )
        full_text = join_non_empty([title, type_text, description, item.get("card_text"), location_text])
        if not _should_keep_event(full_text):
            continue

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(join_non_empty([type_text, title, description])),
                age_min=None,
                age_max=None,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_parse_yes_no(meta.get("registration-required")) or _registration_required(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=IL_TIMEZONE,
                **price_classification_kwargs(full_text, default_is_free=None),
            )
        )

    return rows


def _parse_block_events(payload: dict, *, venue: IlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for item in payload.get("items") or []:
        source_url = _normalize_space(item.get("source_url"))
        detail_html = detail_html_by_url.get(source_url or "") or ""
        if not source_url or not detail_html:
            continue

        soup = BeautifulSoup(detail_html, "html.parser")
        title = _first_non_empty(
            _normalize_space(_node_text(soup.select_one("#page-top h1"))),
            _extract_meta_content(soup, "og:title"),
        )
        tag_text = _normalize_space(_node_text(soup.select_one(".date .tag")))
        if not title:
            continue
        if "/cinema/" in source_url or "cinema" in (tag_text or "").lower():
            continue

        date_column = soup.select_one("#event-info .event-column")
        parsed_range = _parse_explicit_datetime_range(_normalize_space(_node_text(date_column.select_one("p") if date_column else None)))
        if parsed_range is None:
            continue
        start_at, end_at = parsed_range

        columns = soup.select("#event-info .event-column")
        location_text = venue.default_location
        if len(columns) > 1:
            location_text = _normalize_space(columns[1].get_text(" ", strip=True)) or venue.default_location
        detail_block = soup.select_one(".event-row")
        description = _normalize_space(detail_block.get_text(" ", strip=True) if detail_block else None)
        full_text = join_non_empty([title, tag_text, description, item.get("card_text"), location_text])
        if not _should_keep_event(full_text):
            continue

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(full_text),
                age_min=None,
                age_max=None,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=IL_TIMEZONE,
                **price_classification_kwargs(full_text, default_is_free=None),
            )
        )

    return rows


def _parse_elmhurst_events(payload: dict, *, venue: IlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []

    for entry in payload.get("entries") or []:
        title = _normalize_space(entry.get("title"))
        start_at = _parse_iso_datetime(entry.get("startDate"))
        if not title or start_at is None:
            continue

        end_at = _parse_iso_datetime(entry.get("endDate"))
        source_url = _normalize_elmhurst_url(entry.get("slug"), venue.list_url)
        description = join_non_empty(
            [
                entry.get("excerpt"),
                entry.get("category"),
                entry.get("additionalInfo"),
                _strip_html(entry.get("markdownBody")),
                entry.get("ctaLabel"),
            ]
        )
        full_text = join_non_empty([title, description])
        if not _should_keep_event(full_text):
            continue

        rows.append(
            ExtractedActivity(
                source_url=source_url or venue.list_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(full_text),
                age_min=None,
                age_max=None,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(join_non_empty([description, entry.get("ctaLabel")])),
                start_at=start_at.astimezone(ZoneInfo(IL_TIMEZONE)),
                end_at=end_at.astimezone(ZoneInfo(IL_TIMEZONE)) if end_at else None,
                timezone=IL_TIMEZONE,
                **price_classification_kwargs(join_non_empty([description, entry.get("ctaLabel")])),
            )
        )

    return rows


def _parse_krannert_events(payload: dict, *, venue: IlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for item in payload.get("items") or []:
        source_url = _normalize_space(item.get("source_url"))
        detail_html = detail_html_by_url.get(source_url or "") or ""
        if not source_url or not detail_html:
            continue

        soup = BeautifulSoup(detail_html, "html.parser")
        title = _first_non_empty(
            _normalize_space(_node_text(soup.select_one("h1 span"))),
            _extract_meta_content(soup, "og:title"),
        )
        session_text = _normalize_space(_node_text(soup.select_one(".session")))
        if not title or not session_text:
            continue

        parsed_range = _parse_explicit_datetime_range(session_text)
        if parsed_range is None:
            continue
        start_at, end_at = parsed_range

        type_text = _normalize_space(_node_text(soup.select_one(".type")))
        location_text = _first_non_empty(_normalize_space(_node_text(soup.select_one(".location"))), venue.default_location)
        description = _first_non_empty(
            _normalize_space(_node_text(soup.select_one(".paragraph-body.body .field-body-copy"))),
            _normalize_space(_node_text(soup.select_one(".field-body-copy"))),
            _normalize_space(_node_text(soup.select_one(".field-body"))),
        )
        full_text = join_non_empty([title, type_text, description, item.get("card_text"), location_text])
        if not _should_keep_event(full_text):
            continue

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(join_non_empty([type_text, title, description])),
                age_min=None,
                age_max=None,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=IL_TIMEZONE,
                **price_classification_kwargs(full_text, default_is_free=None),
            )
        )

    return rows


def _parse_lizzadro_events(payload: dict, *, venue: IlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for item in payload.get("items") or []:
        source_url = _normalize_space(item.get("source_url"))
        detail_html = detail_html_by_url.get(source_url or "") or ""
        if not source_url or not detail_html:
            continue

        soup = BeautifulSoup(detail_html, "html.parser")
        title = _normalize_space(_node_text(soup.select_one("h1.ecwd-events-single-event-title")))
        if not title:
            continue

        date_text = _normalize_space(_node_text(soup.select_one(".ecwd-event-date")))
        parsed_range = _parse_lizzadro_datetime(date_text)
        if parsed_range is None:
            continue
        start_at, end_at = parsed_range

        description = _normalize_space(_node_text(soup.select_one(".entry-content")))
        location_text = _first_non_empty(_normalize_space(_node_text(soup.select_one(".event-venue"))), venue.default_location)
        full_text = join_non_empty([title, description, item.get("card_text"), location_text])
        if not _should_keep_event(full_text):
            continue

        age_min, age_max = _parse_age_range(full_text)
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(full_text),
                age_min=age_min,
                age_max=age_max,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=IL_TIMEZONE,
                **price_classification_kwargs(full_text, default_is_free=None),
            )
        )

    return rows


def _parse_mca_events(payload: dict, *, venue: IlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for item in payload.get("items") or []:
        source_url = _normalize_space(item.get("source_url"))
        detail_html = detail_html_by_url.get(source_url or "") or ""
        if not source_url or not detail_html:
            continue

        soup = BeautifulSoup(detail_html, "html.parser")
        title = _normalize_space(_node_text(soup.select_one("h1")))
        spans = soup.select("h2 span")
        date_text = _normalize_space(_node_text(spans[0] if len(spans) > 0 else None))
        time_text = _normalize_space(_node_text(spans[1] if len(spans) > 1 else None))
        event_date = _parse_month_day_year(date_text)
        parsed_range = _parse_time_range_on_date(time_text, event_date) if event_date else None
        if not title or parsed_range is None:
            continue
        start_at, end_at = parsed_range

        ticket_notice = _normalize_space(_node_text(soup.select_one(".ticket-notice")))
        description = _normalize_space(_node_text(soup.select_one(".wysiwyg-section")))
        full_text = join_non_empty([title, item.get("program"), description, ticket_notice, item.get("card_text")])
        if not _should_keep_event(full_text):
            continue

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
                age_min=0 if "under 12" in (description or "").lower() else None,
                age_max=12 if "under 12" in (description or "").lower() else None,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(join_non_empty([ticket_notice, description])),
                start_at=start_at,
                end_at=end_at,
                timezone=IL_TIMEZONE,
                **price_classification_kwargs(join_non_empty([ticket_notice, description]), default_is_free=None),
            )
        )

    return rows


def _parse_mocp_events(payload: dict, *, venue: IlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for item in payload.get("items") or []:
        source_url = _normalize_space(item.get("source_url"))
        detail_html = detail_html_by_url.get(source_url or "") or ""
        if not source_url or not detail_html:
            continue

        soup = BeautifulSoup(detail_html, "html.parser")
        title = _first_non_empty(
            _normalize_space(_node_text(soup.select_one("h1"))),
            _extract_meta_content(soup, "og:title"),
        )
        time_text = _normalize_space(_node_text(soup.select_one("time")))
        parsed_range = _parse_explicit_datetime_range(time_text)
        if not title or parsed_range is None:
            continue
        start_at, end_at = parsed_range

        description = _normalize_space(_node_text(soup.select_one(".site-main")))
        schema_event = _extract_event_schema(soup)
        location_text = _first_non_empty(schema_event.get("location_name"), venue.default_location)
        full_text = join_non_empty([title, description, item.get("card_text"), location_text])
        if not _should_keep_event(full_text):
            continue

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(full_text),
                age_min=None,
                age_max=None,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=IL_TIMEZONE,
                **price_classification_kwargs(full_text, default_is_free=None),
            )
        )

    return rows


def _parse_nmma_events(payload: dict, *, venue: IlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for item in payload.get("items") or []:
        source_url = _normalize_space(item.get("source_url"))
        detail_html = detail_html_by_url.get(source_url or "") or ""
        if not source_url or not detail_html:
            continue

        soup = BeautifulSoup(detail_html, "html.parser")
        title = _first_non_empty(
            _normalize_space(_node_text(soup.select_one(".f-heading-6"))),
            _normalize_space(_node_text(soup.select_one("title"))),
        )
        datetime_text = _normalize_space(_node_text(soup.select_one(".f-heading-1")))
        parsed_range = _parse_nmma_datetime(datetime_text)
        if not title or parsed_range is None:
            continue
        start_at, end_at = parsed_range

        location_text = _first_non_empty(_normalize_space(_node_text(soup.select_one(".f-body-3"))), venue.default_location)
        description = _normalize_space(_node_text(soup.select_one("main")))
        free_text = _normalize_space(_node_text(soup.find(string=re.compile(r"^\s*Free\s*$", re.IGNORECASE)).parent if soup.find(string=re.compile(r"^\s*Free\s*$", re.IGNORECASE)) else None))
        full_text = join_non_empty([title, description, free_text, location_text, item.get("card_text")])
        if not _should_keep_event(full_text):
            continue

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(full_text),
                age_min=None,
                age_max=None,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=IL_TIMEZONE,
                **price_classification_kwargs(join_non_empty([description, free_text]), default_is_free=None),
            )
        )

    return rows


def _parse_smart_events(payload: dict, *, venue: IlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for item in payload.get("items") or []:
        source_url = _normalize_space(item.get("source_url"))
        detail_html = detail_html_by_url.get(source_url or "") or ""
        if not source_url or not detail_html:
            continue

        soup = BeautifulSoup(detail_html, "html.parser")
        title = _first_non_empty(
            _normalize_space(_node_text(soup.select_one("h1.calendar-title"))),
            _extract_meta_content(soup, "og:title"),
        )
        detail_date = _normalize_space(_node_text(soup.select_one("p.detail-date")))
        parsed_range = _parse_smart_datetime(detail_date)
        if not title or parsed_range is None:
            continue
        start_at, end_at = parsed_range

        description = _normalize_space(_node_text(soup.select_one(".event-summary")))
        location_text = _first_non_empty(
            _normalize_space(_node_text(soup.select_one(".event-location"))),
            venue.default_location,
        )
        free_text = _normalize_space(_node_text(soup.select_one(".free-open")))
        full_text = join_non_empty([title, description, free_text, item.get("card_text"), location_text])
        if not _should_keep_event(full_text):
            continue

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(full_text),
                age_min=None,
                age_max=None,
                drop_in=_contains_any(full_text, DROP_IN_PATTERNS),
                registration_required=_registration_required(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=IL_TIMEZONE,
                **price_classification_kwargs(join_non_empty([description, free_text]), default_is_free=None),
            )
        )

    return rows


def _parse_explicit_datetime_range(text: str | None) -> tuple[datetime, datetime | None] | None:
    normalized = _normalize_datetime_text(text)
    if not normalized:
        return None

    match = DATE_IN_TEXT_RE.search(normalized)
    if match is not None:
        month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
        if month is None:
            return None
        event_date = date(int(match.group(3)), month, int(match.group(2)))
        return _parse_time_range_on_date(match.group(4), event_date)

    return None


def _parse_nmma_datetime(text: str | None) -> tuple[datetime, datetime | None] | None:
    normalized = _normalize_datetime_text(text)
    if not normalized:
        return None
    match = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s*20\d{2}),\s*(.+)$", normalized)
    if match is None:
        return None
    event_date = _parse_month_day_year(match.group(1))
    if event_date is None:
        return None
    return _parse_time_range_on_date(match.group(2), event_date)


def _parse_smart_datetime(text: str | None) -> tuple[datetime, datetime | None] | None:
    normalized = _normalize_datetime_text(text)
    if not normalized:
        return None
    parts = [part.strip() for part in normalized.split(" ") if part.strip()]
    if not parts:
        return None
    lines = [part.strip() for part in normalized.split(" ") if part.strip()]
    match = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s*20\d{2})\s+(.+)$", normalized)
    if match is None:
        return None
    event_date = _parse_month_day_year(match.group(1))
    if event_date is None:
        return None
    return _parse_time_range_on_date(match.group(2), event_date)


def _parse_lizzadro_datetime(text: str | None) -> tuple[datetime, datetime | None] | None:
    normalized = _normalize_datetime_text(text)
    if not normalized:
        return None

    match = MDY_TIME_RANGE_RE.search(normalized)
    if match is None:
        return None

    start_date = date(int(match.group(3)), int(match.group(1)), int(match.group(2)))
    end_date = date(int(match.group(7)), int(match.group(5)), int(match.group(6)))
    start_at = _clock_datetime(start_date, match.group(4))
    end_at = _clock_datetime(end_date, match.group(8))
    return start_at, end_at


def _parse_month_day_year(text: str | None) -> date | None:
    normalized = _normalize_datetime_text(text)
    if not normalized:
        return None
    match = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2}),\s*(20\d{2})", normalized)
    if match is None:
        return None
    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    if month is None:
        return None
    return date(int(match.group(3)), month, int(match.group(2)))


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _parse_time_range_on_date(text: str | None, event_date: date | None) -> tuple[datetime, datetime | None] | None:
    if event_date is None:
        return None
    normalized = _normalize_datetime_text(text)
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
        tzinfo=ZoneInfo(IL_TIMEZONE),
    )


def _parse_clock_time(label: str, *, default_period: str | None = None) -> time:
    value = _normalize_datetime_text(label).lower().replace(".", "")
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
    match = re.search(r"(am|pm)\b", label.lower().replace(".", ""))
    return match.group(1) if match else None


def _should_keep_event(text: str | None) -> bool:
    blob = _searchable_blob(text)
    if not blob:
        return False

    if _contains_any(blob, ABSOLUTE_REJECT_PATTERNS):
        return False

    strong_include = _contains_any(blob, STRONG_INCLUDE_PATTERNS)
    hard_reject = _contains_any(blob, HARD_REJECT_PATTERNS)
    soft_reject = _contains_any(blob, SOFT_REJECT_PATTERNS)

    if hard_reject and not strong_include:
        return False
    if strong_include and not any(token in blob for token in (" opening reception ", " gala ", " movie ", " film ", " party ")):
        return True
    if soft_reject:
        return False
    return _contains_any(blob, WEAK_INCLUDE_PATTERNS)


def _infer_activity_type(text: str | None) -> str | None:
    blob = _searchable_blob(text)
    if not blob:
        return None
    if any(token in blob for token in (" lecture ", " panel ", " conversation ", " discussion ", " talk ")):
        return "lecture"
    return "workshop"


def _registration_required(text: str | None) -> bool | None:
    blob = _searchable_blob(text)
    if not blob:
        return None
    if _contains_any(blob, DROP_IN_PATTERNS):
        return False
    if " not required " in blob or " no rsvp required " in blob:
        return False
    if _contains_any(blob, REGISTRATION_PATTERNS):
        return True
    return None


def _parse_age_range(text: str | None) -> tuple[int | None, int | None]:
    normalized = _normalize_space(text)
    if not normalized:
        return None, None

    match = AGE_RANGE_RE.search(normalized)
    if match is not None:
        return int(match.group(1)), int(match.group(2))

    match = AGE_PLUS_RE.search(normalized)
    if match is not None:
        return int(match.group(1)), None

    if "all ages" in normalized.lower():
        return None, None
    return None, None


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _extract_event_schema(soup: BeautifulSoup) -> dict[str, str]:
    for node in soup.select('script[type="application/ld+json"]'):
        raw = _node_text(node)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for candidate in _iter_json_objects(data):
            if candidate.get("@type") == "Event":
                location = candidate.get("location") or {}
                return {
                    "location_name": _normalize_space(location.get("name")),
                }
    return {}


def _iter_json_objects(value: object) -> list[dict]:
    out: list[dict] = []

    def walk(obj: object) -> None:
        if isinstance(obj, dict):
            out.append(obj)
            for child in obj.values():
                walk(child)
        elif isinstance(obj, list):
            for child in obj:
                walk(child)

    walk(value)
    return out


def _parse_json_object(text: str | None) -> dict[str, str]:
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_yes_no(value: str | None) -> bool | None:
    normalized = (value or "").strip().lower()
    if normalized == "yes":
        return True
    if normalized == "no":
        return False
    return None


def _clean_artic_title(title: str | None) -> str | None:
    if not title:
        return None
    return re.sub(r"\s*\|\s*The Art Institute of Chicago\s*$", "", title, flags=re.IGNORECASE).strip()


def _normalize_elmhurst_url(slug: str | None, base_url: str) -> str | None:
    normalized = _normalize_space(slug)
    if not normalized:
        return None
    return _normalize_url(f"/events/{quote(normalized, safe='')}", base_url)


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


def _normalize_datetime_text(text: str | None) -> str | None:
    normalized = _normalize_space(text)
    if not normalized:
        return None
    normalized = normalized.replace("CT", "").replace("CDT", "").replace("CST", "")
    normalized = normalized.replace("|", " ")
    return _normalize_space(normalized)


def _normalize_space(text: str | None) -> str | None:
    if text is None:
        return None
    normalized = " ".join(str(text).split())
    return normalized or None


def _strip_html(text: str | None) -> str | None:
    normalized = _normalize_space(text)
    if not normalized:
        return None
    soup = BeautifulSoup(normalized, "html.parser")
    return _normalize_space(soup.get_text(" ", strip=True))


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


def join_non_empty(values: list[str | None]) -> str | None:
    parts = [_normalize_space(value) for value in values]
    cleaned = [part for part in parts if part]
    return " | ".join(cleaned) if cleaned else None
