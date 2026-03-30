from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
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
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

AZ_TIMEZONE = "America/Phoenix"

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

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " art making ",
    " art program ",
    " art programs ",
    " artmaking ",
    " arts club ",
    " class ",
    " classes ",
    " conversation ",
    " create playdate ",
    " curator talk ",
    " discussion ",
    " docent talk ",
    " family day ",
    " hands on ",
    " hands-on ",
    " history social ",
    " lecture ",
    " lectures ",
    " museum day ",
    " object of the month ",
    " panel ",
    " panels ",
    " playdate ",
    " respond ",
    " symposium ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
WEAK_INCLUDE_PATTERNS = (
    " art ",
    " artist ",
    " artists ",
    " family ",
    " kid ",
    " kids ",
    " teen ",
    " teens ",
    " youth ",
)
ALWAYS_REJECT_PATTERNS = (
    " admission ",
    " book club ",
    " concert ",
    " dinner ",
    " film ",
    " marketplace ",
    " meditation ",
    " mindfulness ",
    " movie ",
    " performance ",
    " performances ",
    " preview ",
    " previews ",
    " reception ",
    " sound bath ",
    " storytelling ",
    " storytime ",
    " tour ",
    " tours ",
    " trivia ",
    " yoga ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " after dark ",
    " coffee social ",
    " first friday ",
    " member preview ",
    " members preview ",
    " mystery in the museum ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
)
REGISTRATION_PATTERNS = (
    " apply today ",
    " buy tickets ",
    " register ",
    " registration ",
    " reservation ",
    " reserve ",
    " rsvp ",
    " ticket ",
    " tickets ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
PHOENIX_DATE_TIME_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+"
    r"([A-Za-z]{3,9})\.?\s+(\d{1,2}),\s+(\d{4})\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
    re.IGNORECASE,
)
SCOTTSDALE_DATE_TIME_RE = re.compile(
    r"([A-Za-z]{3,9})\.?\s+(\d{1,2}),\s+(\d{4})\s*/\s*(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
MESA_DATE_RE = re.compile(
    r"([A-Za-z]{3,9})\s+(\d{1,2})(?:\s*(?:-|–|—)\s*(?:(?P<end_month>[A-Za-z]{3,9})\s+)?(?P<end_day>\d{1,2}))?,\s*(\d{4})",
    re.IGNORECASE,
)
MESA_DETAIL_DATE_TIME_RE = re.compile(
    r"DATE\s*&\s*TIME\s+([A-Za-z]{3,9})\s+(\d{1,2}),\s+(\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)",
    re.IGNORECASE,
)
MESA_TIME_RANGE_RE = re.compile(
    r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*-\s*(\d{1,2}):(\d{2})\s*(AM|PM)",
    re.IGNORECASE,
)
MESA_LOCATION_RE = re.compile(
    r"LOCATION\s+(.+?)\s+ALL-IN PRICE",
    re.IGNORECASE,
)
UAMA_DATE_TIME_RE = re.compile(
    r"(\d{2})\s+([A-Za-z]{3,9})\s+(\d{4})\s+\w+\s+"
    r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*[–-]\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
    re.IGNORECASE,
)
HEARD_SINGLE_DATE_TIME_RE = re.compile(
    r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s+"
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)\s+to\s+"
    r"(noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
HEARD_DATE_RANGE_TIME_RE = re.compile(
    r"([A-Za-z]{3,9})\s+(\d{1,2})\s*(?:-|–|—|to)\s*(\d{1,2}),\s*(\d{4})\s+"
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)\s+to\s+"
    r"(noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
HEARD_LOCATION_START_RE = re.compile(
    r"(?:to\s+(?:noon|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)))\s+(.+)",
    re.IGNORECASE,
)
HEARD_LOCATION_STOP_MARKERS = (
    " Artboard ",
    " Included w/ Admission ",
    " Registration Closed ",
    " Home Events & Classes ",
    " Buy Tickets ",
    " Sponsors ",
)
MOCA_FULL_DATE_TIME_RE = re.compile(
    r"([A-Za-z]{3,9})\s+(\d{1,2})\s*(?:-|–|—|to)\s*(?:(?P<end_month>[A-Za-z]{3,9})\s+)?(?P<end_day>\d{1,2}),\s*(?P<year>\d{4})\s+"
    r"(?P<start_hour>\d{1,2})(?::(?P<start_min>\d{2}))?\s*(?P<start_period>a\.?m\.?|p\.?m\.?|am|pm)?\s*(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_min>\d{2}))?\s*(?P<end_period>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
MOCA_DATES_TIME_RE = re.compile(
    r"Dates:\s*(?:[A-Za-z]+,\s+)?([A-Za-z]{3,9})\s+(\d{1,2})\s*(?:-|–|—|to)\s*(?:(?P<end_month>[A-Za-z]{3,9})\s+)?(?P<end_day>\d{1,2})(?:\s*\([^)]*\))?(?:,\s*(?P<year>20\d{2}))?.*?"
    r"Time\s*:\s*(?:[A-Za-z]+,\s+)?(?P<start_hour>\d{1,2})(?::(?P<start_min>\d{2}))?\s*(?P<start_period>a\.?m\.?|p\.?m\.?|am|pm)\s*(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_min>\d{2}))?\s*(?P<end_period>a\.?m\.?|p\.?m\.?|am|pm)",
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
SCOTTSDALE_LABELS = (
    "Programs & Workshops",
    "Events",
    "Lifestyle",
    "Wellness",
    "Family",
    "Teen",
    "Adult",
    "Free",
)


@dataclass(frozen=True, slots=True)
class AzHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    discovery_mode: str
    extra_urls: tuple[str, ...] = ()


AZ_HTML_VENUES: tuple[AzHtmlVenueConfig, ...] = (
    AzHtmlVenueConfig(
        slug="heard_museum",
        source_name="az_heard_museum_events",
        venue_name="Heard Museum",
        city="Phoenix",
        state="AZ",
        list_url="https://heard.org/experience/events-and-classes/",
        discovery_mode="heard_detail",
    ),
    AzHtmlVenueConfig(
        slug="mesa_contemporary_arts_museum",
        source_name="az_mesa_contemporary_arts_museum_events",
        venue_name="Mesa Contemporary Arts Museum",
        city="Mesa",
        state="AZ",
        list_url="https://www.mesaartscenter.com/programs-events",
        discovery_mode="mesa_list",
    ),
    AzHtmlVenueConfig(
        slug="moca_tucson",
        source_name="az_moca_tucson_events",
        venue_name="Museum of Contemporary Art Tucson",
        city="Tucson",
        state="AZ",
        list_url="https://moca-tucson.org/learn/",
        discovery_mode="moca_classes",
    ),
    AzHtmlVenueConfig(
        slug="phoenix_art_museum",
        source_name="az_phoenix_art_museum_events",
        venue_name="Phoenix Art Museum",
        city="Phoenix",
        state="AZ",
        list_url="https://phxart.org/events/calendar/",
        discovery_mode="phoenix_multi",
        extra_urls=(
            "https://phxart.org/events/workshops/",
            "https://phxart.org/events/lectures/",
        ),
    ),
    AzHtmlVenueConfig(
        slug="scottsdale_museum_of_contemporary_art",
        source_name="az_scottsdale_museum_of_contemporary_art_events",
        venue_name="Scottsdale Museum of Contemporary Art",
        city="Scottsdale",
        state="AZ",
        list_url="https://scottsdalearts.org/whats-on/?categories=events&venues=smoca",
        discovery_mode="scottsdale_playwright",
    ),
    AzHtmlVenueConfig(
        slug="university_of_arizona_museum_of_art",
        source_name="az_university_of_arizona_museum_of_art_events",
        venue_name="University of Arizona Museum of Art",
        city="Tucson",
        state="AZ",
        list_url="https://artmuseum.arizona.edu/see-do/events/upcoming",
        discovery_mode="uama_detail",
    ),
)

AZ_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in AZ_HTML_VENUES}


class AzHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "az_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_az_html_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_az_html_bundle_payload/parse_az_html_events from script runner.")


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
            timezone_id=AZ_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def load_az_html_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    max_detail_pages: int = 80,
) -> dict[str, dict]:
    selected = [AZ_HTML_VENUES_BY_SLUG[slug] for slug in venue_slugs] if venue_slugs else list(AZ_HTML_VENUES)
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            list_html = ""
            detail_pages: dict[str, str] = {}
            page_htmls: dict[str, str] = {}

            if venue.discovery_mode == "heard_detail":
                list_html = await fetch_html(venue.list_url, client=client)
                for detail_url in _extract_heard_detail_urls(list_html, venue.list_url)[:max_detail_pages]:
                    try:
                        detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                    except Exception as exc:
                        print(f"[az-html-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")
            elif venue.discovery_mode == "mesa_list":
                list_html = await fetch_html(venue.list_url, client=client)
                for detail_url in _extract_mesa_detail_urls(list_html, venue.list_url)[:max_detail_pages]:
                    try:
                        detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                    except Exception as exc:
                        print(f"[az-html-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")
            elif venue.discovery_mode == "moca_classes":
                list_html = await fetch_html(venue.list_url, client=client)
                for detail_url in _extract_moca_class_urls(list_html, venue.list_url)[:max_detail_pages]:
                    try:
                        detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                    except Exception as exc:
                        print(f"[az-html-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")
            elif venue.discovery_mode == "phoenix_multi":
                for url in venue.extra_urls:
                    page_htmls[url] = await fetch_html(url, client=client)
            elif venue.discovery_mode == "scottsdale_playwright":
                list_html = await fetch_html_playwright(venue.list_url)
            elif venue.discovery_mode == "uama_detail":
                list_html = await fetch_html(venue.list_url, client=client)
                for detail_url in _extract_uama_detail_urls(list_html, venue.list_url)[:max_detail_pages]:
                    try:
                        detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                    except Exception as exc:
                        print(f"[az-html-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")
            else:
                raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")

            payload[venue.slug] = {
                "list_html": list_html,
                "detail_pages": detail_pages,
                "page_htmls": page_htmls,
            }

    return payload


def parse_az_html_events(payload: dict, *, venue: AzHtmlVenueConfig) -> list[ExtractedActivity]:
    if venue.discovery_mode == "heard_detail":
        rows = _parse_heard_events(payload, venue=venue)
    elif venue.discovery_mode == "mesa_list":
        rows = _parse_mesa_events(payload, venue=venue)
    elif venue.discovery_mode == "moca_classes":
        rows = _parse_moca_events(payload, venue=venue)
    elif venue.discovery_mode == "phoenix_multi":
        rows = _parse_phoenix_events(payload, venue=venue)
    elif venue.discovery_mode == "scottsdale_playwright":
        rows = _parse_scottsdale_events(payload, venue=venue)
    elif venue.discovery_mode == "uama_detail":
        rows = _parse_uama_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(AZ_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


def get_az_html_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in AZ_HTML_VENUES:
        for url in (venue.list_url,) + venue.extra_urls:
            parsed = urlparse(url)
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(dict.fromkeys(prefixes))


def _parse_heard_events(payload: dict, *, venue: AzHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _normalize_space(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")
        if not title:
            continue
        full_text = _normalize_space(soup.get_text(" ", strip=True))
        parsed = _parse_heard_datetime(full_text)
        if parsed is None:
            continue
        start_at, end_at = parsed
        if not _should_keep_event(title=title, description=full_text):
            continue

        location = _extract_heard_location(full_text)
        description = full_text
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location or f"{venue.venue_name}, {venue.city}, {venue.state}",
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(f"{title} {description}"),
                age_min=_parse_age_range(description)[0],
                age_max=_parse_age_range(description)[1],
                drop_in=_contains_any(description, DROP_IN_PATTERNS),
                registration_required=_registration_required(description),
                start_at=start_at,
                end_at=end_at,
                timezone=AZ_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )
    return rows


def _parse_mesa_events(payload: dict, *, venue: AzHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _normalize_space(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")
        main_text = _normalize_space(soup.select_one("main").get_text(" ", strip=True) if soup.select_one("main") else "")
        detail_text = main_text or _normalize_space(soup.get_text(" ", strip=True))
        if not title or not detail_text:
            continue

        parsed = _parse_mesa_detail_datetime(detail_text)
        if parsed is None:
            continue
        start_at, end_at = parsed

        if not _should_keep_event(title=title, description=detail_text):
            continue

        age_min, age_max = _parse_age_range(detail_text)
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=detail_text,
                venue_name=venue.venue_name,
                location_text=_extract_mesa_location(detail_text) or f"{venue.venue_name}, {venue.city}, {venue.state}",
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(f"{title} {detail_text}"),
                age_min=age_min,
                age_max=age_max,
                drop_in=None,
                registration_required=_registration_required(detail_text),
                start_at=start_at,
                end_at=end_at,
                timezone=AZ_TIMEZONE,
                **price_classification_kwargs(detail_text),
            )
        )

    return rows


def _parse_moca_events(payload: dict, *, venue: AzHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _normalize_space(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")
        main_text = _normalize_space(soup.select_one("main").get_text(" ", strip=True) if soup.select_one("main") else "")
        if not title or not main_text:
            continue

        parsed = _parse_moca_datetime(main_text)
        if parsed is None:
            continue
        start_at, end_at = parsed

        if not _should_keep_event(title=title, description=main_text):
            continue

        age_min, age_max = _parse_age_range(main_text)
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=main_text,
                venue_name=venue.venue_name,
                location_text=f"{venue.venue_name}, {venue.city}, {venue.state}",
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(f"{title} {main_text}"),
                age_min=age_min,
                age_max=age_max,
                drop_in=None,
                registration_required=_registration_required(main_text),
                start_at=start_at,
                end_at=end_at,
                timezone=AZ_TIMEZONE,
                **price_classification_kwargs(main_text),
            )
        )

    return rows


def _parse_phoenix_events(payload: dict, *, venue: AzHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []

    for page_url, html in (payload.get("page_htmls") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        page_label = "lectures" if "lectures" in page_url else "workshops"
        for card in soup.select(".p-event-list__all-event-container > *"):
            text = _normalize_space(card.get_text(" ", strip=True))
            if not text:
                continue
            hrefs = [href for href in (node.get("href") for node in card.select("[href]")) if href]
            if not hrefs:
                continue
            source_url = hrefs[0]
            parsed = _parse_phoenix_card(text)
            if parsed is None:
                continue
            title, start_at = parsed
            if not _should_keep_event(title=title, description=text, page_label=page_label):
                continue

            description = f"Category page: {page_label} | {text}"
            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=description,
                    venue_name=venue.venue_name,
                    location_text=f"{venue.venue_name}, {venue.city}, {venue.state}",
                    city=venue.city,
                    state=venue.state,
                    activity_type=_infer_activity_type(f"{page_label} {title}"),
                    age_min=_parse_age_range(text)[0],
                    age_max=_parse_age_range(text)[1],
                    drop_in=None,
                    registration_required=True if "Buy Tickets" in text else None,
                    start_at=start_at,
                    end_at=None,
                    timezone=AZ_TIMEZONE,
                    **price_classification_kwargs(text),
                )
            )

    return rows


def _parse_scottsdale_events(payload: dict, *, venue: AzHtmlVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    rows: list[ExtractedActivity] = []

    for card in soup.select("#grid-view .events > *"):
        text = _normalize_space(card.get_text(" ", strip=True))
        href = next((node.get("href") for node in card.select("[href]") if node.get("href")), None)
        if not text or not href:
            continue

        parsed = _parse_scottsdale_card(text)
        if parsed is None:
            continue
        title, start_at, description = parsed
        if not _should_keep_event(title=title, description=description):
            continue

        rows.append(
            ExtractedActivity(
                source_url=urljoin(venue.list_url, href),
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=f"{venue.venue_name}, {venue.city}, {venue.state}",
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(f"{title} {description}"),
                age_min=_parse_age_range(description)[0],
                age_max=_parse_age_range(description)[1],
                drop_in=None,
                registration_required=None,
                start_at=start_at,
                end_at=None,
                timezone=AZ_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )

    return rows


def _parse_uama_events(payload: dict, *, venue: AzHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _normalize_space(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")
        summary_text = _normalize_space(soup.select_one(".content").get_text(" ", strip=True) if soup.select_one(".content") else "")
        main_text = _normalize_space(soup.select_one("main").get_text(" ", strip=True) if soup.select_one("main") else "")
        description = _trim_uama_description(main_text)
        if not title or not description:
            continue

        parsed = _parse_uama_datetime(summary_text)
        if parsed is None:
            continue
        start_at, end_at = parsed

        if not _should_keep_event(title=title, description=description):
            continue

        age_min, age_max = _parse_age_range(description)
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=f"{venue.venue_name}, {venue.city}, {venue.state}",
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(f"{title} {description}"),
                age_min=age_min,
                age_max=age_max,
                drop_in=None,
                registration_required=_registration_required(description),
                start_at=start_at,
                end_at=end_at,
                timezone=AZ_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )
    return rows


def _extract_heard_detail_urls(html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if "/event/" not in href:
            continue
        absolute = urljoin(list_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        urls.append(absolute)
    return urls


def _extract_moca_class_urls(html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if "/class/" not in href:
            continue
        absolute = urljoin(list_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        urls.append(absolute)
    return urls


def _extract_mesa_detail_urls(html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select('a[href*="/show-details/"]'):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        absolute = urljoin(list_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        urls.append(absolute)
    return urls


def _extract_uama_detail_urls(html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    root_url = f"{urlparse(list_url).scheme}://{urlparse(list_url).netloc}/"
    for anchor in soup.select('#upcoming-events-list a[href*="see-do/events/calendar/"]'):
        href = (anchor.get("href") or "").strip()
        absolute = urljoin(root_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        urls.append(absolute)
    return urls


def _find_mesa_card_container(anchor) -> BeautifulSoup | None:
    node = anchor
    while node is not None:
        text = _normalize_space(node.get_text(" ", strip=True))
        if "Learn More" in text and 10 <= len(text) <= 180:
            return node
        node = node.parent
    return None


def _parse_mesa_date(text: str) -> date | None:
    match = MESA_DATE_RE.search(text)
    if match is None:
        return None
    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    day = int(match.group(2))
    year = int(match.group(5))
    if month is None:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _parse_mesa_detail_datetime(text: str) -> tuple[datetime, datetime | None] | None:
    match = MESA_DETAIL_DATE_TIME_RE.search(text)
    if match is None:
        parsed_date = _parse_mesa_date(text)
        if parsed_date is None:
            return None
        return datetime.combine(parsed_date, time.min), None

    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    if month is None:
        return None
    year = int(match.group(3))
    day = int(match.group(2))
    start_at = datetime(year, month, day, *_parse_time_parts(match.group(4), match.group(5), match.group(6)))

    range_match = MESA_TIME_RANGE_RE.search(text[match.end() :])
    if range_match is None:
        return start_at, None
    end_time = _parse_time_value(f"{range_match.group(4)}:{range_match.group(5)} {range_match.group(6)}")
    if end_time is None:
        return start_at, None
    return start_at, datetime(year, month, day, end_time.hour, end_time.minute)


def _parse_phoenix_card(text: str) -> tuple[str, datetime] | None:
    match = PHOENIX_DATE_TIME_RE.search(text)
    if match is None:
        return None
    title = _normalize_space(text[: match.start()].replace("event-image-link", ""))
    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    if month is None or not title:
        return None
    start_at = datetime(
        int(match.group(3)),
        month,
        int(match.group(2)),
        *_parse_time_parts(match.group(4), match.group(5), match.group(6)),
    )
    return title, start_at


def _parse_scottsdale_card(text: str) -> tuple[str, datetime, str] | None:
    match = SCOTTSDALE_DATE_TIME_RE.search(text)
    if match is None:
        return None
    prefix = _normalize_space(text[: match.start()])
    for label in SCOTTSDALE_LABELS:
        if prefix.endswith(label):
            prefix = _normalize_space(prefix[: -len(label)])
    title = prefix
    if title.endswith(" Events"):
        title = _normalize_space(title[:-7])
    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    if month is None or not title:
        return None
    start_at = datetime(
        int(match.group(3)),
        month,
        int(match.group(2)),
        *_parse_time_parts(match.group(4), match.group(5), match.group(6)),
    )
    description = _normalize_space(text[match.end() :])
    return title, start_at, description


def _parse_heard_datetime(text: str) -> tuple[datetime, datetime | None] | None:
    match = HEARD_DATE_RANGE_TIME_RE.search(text)
    if match is not None:
        month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
        year = int(match.group(4))
        day = int(match.group(2))
        if month is None:
            return None
        start_at = datetime(year, month, day, *_parse_time_parts(match.group(5), match.group(6), match.group(7)))
        end_time = _parse_time_value(match.group(8))
        end_at = datetime(year, month, day, end_time.hour, end_time.minute) if end_time is not None else None
        return start_at, end_at

    match = HEARD_SINGLE_DATE_TIME_RE.search(text)
    if match is not None:
        parsed_date = _parse_long_date(match.group(1))
        if parsed_date is None:
            return None
        start_at = datetime(
            parsed_date.year,
            parsed_date.month,
            parsed_date.day,
            *_parse_time_parts(match.group(2), match.group(3), match.group(4)),
        )
        end_time = _parse_time_value(match.group(5))
        end_at = datetime(parsed_date.year, parsed_date.month, parsed_date.day, end_time.hour, end_time.minute) if end_time is not None else None
        return start_at, end_at

    return None


def _extract_heard_location(text: str) -> str | None:
    match = HEARD_LOCATION_START_RE.search(text)
    if match is None:
        return None
    location = match.group(1)
    for marker in HEARD_LOCATION_STOP_MARKERS:
        if marker in location:
            location = location.split(marker, 1)[0]
            break
    location = _normalize_space(location)
    if location.endswith(" The Heard Museum"):
        location = _normalize_space(location[: -len(" The Heard Museum")])
    if not location or len(location) > 120:
        return None
    return f"{location}, The Heard Museum, Phoenix, AZ" if location else None


def _extract_mesa_location(text: str) -> str | None:
    match = MESA_LOCATION_RE.search(text)
    if match is None:
        return None
    location = _normalize_space(match.group(1))
    return location or None


def _parse_uama_datetime(text: str) -> tuple[datetime, datetime | None] | None:
    match = UAMA_DATE_TIME_RE.search(text)
    if match is None:
        return None
    month = MONTH_LOOKUP.get(match.group(2)[:3].lower())
    if month is None:
        return None
    year = int(match.group(3))
    day = int(match.group(1))
    start_at = datetime(year, month, day, *_parse_time_parts(match.group(4), match.group(5), match.group(6)))
    end_at = datetime(year, month, day, *_parse_time_parts(match.group(7), match.group(8), match.group(9)))
    return start_at, end_at


def _trim_uama_description(text: str) -> str:
    trimmed = text
    for marker in ("With questions about access", "Related Exhibition", "Back Previous Next"):
        if marker in trimmed:
            trimmed = trimmed.split(marker, 1)[0]
    return _normalize_space(trimmed)


def _parse_moca_datetime(text: str) -> tuple[datetime, datetime | None] | None:
    match = MOCA_FULL_DATE_TIME_RE.search(text)
    if match is not None:
        month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
        end_month = MONTH_LOOKUP.get((match.group("end_month") or match.group(1))[:3].lower())
        year = int(match.group("year"))
        if month is None or end_month is None:
            return None
        start_time = _parse_time_value(
            f"{match.group('start_hour')}:{match.group('start_min') or '00'} {match.group('start_period') or match.group('end_period')}"
        )
        end_time = _parse_time_value(
            f"{match.group('end_hour')}:{match.group('end_min') or '00'} {match.group('end_period')}"
        )
        if start_time is None or end_time is None:
            return None
        start_at = datetime(year, month, int(match.group(2)), start_time.hour, start_time.minute)
        end_at = datetime(year, month, int(match.group(2)), end_time.hour, end_time.minute)
        return start_at, end_at

    match = MOCA_DATES_TIME_RE.search(text)
    if match is not None:
        year_text = match.group("year")
        year_candidates = [int(value) for value in re.findall(r"\b(20\d{2})\b", text)] if not year_text else []
        if year_text is None and not year_candidates:
            return None
        year = int(year_text) if year_text else max(year_candidates)
        month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
        if month is None:
            return None
        start_time = _parse_time_value(
            f"{match.group('start_hour')}:{match.group('start_min') or '00'} {match.group('start_period')}"
        )
        end_time = _parse_time_value(
            f"{match.group('end_hour')}:{match.group('end_min') or '00'} {match.group('end_period')}"
        )
        if start_time is None or end_time is None:
            return None
        start_at = datetime(year, month, int(match.group(2)), start_time.hour, start_time.minute)
        end_at = datetime(year, month, int(match.group(2)), end_time.hour, end_time.minute)
        return start_at, end_at

    return None


def _parse_long_date(value: str) -> date | None:
    text = _normalize_space(value)
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_time_parts(hour_text: str, minute_text: str | None, period_text: str) -> tuple[int, int]:
    parsed = _parse_time_value(f"{hour_text}:{minute_text or '00'} {period_text}")
    if parsed is None:
        return 0, 0
    return parsed.hour, parsed.minute


def _parse_time_value(value: str | None) -> time | None:
    if not value:
        return None
    text = _normalize_space(value).lower().replace(".", "")
    if text == "noon":
        return time(12, 0)
    for fmt in ("%I:%M %p", "%I %p", "%I:%M%p", "%I%p"):
        try:
            return datetime.strptime(text.upper(), fmt).time()
        except ValueError:
            continue
    return None


def _should_keep_event(*, title: str, description: str, page_label: str = "") -> bool:
    title_blob = _searchable_blob(title)
    token_blob = _searchable_blob(" ".join([title, description, page_label]))

    if any(pattern in title_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if any(pattern in title_blob for pattern in CONTEXTUAL_REJECT_PATTERNS):
        return False

    strong_in_title = any(pattern in title_blob for pattern in STRONG_INCLUDE_PATTERNS)
    strong_anywhere = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)

    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS) and not strong_in_title:
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_in_title:
        return False

    if strong_anywhere:
        return True

    if page_label and ("lectures" in page_label or "workshops" in page_label):
        return not any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS)

    return any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS) and " art " in token_blob


def _infer_activity_type(token_blob: str) -> str:
    lead_searchable = _searchable_blob(token_blob[:160])
    if any(
        pattern in lead_searchable
        for pattern in (
            " class ",
            " classes ",
            " create playdate ",
            " family day ",
            " workshop ",
            " workshops ",
        )
    ):
        return "workshop"
    if any(
        pattern in lead_searchable
        for pattern in (
            " conversation ",
            " discussion ",
            " lecture ",
            " lectures ",
            " panel ",
            " panels ",
            " symposium ",
            " talk ",
            " talks ",
        )
    ):
        return "lecture"

    searchable = _searchable_blob(token_blob)
    if any(
        pattern in searchable
        for pattern in (
            " conversation ",
            " discussion ",
            " lecture ",
            " lectures ",
            " panel ",
            " panels ",
            " symposium ",
            " talk ",
            " talks ",
        )
    ):
        return "lecture"
    if any(
        pattern in searchable
        for pattern in (
            " class ",
            " classes ",
            " create playdate ",
            " family day ",
            " workshop ",
            " workshops ",
        )
    ):
        return "workshop"
    return "workshop"


def _registration_required(text: str) -> bool | None:
    searchable = _searchable_blob(text)
    if " registration closed " in searchable:
        return True
    if any(pattern in searchable for pattern in REGISTRATION_PATTERNS):
        return True
    return None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _contains_any(text: str, patterns: tuple[str, ...]) -> bool:
    searchable = _searchable_blob(text)
    return any(pattern in searchable for pattern in patterns)


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())
