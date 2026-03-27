from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from urllib.parse import parse_qs
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

NY_TIMEZONE = "America/New_York"

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

DATE_WITH_YEAR_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+([A-Za-z]{3,9})\s+(\d{1,2}),\s*(\d{4})",
    re.IGNORECASE,
)
DATE_NO_YEAR_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+([A-Za-z]{3,9})\s+(\d{1,2})",
    re.IGNORECASE,
)
SHORT_DATE_RE = re.compile(r"\b(\d{2})\s+([A-Za-z]{3})\b", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?|noon|midnight)?\s*(?:-|–|—|to)\s*(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?|noon|midnight)",
    re.IGNORECASE,
)
TIME_RANGE_TO_SPECIAL_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?)\s*(?:-|–|—|to)\s*(noon|midnight)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?|noon|midnight)\b", re.IGNORECASE)
TIME_ADJACENT_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?)\s+(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?)",
    re.IGNORECASE,
)
PAFA_DATE_RANGE_RE = re.compile(
    r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s*[–-]\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
    re.IGNORECASE,
)
PAFA_SINGLE_DATE_RE = re.compile(r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", re.IGNORECASE)
BRANDYWINE_EVENT_DETAILS_RE = re.compile(
    r"Event Details\s+Date:\s*(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*"
    r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s+Time:\s*(.+?)\s+Location:\s*(.+?)\s+Price:\s*(.+?)"
    r"(?:\s+The Brandywine River Museum|\s+Contact Us|\s*$)",
    re.IGNORECASE,
)
ERIE_EVENT_DETAILS_RE = re.compile(
    r"Back to All Events\s+(.+?)\s+"
    r"((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s+"
    r"(\d{1,2}:\d{2}\s*[AP]M)\s+(\d{1,2}:\d{2}\s*[AP]M)\s+(.+?)\s+Google Calendar ICS\s+(.+?)\s+Posted In:",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
PRICE_RE = re.compile(r"\$\s*\d+(?:\.\d{2})?(?:\s*[–-]\s*\$\s*\d+(?:\.\d{2})?)?")


@dataclass(frozen=True, slots=True)
class PaHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_urls: tuple[str, ...]
    needs_playwright: bool = False


PA_HTML_VENUES: tuple[PaHtmlVenueConfig, ...] = (
    PaHtmlVenueConfig(
        slug="allentown",
        source_name="allentown_events",
        venue_name="Allentown Art Museum",
        city="Allentown",
        state="PA",
        list_urls=("https://www.allentownartmuseum.org/calendar/",),
        needs_playwright=True,
    ),
    PaHtmlVenueConfig(
        slug="barnes",
        source_name="barnes_events",
        venue_name="Barnes Foundation",
        city="Philadelphia",
        state="PA",
        list_urls=(
            "https://www.barnesfoundation.org/whats-on/talks",
            "https://www.barnesfoundation.org/whats-on/workshops",
            "https://www.barnesfoundation.org/whats-on/family",
        ),
    ),
    PaHtmlVenueConfig(
        slug="brandywine",
        source_name="brandywine_events",
        venue_name="Brandywine Museum of Art",
        city="Chadds Ford",
        state="PA",
        list_urls=("https://www.brandywine.org/museum/events",),
    ),
    PaHtmlVenueConfig(
        slug="erie",
        source_name="erie_events",
        venue_name="Erie Art Museum",
        city="Erie",
        state="PA",
        list_urls=(
            "https://www.erieartmuseum.org/classes",
            "https://www.erieartmuseum.org/paint-workshops",
        ),
    ),
    PaHtmlVenueConfig(
        slug="palmer",
        source_name="palmer_events",
        venue_name="Palmer Museum of Art",
        city="University Park",
        state="PA",
        list_urls=("https://palmermuseum.psu.edu/programs/",),
    ),
    PaHtmlVenueConfig(
        slug="pafa",
        source_name="pafa_events",
        venue_name="Pennsylvania Academy of the Fine Arts",
        city="Philadelphia",
        state="PA",
        list_urls=("https://www.pafa.org/events",),
    ),
    PaHtmlVenueConfig(
        slug="philadelphia",
        source_name="philamuseum_events",
        venue_name="Philadelphia Museum of Art",
        city="Philadelphia",
        state="PA",
        list_urls=("https://www.philamuseum.org/events",),
        needs_playwright=True,
    ),
    PaHtmlVenueConfig(
        slug="woodmere",
        source_name="woodmere_events",
        venue_name="Woodmere Art Museum",
        city="Philadelphia",
        state="PA",
        list_urls=(
            "https://woodmeremuseum.org/learn-create/adult-classes-and-workshops",
            "https://woodmeremuseum.org/learn-create/children-and-teen-classes",
        ),
    ),
)

PA_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in PA_HTML_VENUES}


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
            timezone_id=NY_TIMEZONE,
        )
        await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
        await page.wait_for_timeout(2000)
        html = await page.content()
        await browser.close()
        return html


async def load_pa_html_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    max_links_per_venue: int = 80,
) -> dict[str, dict]:
    selected = (
        [PA_HTML_VENUES_BY_SLUG[slug] for slug in venue_slugs]
        if venue_slugs
        else list(PA_HTML_VENUES)
    )
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            list_htmls: dict[str, str] = {}
            for list_url in venue.list_urls:
                list_htmls[list_url] = (
                    await fetch_html_playwright(list_url) if venue.needs_playwright else await fetch_html(list_url, client=client)
                )

            detail_pages: dict[str, str] = {}
            if venue.slug in {"barnes", "brandywine", "erie", "pafa"}:
                seen: set[str] = set()
                for list_url, html in list_htmls.items():
                    for detail_url, _ in _extract_listing_links(venue.slug, html, list_url)[:max_links_per_venue]:
                        if detail_url in seen:
                            continue
                        seen.add(detail_url)
                        try:
                            detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                        except Exception as exc:
                            print(f"[pa-html-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")

            payload[venue.slug] = {
                "list_htmls": list_htmls,
                "detail_pages": detail_pages,
            }

    return payload


def parse_pa_html_events(payload: dict, *, venue: PaHtmlVenueConfig) -> list[ExtractedActivity]:
    if venue.slug == "allentown":
        rows = _parse_allentown_events(payload, venue=venue)
    elif venue.slug == "barnes":
        rows = _parse_barnes_events(payload, venue=venue)
    elif venue.slug == "brandywine":
        rows = _parse_brandywine_events(payload, venue=venue)
    elif venue.slug == "erie":
        rows = _parse_erie_events(payload, venue=venue)
    elif venue.slug == "palmer":
        rows = _parse_palmer_events(payload, venue=venue)
    elif venue.slug == "pafa":
        rows = _parse_pafa_events(payload, venue=venue)
    elif venue.slug == "philadelphia":
        rows = _parse_philadelphia_events(payload, venue=venue)
    elif venue.slug == "woodmere":
        rows = _parse_woodmere_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


class PaHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "pa_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_pa_html_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_pa_html_bundle_payload/parse_pa_html_events from script runner.")


def get_pa_html_source_prefixes() -> tuple[str, ...]:
    return tuple(url for venue in PA_HTML_VENUES for url in venue.list_urls)


def _extract_listing_links(venue_slug: str, html: str, list_url: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links_by_url: dict[str, str] = {}
    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        text = _normalize_space(anchor.get_text(" ", strip=True))
        absolute_url = urljoin(list_url, href)
        parsed = urlparse(absolute_url)
        path = parsed.path.rstrip("/")
        segments = [segment for segment in path.split("/") if segment]
        if venue_slug == "barnes":
            if not path.startswith("/whats-on") or len(segments) < 3:
                continue
        elif venue_slug == "brandywine":
            if not path.startswith("/museum/events/") or path == "/museum/events":
                continue
        elif venue_slug == "erie":
            if "/eam-events/" not in path:
                continue
        elif venue_slug == "pafa":
            if not path.startswith("/events/") or path == "/events":
                continue
        else:
            continue

        container = anchor.find_parent(["article", "li", "div", "section"]) or anchor
        blob = _normalize_space(container.get_text(" ", strip=True)) or text
        previous = links_by_url.get(absolute_url)
        if previous is None or len(blob) > len(previous):
            links_by_url[absolute_url] = blob

    return list(links_by_url.items())


def _parse_allentown_events(payload: dict, *, venue: PaHtmlVenueConfig) -> list[ExtractedActivity]:
    html = next(iter((payload.get("list_htmls") or {}).values()), "")
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []

    for card in soup.select(".eventon_list_event"):
        source_anchor = card.find("a", href=re.compile(r"/blog/events/"))
        if source_anchor is None:
            continue
        source_url = urljoin(venue.list_urls[0], source_anchor.get("href") or "")
        blob = _normalize_space(card.get_text(" ", strip=True))
        if not blob:
            continue
        title = _extract_allentown_title(blob)
        if not title:
            continue
        token_blob = _searchable_blob(" ".join([title, blob]))
        if not _allow_allentown(token_blob):
            continue

        dates_link = card.find("a", href=re.compile(r"google\.com/calendar/event"))
        start_date = _extract_date_from_google_calendar(dates_link.get("href") if dates_link else None)
        if start_date is None:
            short_date = SHORT_DATE_RE.search(blob)
            if short_date:
                month = MONTH_LOOKUP.get(short_date.group(2).lower())
                if month is not None:
                    start_date = date(datetime.now(ZoneInfo(NY_TIMEZONE)).year, month, int(short_date.group(1)))
        if start_date is None:
            continue

        start_at, end_at = _combine_date_and_times(start_date, _parse_time_range(blob))
        description = _extract_allentown_description(blob)
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=description,
                location_text="Allentown Art Museum, Allentown, PA",
                activity_type=_infer_activity_type(token_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=token_blob,
                price_text=description or blob,
            )
        )

    return rows


def _parse_barnes_events(payload: dict, *, venue: PaHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _extract_page_title(soup)
        body_text = _content_text(soup)
        header_text = _extract_near_title(body_text, title, max_chars=700)
        description = _extract_barnes_description(body_text, title) or header_text[:1600]
        token_blob = _searchable_blob(" ".join([title, header_text, description or ""]))
        if not title or not _allow_barnes_source(source_url, token_blob):
            continue
        start_date = _parse_first_date_with_year(header_text) or _parse_first_date_without_year(
            header_text,
            year=datetime.now(ZoneInfo(NY_TIMEZONE)).year,
        )
        if start_date is None:
            continue
        start_at, end_at = _combine_date_and_times(start_date, _parse_time_range(header_text))
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=description,
                location_text="Barnes Foundation, Philadelphia, PA",
                activity_type=_infer_activity_type(token_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=token_blob,
                price_text=_extract_price_text(header_text),
            )
        )
    return rows


def _parse_brandywine_events(payload: dict, *, venue: PaHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _extract_page_title(soup)
        body_text = _content_text(soup)
        description = _extract_first_long_text(soup, min_length=70)
        token_blob = _searchable_blob(" ".join([title, description or ""]))
        if not title or not _allow_brandywine(token_blob):
            continue
        start_date, times_text, location_text, price_text = _extract_brandywine_event_details(body_text)
        if start_date is None:
            start_date = _parse_first_date_with_year(body_text)
        if start_date is None:
            continue
        start_at, end_at = _combine_date_and_times(
            start_date,
            _parse_time_range(times_text or body_text),
        )
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=description,
                location_text=location_text or "Brandywine Museum of Art, Chadds Ford, PA",
                activity_type=_infer_activity_type(token_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=token_blob,
                price_text=price_text or _extract_price_text(body_text),
            )
        )
    return rows


def _parse_erie_events(payload: dict, *, venue: PaHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _extract_page_title(soup)
        body_text = _content_text(soup)
        event_details = _extract_erie_event_details(body_text)
        description = event_details["description"] or _extract_first_long_text(soup, min_length=80)
        token_blob = _searchable_blob(" ".join([title, description or "", body_text[:300]]))
        if not title or not _allow_erie(token_blob):
            continue
        start_date = event_details["date"] or _parse_first_date_with_year(body_text)
        if start_date is None:
            continue
        times = _parse_time_range(event_details["time_text"] or body_text)
        if times == (None, None):
            time_nodes = [
                _normalize_space(node.get_text(" ", strip=True))
                for node in soup.select("time")
                if _normalize_space(node.get_text(" ", strip=True))
            ]
            times = _parse_time_range(" ".join(time_nodes))
        start_at, end_at = _combine_date_and_times(start_date, times)
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=description,
                location_text=event_details["location_text"] or "Erie Art Museum, Erie, PA",
                activity_type=_infer_activity_type(token_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=token_blob,
                price_text=_extract_price_text(event_details["description"] or body_text),
            )
        )
    return rows


def _parse_palmer_events(payload: dict, *, venue: PaHtmlVenueConfig) -> list[ExtractedActivity]:
    html = next(iter((payload.get("list_htmls") or {}).values()), "")
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    current_year = datetime.now(ZoneInfo(NY_TIMEZONE)).year

    for node in soup.select("h3.staff-name"):
        title = _normalize_space(node.get_text(" ", strip=True))
        price_node = node.find_next_sibling("span", class_="staff-title")
        bio_node = node.find_next_sibling("span", class_="staff-bio")
        bio_text = _normalize_space(bio_node.get_text(" ", strip=True)) if bio_node else ""
        token_blob = _searchable_blob(" ".join(part for part in [title, bio_text, price_node.get_text(' ', strip=True) if price_node else ''] if part))
        if not title or not _allow_palmer(token_blob):
            continue
        start_date = _parse_first_date_without_year(bio_text, year=current_year)
        if start_date is None:
            continue
        start_at, end_at = _combine_date_and_times(start_date, _parse_time_range(bio_text))
        description = _extract_first_paragraph_text(bio_node)
        location_text = _extract_last_h6_text(bio_node) or "Palmer Museum of Art, University Park, PA"
        source_url = f"{venue.list_urls[0]}#palmer-{_slugify(title)}-{start_date.isoformat()}"
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=description,
                location_text=location_text,
                activity_type=_infer_activity_type(token_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=token_blob,
                price_text=_normalize_space(price_node.get_text(" ", strip=True)) if price_node else description,
            )
        )

    return rows


def _parse_pafa_events(payload: dict, *, venue: PaHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _extract_page_title(soup)
        body_text = _content_text(soup)
        event_info_text = _extract_pafa_event_info(body_text)
        description = _extract_pafa_description(body_text, title) or _extract_first_long_text(soup, min_length=100)
        token_blob = _searchable_blob(" ".join([title, event_info_text or "", description or ""]))
        if not title or not _allow_pafa(token_blob):
            continue
        start_date, end_date = _parse_pafa_dates(event_info_text or body_text)
        if start_date is None:
            continue
        start_at, _ = _combine_date_and_times(start_date, _parse_time_range(event_info_text or body_text))
        end_at = None
        if end_date is not None:
            _, end_at = _combine_date_and_times(end_date, _parse_time_range(event_info_text or body_text))
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=description,
                location_text="Pennsylvania Academy of the Fine Arts, Philadelphia, PA",
                activity_type=_infer_activity_type(token_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=token_blob,
                price_text=_extract_price_text(event_info_text or description or ""),
            )
        )
    return rows


def _parse_philadelphia_events(payload: dict, *, venue: PaHtmlVenueConfig) -> list[ExtractedActivity]:
    html = next(iter((payload.get("list_htmls") or {}).values()), "")
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []

    for anchor in soup.find_all("a", href=True):
        href = urljoin(venue.list_urls[0], anchor.get("href") or "")
        if "start=" not in href or "/events/" not in href:
            continue
        title = _normalize_space(anchor.get_text(" ", strip=True))
        if not title:
            continue
        card = anchor.find_parent(
            "div",
            class_=lambda value: (
                isinstance(value, str) and "group" in value.split()
            ) or (
                isinstance(value, list) and "group" in value
            ),
        )
        if card is None:
            continue
        card_text = _normalize_space(card.get_text(" ", strip=True))
        event_type = card_text.split(" ", 1)[0]
        token_blob = _searchable_blob(" ".join([event_type, title, card_text]))
        if not _allow_philadelphia(event_type, token_blob):
            continue
        start_at = _parse_query_datetime(href, "start")
        end_at = _parse_query_datetime(href, "end")
        if start_at is None:
            continue
        location_text = _extract_philadelphia_location(card_text)
        rows.append(
            _make_row(
                venue=venue,
                source_url=href,
                title=title,
                description=f"Type: {event_type} | Location: {location_text}" if location_text else f"Type: {event_type}",
                location_text=location_text or "Philadelphia Museum of Art, Philadelphia, PA",
                activity_type=_infer_activity_type(token_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=token_blob,
                price_text=card_text,
            )
        )

    return rows


def _parse_woodmere_events(payload: dict, *, venue: PaHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for list_url, html in (payload.get("list_htmls") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        for card in soup.select(".event_item.w-dyn-item"):
            blob = _normalize_space(card.get_text(" ", strip=True))
            if not blob or "$" not in blob:
                continue
            title = _extract_woodmere_title(blob)
            if not title:
                continue
            token_blob = _searchable_blob(" ".join([title, blob]))
            if not _allow_woodmere(token_blob):
                continue
            start_date = _extract_iso_date(blob)
            if start_date is None:
                continue
            start_at, end_at = _combine_date_and_times(start_date, _parse_time_range(blob))
            source_anchor = card.find("a", href=True)
            source_url = urljoin(list_url, source_anchor.get("href") or "") if source_anchor else list_url
            rows.append(
                _make_row(
                    venue=venue,
                    source_url=source_url,
                    title=title,
                    description=blob,
                    location_text="Woodmere Art Museum, Philadelphia, PA",
                    activity_type=_infer_activity_type(token_blob),
                    start_at=start_at,
                    end_at=end_at,
                    token_blob=token_blob,
                    price_text=_extract_price_text(blob),
                )
            )
    return rows


def _make_row(
    *,
    venue: PaHtmlVenueConfig,
    source_url: str,
    title: str,
    description: str | None,
    location_text: str,
    activity_type: str,
    start_at: datetime,
    end_at: datetime | None,
    token_blob: str,
    price_text: str | None,
) -> ExtractedActivity:
    is_free, free_status = infer_price_classification(price_text, default_is_free=None)
    registration_blob = _searchable_blob(" ".join(part for part in [title, description or "", price_text or ""] if part))
    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))
    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=activity_type,
        age_min=age_min,
        age_max=age_max,
        drop_in=(" drop in " in token_blob or " drop-in " in token_blob or " open studio " in token_blob),
        registration_required=(" register " in registration_blob or " rsvp " in registration_blob or " ticket " in registration_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _allow_allentown(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in (" camp ", " camps ", " tour ", " tours ", " exhibition ", " film ")):
        return False
    return any(pattern in token_blob for pattern in (" artventures ", " art can ", " family ", " activity ", " accessible art "))


def _allow_barnes(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in (" exhibition ", " exhibitions ", " storytime ", " film ", " performance ", " party ")):
        return False
    return any(pattern in token_blob for pattern in (" workshop ", " workshops ", " talk ", " talks ", " family ", " art making ", " art-making ", " class ", " classes "))


def _allow_barnes_source(source_url: str, token_blob: str) -> bool:
    path = urlparse(source_url).path
    if "/whats-on/film/" in path or "/whats-on/tours/" in path:
        return False
    if any(segment in path for segment in ("/whats-on/talks/", "/whats-on/workshops/", "/whats-on/family/")):
        return True
    if "/whats-on/free/" in path:
        return any(pattern in token_blob for pattern in (" workshop ", " workshops ", " talk ", " talks ", " family ", " art making ", " art-making "))
    return False


def _allow_brandywine(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in (" free first sunday ", " tai chi ", " t ai chi ", " yoga ")):
        return False
    if any(pattern in token_blob for pattern in (" art chat ", " workshop ", " sensory-friendly ", " family ", " artz ")):
        return True
    if any(pattern in token_blob for pattern in (" tour ", " tours ")):
        return False
    return False


def _allow_erie(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in (" access for all ", " camp ", " camps ", " tour ", " tours ", " gallery night ")):
        return False
    return any(pattern in token_blob for pattern in (" class ", " classes ", " workshop ", " workshops ", " lecture ", " talk ", " painting "))


def _allow_palmer(token_blob: str) -> bool:
    if any(
        pattern in token_blob
        for pattern in (" yoga ", " mindfulness ", " screening ", " recruitment ", " pop up exhibition ", " pop-up exhibition ", " drop in tour ", " drop-in tour ", " sensory friendly hours ", " slow art day ")
    ):
        return False
    return any(
        pattern in token_blob
        for pattern in (" gallery talk ", " conversation ", " lecture ", " workshop ", " family day ", " palmer art kids ", " creative studio ", " curator talk ", " artist lecture ")
    )


def _allow_pafa(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in (" exhibition reception ", " exhibition tour ", " exhibition opening ", " members day ", " soirée ", " soiree ", " special events ")):
        return False
    return any(pattern in token_blob for pattern in (" workshop ", " workshops ", " art at noon ", " lecture ", " talk ", " continuing education "))


def _allow_philadelphia(event_type: str, token_blob: str) -> bool:
    if event_type == "Activities":
        return True
    if event_type == "Talks":
        return True
    if event_type == "Social":
        return any(pattern in token_blob for pattern in (" sketch ", " workshop ", " class ", " conversation ", " talk "))
    return False


def _allow_woodmere(token_blob: str) -> bool:
    return any(pattern in token_blob for pattern in (" class ", " classes ", " workshop ", " workshops ", " teen ", " teens ", " youth ", " kids ", " children "))


def _extract_allentown_title(blob: str) -> str | None:
    match = re.search(r"\d{2}\s+[A-Za-z]{3}\s+\d{1,2}:\d{2}\s*[ap]m\s+\d{1,2}:\d{2}\s*[ap]m\s+(.+?)\s+Event Details", blob, re.IGNORECASE)
    return _normalize_space(match.group(1)) if match else None


def _extract_allentown_description(blob: str) -> str | None:
    match = re.search(r"Event Details\s+(.+?)\s+Time\s+\(", blob, re.IGNORECASE)
    if match:
        return _normalize_space(match.group(1))
    return None


def _extract_woodmere_title(blob: str) -> str | None:
    match = re.search(
        r"(?:am|pm)\s*-\s*(?:\d{1,2}:\d{2}\s*[ap]m)\s+(.+?)\s+\$\d",
        blob,
        re.IGNORECASE,
    )
    if match:
        return _normalize_space(match.group(1))
    return None


def _extract_date_from_google_calendar(href: str | None) -> date | None:
    if not href:
        return None
    parsed = urlparse(href)
    dates = parse_qs(parsed.query).get("dates")
    if not dates:
        return None
    value = dates[0].split("/", 1)[0]
    if len(value) < 8:
        return None
    try:
        return datetime.strptime(value[:8], "%Y%m%d").date()
    except ValueError:
        return None


def _extract_iso_date(text: str) -> date | None:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", text)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y-%m-%d").date()


def _extract_page_title(soup: BeautifulSoup) -> str:
    for node in soup.find_all(["h1", "title"]):
        text = _normalize_space(node.get_text(" ", strip=True))
        if not text:
            continue
        if " | " in text:
            text = text.split(" | ", 1)[0].strip()
        if " — " in text:
            text = text.split(" — ", 1)[0].strip()
        return text
    return ""


def _page_text(soup: BeautifulSoup) -> str:
    return _normalize_space(soup.get_text(" ", strip=True))


def _content_text(soup: BeautifulSoup) -> str:
    content_nodes = soup.select("main, article")
    if not content_nodes:
        return _page_text(soup)
    return _normalize_space(" ".join(node.get_text(" ", strip=True) for node in content_nodes))


def _extract_first_long_text(soup: BeautifulSoup, *, min_length: int) -> str | None:
    for node in soup.find_all(["p", "div"]):
        text = _normalize_space(node.get_text(" ", strip=True))
        if len(text) >= min_length:
            return text
    return None


def _extract_first_paragraph_text(node) -> str | None:
    if node is None:
        return None
    for paragraph in node.find_all("p"):
        text = _normalize_space(paragraph.get_text(" ", strip=True))
        if len(text) >= 40:
            return text
    return _normalize_space(node.get_text(" ", strip=True)) or None


def _extract_last_h6_text(node) -> str | None:
    if node is None:
        return None
    values = [_normalize_space(tag.get_text(" ", strip=True)) for tag in node.find_all("h6")]
    values = [value for value in values if value]
    return values[-1] if values else None


def _extract_price_text(text: str) -> str | None:
    match = PRICE_RE.search(text)
    if match:
        return _normalize_space(text[max(0, match.start() - 20): match.end() + 60])

    lower = text.lower()
    for marker in ("free", "included with museum admission", "included with admission", "requires registration", "tuition"):
        if marker in lower:
            idx = lower.index(marker)
            return _normalize_space(text[max(0, idx - 30): idx + 120])
    return None


def _extract_near_title(text: str, title: str, *, max_chars: int = 800) -> str:
    if not text:
        return ""
    if title:
        idx = text.lower().find(title.lower())
        if idx >= 0:
            return text[idx: idx + max_chars]
    return text[:max_chars]


def _extract_barnes_description(text: str, title: str) -> str | None:
    segment = _extract_near_title(text, title, max_chars=1800)
    for marker in (" Go Further ", " Speaker ", " Related Programs ", " Image ", " Credits "):
        if marker in segment:
            segment = segment.split(marker, 1)[0].strip()
            break
    return _normalize_space(segment) or None


def _extract_brandywine_event_details(text: str) -> tuple[date | None, str | None, str | None, str | None]:
    match = BRANDYWINE_EVENT_DETAILS_RE.search(text)
    if not match:
        return None, None, None, None
    try:
        start_date = datetime.strptime(match.group(1), "%B %d, %Y").date()
    except ValueError:
        start_date = None
    return (
        start_date,
        _normalize_space(match.group(2)),
        _normalize_space(match.group(3)),
        _normalize_space(match.group(4)),
    )


def _extract_erie_event_details(text: str) -> dict[str, str | date | None]:
    match = ERIE_EVENT_DETAILS_RE.search(text)
    if not match:
        return {
            "date": None,
            "time_text": None,
            "location_text": None,
            "description": None,
        }
    try:
        start_date = datetime.strptime(match.group(2), "%A, %B %d, %Y").date()
    except ValueError:
        start_date = None
    location_text = _normalize_space(match.group(5))
    return {
        "date": start_date,
        "time_text": f"{match.group(3)} - {match.group(4)}",
        "location_text": location_text,
        "description": _normalize_space(match.group(6)),
    }


def _extract_pafa_event_info(text: str) -> str | None:
    start = text.find("Event Information")
    if start < 0:
        return None
    segment = text[start:]
    for marker in (" We're so excited you're planning to visit PAFA!", " Subscribe to PAFA Happenings ", " Location 118-128 North Broad Street "):
        if marker in segment:
            segment = segment.split(marker, 1)[0]
            break
    return _normalize_space(segment) or None


def _extract_pafa_description(text: str, title: str) -> str | None:
    segment = _extract_near_title(text, title, max_chars=2200)
    for marker in (" Links Full Class Catalog ", " We're so excited you're planning to visit PAFA!", " Subscribe to PAFA Happenings "):
        if marker in segment:
            segment = segment.split(marker, 1)[0].strip()
            break
    return _normalize_space(segment) or None


def _extract_philadelphia_location(card_text: str) -> str | None:
    match = re.search(r"\d{1,2}:\d{2}\s*[ap]m\s*[–-]\s*\d{1,2}:\d{2}\s*[ap]m\s+(.+)$", card_text, re.IGNORECASE)
    return _normalize_space(match.group(1)) if match else None


def _parse_first_date_with_year(text: str) -> date | None:
    match = DATE_WITH_YEAR_RE.search(text)
    if not match:
        return None
    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    if month is None:
        return None
    return date(int(match.group(3)), month, int(match.group(2)))


def _parse_first_date_without_year(text: str, *, year: int) -> date | None:
    match = DATE_NO_YEAR_RE.search(text)
    if not match:
        return None
    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    if month is None:
        return None
    return date(year, month, int(match.group(2)))


def _parse_time_range(text: str) -> tuple[time | None, time | None]:
    match = TIME_RANGE_RE.search(text)
    if match:
        start_meridiem = match.group(3)
        end_meridiem = match.group(6)
        start_time = _parse_clock(match.group(1), match.group(2), start_meridiem, fallback_meridiem=end_meridiem)
        end_time = _parse_clock(match.group(4), match.group(5), end_meridiem, fallback_meridiem=start_meridiem)
        return start_time, end_time

    special_end = TIME_RANGE_TO_SPECIAL_RE.search(text)
    if special_end:
        start_time = _parse_clock(special_end.group(1), special_end.group(2), special_end.group(3))
        end_time = _parse_clock("12" if special_end.group(4).lower() == "noon" else "12", None, special_end.group(4))
        return start_time, end_time

    adjacent = TIME_ADJACENT_RE.search(text)
    if adjacent:
        start_time = _parse_clock(adjacent.group(1), adjacent.group(2), adjacent.group(3))
        end_time = _parse_clock(adjacent.group(4), adjacent.group(5), adjacent.group(6))
        return start_time, end_time

    single = TIME_SINGLE_RE.search(text)
    if single:
        return _parse_clock(single.group(1), single.group(2), single.group(3)), None

    return None, None


def _parse_clock(
    hour_text: str,
    minute_text: str | None,
    meridiem_text: str | None,
    *,
    fallback_meridiem: str | None = None,
) -> time | None:
    minute = int(minute_text or 0)
    meridiem = (meridiem_text or fallback_meridiem or "").lower().replace(".", "")
    if meridiem == "noon":
        return time(12, 0)
    if meridiem == "midnight":
        return time(0, 0)
    hour = int(hour_text)
    if hour > 23:
        return None
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return time(hour, minute)


def _combine_date_and_times(value: date, times: tuple[time | None, time | None]) -> tuple[datetime, datetime | None]:
    start_time, end_time = times
    if start_time is None:
        start_time = time(0, 0)
    start_at = datetime.combine(value, start_time).replace(tzinfo=ZoneInfo(NY_TIMEZONE))
    end_at = datetime.combine(value, end_time).replace(tzinfo=ZoneInfo(NY_TIMEZONE)) if end_time is not None else None
    return start_at, end_at


def _parse_query_datetime(url: str, key: str) -> datetime | None:
    parsed = urlparse(url)
    values = parse_qs(parsed.query).get(key)
    if not values:
        return None
    try:
        return datetime.fromisoformat(values[0].replace("Z", "+00:00")).astimezone(ZoneInfo(NY_TIMEZONE))
    except ValueError:
        return None


def _parse_pafa_dates(text: str) -> tuple[date | None, date | None]:
    range_match = PAFA_DATE_RANGE_RE.search(text)
    if range_match:
        start_date = datetime.strptime(range_match.group(1), "%B %d, %Y").date()
        end_date = datetime.strptime(range_match.group(2), "%B %d, %Y").date()
        return start_date, end_date

    single_match = PAFA_SINGLE_DATE_RE.search(text)
    if single_match:
        start_date = datetime.strptime(single_match.group(1), "%B %d, %Y").date()
        return start_date, None
    return None, None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _infer_activity_type(token_blob: str) -> str:
    if any(
        pattern in token_blob
        for pattern in (
            " art making ",
            " art-making ",
            " class ",
            " classes ",
            " creative ",
            " drawing ",
            " hands on ",
            " hands-on ",
            " open studio ",
            " paint ",
            " painting ",
            " printmaking ",
            " sculpture ",
            " studio ",
            " workshop ",
            " workshops ",
        )
    ):
        return "workshop"
    if any(pattern in token_blob for pattern in (" lecture ", " lectures ", " talk ", " talks ", " conversation ", " panel ", " discussion ")):
        return "lecture"
    return "workshop"


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9+]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())
