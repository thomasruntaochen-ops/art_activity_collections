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
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

WA_TIMEZONE = "America/Los_Angeles"

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

DATE_WITH_YEAR_RE = re.compile(r"([A-Za-z]{3,9})\s+(\d{1,2})(?:,)?\s+(\d{4})")
DATE_WITHOUT_YEAR_RE = re.compile(
    r"(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?),?\s+"
    r"([A-Za-z]{3,9})\s+(\d{1,2})",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?)?\s*(?:-|–|—|to)\s*(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?)\b", re.IGNORECASE)
SAM_MULTI_DATE_RE = re.compile(r"\b([A-Z][a-z]{2})\s+(\d{1,2})(?:\s*&\s*(\d{1,2}))?")


@dataclass(frozen=True, slots=True)
class WaHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    needs_playwright: bool = False


WA_HTML_VENUES: tuple[WaHtmlVenueConfig, ...] = (
    WaHtmlVenueConfig(
        slug="bima",
        source_name="wa_bima_events",
        venue_name="Bainbridge Island Museum of Art",
        city="Bainbridge Island",
        state="WA",
        list_url="https://www.biartmuseum.org/calendar-events/",
    ),
    WaHtmlVenueConfig(
        slug="frye",
        source_name="wa_frye_events",
        venue_name="Frye Art Museum",
        city="Seattle",
        state="WA",
        list_url="https://fryemuseum.org/calendar",
    ),
    WaHtmlVenueConfig(
        slug="henry",
        source_name="wa_henry_events",
        venue_name="Henry Art Gallery",
        city="Seattle",
        state="WA",
        list_url="https://henryart.org/programs/all",
    ),
    WaHtmlVenueConfig(
        slug="mona",
        source_name="wa_mona_events",
        venue_name="Museum of Northwest Art",
        city="La Conner",
        state="WA",
        list_url="https://www.monamuseum.org/events",
    ),
    WaHtmlVenueConfig(
        slug="sam",
        source_name="wa_sam_events",
        venue_name="Seattle Art Museum",
        city="Seattle",
        state="WA",
        list_url="https://www.seattleartmuseum.org/whats-on/events",
        needs_playwright=True,
    ),
    WaHtmlVenueConfig(
        slug="western",
        source_name="wa_western_events",
        venue_name="Western Gallery",
        city="Bellingham",
        state="WA",
        list_url="https://westerngallery.wwu.edu/events-calendar",
    ),
    WaHtmlVenueConfig(
        slug="mac",
        source_name="wa_mac_events",
        venue_name="Northwest Museum of Arts and Culture",
        city="Spokane",
        state="WA",
        list_url="https://sales.northwestmuseum.org/calendar.aspx?view=c&et=p+c",
        needs_playwright=True,
    ),
)

WA_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in WA_HTML_VENUES}


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
            timezone_id=WA_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def load_wa_html_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    max_links_per_venue: int = 120,
) -> dict[str, dict]:
    selected = (
        [WA_HTML_VENUES_BY_SLUG[slug] for slug in venue_slugs]
        if venue_slugs
        else list(WA_HTML_VENUES)
    )
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            if venue.needs_playwright:
                list_html = await fetch_html_playwright(venue.list_url)
            else:
                list_html = await fetch_html(venue.list_url, client=client)

            detail_pages: dict[str, str] = {}
            link_candidates = _extract_listing_links(venue.slug, list_html, venue.list_url)[:max_links_per_venue]
            if link_candidates:
                for detail_url, _ in link_candidates:
                    if venue.slug in {"sam", "mona"}:
                        break
                    try:
                        detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                    except Exception as exc:
                        print(f"[wa-html-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")

            payload[venue.slug] = {
                "list_html": list_html,
                "detail_pages": detail_pages,
            }

    return payload


def parse_wa_html_events(payload: dict, *, venue: WaHtmlVenueConfig) -> list[ExtractedActivity]:
    if venue.slug == "bima":
        rows = _parse_bima_events(payload, venue=venue)
    elif venue.slug == "frye":
        rows = _parse_frye_events(payload, venue=venue)
    elif venue.slug == "henry":
        rows = _parse_henry_events(payload, venue=venue)
    elif venue.slug == "mona":
        rows = _parse_mona_events(payload, venue=venue)
    elif venue.slug == "sam":
        rows = _parse_sam_events(payload, venue=venue)
    elif venue.slug == "western":
        rows = _parse_western_events(payload, venue=venue)
    elif venue.slug == "mac":
        rows = _parse_mac_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(WA_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


class WaHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "wa_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_wa_html_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_wa_html_bundle_payload/parse_wa_html_events from script runner.")


def _extract_listing_links(venue_slug: str, html: str, list_url: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links_by_url: dict[str, str] = {}

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        text = _normalize_space(anchor.get_text(" ", strip=True))
        if venue_slug == "bima":
            if "/event/" not in href:
                continue
        elif venue_slug == "frye":
            if "/node/" not in href and "/programs/" not in href:
                continue
        elif venue_slug == "henry":
            if "/programs/" not in href or href.endswith("/programs/all"):
                continue
        elif venue_slug == "western":
            if not href.startswith("/artist-talk"):
                continue
        elif venue_slug == "mac":
            if "Performance.aspx?pid=" not in href:
                continue
        elif venue_slug == "mona":
            if "/events/" not in href or "?format=ical" in href or text == "View Event →":
                continue
        elif venue_slug == "sam":
            if "/whats-on/events/" not in href:
                continue
        else:
            continue

        container = anchor.find_parent(["article", "li", "div", "section", "td"]) or anchor
        blob = _normalize_space(container.get_text(" ", strip=True)) or text
        absolute_url = urljoin(list_url, href)
        previous = links_by_url.get(absolute_url)
        if previous is None or len(blob) > len(previous):
            links_by_url[absolute_url] = blob

    return list(links_by_url.items())


def _parse_bima_events(payload: dict, *, venue: WaHtmlVenueConfig) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []

    for source_url, blob in _extract_listing_links("bima", list_html, venue.list_url):
        if " every " in _searchable_blob(blob):
            continue
        detail_html = detail_pages.get(source_url) or ""
        title = _extract_page_title(detail_html) or _extract_bima_title(blob)
        when_text = _extract_bima_detail(detail_html, "When")
        description = _extract_bima_description(detail_html)
        signal_blob = _searchable_blob(" ".join(part for part in [title, blob, when_text or "", description or ""] if part))
        if not _allow_bima(signal_blob):
            continue
        start_date = _parse_first_date_with_year(when_text or blob) or _parse_first_date_with_year(detail_html)
        if start_date is None:
            continue
        start_at, end_at = _combine_date_and_times(start_date, _parse_time_range(when_text or detail_html))
        price_text = _extract_bima_detail(detail_html, "Tickets")
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=_join_non_empty([description, price_text]),
                location_text="Bainbridge Island Museum of Art, Bainbridge Island, WA",
                activity_type=_infer_activity_type(signal_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=signal_blob,
                price_text=price_text or description,
            )
        )

    return rows


def _parse_frye_events(payload: dict, *, venue: WaHtmlVenueConfig) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []
    today = datetime.now(ZoneInfo(WA_TIMEZONE)).date()

    for source_url, blob in _extract_listing_links("frye", list_html, venue.list_url):
        if not any(day in blob for day in ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")):
            continue
        detail_html = detail_pages.get(source_url) or ""
        title = _extract_frye_title(blob, detail_html)
        price_text = _extract_price_text(detail_html)
        signal_blob = _searchable_blob(" ".join(part for part in [title, blob, detail_html] if part))
        if not _allow_frye(signal_blob):
            continue
        start_date = _parse_month_day_without_year(blob, today=today)
        if start_date is None:
            continue
        start_at, end_at = _combine_date_and_times(start_date, _parse_time_range(detail_html))
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=None,
                location_text="Frye Art Museum, Seattle, WA",
                activity_type=_infer_activity_type(signal_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=signal_blob,
                price_text=price_text,
            )
        )

    return rows


def _parse_henry_events(payload: dict, *, venue: WaHtmlVenueConfig) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []

    for source_url, blob in _extract_listing_links("henry", list_html, venue.list_url):
        detail_html = detail_pages.get(source_url) or ""
        title = _extract_page_title(detail_html) or _extract_henry_title(blob)
        title_blob = _searchable_blob(" ".join(part for part in [title, blob] if part))
        if not _allow_henry_title_blob(title_blob):
            continue
        start_at, end_at = _parse_henry_datetime(detail_html)
        if start_at is None:
            continue
        price_text = _extract_price_text(detail_html)
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=None,
                location_text="Henry Art Gallery, Seattle, WA",
                activity_type=_infer_activity_type(title_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=title_blob,
                price_text=price_text,
            )
        )

    return rows


def _parse_mona_events(payload: dict, *, venue: WaHtmlVenueConfig) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    rows: list[ExtractedActivity] = []

    for source_url, blob in _extract_listing_links("mona", list_html, venue.list_url):
        title = _extract_mona_title(blob)
        signal_blob = _searchable_blob(blob)
        if not _allow_mona(signal_blob):
            continue
        start_at, end_at = _parse_mona_datetime(blob)
        if start_at is None:
            continue
        description = _extract_mona_description(blob)
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=description,
                location_text="Museum of Northwest Art, La Conner, WA",
                activity_type=_infer_activity_type(signal_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=signal_blob,
                price_text=blob,
            )
        )

    return rows


def _parse_sam_events(payload: dict, *, venue: WaHtmlVenueConfig) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    rows: list[ExtractedActivity] = []

    for source_url, blob in _extract_listing_links("sam", list_html, venue.list_url):
        signal_blob = _searchable_blob(blob)
        if not _allow_sam(signal_blob):
            continue
        title = _extract_sam_title(blob)
        times = _parse_time_range(blob)
        if " & " in title or "sam creates: figure drawing" in title.lower():
            for start_date in _parse_sam_multi_dates(blob):
                if start_date < datetime.now(ZoneInfo(WA_TIMEZONE)).date():
                    continue
                start_at, end_at = _combine_date_and_times(start_date, times)
                rows.append(
                    _make_row(
                        venue=venue,
                        source_url=source_url,
                        title="SAM Creates: Figure Drawing",
                        description=None,
                        location_text=_extract_sam_location(blob),
                        activity_type="workshop",
                        start_at=start_at,
                        end_at=end_at,
                        token_blob=signal_blob,
                        price_text=blob,
                    )
                )
            continue

        start_date = _parse_first_date_with_year(blob)
        if start_date is None:
            continue
        start_at, end_at = _combine_date_and_times(start_date, times)
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=None,
                location_text=_extract_sam_location(blob),
                activity_type=_infer_sam_activity_type(signal_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=signal_blob,
                price_text=blob,
            )
        )

    return rows


def _parse_western_events(payload: dict, *, venue: WaHtmlVenueConfig) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []

    for source_url, blob in _extract_listing_links("western", list_html, venue.list_url):
        detail_html = detail_pages.get(source_url) or ""
        title = _extract_page_title(detail_html) or _extract_western_title(blob)
        signal_blob = _searchable_blob(" ".join(part for part in [title, blob, detail_html] if part))
        if not _allow_western(signal_blob):
            continue
        start_at, end_at = _parse_western_datetime(detail_html)
        if start_at is None:
            continue
        price_text = _extract_price_text(detail_html)
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=None,
                location_text="Western Gallery, Bellingham, WA",
                activity_type="lecture",
                start_at=start_at,
                end_at=end_at,
                token_blob=signal_blob,
                price_text=price_text,
            )
        )

    return rows


def _parse_mac_events(payload: dict, *, venue: WaHtmlVenueConfig) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []

    for source_url, blob in _extract_listing_links("mac", list_html, venue.list_url):
        detail_html = detail_pages.get(source_url) or ""
        title = _extract_mac_title(detail_html) or _extract_mac_title(blob)
        signal_blob = _searchable_blob(" ".join(part for part in [title, blob, detail_html] if part))
        if not _allow_mac(signal_blob):
            continue
        start_at, end_at = _parse_mac_datetime(blob, detail_html)
        if start_at is None:
            continue
        price_text = _extract_price_text(detail_html)
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=None,
                location_text="Northwest Museum of Arts and Culture, Spokane, WA",
                activity_type=_infer_activity_type(_searchable_blob(title)),
                start_at=start_at,
                end_at=end_at,
                token_blob=signal_blob,
                price_text=price_text,
            )
        )

    return rows


def _make_row(
    *,
    venue: WaHtmlVenueConfig,
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
    is_free, free_status = infer_price_classification(price_text)
    registration_blob = _searchable_blob(" ".join(part for part in [title, description or "", price_text or ""] if part))
    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=activity_type,
        age_min=_parse_age_min(token_blob),
        age_max=_parse_age_max(token_blob),
        drop_in=("drop-in" in token_blob or "drop in" in token_blob),
        registration_required=("register" in registration_blob or "rsvp" in registration_blob or "ticket" in registration_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=WA_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _allow_bima(token_blob: str) -> bool:
    if any(text in token_blob for text in (" film ", " performance ", " music ", " reception ", " poetry ", " reading ", " camp ", " camps ", " tour ", " tours ")):
        return False
    return any(text in token_blob for text in (" workshop ", " activity ", " lecture ", " talk ", " discussion ", " panel "))


def _allow_frye(token_blob: str) -> bool:
    if any(text in token_blob for text in (" film ", " mindfulness ", " meditation ", " story time ", " storytime ")):
        return False
    return any(text in token_blob for text in (" art-making ", " discussion ", " conversation ", " class ", " studio ", " lecture ", " talk "))


def _allow_henry(token_blob: str) -> bool:
    if any(text in token_blob for text in (" performance ", " reading ", " film ", " meditation ", " gala ", " preview ", " opening ", " launch ")):
        return False
    return any(text in token_blob for text in (" lecture ", " talk ", " discussion ", " conversation ", " panel ", " workshop "))


def _allow_henry_title_blob(token_blob: str) -> bool:
    if any(text in token_blob for text in (" performance ", " reading ", " film ", " meditation ", " gala ", " preview ", " opening ", " launch ", " mindfulness ", " studio ")):
        return False
    if " talks performances " in token_blob:
        return True
    return any(text in token_blob for text in (" lecture ", " talk ", " discussion ", " conversation ", " panel ", " workshop "))


def _allow_mona(token_blob: str) -> bool:
    if any(text in token_blob for text in (" book club ", " open mic ", " auction ", " camp ", " soirée ", " soiree ", " opening ", " member meeting ", " last day to view ", " writing ", " poetry ")):
        return False
    return any(text in token_blob for text in (" workshop ", " youth ", " teen ", " early enrichment ", " conversation ", " talk ", " class ", " activity ", " art club "))


def _allow_sam(token_blob: str) -> bool:
    if any(
        text in token_blob
        for text in (
            " public tour ",
            " tours ",
            " happy hour ",
            " remix ",
            " gallery presents ",
            " art show ",
            " dinner ",
            " film ",
            " performance ",
            " demonstration ",
            " first thursday ",
        )
    ):
        return False
    return any(
        text in token_blob
        for text in (
            " sam creates ",
            " conversations with curators ",
            " sam talks ",
            " educator workshop ",
            " family saturday ",
            " teen night out ",
            " saturday university ",
            " saturdays at sam ",
            " family day ",
        )
    )


def _allow_western(token_blob: str) -> bool:
    return " artist talk " in token_blob or " discussion " in token_blob


def _allow_mac(token_blob: str) -> bool:
    if any(text in token_blob for text in (" storytime ", " tour ", " screening ", " film ")):
        return False
    return any(text in token_blob for text in (" gallery talk ", " lecture ", " class ", " workshop ", " author talk "))


def _extract_page_title(html: str) -> str | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    for node in soup.find_all(["h1", "title"]):
        text = _normalize_space(node.get_text(" ", strip=True))
        if not text:
            continue
        if " | " in text:
            text = text.split(" | ", 1)[0].strip()
        if " - " in text:
            text = text.split(" - ", 1)[0].strip()
        return text
    return None


def _extract_bima_description(html: str) -> str | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    node = soup.select_one(".event-description")
    if node is None:
        return None
    text = _normalize_space(node.get_text(" ", strip=True))
    return text or None


def _extract_bima_detail(html: str, heading: str) -> str | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    for node in soup.select(".event-detail-heading"):
        if _normalize_space(node.get_text(" ", strip=True)).lower() != heading.lower():
            continue
        detail_node = node.find_next_sibling("div")
        if detail_node is None:
            continue
        text = _normalize_space(detail_node.get_text(" ", strip=True))
        return text or None
    return None


def _extract_first_description(html: str) -> str | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    for paragraph in soup.find_all(["p", "div"]):
        text = _normalize_space(paragraph.get_text(" ", strip=True))
        if len(text) < 40:
            continue
        lowered = text.lower()
        if any(
            marker in lowered
            for marker in (
                "instagram",
                "hours & admission",
                "questions about programs",
                "header navigation",
                "closed today",
            )
        ):
            continue
        return text
    return None


def _extract_price_text(html: str) -> str | None:
    if not html:
        return None
    text = _html_to_text(html)
    for marker in (
        "cost:",
        "admission",
        "free, donations welcome",
        "rsvp encouraged",
        "pay-what-you-can",
        "$",
    ):
        index = text.lower().find(marker)
        if index != -1:
            return _normalize_space(text[index:index + 220])
    return None


def _extract_bima_title(blob: str) -> str:
    text = _normalize_space(blob)
    text = re.sub(r"^(Workshops|Lectures & Presentations|Artist Talk|Exhibition-related|Kids|Teens|Creative Aging|Drop-in Programs)\s+", "", text)
    text = re.sub(r"\b[A-Z][a-z]{2,8}\s+\d{1,2},\s+\d{4}.*$", "", text).strip()
    return text


def _extract_frye_title(blob: str, detail_html: str) -> str:
    page_title = _extract_page_title(detail_html)
    text = _normalize_space(blob)
    blob_title = text
    for marker in (" Monday", " Tuesday", " Wednesday", " Thursday", " Friday", " Saturday", " Sunday"):
        if marker in blob_title:
            blob_title = blob_title.split(marker, 1)[0].strip()
    if page_title and len(page_title) >= len(blob_title):
        return page_title
    return blob_title


def _extract_henry_title(blob: str) -> str:
    text = _normalize_space(blob)
    text = re.sub(r"^(Programs|Events|Talks & Performances|Workshop|Screenings|Meditation)\s+", "", text)
    text = re.sub(r"[A-Z][a-z]{2,8}\s+\d{1,2},\s+\d{4}.*$", "", text).strip()
    return text


def _extract_mona_title(blob: str) -> str:
    markers = (
        " Monday,",
        " Tuesday,",
        " Wednesday,",
        " Thursday,",
        " Friday,",
        " Saturday,",
        " Sunday,",
    )
    text = _normalize_space(blob)
    for marker in markers:
        if marker in text:
            text = text.split(marker, 1)[0].strip()
            break
    text = re.sub(r"^[A-Z][a-z]{2}\s+\d{1,2}\s+", "", text).strip()
    return text


def _extract_mona_description(blob: str) -> str | None:
    text = _normalize_space(blob)
    marker = "Google Calendar ICS"
    if marker not in text:
        return None
    desc = text.split(marker, 1)[1].strip()
    desc = desc.rsplit("View Event →", 1)[0].strip()
    return desc or None


def _extract_sam_title(blob: str) -> str:
    text = _normalize_space(blob)
    text = re.sub(r"^Event\s+", "", text)
    date_match = DATE_WITH_YEAR_RE.search(text)
    if date_match:
        text = text[:date_match.start()].strip()
    text = re.sub(r"\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b$", "", text).strip()
    return text


def _extract_sam_location(blob: str) -> str:
    text = _normalize_space(blob)
    for location in (
        "Seattle Art Museum",
        "Seattle Asian Art Museum",
        "Olympic Sculpture Park",
    ):
        if location in text:
            return f"{location}, Seattle, WA"
    return "Seattle, WA"


def _extract_western_title(blob: str) -> str:
    return _normalize_space(blob)


def _extract_mac_title(text: str) -> str:
    normalized = _normalize_space(_html_to_text(text))
    if "Event Name:" in normalized:
        remainder = normalized.split("Event Name:", 1)[1].strip()
        for marker in ("Location:", "Event Date:", "Event Time:", "Duration:"):
            if marker in remainder:
                remainder = remainder.split(marker, 1)[0].strip()
                break
        return remainder
    return normalized.split(" March", 1)[0].split(" April", 1)[0].strip()


def _parse_first_date_with_year(text: str) -> date | None:
    match = DATE_WITH_YEAR_RE.search(_html_to_text(text))
    if not match:
        return None
    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    if month is None:
        return None
    return date(int(match.group(3)), month, int(match.group(2)))


def _parse_month_day_without_year(text: str, *, today: date) -> date | None:
    match = DATE_WITHOUT_YEAR_RE.search(_html_to_text(text))
    if not match:
        return None
    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    if month is None:
        return None
    year = today.year
    candidate = date(year, month, int(match.group(2)))
    if candidate < today.replace(day=1) and month < today.month - 1:
        candidate = date(year + 1, month, int(match.group(2)))
    return candidate


def _parse_time_range(text: str) -> tuple[time | None, time | None]:
    normalized = _html_to_text(text)
    match = TIME_RANGE_RE.search(normalized)
    if match:
        end_meridiem = match.group(6)
        start_meridiem = match.group(3) or end_meridiem
        if start_meridiem is None or end_meridiem is None:
            return None, None
        start = _build_time(match.group(1), match.group(2), start_meridiem)
        end = _build_time(match.group(4), match.group(5), end_meridiem)
        return start, end
    single_matches = TIME_SINGLE_RE.findall(normalized)
    if single_matches:
        start = _build_time(*single_matches[0])
        end = _build_time(*single_matches[1]) if len(single_matches) > 1 else None
        return start, end
    return None, None


def _combine_date_and_times(
    start_date: date,
    times: tuple[time | None, time | None],
) -> tuple[datetime, datetime | None]:
    start_time, end_time = times
    start_at = datetime.combine(start_date, start_time or time.min)
    end_at = datetime.combine(start_date, end_time) if end_time is not None else None
    return start_at, end_at


def _parse_henry_datetime(html: str) -> tuple[datetime | None, datetime | None]:
    text = _html_to_text(html)
    match = re.search(
        r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+([A-Za-z]{3,9})\s+(\d{1,2}),\s+(\d{4}),\s+"
        r"(\d{1,2}:\d{2}\s*[APap]\.?M\.?)\s*[—-]\s*(\d{1,2}:\d{2}\s*[APap]\.?M\.?)",
        text,
    )
    if not match:
        return None, None
    start_date = date(int(match.group(4)), MONTH_LOOKUP[match.group(2)[:3].lower()], int(match.group(3)))
    start_at, end_at = _combine_date_and_times(
        start_date,
        (_parse_single_time(match.group(5)), _parse_single_time(match.group(6))),
    )
    return start_at, end_at


def _parse_mona_datetime(blob: str) -> tuple[datetime | None, datetime | None]:
    start_date = _parse_first_date_with_year(blob)
    if start_date is None:
        return None, None
    return _combine_date_and_times(start_date, _parse_time_range(blob))


def _parse_sam_multi_dates(blob: str) -> list[date]:
    normalized = _normalize_space(blob)
    dates: list[date] = []
    current_year = datetime.now(ZoneInfo(WA_TIMEZONE)).year
    for match in SAM_MULTI_DATE_RE.finditer(normalized):
        month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
        if month is None:
            continue
        dates.append(date(current_year, month, int(match.group(2))))
        if match.group(3):
            dates.append(date(current_year, month, int(match.group(3))))
    return dates


def _infer_sam_activity_type(token_blob: str) -> str:
    if any(text in token_blob for text in (" conversations with curators ", " sam talks ", " saturday university ")):
        return "lecture"
    return "workshop"


def _parse_western_datetime(html: str) -> tuple[datetime | None, datetime | None]:
    text = _html_to_text(html)
    match = re.search(
        r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+([A-Za-z]{3,9})\s+(\d{1,2}),\s+(\d{4})\s*-\s*(\d{1,2}:\d{2}\s*[ap]m)",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None, None
    start_date = date(int(match.group(4)), MONTH_LOOKUP[match.group(2)[:3].lower()], int(match.group(3)))
    end_match = re.search(r"Concludes:\s*(\d{1,2}:\d{2}\s*[AP]M)", text, re.IGNORECASE)
    start_at, end_at = _combine_date_and_times(
        start_date,
        (_parse_single_time(match.group(5)), _parse_single_time(end_match.group(1)) if end_match else None),
    )
    return start_at, end_at


def _parse_mac_datetime(blob: str, html: str) -> tuple[datetime | None, datetime | None]:
    detail_text = _html_to_text(html)
    start_date = _parse_first_date_with_year(detail_text) or _parse_first_date_with_year(blob)
    if start_date is None:
        return None, None
    return _combine_date_and_times(start_date, _parse_time_range(blob or detail_text))


def _parse_single_time(value: str) -> time | None:
    match = TIME_SINGLE_RE.search(_normalize_space(value))
    if not match:
        return None
    return _build_time(match.group(1), match.group(2), match.group(3))


def _build_time(hour_text: str, minute_text: str | None, meridiem: str) -> time:
    hour = int(hour_text)
    minute = int(minute_text or "0")
    meridiem = meridiem.replace(".", "").lower()
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return time(hour, minute)


def _infer_activity_type(token_blob: str) -> str:
    if any(text in token_blob for text in (" workshop ", " activity ", " class ", " studio ", " art-making ", " figure drawing ", " early enrichment ")):
        return "workshop"
    if any(text in token_blob for text in (" lecture ", " lectures ", " talk ", " talks ", " conversation ", " discussions ", " discussion ", " panel ")):
        return "lecture"
    return "workshop"


def _parse_age_min(token_blob: str) -> int | None:
    match = re.search(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", token_blob)
    if match:
        return int(match.group(1))
    match = re.search(r"\bages?\s*(\d{1,2})\+\b", token_blob)
    if match:
        return int(match.group(1))
    return None


def _parse_age_max(token_blob: str) -> int | None:
    match = re.search(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", token_blob)
    if match:
        return int(match.group(2))
    return None


def _html_to_text(value: str) -> str:
    if not value:
        return ""
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _join_non_empty(parts: list[str | None]) -> str | None:
    filtered = [part.strip() for part in parts if part and part.strip()]
    return " | ".join(filtered) if filtered else None


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _searchable_blob(value: str) -> str:
    normalized = _normalize_space(value).lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return f" {' '.join(normalized.split())} "


def get_wa_html_source_prefixes() -> list[str]:
    prefixes: list[str] = []
    for venue in WA_HTML_VENUES:
        parsed = urlparse(venue.list_url)
        if parsed.scheme and parsed.netloc:
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}")
    return sorted(set(prefixes))
