from __future__ import annotations

import asyncio
import html
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from urllib.parse import parse_qs
from urllib.parse import unquote
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

CHALLENGE_MARKERS = (
    "just a moment",
    "verify you are human",
    "performing security verification",
    "security check",
    "access denied",
    "cf-browser-verification",
)

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " activities ",
    " art class ",
    " art classes ",
    " art project ",
    " artist talk ",
    " author talk ",
    " chat ",
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " craft ",
    " crafts ",
    " discussion ",
    " discussions ",
    " explorer ",
    " explorers ",
    " gallery talk ",
    " homeschool ",
    " lecture ",
    " lectures ",
    " open studio ",
    " sketch ",
    " studio ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
WEAK_INCLUDE_PATTERNS = (
    " art ",
    " child ",
    " children ",
    " families ",
    " family ",
    " kid ",
    " kids ",
    " teen ",
    " teens ",
    " youth ",
)
ALWAYS_REJECT_PATTERNS = (
    " admission ",
    " block party ",
    " camp ",
    " camps ",
    " closed ",
    " concert ",
    " concerts ",
    " dinner ",
    " film ",
    " films ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " jazz ",
    " market ",
    " mindfulness ",
    " music ",
    " open house ",
    " orchestra ",
    " party ",
    " performance ",
    " performances ",
    " poetry ",
    " reception ",
    " sale ",
    " screening ",
    " storytime ",
    " tea ",
    " tour ",
    " tours ",
    " writing ",
    " yoga ",
    " dance ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " exhibition ",
    " exhibitions ",
    " opening celebration ",
    " opening reception ",
)
NON_OVERRIDE_REJECT_PATTERNS = (
    " film ",
    " films ",
    " guided tour ",
    " screening ",
    " tai chi ",
    " walking tour ",
    " yoga ",
)

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

DATE_WITH_YEAR_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
    re.IGNORECASE,
)
MONTH_DAY_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+\d{1,2}(?:,\s*\d{4})?",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*[AaPp]\.?[Mm]\.?)\s*(?:-|–|—|to|--)\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?\s*[AaPp]\.?[Mm]\.?)",
    re.IGNORECASE,
)
TIME_PAIR_RE = re.compile(
    r"(?P<start>\d{1,2}:\d{2}\s*[AaPp]\.?[Mm]\.?)\s+(?P<end>\d{1,2}:\d{2}\s*[AaPp]\.?[Mm]\.?)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(\d{1,2}(?::\d{2})?\s*[AaPp]\.?[Mm]\.?)\b", re.IGNORECASE)
GREGG_WIDGET_EVENT_RE = re.compile(r'href="(https://calendar\.ncsu\.edu/event/[^"]+)"')

GREGG_WIDGET_URL = (
    "https://calendar.ncsu.edu/widget/view?schools=ncsu&groups=gregg_museum_of_art_design"
    "&days=365&num=50&tags=GreggMuseumProgram&match=all&embed=false"
)
REYNOLDA_YOUTH_URL = "https://reynolda.org/youth-family-events/"


@dataclass(frozen=True, slots=True)
class NcHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    use_playwright_fallback: bool = False


NC_HTML_VENUES: tuple[NcHtmlVenueConfig, ...] = (
    NcHtmlVenueConfig(
        slug="bechtler",
        source_name="bechtler_events",
        venue_name="Bechtler Museum of Modern Art",
        city="Charlotte",
        state="NC",
        list_url="https://www.bechtler.org/events",
        use_playwright_fallback=True,
    ),
    NcHtmlVenueConfig(
        slug="gregg",
        source_name="gregg_events",
        venue_name="Gregg Museum of Art + Design",
        city="Raleigh",
        state="NC",
        list_url="https://gregg.arts.ncsu.edu/programs/",
    ),
    NcHtmlVenueConfig(
        slug="hickory",
        source_name="hickory_events",
        venue_name="Hickory Museum of Art",
        city="Hickory",
        state="NC",
        list_url="https://www.hickoryart.org/events",
        use_playwright_fallback=True,
    ),
    NcHtmlVenueConfig(
        slug="ncma",
        source_name="ncma_events",
        venue_name="North Carolina Museum of Art",
        city="Raleigh",
        state="NC",
        list_url="https://ncartmuseum.org/events-and-exhibitions/events-calendar/",
    ),
    NcHtmlVenueConfig(
        slug="reynolda",
        source_name="reynolda_events",
        venue_name="Reynolda House Museum of American Art",
        city="Winston-Salem",
        state="NC",
        list_url="https://reynolda.org/visit/calendar/",
    ),
    NcHtmlVenueConfig(
        slug="weatherspoon",
        source_name="weatherspoon_events",
        venue_name="Weatherspoon Art Museum",
        city="Greensboro",
        state="NC",
        list_url="https://weatherspoonart.org/calendar/",
    ),
)

NC_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in NC_HTML_VENUES}


async def fetch_html(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
    use_playwright_fallback: bool = False,
) -> str:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS)

    last_exception: Exception | None = None
    last_html: str | None = None
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
                last_html = response.text
                if not use_playwright_fallback or not _looks_like_challenge(response.text):
                    return response.text
                break

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
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
    if last_html is not None:
        return last_html
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
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html_text = await page.content()
        await browser.close()
        return html_text


async def load_nc_html_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    max_links_per_venue: int = 120,
) -> dict[str, dict]:
    selected = (
        [NC_HTML_VENUES_BY_SLUG[slug] for slug in venue_slugs]
        if venue_slugs
        else list(NC_HTML_VENUES)
    )
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            if venue.slug == "reynolda":
                list_html = await fetch_html(REYNOLDA_YOUTH_URL, client=client)
            else:
                list_html = await fetch_html(
                    venue.list_url,
                    client=client,
                    use_playwright_fallback=venue.use_playwright_fallback,
                )

            if venue.slug == "bechtler":
                payload[venue.slug] = {"list_html": list_html}
                continue

            if venue.slug == "hickory":
                payload[venue.slug] = {"list_html": list_html}
                continue

            if venue.slug == "gregg":
                widget_js = await fetch_html(GREGG_WIDGET_URL, client=client)
                detail_pages: dict[str, str] = {}
                for detail_url in _extract_gregg_detail_urls(widget_js)[:max_links_per_venue]:
                    try:
                        detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                    except Exception as exc:
                        print(f"[nc-html-fetch] gregg detail failed url={detail_url}: {exc}")
                payload[venue.slug] = {
                    "list_html": list_html,
                    "widget_js": widget_js,
                    "detail_pages": detail_pages,
                }
                continue

            if venue.slug == "reynolda":
                list_blobs = _extract_reynolda_list_blobs(list_html)
                detail_pages: dict[str, str] = {}
                for detail_url in list(list_blobs)[:max_links_per_venue]:
                    try:
                        detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                    except Exception as exc:
                        print(f"[nc-html-fetch] reynolda detail failed url={detail_url}: {exc}")
                payload[venue.slug] = {
                    "list_html": list_html,
                    "list_blobs": list_blobs,
                    "detail_pages": detail_pages,
                }
                continue

            detail_pages = {}
            for detail_url in _extract_detail_urls(venue.slug, list_html, venue.list_url)[:max_links_per_venue]:
                try:
                    detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                except Exception as exc:
                    print(f"[nc-html-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")

            payload[venue.slug] = {
                "list_html": list_html,
                "detail_pages": detail_pages,
            }

    return payload


def parse_nc_html_events(payload: dict, *, venue: NcHtmlVenueConfig) -> list[ExtractedActivity]:
    if venue.slug == "bechtler":
        rows = _parse_bechtler_events(payload, venue=venue)
    elif venue.slug == "gregg":
        rows = _parse_gregg_events(payload, venue=venue)
    elif venue.slug == "hickory":
        rows = _parse_hickory_events(payload, venue=venue)
    elif venue.slug == "ncma":
        rows = _parse_mec_events(payload, venue=venue)
    elif venue.slug == "reynolda":
        rows = _parse_reynolda_events(payload, venue=venue)
    elif venue.slug == "weatherspoon":
        rows = _parse_mec_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row
    return sorted(deduped.values(), key=lambda row: (row.start_at, row.title, row.source_url))


class NcHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "nc_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_nc_html_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_nc_html_bundle_payload/parse_nc_html_events from script runner.")


def _parse_bechtler_events(payload: dict, *, venue: NcHtmlVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    rows: list[ExtractedActivity] = []
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    for heading in soup.find_all("h5"):
        anchor = heading.find("a", href=True)
        if anchor is None:
            continue
        section_label = _normalize_space(heading.get_text(" ", strip=True))
        if not section_label:
            continue
        body_container = heading.parent.find_next_sibling("div")
        body_text = _normalize_space(body_container.get_text(" ", strip=True)) if body_container else ""
        if not body_text:
            continue

        title = _extract_bechtler_title(body_text, fallback=section_label)
        start_date = _parse_month_day_without_year(body_text, today=today)
        if start_date is None:
            continue
        start_at, end_at = _combine_date_and_times(start_date, _parse_time_range(body_text))
        token_blob = _searchable_blob(" ".join(part for part in [section_label, title, body_text] if part))
        if not _should_keep_event(title=title, token_blob=token_blob):
            continue

        rows.append(
            _make_row(
                venue=venue,
                source_url=urljoin(venue.list_url, anchor.get("href") or ""),
                title=title,
                description=body_text,
                location_text="Bechtler Museum of Modern Art, Charlotte, NC",
                activity_type=_infer_activity_type(token_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=token_blob,
                price_text=body_text,
            )
        )

    return rows


def _parse_gregg_events(payload: dict, *, venue: NcHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for source_url, detail_html in (payload.get("detail_pages") or {}).items():
        event_obj = _extract_first_event_ld_json(detail_html)
        if event_obj is None:
            continue
        title = _normalize_space(event_obj.get("name"))
        description = _normalize_space(event_obj.get("description"))
        start_at = _parse_iso_datetime(event_obj.get("startDate"))
        if not title or start_at is None:
            continue
        end_at = _parse_iso_datetime(event_obj.get("endDate"))
        location_text = _extract_ld_location(event_obj.get("location")) or "Gregg Museum of Art + Design, Raleigh, NC"
        page_text = _html_to_text(detail_html)
        price_text = "Free Event" if " free event " in _searchable_blob(page_text) else page_text
        token_blob = _searchable_blob(" ".join(part for part in [title, description or ""] if part))
        if not _should_keep_event(title=title, token_blob=token_blob):
            continue
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=description or None,
                location_text=location_text,
                activity_type=_infer_activity_type(token_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=token_blob,
                price_text=price_text,
            )
        )
    return rows


def _parse_hickory_events(payload: dict, *, venue: NcHtmlVenueConfig) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    soup = BeautifulSoup(list_html, "html.parser")
    items: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if "/events/" not in href or "?format=ical" in href:
            continue
        absolute_url = urljoin(venue.list_url, href)
        container = anchor.find_parent(["article", "li", "div", "section"]) or anchor
        blob = _normalize_space(container.get_text(" ", strip=True))
        if not blob:
            continue
        title_text = _normalize_space(anchor.get_text(" ", strip=True))
        google_link = None
        for google_anchor in container.find_all("a", href=True):
            google_href = google_anchor.get("href") or ""
            if "calendar.google.com/calendar/render" in google_href:
                google_link = urljoin(venue.list_url, google_href)
                break
        entry = items.setdefault(absolute_url, {"title": "", "blob": "", "google_link": ""})
        if title_text and title_text not in {"Google Calendar", "ICS", "View Event ->", "View Event", "View Event ->", "View Event →"}:
            entry["title"] = title_text
        if len(blob) > len(entry["blob"]):
            entry["blob"] = blob
        if google_link:
            entry["google_link"] = google_link

    rows: list[ExtractedActivity] = []
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    for source_url, entry in items.items():
        blob = entry["blob"]
        title = entry["title"] or _extract_hickory_title(blob)
        if not title:
            continue
        google_meta = _parse_google_calendar_href(entry["google_link"], timezone_name=NY_TIMEZONE)
        if google_meta is not None:
            start_at = google_meta["start_at"]
            end_at = google_meta["end_at"]
            description = google_meta["details"] or blob
        else:
            start_date = _parse_first_date_with_year(blob)
            if start_date is None:
                start_date = _parse_month_day_without_year(blob, today=today)
            if start_date is None:
                continue
            start_at, end_at = _combine_date_and_times(start_date, _parse_time_range(blob))
            description = blob
        location_text = _extract_hickory_location(blob) or "Hickory Museum of Art, Hickory, NC"
        price_text = _extract_price_text(blob)
        token_blob = _searchable_blob(" ".join(part for part in [title, blob, description or ""] if part))
        if not _should_keep_event(title=title, token_blob=token_blob):
            continue
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
                price_text=price_text or description,
            )
        )

    return rows


def _parse_mec_events(payload: dict, *, venue: NcHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for source_url, detail_html in (payload.get("detail_pages") or {}).items():
        parsed = _parse_mec_detail_page(detail_html, source_url=source_url, venue=venue)
        if parsed is None:
            continue
        title, description, start_at, end_at, location_text, category_text, price_text = parsed
        token_blob = _searchable_blob(
            " ".join(
                part for part in [title, description or "", category_text or "", location_text or "", price_text or ""]
                if part
            )
        )
        if not _should_keep_event(title=title, token_blob=token_blob):
            continue
        rows.append(
            _make_row(
                venue=venue,
                source_url=source_url,
                title=title,
                description=_join_non_empty([description, category_text and f"Category: {category_text}"]),
                location_text=location_text or f"{venue.venue_name}, {venue.city}, {venue.state}",
                activity_type=_infer_activity_type(token_blob),
                start_at=start_at,
                end_at=end_at,
                token_blob=token_blob,
                price_text=price_text or description,
            )
        )
    return rows


def _parse_reynolda_events(payload: dict, *, venue: NcHtmlVenueConfig) -> list[ExtractedActivity]:
    list_blobs = payload.get("list_blobs") or {}
    rows: list[ExtractedActivity] = []
    for source_url, detail_html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(detail_html, "html.parser")
        title = _extract_page_title(detail_html) or _normalize_space(
            soup.select_one(".event-detail__content__title, h1").get_text(" ", strip=True)
        )
        if not title:
            continue
        price_text = _extract_reynolda_cost(soup)
        list_blob = _normalize_space(list_blobs.get(source_url) or "")
        page_text = _html_to_text(detail_html)
        description = _extract_reynolda_description(list_blob=list_blob, page_text=page_text, title=title)
        token_blob = _searchable_blob(" ".join(part for part in [title, list_blob, price_text or ""] if part))
        if not _should_keep_event(title=title, token_blob=token_blob):
            continue

        for start_at, end_at in _extract_reynolda_occurrences(soup):
            rows.append(
                _make_row(
                    venue=venue,
                    source_url=source_url,
                    title=title,
                    description=_join_non_empty([description, price_text and f"Cost: {price_text}"]),
                    location_text="Reynolda House Museum of American Art, Winston-Salem, NC",
                    activity_type=_infer_activity_type(token_blob),
                    start_at=start_at,
                    end_at=end_at,
                    token_blob=token_blob,
                    price_text=price_text or description,
                )
            )

    return rows


def _extract_detail_urls(slug: str, list_html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if slug == "ncma":
            if "/events/" not in href or "occurrence=" not in href:
                continue
        elif slug == "weatherspoon":
            if "/events/" not in href:
                continue
        else:
            continue
        absolute_url = urljoin(list_url, href)
        if slug == "ncma":
            occurrence = (parse_qs(urlparse(absolute_url).query).get("occurrence") or [None])[0]
            if occurrence:
                try:
                    if datetime.strptime(occurrence, "%Y-%m-%d").date() < today:
                        continue
                except ValueError:
                    pass
        if absolute_url in seen:
            continue
        seen.add(absolute_url)
        urls.append(absolute_url)

    return urls


def _extract_gregg_detail_urls(widget_js: str) -> list[str]:
    decoded = _decode_document_write_js(widget_js)
    urls: list[str] = []
    seen: set[str] = set()
    for match in GREGG_WIDGET_EVENT_RE.findall(decoded):
        normalized = html.unescape(match)
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def _extract_reynolda_list_blobs(list_html: str) -> dict[str, str]:
    soup = BeautifulSoup(list_html, "html.parser")
    blobs: dict[str, str] = {}
    for article in soup.select("article.feature-item--event"):
        anchor = article.select_one(".feature-item__content__title a[href]")
        if anchor is None:
            continue
        url = urljoin(REYNOLDA_YOUTH_URL, anchor.get("href") or "")
        blob = _normalize_space(article.get_text(" ", strip=True))
        if blob:
            blobs[url] = blob
    return blobs


def _parse_mec_detail_page(
    detail_html: str,
    *,
    source_url: str,
    venue: NcHtmlVenueConfig,
) -> tuple[str, str | None, datetime, datetime | None, str | None, str | None, str | None] | None:
    soup = BeautifulSoup(detail_html, "html.parser")
    title = _extract_page_title(detail_html) or _normalize_space(
        (soup.select_one("h1") or soup.select_one(".mec-single-title")).get_text(" ", strip=True)
        if (soup.select_one("h1") or soup.select_one(".mec-single-title"))
        else ""
    )
    if not title:
        return None

    google_anchor = soup.select_one('a[href*="calendar.google.com/calendar/render"]')
    google_meta = _parse_google_calendar_href(google_anchor.get("href") if google_anchor else None, timezone_name=NY_TIMEZONE)
    if google_meta is None:
        return None

    description = google_meta["details"] or _extract_meta_description(soup)
    cost_text = _extract_mec_meta_text(soup, selectors=[".mec-event-cost", "h3.mec-cost"], fallback_label="Cost")
    location_text = _extract_mec_meta_text(soup, selectors=[".mec-location", "h3.mec-location"], fallback_label="Location")
    category_text = _extract_mec_category_text(soup)

    return (
        title,
        description,
        google_meta["start_at"],
        google_meta["end_at"],
        location_text or f"{venue.venue_name}, {venue.city}, {venue.state}",
        category_text,
        cost_text,
    )


def _make_row(
    *,
    venue: NcHtmlVenueConfig,
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
        drop_in=("drop-in" in token_blob or "drop in" in token_blob or " open studio " in token_blob),
        registration_required=(
            " register " in registration_blob
            or " registration " in registration_blob
            or " preregister " in registration_blob
            or " pre-register " in registration_blob
            or " rsvp " in registration_blob
            or " ticket " in registration_blob
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _should_keep_event(*, title: str, token_blob: str) -> bool:
    title_blob = _searchable_blob(title)
    if " cancelled " in token_blob or " canceled " in token_blob or " sold out " in token_blob:
        return False
    if any(pattern in title_blob for pattern in NON_OVERRIDE_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in NON_OVERRIDE_REJECT_PATTERNS):
        return False

    strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    weak_include = any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS)
    art_context = any(pattern in token_blob for pattern in (" art ", " gallery ", " museum ", " sketch ", " studio "))
    if not strong_include and not (weak_include and art_context):
        return False

    if any(pattern in title_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS) and not strong_include:
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_include:
        return False

    return True


def _infer_activity_type(token_blob: str) -> str:
    lecture_patterns = (
        " artist talk ",
        " author talk ",
        " conversation ",
        " conversations ",
        " discussion ",
        " discussions ",
        " gallery talk ",
        " lecture ",
        " lectures ",
        " panel ",
        " talk ",
        " talks ",
    )
    if any(pattern in token_blob for pattern in lecture_patterns):
        return "lecture"
    return "workshop"


def _extract_first_event_ld_json(detail_html: str) -> dict | None:
    soup = BeautifulSoup(detail_html, "html.parser")
    for node in soup.select('script[type="application/ld+json"]'):
        raw = (node.string or node.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        objects = data if isinstance(data, list) else [data]
        for obj in objects:
            if isinstance(obj, dict) and obj.get("@type") == "Event":
                return obj
    return None


def _extract_ld_location(location_obj: object) -> str | None:
    if not isinstance(location_obj, dict):
        return None
    name = _normalize_space(location_obj.get("name"))
    address = location_obj.get("address")
    if isinstance(address, dict):
        address_text = ", ".join(
            part
            for part in [
                _normalize_space(address.get("streetAddress")),
                _normalize_space(address.get("addressLocality")),
                _normalize_space(address.get("addressRegion")),
            ]
            if part
        )
    else:
        address_text = _normalize_space(address)
    parts = [part for part in [name, address_text] if part]
    return ", ".join(parts) if parts else None


def _parse_google_calendar_href(url: str | None, *, timezone_name: str) -> dict[str, object] | None:
    if not url:
        return None
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    dates_value = (query.get("dates") or [None])[0]
    if not dates_value or "/" not in dates_value:
        return None
    start_raw, end_raw = dates_value.split("/", 1)
    start_at = _parse_google_calendar_datetime(start_raw, timezone_name=timezone_name)
    end_at = _parse_google_calendar_datetime(end_raw, timezone_name=timezone_name)
    if start_at is None:
        return None
    return {
        "title": _normalize_space(unquote((query.get("text") or [""])[0])),
        "details": _normalize_space(unquote((query.get("details") or [""])[0])) or None,
        "location": _normalize_space(unquote((query.get("location") or [""])[0])) or None,
        "start_at": start_at,
        "end_at": end_at,
    }


def _parse_google_calendar_datetime(value: str, *, timezone_name: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None

    try:
        if text.endswith("Z"):
            parsed = datetime.strptime(text, "%Y%m%dT%H%M%SZ").replace(tzinfo=ZoneInfo("UTC"))
            return parsed.astimezone(ZoneInfo(timezone_name)).replace(tzinfo=None)
        if "T" in text:
            return datetime.strptime(text, "%Y%m%dT%H%M%S")
        parsed_date = datetime.strptime(text, "%Y%m%d").date()
        return datetime.combine(parsed_date, time.min)
    except ValueError:
        return None


def _extract_reynolda_occurrences(soup: BeautifulSoup) -> list[tuple[datetime, datetime | None]]:
    rows: list[tuple[datetime, datetime | None]] = []
    for node in soup.select("add-to-calendar-button"):
        start_date = _normalize_space(node.get("startdate"))
        start_time = _normalize_space(node.get("starttime"))
        end_date = _normalize_space(node.get("enddate"))
        end_time = _normalize_space(node.get("endtime"))
        if not start_date:
            continue
        try:
            start_day = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError:
            continue
        start_at = datetime.combine(start_day, _parse_clock_time(start_time) or time.min)
        end_at = None
        if end_date:
            try:
                end_day = datetime.strptime(end_date, "%Y-%m-%d").date()
                end_at = datetime.combine(end_day, _parse_clock_time(end_time) or time.min)
            except ValueError:
                end_at = None
        rows.append((start_at, end_at))
    return rows


def _extract_reynolda_cost(soup: BeautifulSoup) -> str | None:
    for strong in soup.find_all("strong"):
        if _normalize_space(strong.get_text(" ", strip=True)).lower().startswith("cost"):
            parent = strong.find_parent(["p", "div", "li"]) or strong.parent
            text = _normalize_space(parent.get_text(" ", strip=True))
            return text.replace("Cost:", "", 1).strip() if text else None
    return None


def _extract_reynolda_description(*, list_blob: str, page_text: str, title: str) -> str | None:
    if list_blob:
        reduced = list_blob.replace(title, "", 1).strip(" |:")
        if reduced:
            return reduced
    marker = " Add to Calendar "
    if marker in page_text:
        before_marker = page_text.split(marker, 1)[0]
        if title in before_marker:
            snippet = before_marker.split(title, 1)[-1].strip()
            return snippet or None
    return None


def _extract_bechtler_title(body_text: str, *, fallback: str) -> str:
    lowered_fallback = fallback.strip().lower()
    if lowered_fallback == "artbreak":
        return "ArtBreak"
    date_match = MONTH_DAY_RE.search(body_text)
    if date_match:
        title = body_text[: date_match.start()].strip(" :-,")
        title = re.sub(
            r"\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)$",
            "",
            title,
            flags=re.IGNORECASE,
        ).strip(" :-,")
        if title:
            return title
    return fallback.title()


def _extract_hickory_title(blob: str) -> str:
    if not blob:
        return ""
    date_match = DATE_WITH_YEAR_RE.search(blob)
    prefix = blob[: date_match.start()].strip() if date_match else blob
    month_day_match = re.match(r"^[A-Za-z]{3,4}\s+\d{1,2}\s+", prefix)
    if month_day_match:
        prefix = prefix[month_day_match.end() :]
    for category in (
        "Adult Classes",
        "Children",
        "Children's Classes",
        "Community Programs",
        "Schools",
        "Summer Art Camps",
    ):
        if prefix.startswith(category + " "):
            prefix = prefix[len(category) + 1 :]
            break
    prefix = prefix.replace(" , ", " ")
    return prefix.strip(" :-,")


def _extract_hickory_location(blob: str) -> str | None:
    parts = blob.split("Google Calendar", 1)[0]
    time_match = TIME_PAIR_RE.search(parts) or TIME_RANGE_RE.search(parts)
    if time_match:
        trailing = parts[time_match.end() :].strip()
        if trailing.endswith("(map)"):
            return trailing[: -len("(map)")].strip()
    return None


def _extract_price_text(text: str) -> str | None:
    match = re.search(r"(Fee:\s*[^|]+)", text, re.IGNORECASE)
    if match:
        return _normalize_space(match.group(1))
    match = re.search(r"(Cost:\s*[^|]+)", text, re.IGNORECASE)
    if match:
        return _normalize_space(match.group(1))
    if "$" in text:
        snippet = re.search(r"(\$[\d].{0,80})", text)
        if snippet:
            return _normalize_space(snippet.group(1))
    return None


def _extract_page_title(html_text: str) -> str | None:
    soup = BeautifulSoup(html_text, "html.parser")
    if soup.title is None:
        return None
    title = _normalize_space(soup.title.get_text(" ", strip=True))
    for suffix in (
        " - Weatherspoon Art Museum",
        " - North Carolina Museum of Art",
        " - Reynolda",
        " - NC State University Calendar",
        " | The Bechtler",
        " - Hickory Museum of Art",
    ):
        if title.endswith(suffix):
            title = title[: -len(suffix)].strip()
    return title or None


def _extract_meta_description(soup: BeautifulSoup) -> str | None:
    node = soup.select_one('meta[property="og:description"], meta[name="description"]')
    if node is None:
        return None
    value = _normalize_space(node.get("content"))
    return value or None


def _extract_mec_meta_text(soup: BeautifulSoup, *, selectors: list[str], fallback_label: str) -> str | None:
    for selector in selectors:
        for node in soup.select(selector):
            text = _normalize_space(node.get_text(" ", strip=True))
            if not text:
                continue
            if fallback_label.lower() in text.lower():
                text = re.sub(rf"^{re.escape(fallback_label)}\s*", "", text, flags=re.IGNORECASE).strip(" :-")
            if text and text.lower() != fallback_label.lower():
                return text
    flat_text = _html_to_text(str(soup))
    match = re.search(
        rf"{re.escape(fallback_label)}\s+(.+?)(?:\s+(?:Category|Location|Cost|For More Information|REGISTER)\b|$)",
        flat_text,
        re.IGNORECASE,
    )
    if match:
        return _normalize_space(match.group(1))
    return None


def _extract_mec_category_text(soup: BeautifulSoup) -> str | None:
    for node in soup.select(".mec-single-event-category dd, .mec-single-event-category dl, .mec-single-event-category"):
        text = _normalize_space(node.get_text(" ", strip=True))
        text = re.sub(r"^Category\s*", "", text, flags=re.IGNORECASE).strip(" :-")
        if text:
            return text
    return None


def _decode_document_write_js(value: str) -> str:
    match = re.search(r'document\.write\("(?P<body>.*)"\)\s*;?\s*$', value, re.DOTALL)
    body = match.group("body") if match else value
    body = body.replace('\\"', '"').replace("\\/", "/")
    try:
        body = bytes(body, "utf-8").decode("unicode_escape")
    except UnicodeDecodeError:
        pass
    return html.unescape(body)


def _parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(ZoneInfo(NY_TIMEZONE)).replace(tzinfo=None)
    return parsed


def _parse_first_date_with_year(value: str) -> date | None:
    match = DATE_WITH_YEAR_RE.search(value)
    if not match:
        return None
    text = match.group(0).replace(".", "")
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_month_day_without_year(value: str, *, today: date) -> date | None:
    match = MONTH_DAY_RE.search(value)
    if not match:
        return None
    text = match.group(0).replace(".", "")
    if re.search(r"\d{4}", text):
        return _parse_first_date_with_year(text)
    for fmt in ("%B %d", "%b %d"):
        try:
            parsed = datetime.strptime(text, fmt).date()
            candidate = parsed.replace(year=today.year)
            if candidate < today and (today - candidate).days > 30:
                candidate = candidate.replace(year=today.year + 1)
            return candidate
        except ValueError:
            continue
    return None


def _parse_time_range(value: str) -> tuple[time | None, time | None]:
    match = TIME_RANGE_RE.search(value)
    if match:
        return _parse_clock_time(match.group("start")), _parse_clock_time(match.group("end"))
    match = TIME_PAIR_RE.search(value)
    if match:
        return _parse_clock_time(match.group("start")), _parse_clock_time(match.group("end"))
    match = TIME_SINGLE_RE.search(value)
    if match:
        parsed = _parse_clock_time(match.group(1))
        return parsed, None
    return None, None


def _parse_clock_time(value: str | None) -> time | None:
    if not value:
        return None
    normalized = value.strip().upper().replace(".", "")
    normalized = normalized.replace("AM", " AM").replace("PM", " PM")
    normalized = " ".join(normalized.split())
    for fmt in ("%I:%M %p", "%I %p", "%H:%M"):
        try:
            return datetime.strptime(normalized, fmt).time()
        except ValueError:
            continue
    return None


def _combine_date_and_times(start_date: date, parsed_times: tuple[time | None, time | None]) -> tuple[datetime, datetime | None]:
    start_time, end_time = parsed_times
    start_at = datetime.combine(start_date, start_time or time.min)
    end_at = datetime.combine(start_date, end_time) if end_time is not None else None
    return start_at, end_at


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _parse_age_min(token_blob: str) -> int | None:
    match = AGE_RANGE_RE.search(token_blob)
    if match:
        return int(match.group(1))
    match = AGE_PLUS_RE.search(token_blob)
    if match:
        return int(match.group(1))
    return None


def _parse_age_max(token_blob: str) -> int | None:
    match = AGE_RANGE_RE.search(token_blob)
    if match:
        return int(match.group(2))
    return None


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _html_to_text(value: str) -> str:
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    return " | ".join(values) if values else None


def _looks_like_challenge(html_text: str) -> bool:
    lowered = html_text.lower()
    return any(marker in lowered for marker in CHALLENGE_MARKERS)


def get_nc_html_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in NC_HTML_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)
