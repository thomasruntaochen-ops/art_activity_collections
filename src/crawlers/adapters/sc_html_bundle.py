from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
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
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
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
    "access denied",
    "cf-browser-verification",
    "just a moment",
    "performing security verification",
    "verify you are human",
)

FULL_DATE_RE = re.compile(
    r"(?P<date>"
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+\d{4})",
    re.IGNORECASE,
)
MONTH_DAY_RE = re.compile(
    r"(?P<month>"
    r"January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
    r")\.?\s+(?P<day>\d{1,2})",
    re.IGNORECASE,
)
TIME_SECTION_RE = re.compile(
    r"(?P<time>"
    r"\b\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?\s*(?:-|–|—|to)\s*"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)"
    r"|"
    r"\b\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)"
    r")",
    re.IGNORECASE,
)

DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
)
REGISTER_PATTERNS = (
    " preregister ",
    " pre-register ",
    " register ",
    " registration ",
    " sign up ",
    " tickets ",
    " ticket ",
)
NON_OVERRIDE_REJECT_PATTERNS = (
    " camp ",
    " camps ",
    " concert ",
    " concerts ",
    " dinner ",
    " documentary ",
    " film ",
    " films ",
    " fundraiser ",
    " fundraising ",
    " jazz ",
    " music in the galleries ",
    " pilates ",
    " public tour ",
    " run away runway ",
    " runaway runway ",
    " sale ",
    " sound healing ",
    " sound bath ",
    " stroller tour ",
    " tag sale ",
    " yoga ",
    " lie down and listen ",
)
TITLE_REJECT_PATTERNS = (
    " camp ",
    " camps ",
    " public tour ",
    " curator-led tour ",
    " curator led tour ",
    " president-led tour ",
    " president led tour ",
    " stroller tour ",
    " tour ",
    " tours ",
)
GENERAL_REJECT_PATTERNS = (
    " admission ",
    " easter ",
    " gala ",
    " holiday ",
    " mindfulness ",
    " music ",
    " reception ",
    " tour ",
    " tours ",
)
STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " activities ",
    " art class ",
    " art classes ",
    " author ",
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " discussion ",
    " discussions ",
    " drawing ",
    " family art adventure ",
    " gallery talk ",
    " gladys ",
    " hands on ",
    " hands-on ",
    " lecture ",
    " lectures ",
    " preschool ",
    " studio ",
    " talk ",
    " talks ",
    " toddler ",
    " workshop ",
    " workshops ",
)
WEAK_INCLUDE_PATTERNS = (
    " child ",
    " children ",
    " family ",
    " families ",
    " kid ",
    " kids ",
    " teen ",
    " teens ",
    " youth ",
)


@dataclass(frozen=True, slots=True)
class ScHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    mode: str
    use_playwright_fallback: bool = False


SC_HTML_VENUES: tuple[ScHtmlVenueConfig, ...] = (
    ScHtmlVenueConfig(
        slug="columbia",
        source_name="sc_columbia_events",
        venue_name="Columbia Museum of Art",
        city="Columbia",
        state="SC",
        list_url="https://www.columbiamuseum.org/events",
        default_location="Columbia Museum of Art, Columbia, SC",
        mode="columbia",
    ),
    ScHtmlVenueConfig(
        slug="myrtle_beach",
        source_name="sc_myrtle_beach_events",
        venue_name="Franklin G. Burroughs-Simeon B. Chapin Art Museum",
        city="Myrtle Beach",
        state="SC",
        list_url="https://myrtlebeachartmuseum.org/myrtle-beach-events/events-calendar/",
        default_location="Franklin G. Burroughs-Simeon B. Chapin Art Museum, Myrtle Beach, SC",
        mode="myrtle_beach",
        use_playwright_fallback=True,
    ),
    ScHtmlVenueConfig(
        slug="gibbes",
        source_name="sc_gibbes_events",
        venue_name="Gibbes Museum of Art",
        city="Charleston",
        state="SC",
        list_url="https://www.gibbesmuseum.org/programs-events/",
        default_location="Gibbes Museum of Art, Charleston, SC",
        mode="gibbes",
    ),
    ScHtmlVenueConfig(
        slug="greenville",
        source_name="sc_gcma_events",
        venue_name="Greenville County Museum of Art",
        city="Greenville",
        state="SC",
        list_url="https://gcma.org/events/",
        default_location="Greenville County Museum of Art, Greenville, SC",
        mode="greenville",
        use_playwright_fallback=True,
    ),
)

SC_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in SC_HTML_VENUES}


class ScHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "sc_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_sc_html_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_sc_html_bundle_payload/parse_sc_html_events from the script runner.")


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
                if use_playwright_fallback:
                    break
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            if response.status_code < 400:
                last_html = response.text
                if not use_playwright_fallback or not _looks_like_challenge(response.text):
                    return response.text
                break

            if use_playwright_fallback and response.status_code in (403, 429):
                return await fetch_html_playwright(url)

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
        await page.wait_for_timeout(2000)
        html = await page.content()
        await browser.close()
        return html


async def load_sc_html_bundle_payload(
    *,
    venues: list[ScHtmlVenueConfig] | None = None,
    page_limit: int | None = None,
) -> dict:
    selected_venues = venues or list(SC_HTML_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected_venues:
            try:
                if venue.mode == "columbia":
                    payload_by_slug[venue.slug] = await _load_columbia_payload(
                        venue=venue,
                        client=client,
                        page_limit=page_limit,
                    )
                elif venue.mode == "gibbes":
                    payload_by_slug[venue.slug] = await _load_gibbes_payload(
                        venue=venue,
                        client=client,
                        page_limit=page_limit,
                    )
                elif venue.mode == "greenville":
                    payload_by_slug[venue.slug] = await _load_greenville_payload(
                        venue=venue,
                        client=client,
                        page_limit=page_limit,
                    )
                elif venue.mode == "myrtle_beach":
                    payload_by_slug[venue.slug] = await _load_myrtle_beach_payload(
                        venue=venue,
                        client=client,
                    )
                else:
                    raise RuntimeError(f"Unsupported SC venue mode: {venue.mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)

    return {
        "payload_by_slug": payload_by_slug,
        "errors_by_slug": errors_by_slug,
    }


def parse_sc_html_events(
    payload: dict,
    *,
    venue: ScHtmlVenueConfig,
) -> list[ExtractedActivity]:
    if venue.mode == "columbia":
        return _parse_columbia_events(payload, venue=venue)
    if venue.mode == "gibbes":
        return _parse_gibbes_events(payload, venue=venue)
    if venue.mode == "greenville":
        return _parse_greenville_events(payload, venue=venue)
    if venue.mode == "myrtle_beach":
        return _parse_myrtle_beach_events(payload, venue=venue)
    raise RuntimeError(f"Unsupported SC venue mode: {venue.mode}")


def get_sc_source_prefixes() -> list[str]:
    return [venue.list_url.rstrip("/") for venue in SC_HTML_VENUES]


async def _load_columbia_payload(
    *,
    venue: ScHtmlVenueConfig,
    client: httpx.AsyncClient,
    page_limit: int | None,
) -> dict:
    max_pages = page_limit or 12
    cards: list[dict] = []
    seen_urls: set[str] = set()
    has_next = True
    page_index = 0

    while has_next and page_index < max_pages:
        page_url = venue.list_url if page_index == 0 else f"{venue.list_url}?page={page_index}"
        html = await fetch_html(page_url, client=client)
        soup = BeautifulSoup(html, "html.parser")
        page_cards = _extract_columbia_cards(soup, venue.list_url)
        if not page_cards:
            break
        cards.extend(page_cards)
        has_next = soup.select_one("li.pager__item--next a[href]") is not None
        page_index += 1

    candidate_cards: list[dict] = []
    detail_html_by_url: dict[str, str] = {}
    for card in cards:
        if card["detail_url"] in seen_urls:
            continue
        seen_urls.add(card["detail_url"])
        if not _should_fetch_card(card.get("title"), card.get("body"), card.get("category")):
            continue
        detail_html_by_url[card["detail_url"]] = await fetch_html(card["detail_url"], client=client)
        candidate_cards.append(card)

    return {
        "cards": candidate_cards,
        "detail_html_by_url": detail_html_by_url,
    }


def _extract_columbia_cards(soup: BeautifulSoup, list_url: str) -> list[dict]:
    cards: list[dict] = []
    for article in soup.select("li.events-list__item article.event-teaser"):
        anchor = article.select_one("a.event-teaser__anchor[href]")
        title = normalize_space(article.select_one(".event-teaser__title").get_text(" ", strip=True) if article.select_one(".event-teaser__title") else "")
        if anchor is None or not title:
            continue

        month_text = normalize_space(article.select_one(".event-teaser__month-start").get_text(" ", strip=True) if article.select_one(".event-teaser__month-start") else "")
        day_text = normalize_space(article.select_one(".event-teaser__date-range").get_text(" ", strip=True) if article.select_one(".event-teaser__date-range") else "")
        time_text = normalize_space(article.select_one(".event-teaser__time").get_text(" ", strip=True) if article.select_one(".event-teaser__time") else "")
        body = normalize_space(article.select_one(".event-teaser__body").get_text(" ", strip=True) if article.select_one(".event-teaser__body") else "")

        cards.append(
            {
                "detail_url": urljoin(list_url, anchor.get("href") or ""),
                "title": title,
                "body": body,
                "category": None,
                "date_text": f"{month_text} {day_text}".strip(),
                "time_text": time_text,
            }
        )
    return cards


def _parse_columbia_events(payload: dict, *, venue: ScHtmlVenueConfig) -> list[ExtractedActivity]:
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in payload.get("cards") or []:
        html = (payload.get("detail_html_by_url") or {}).get(card["detail_url"])
        if not html:
            continue
        row = _parse_columbia_detail(card=card, html=html, venue=venue)
        if row is None or row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _parse_columbia_detail(
    *,
    card: dict,
    html: str,
    venue: ScHtmlVenueConfig,
) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = normalize_space(
        soup.select_one(".page-detail__title").get_text(" ", strip=True)
        if soup.select_one(".page-detail__title")
        else card.get("title")
    )
    subtitle = normalize_space(
        soup.select_one(".page-detail__subtitle").get_text(" ", strip=True)
        if soup.select_one(".page-detail__subtitle")
        else ""
    )
    category = normalize_space(
        soup.select_one(".page-detail__category").get_text(" ", strip=True)
        if soup.select_one(".page-detail__category")
        else ""
    )
    body = normalize_space(
        soup.select_one(".page-detail .body").get_text(" ", strip=True)
        if soup.select_one(".page-detail .body")
        else card.get("body")
    )
    if not title:
        return None
    if not _should_keep_event(title=title, description=body, category=category):
        return None

    base_date = _parse_full_date_text(_extract_meta_description(soup))
    if base_date is None:
        base_date = _parse_inferred_date(card.get("date_text"))
    if base_date is None:
        base_date = _parse_inferred_date(subtitle)
    if base_date is None:
        return None

    time_text = _extract_time_text(subtitle) or card.get("time_text") or ""
    start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
    if start_at is None:
        return None

    full_description = join_non_empty(
        [
            body,
            category and f"Category: {category}",
        ]
    )
    age_min, age_max = parse_age_range(" ".join(part for part in [title, body] if part))
    token_blob = _searchable_blob(" ".join(part for part in [title, body, category] if part))

    return ExtractedActivity(
        source_url=card["detail_url"],
        title=title,
        description=full_description,
        venue_name=venue.venue_name,
        location_text=venue.default_location,
        city=venue.city,
        state=venue.state,
        activity_type=infer_activity_type(title, body, category),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=any(pattern in token_blob for pattern in REGISTER_PATTERNS),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **_price_kwargs(title, subtitle, body, category),
    )


async def _load_gibbes_payload(
    *,
    venue: ScHtmlVenueConfig,
    client: httpx.AsyncClient,
    page_limit: int | None,
) -> dict:
    max_pages = page_limit or 10
    cards: list[dict] = []
    seen_urls: set[str] = set()

    for page_number in range(1, max_pages + 1):
        page_url = venue.list_url if page_number == 1 else f"{venue.list_url}?page={page_number}"
        html = await fetch_html(page_url, client=client)
        soup = BeautifulSoup(html, "html.parser")
        page_cards = _extract_gibbes_cards(soup, venue.list_url)
        if not page_cards:
            break
        cards.extend(page_cards)
        if len(page_cards) < 4:
            break

    candidate_cards: list[dict] = []
    detail_html_by_url: dict[str, str] = {}
    for card in cards:
        if card["detail_url"] in seen_urls:
            continue
        seen_urls.add(card["detail_url"])
        if not _should_fetch_card(card.get("title"), card.get("body"), card.get("category")):
            continue
        detail_html_by_url[card["detail_url"]] = await fetch_html(card["detail_url"], client=client)
        candidate_cards.append(card)

    return {
        "cards": candidate_cards,
        "detail_html_by_url": detail_html_by_url,
    }


def _extract_gibbes_cards(soup: BeautifulSoup, list_url: str) -> list[dict]:
    cards: list[dict] = []
    for post in soup.select("div.post.margin-40"):
        anchor = post.select_one("a.button[href]")
        title = normalize_space(post.select_one("h2").get_text(" ", strip=True) if post.select_one("h2") else "")
        if anchor is None or not title:
            continue
        category = normalize_space(post.select_one("h4.category").get_text(" ", strip=True) if post.select_one("h4.category") else "")
        date_text = normalize_space(post.select_one("h4.date").get_text(" ", strip=True) if post.select_one("h4.date") else "")
        body = normalize_space(post.select_one("p").get_text(" ", strip=True) if post.select_one("p") else "")
        cards.append(
            {
                "detail_url": urljoin(list_url, anchor.get("href") or ""),
                "title": title,
                "body": body,
                "category": category,
                "date_text": date_text,
                "time_text": _extract_time_text(date_text),
            }
        )
    return cards


def _parse_gibbes_events(payload: dict, *, venue: ScHtmlVenueConfig) -> list[ExtractedActivity]:
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in payload.get("cards") or []:
        html = (payload.get("detail_html_by_url") or {}).get(card["detail_url"])
        if not html:
            continue
        row = _parse_gibbes_detail(card=card, html=html, venue=venue)
        if row is None or row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _parse_gibbes_detail(
    *,
    card: dict,
    html: str,
    venue: ScHtmlVenueConfig,
) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = normalize_space(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else card.get("title"))
    details_root = soup.select_one(".exhibit-details")
    date_line = normalize_space(
        soup.select_one(".exhibit-details .red").get_text(" ", strip=True)
        if soup.select_one(".exhibit-details .red")
        else card.get("date_text")
    )
    content_root = soup.select_one(".single__content-wrapper") or soup
    content_paragraphs: list[str] = []
    for paragraph in content_root.select("p"):
        if details_root is not None and details_root in paragraph.parents:
            continue
        text = normalize_space(paragraph.get_text(" ", strip=True))
        if text:
            content_paragraphs.append(text)
    content_paragraphs = _dedupe_texts(content_paragraphs)

    description = normalize_space(join_non_empty(content_paragraphs) or card.get("body"))
    location_parts: list[str] = []
    if details_root is not None:
        for paragraph in details_root.select("p"):
            text = normalize_space(paragraph.get_text(" ", strip=True))
            if not text or text == date_line:
                continue
            location_parts.append(text)
    location_parts = _dedupe_texts(location_parts)
    location_text = normalize_space(" ".join(location_parts))
    category = card.get("category") or _category_from_title_tag(soup.title.string if soup.title else "")
    if not title or not _should_keep_event(title=title, description=description, category=category):
        return None

    base_date = _parse_full_date_text(date_line) or _parse_inferred_date(date_line)
    if base_date is None:
        return None

    time_text = _extract_time_text(date_line)
    start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
    if start_at is None:
        return None

    full_description = join_non_empty(
        [
            _strip_gibbes_date_from_description(description, date_line),
            location_text and f"Location: {location_text}",
            category and f"Category: {category}",
        ]
    )
    age_min, age_max = parse_age_range(" ".join(part for part in [title, full_description or ""] if part))
    token_blob = _searchable_blob(" ".join(part for part in [title, full_description or "", category] if part))

    return ExtractedActivity(
        source_url=card["detail_url"],
        title=title,
        description=full_description,
        venue_name=venue.venue_name,
        location_text=venue.default_location,
        city=venue.city,
        state=venue.state,
        activity_type=infer_activity_type(title, full_description, category),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=any(pattern in token_blob for pattern in REGISTER_PATTERNS),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **_price_kwargs(title, date_line, full_description, category),
    )


async def _load_greenville_payload(
    *,
    venue: ScHtmlVenueConfig,
    client: httpx.AsyncClient,
    page_limit: int | None,
) -> dict:
    list_html = await fetch_html(
        venue.list_url,
        client=client,
        use_playwright_fallback=venue.use_playwright_fallback,
    )
    soup = BeautifulSoup(list_html, "html.parser")
    detail_anchors = soup.select("#cal-list a.block[href]")
    if not detail_anchors:
        raise RuntimeError("GCMA list page did not expose any event detail links.")

    max_events = page_limit or 60
    event_pages: list[dict] = []
    seen_urls: set[str] = set()
    current_url = urljoin(venue.list_url, detail_anchors[0].get("href") or "")

    while current_url and current_url not in seen_urls and len(event_pages) < max_events:
        seen_urls.add(current_url)
        html = await fetch_html(
            current_url,
            client=client,
            use_playwright_fallback=venue.use_playwright_fallback,
        )
        page_payload = {
            "url": current_url,
            "html": html,
        }
        event_pages.append(page_payload)

        detail_soup = BeautifulSoup(html, "html.parser")
        next_anchor = detail_soup.select_one("#ctl00_body_hlNext[href]")
        if next_anchor is None:
            break
        current_url = urljoin(current_url, next_anchor.get("href") or "")

    return {"event_pages": event_pages}


def _parse_greenville_events(payload: dict, *, venue: ScHtmlVenueConfig) -> list[ExtractedActivity]:
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for page in payload.get("event_pages") or []:
        row = _parse_greenville_detail(url=page["url"], html=page["html"], venue=venue)
        if row is None or row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _parse_greenville_detail(
    *,
    url: str,
    html: str,
    venue: ScHtmlVenueConfig,
) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = normalize_space(
        soup.select_one("meta[property='og:title']").get("content")
        if soup.select_one("meta[property='og:title']")
        else ""
    )
    title = title.split("::", 1)[0].strip() if title else ""
    if not title:
        title = normalize_space(_nth_text_after_heading(soup, 2))
    description = normalize_space(
        _extract_meta_description(soup)
        or _nth_text_after_heading(soup, 3)
    )
    if not title or not _should_keep_event(title=title, description=description, category=None):
        return None

    date_text = normalize_space(_first_matching_text(soup, "h3.no-bottom-margin"))
    time_text = normalize_space(_first_matching_text(soup, "p.no-top-margin strong"))
    base_date = _parse_full_date_text(date_text) or _parse_inferred_date(date_text)
    if base_date is None:
        base_date = _parse_full_date_text(description)
    if base_date is None:
        return None
    start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
    if start_at is None:
        return None

    age_min, age_max = parse_age_range(" ".join(part for part in [title, description] if part))
    token_blob = _searchable_blob(" ".join(part for part in [title, description] if part))

    return ExtractedActivity(
        source_url=url,
        title=title,
        description=description or None,
        venue_name=venue.venue_name,
        location_text=venue.default_location,
        city=venue.city,
        state=venue.state,
        activity_type=infer_activity_type(title, description, None),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=any(pattern in token_blob for pattern in REGISTER_PATTERNS),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **_price_kwargs(title, date_text, time_text, description),
    )


async def _load_myrtle_beach_payload(
    *,
    venue: ScHtmlVenueConfig,
    client: httpx.AsyncClient,
) -> dict:
    curated_url = "https://myrtlebeachartmuseum.org/myrtle-beach-events/workshops-lectures-gallery-talks/"
    html = await fetch_html(
        curated_url,
        client=client,
        use_playwright_fallback=venue.use_playwright_fallback,
    )
    soup = BeautifulSoup(html, "html.parser")
    detail_urls: list[str] = []
    seen_urls: set[str] = set()
    for anchor in soup.select("#interior a[href]"):
        href = normalize_space(anchor.get("href"))
        if not href:
            continue
        absolute_url = urljoin(curated_url, href)
        parsed = urlparse(absolute_url)
        if parsed.netloc != "myrtlebeachartmuseum.org":
            continue
        if absolute_url.lower().endswith(".pdf"):
            continue
        if absolute_url in seen_urls:
            continue
        seen_urls.add(absolute_url)
        detail_urls.append(absolute_url)

    detail_pages: list[dict] = []
    for detail_url in detail_urls:
        detail_pages.append(
            {
                "url": detail_url,
                "html": await fetch_html(
                    detail_url,
                    client=client,
                    use_playwright_fallback=venue.use_playwright_fallback,
                ),
            }
        )

    return {
        "curated_url": curated_url,
        "detail_pages": detail_pages,
    }


def _parse_myrtle_beach_events(payload: dict, *, venue: ScHtmlVenueConfig) -> list[ExtractedActivity]:
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for page in payload.get("detail_pages") or []:
        row = _parse_myrtle_beach_detail(url=page["url"], html=page["html"], venue=venue)
        if row is None or row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _parse_myrtle_beach_detail(
    *,
    url: str,
    html: str,
    venue: ScHtmlVenueConfig,
) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = normalize_space(soup.select_one("#interior h1").get_text(" ", strip=True) if soup.select_one("#interior h1") else "")
    if not title:
        return None

    content_root = soup.select_one("#interior .six#cent") or soup.select_one("#interior") or soup
    paragraphs = [normalize_space(p.get_text(" ", strip=True)) for p in content_root.select("p")]
    paragraphs = [paragraph for paragraph in paragraphs if paragraph and paragraph != title]
    full_text = " ".join(paragraphs)
    if not _should_keep_event(title=title, description=full_text, category=None):
        return None

    date_text = ""
    time_text = ""
    price_text = ""
    for paragraph in paragraphs:
        if not date_text and _parse_full_date_text(paragraph):
            date_text = paragraph
        if not time_text:
            maybe_time = _extract_time_text(paragraph)
            if maybe_time:
                time_text = maybe_time
        if "$" in paragraph or "supplies included" in paragraph.lower():
            price_text = paragraph

    base_date = _parse_full_date_text(date_text)
    if base_date is None:
        base_date = _parse_full_date_text(full_text)
    if base_date is None:
        return None

    start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
    if start_at is None:
        return None

    full_description = join_non_empty(paragraphs)
    age_min, age_max = parse_age_range(" ".join(part for part in [title, full_description or ""] if part))
    token_blob = _searchable_blob(" ".join(part for part in [title, full_description or ""] if part))

    return ExtractedActivity(
        source_url=url,
        title=title,
        description=full_description,
        venue_name=venue.venue_name,
        location_text=venue.default_location,
        city=venue.city,
        state=venue.state,
        activity_type=infer_activity_type(title, full_description, None),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=(
            any(pattern in token_blob for pattern in REGISTER_PATTERNS)
            or bool((soup.select_one("#interior") or content_root).select_one("a.maxbutton"))
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **_price_kwargs(title, date_text, time_text, price_text, full_description),
    )


def _should_fetch_card(title: str | None, body: str | None, category: str | None) -> bool:
    title_blob = _searchable_blob(title)
    if any(pattern in title_blob for pattern in TITLE_REJECT_PATTERNS):
        return False
    blob = _searchable_blob(" ".join(part for part in [title or "", body or "", category or ""] if part))
    if any(pattern in blob for pattern in NON_OVERRIDE_REJECT_PATTERNS):
        return False
    return True


def _should_keep_event(*, title: str, description: str | None, category: str | None) -> bool:
    title_blob = _searchable_blob(title)
    description_blob = _searchable_blob(join_non_empty([description, category]) or "")
    combined_blob = f"{title_blob}{description_blob}"

    if any(pattern in title_blob for pattern in TITLE_REJECT_PATTERNS):
        return False
    if any(pattern in combined_blob for pattern in NON_OVERRIDE_REJECT_PATTERNS):
        return False

    strong_include = any(pattern in combined_blob for pattern in STRONG_INCLUDE_PATTERNS)
    weak_include = any(pattern in combined_blob for pattern in WEAK_INCLUDE_PATTERNS)
    general_reject = any(pattern in combined_blob for pattern in GENERAL_REJECT_PATTERNS)

    if strong_include:
        return True
    if weak_include and not general_reject:
        return True
    if general_reject:
        return False
    return False


def _extract_meta_description(soup: BeautifulSoup) -> str:
    for selector in (
        "meta[name='description']",
        "meta[property='og:description']",
    ):
        node = soup.select_one(selector)
        content = normalize_space(node.get("content") if node else "")
        if content:
            return content
    return ""


def _price_kwargs(*parts: str | None) -> dict[str, bool | None | str]:
    raw_text = join_non_empty(parts)
    normalized_text = normalize_space(raw_text)
    if not normalized_text:
        return price_classification_kwargs(None, default_is_free=None)

    sanitized_text = re.sub(r"[^a-zA-Z0-9$]+", " ", normalized_text)
    sanitized_blob = _searchable_blob(sanitized_text)
    default_is_free = True if " free " in sanitized_blob else None
    return price_classification_kwargs(sanitized_text, default_is_free=default_is_free)


def _parse_full_date_text(value: str | None) -> date | None:
    text = normalize_space(value)
    if not text:
        return None
    match = FULL_DATE_RE.search(text)
    if not match:
        return None
    return datetime.strptime(match.group("date"), "%B %d, %Y").date()


def _parse_inferred_date(value: str | None) -> date | None:
    text = normalize_space(value)
    if not text:
        return None
    match = MONTH_DAY_RE.search(text)
    if not match:
        return None

    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    month = match.group("month").replace(".", "")
    day = int(match.group("day"))
    candidate: date | None = None
    for fmt in ("%B %d %Y", "%b %d %Y"):
        try:
            candidate = datetime.strptime(f"{month} {day} {today.year}", fmt).date()
            break
        except ValueError:
            continue
    if candidate is None:
        return None
    if candidate < today - timedelta(days=120):
        return date(today.year + 1, candidate.month, candidate.day)
    return candidate


def _extract_time_text(value: str | None) -> str:
    text = normalize_space(value)
    if not text:
        return ""
    match = TIME_SECTION_RE.search(text)
    if not match:
        return ""
    return normalize_space(match.group("time"))


def _strip_gibbes_date_from_description(description: str, date_line: str) -> str:
    normalized_description = normalize_space(description)
    normalized_date_line = normalize_space(date_line)
    if normalized_description.startswith(normalized_date_line):
        return normalize_space(normalized_description[len(normalized_date_line) :])
    return normalized_description


def _category_from_title_tag(title_text: str | None) -> str:
    text = normalize_space(title_text)
    if "classes & workshops" in text.lower():
        return "Classes & Workshops"
    if "programs & lectures" in text.lower():
        return "Programs & Lectures"
    return ""


def _first_matching_text(soup: BeautifulSoup, selector: str) -> str:
    node = soup.select_one(selector)
    return normalize_space(node.get_text(" ", strip=True) if node else "")


def _nth_text_after_heading(soup: BeautifulSoup, ordinal: int) -> str:
    heading = soup.select_one("h3.no-bottom-margin")
    if heading is None:
        return ""
    texts: list[str] = []
    for sibling in heading.find_all_next(["p", "div"], limit=8):
        text = normalize_space(sibling.get_text(" ", strip=True))
        if not text:
            continue
        if sibling.get("class") and "functions" in sibling.get("class", []):
            break
        texts.append(text)
    if len(texts) < ordinal:
        return ""
    return texts[ordinal - 1]


def _looks_like_challenge(html: str) -> bool:
    blob = html.lower()
    return any(marker in blob for marker in CHALLENGE_MARKERS)


def _dedupe_texts(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _searchable_blob(value: str | None) -> str:
    normalized = normalize_space(value).lower()
    if not normalized:
        return " "
    sanitized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return f" {normalize_space(sanitized)} "
