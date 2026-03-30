from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

NM_TIMEZONE = "America/Denver"
NM_ZONEINFO = ZoneInfo(NM_TIMEZONE)

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
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

INCLUDE_PATTERNS = (
    " activity ",
    " art making ",
    " accordion books ",
    " artist talk ",
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " create with ",
    " educator night ",
    " family art making ",
    " family day ",
    " final friday ",
    " fine art friday ",
    " first friday ",
    " hands-on ",
    " lecture ",
    " lectures ",
    " lesson plans ",
    " paint your own ",
    " painting ",
    " printmaking ",
    " still life ",
    " talk ",
    " talks ",
    " watercolor ",
    " workshop ",
    " workshops ",
)
HARD_REJECT_PATTERNS = (
    " admission ",
    " annual benefit ",
    " benefit ",
    " closing reception ",
    " concert ",
    " exhibition preview ",
    " exhibition ",
    " exhibitions ",
    " free night ",
    " fundraiser ",
    " fundraising ",
    " holiday party ",
    " member preview ",
    " opening celebration ",
    " party ",
    " performance ",
    " performances ",
    " recital ",
    " reception ",
    " tour ",
    " tours ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
)
REGISTRATION_PATTERNS = (
    " get tickets ",
    " purchase tickets ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
    " tickets ",
)

DATE_WITH_YEAR_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2}),?\s+(?P<year>\d{4})",
    re.IGNORECASE,
)
DAY_MONTH_YEAR_RE = re.compile(
    r"(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]{3,9})\s+(?P<year>\d{4})",
    re.IGNORECASE,
)
DATE_WITHOUT_YEAR_RE = re.compile(
    r"(?:(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+)?"
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2})(?!\d)",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm|AM|PM)?)"
    r"\s*(?:-|–|—|to)\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm|AM|PM))",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"\b(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm|AM|PM))\b",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})(?:\+|\s+and\s+up)\b", re.IGNORECASE)
SITE_FUTURE_CARD_RE = re.compile(
    r"^(?P<title>.+?)\s+(?P<day>\d{2})\s+(?P<month>[A-Z]{3})\s+(?P<year>\d{4})"
    r"(?:\s*,\s*(?P<time>.+?))?\s+(?P<place>SITE SANTA FE.*?)(?:\s+READ MORE)?$"
)
NMART_LOCATION_SPLIT_RE = re.compile(
    r"\b(?:Join us|We invite|Celebrate|Come|When|Tickets are|Admission is|Light snacks|Refreshments|Bring the whole family)\b"
)


@dataclass(frozen=True, slots=True)
class NmVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    source_prefixes: tuple[str, ...]


NM_VENUES: tuple[NmVenueConfig, ...] = (
    NmVenueConfig(
        slug="okeeffe",
        source_name="nm_okeeffe_events",
        venue_name="Georgia O'Keeffe Museum",
        city="Santa Fe",
        state="NM",
        list_url="https://www.okeeffemuseum.org/events/",
        source_prefixes=("https://www.okeeffemuseum.org/events/",),
    ),
    NmVenueConfig(
        slug="harwood",
        source_name="nm_harwood_events",
        venue_name="Harwood Museum of Art",
        city="Taos",
        state="NM",
        list_url="https://harwoodmuseum.org/events-calendar/",
        source_prefixes=("https://harwoodmuseum.org/events-calendar/",),
    ),
    NmVenueConfig(
        slug="nmart",
        source_name="nm_nmart_events",
        venue_name="New Mexico Museum of Art",
        city="Santa Fe",
        state="NM",
        list_url="https://www.nmartmuseum.org/events/",
        source_prefixes=("https://www.nmartmuseum.org/events/",),
    ),
    NmVenueConfig(
        slug="sitesf",
        source_name="nm_sitesf_events",
        venue_name="SITE SANTA FE",
        city="Santa Fe",
        state="NM",
        list_url="https://www.sitesantafe.org/en/events/",
        source_prefixes=("https://www.sitesantafe.org/en/events/",),
    ),
)

NM_VENUES_BY_SLUG = {venue.slug: venue for venue in NM_VENUES}


class NmBundleAdapter(BaseSourceAdapter):
    source_name = "nm_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_nm_bundle_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_nm_bundle_payload/parse_nm_events from the script runner.")


def get_nm_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in NM_VENUES:
        prefixes.extend(venue.source_prefixes)
    return tuple(prefixes)


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

            if response.status_code in (403, 429, 500, 502, 503, 504) and attempt < max_attempts:
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
            timezone_id=NM_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(8000)
        html = await page.content()
        await browser.close()
        return html


async def _fetch_detail_pages(
    *,
    urls: set[str],
    client: httpx.AsyncClient,
    label: str,
    concurrency: int = 8,
) -> dict[str, str]:
    semaphore = asyncio.Semaphore(max(concurrency, 1))

    async def _fetch_one(url: str) -> tuple[str, str | None]:
        async with semaphore:
            try:
                return url, await fetch_html(url, client=client)
            except Exception as exc:
                print(f"[nm-bundle] {label} detail fetch failed url={url}: {exc}")
                return url, None

    pages: dict[str, str] = {}
    for url, html in await asyncio.gather(*(_fetch_one(url) for url in sorted(urls))):
        if html:
            pages[url] = html
    return pages


async def load_nm_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
) -> dict[str, dict]:
    selected = [NM_VENUES_BY_SLUG[slug] for slug in venue_slugs] if venue_slugs else list(NM_VENUES)
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            if venue.slug == "okeeffe":
                list_html = await fetch_html(venue.list_url, client=client)
                cards_by_url = _extract_okeeffe_cards(list_html, venue.list_url)
                detail_pages = await _fetch_detail_pages(
                    urls=set(cards_by_url),
                    client=client,
                    label=venue.slug,
                )
                payload[venue.slug] = {
                    "list_html": list_html,
                    "cards_by_url": cards_by_url,
                    "detail_pages": detail_pages,
                }
            elif venue.slug == "harwood":
                try:
                    list_html = await fetch_html_playwright(venue.list_url)
                except Exception as exc:
                    print(f"[nm-bundle] harwood Playwright fallback failed: {exc}")
                    list_html = await fetch_html(venue.list_url, client=client)
                payload[venue.slug] = {
                    "list_html": list_html,
                }
            elif venue.slug == "nmart":
                list_html = await fetch_html(venue.list_url, client=client)
                cards_by_url = _extract_nmart_cards(list_html, venue.list_url)
                detail_pages = await _fetch_detail_pages(
                    urls=set(cards_by_url),
                    client=client,
                    label=venue.slug,
                )
                payload[venue.slug] = {
                    "list_html": list_html,
                    "cards_by_url": cards_by_url,
                    "detail_pages": detail_pages,
                }
            elif venue.slug == "sitesf":
                list_html = await fetch_html(venue.list_url, client=client)
                cards = _extract_sitesf_future_cards(list_html, venue.list_url)
                detail_pages = await _fetch_detail_pages(
                    urls={card["source_url"] for card in cards},
                    client=client,
                    label=venue.slug,
                )
                payload[venue.slug] = {
                    "list_html": list_html,
                    "cards": cards,
                    "detail_pages": detail_pages,
                }
            else:
                payload[venue.slug] = {}

    return payload


def parse_nm_events(payload: dict, *, venue: NmVenueConfig) -> list[ExtractedActivity]:
    if venue.slug == "okeeffe":
        rows = _parse_okeeffe_events(payload, venue=venue)
    elif venue.slug == "harwood":
        rows = _parse_harwood_events(payload, venue=venue)
    elif venue.slug == "nmart":
        rows = _parse_nmart_events(payload, venue=venue)
    elif venue.slug == "sitesf":
        rows = _parse_sitesf_events(payload, venue=venue)
    else:
        rows = []

    today = datetime.now(NM_ZONEINFO).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < today:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


def _parse_okeeffe_events(payload: dict, *, venue: NmVenueConfig) -> list[ExtractedActivity]:
    cards_by_url = payload.get("cards_by_url") or {}
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []

    for source_url, html in sorted(detail_pages.items()):
        soup = BeautifulSoup(html, "html.parser")
        title = _normalize_space(_text_or_none(soup.select_one(".EventHero-title")) or _text_or_none(soup.find("h1")))
        if not title:
            continue

        meta_items = [
            _normalize_space(node.get_text(" ", strip=True))
            for node in soup.select(".EventHero-meta-item")
            if _normalize_space(node.get_text(" ", strip=True))
        ]
        description = _normalize_space(_text_or_none(soup.select_one(".EventHero-description")))
        card_text = cards_by_url.get(source_url)
        full_text = _combine_text(title, description, " ".join(meta_items), card_text)
        if not _should_keep_event(title=title, text=full_text):
            continue

        date_obj = _parse_okeeffe_date(" ".join(meta_items))
        if date_obj is None:
            date_obj = _parse_okeeffe_date(card_text)
        if date_obj is None:
            continue

        start_time, end_time = _extract_time_range_from_text(" ".join(meta_items))
        start_at = _build_local_datetime(date_obj, start_time)
        end_at = _build_local_datetime(date_obj, end_time) if end_time else None
        if start_at is None:
            continue

        location_text = _extract_okeeffe_location(meta_items) or "Santa Fe, NM"
        age_min, age_max = _parse_age_range(full_text)
        is_free, free_status = infer_price_classification(full_text, default_is_free=None)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title, full_text),
                age_min=age_min,
                age_max=age_max,
                drop_in=_has_any_pattern(full_text, DROP_IN_PATTERNS),
                registration_required=_has_registration_text(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=NM_TIMEZONE,
                free_verification_status=free_status,
                is_free=is_free,
            )
        )

    return rows


def _parse_harwood_events(payload: dict, *, venue: NmVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("list_html") or ""
    soup = BeautifulSoup(html, "html.parser")
    body_text = _normalize_space(soup.get_text(" ", strip=True))
    if " NO EVENTS " in f" {body_text.upper()} ":
        return []
    return []


def _parse_nmart_events(payload: dict, *, venue: NmVenueConfig) -> list[ExtractedActivity]:
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []

    for source_url, html in sorted(detail_pages.items()):
        soup = BeautifulSoup(html, "html.parser")
        title = _normalize_space(_text_or_none(soup.find("h1")))
        article_text = _normalize_space(_text_or_none(soup.find("article")) or _text_or_none(soup.find("main")))
        if not title or not article_text:
            continue
        if not _should_keep_event(title=title, text=article_text):
            continue

        date_obj = _parse_named_date(article_text)
        if date_obj is None:
            continue
        start_time, end_time = _extract_time_range_from_text(article_text)
        start_at = _build_local_datetime(date_obj, start_time)
        end_at = _build_local_datetime(date_obj, end_time) if end_time else None
        if start_at is None:
            continue

        location_text = _extract_nmart_location(article_text)
        description = _extract_nmart_description(article_text, title=title, location_text=location_text)
        full_text = _combine_text(title, article_text, description)
        age_min, age_max = _parse_age_range(full_text)
        is_free, free_status = infer_price_classification(full_text, default_is_free=None)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text or f"{venue.city}, {venue.state}",
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title, full_text),
                age_min=age_min,
                age_max=age_max,
                drop_in=_has_any_pattern(full_text, DROP_IN_PATTERNS),
                registration_required=_has_registration_text(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=NM_TIMEZONE,
                free_verification_status=free_status,
                is_free=is_free,
            )
        )

    return rows


def _parse_sitesf_events(payload: dict, *, venue: NmVenueConfig) -> list[ExtractedActivity]:
    cards = payload.get("cards") or []
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []

    for card in cards:
        source_url = card["source_url"]
        title = card["title"]
        if not _should_keep_event(title=title, text=card["card_text"]):
            continue

        html = detail_pages.get(source_url)
        soup = BeautifulSoup(html, "html.parser") if html else None
        description = _extract_sitesf_description(soup) if soup is not None else None
        date_text = _extract_sitesf_date_text(soup) if soup is not None else None
        place_text = _extract_sitesf_place_text(soup) if soup is not None else None

        date_obj = _parse_named_date(date_text) or _parse_named_date(card["card_text"])
        if date_obj is None:
            continue

        start_time, end_time = _extract_time_range_from_text(date_text or card["card_text"])
        start_at = _build_local_datetime(date_obj, start_time)
        end_at = _build_local_datetime(date_obj, end_time) if end_time else None
        if start_at is None:
            continue

        full_text = _combine_text(title, description, card["card_text"], date_text, place_text)
        age_min, age_max = _parse_age_range(full_text)
        is_free, free_status = infer_price_classification(full_text, default_is_free=None)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=place_text or "SITE SANTA FE, Santa Fe, NM",
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title, full_text),
                age_min=age_min,
                age_max=age_max,
                drop_in=_has_any_pattern(full_text, DROP_IN_PATTERNS),
                registration_required=_has_registration_text(full_text),
                start_at=start_at,
                end_at=end_at,
                timezone=NM_TIMEZONE,
                free_verification_status=free_status,
                is_free=is_free,
            )
        )

    return rows


def _extract_okeeffe_cards(html: str, list_url: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    cards_by_url: dict[str, str] = {}
    for anchor in soup.find_all("a", href=True):
        href = urljoin(list_url, anchor.get("href", ""))
        if not href.startswith(list_url):
            continue
        if href.rstrip("/") == list_url.rstrip("/") or "?" in href or "#" in href:
            continue
        container = anchor.find_parent(["article", "li", "div", "section"]) or anchor
        card_text = _normalize_space(container.get_text(" ", strip=True))
        if not card_text:
            continue
        cards_by_url[href] = card_text
    return cards_by_url


def _extract_nmart_cards(html: str, list_url: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    cards_by_url: dict[str, str] = {}
    for anchor in soup.find_all("a", href=True):
        href = urljoin(list_url, anchor.get("href", ""))
        if not href.startswith(list_url):
            continue
        if href.rstrip("/") == list_url.rstrip("/") or "?" in href or "#" in href:
            continue
        if anchor.get_text(" ", strip=True).strip().lower() == "learn more":
            container = anchor.find_parent(["article", "li", "div", "section"]) or anchor
            card_text = _normalize_space(container.get_text(" ", strip=True))
            if card_text:
                cards_by_url[href] = card_text
            continue
        if href not in cards_by_url:
            container = anchor.find_parent(["article", "li", "div", "section"]) or anchor
            card_text = _normalize_space(container.get_text(" ", strip=True))
            if card_text:
                cards_by_url[href] = card_text
    return cards_by_url


def _extract_sitesf_future_cards(html: str, list_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict[str, str]] = []
    for card in soup.select("[class*=FutureEvents_content]"):
        card_text = _normalize_space(card.get_text(" ", strip=True))
        if not card_text:
            continue
        match = SITE_FUTURE_CARD_RE.match(card_text)
        if not match:
            continue
        link = card.find_parent().select_one("a[href]") if card.find_parent() else None
        if link is None:
            link = card.find("a", href=True)
        if link is None:
            read_more = card.parent.select_one("a[href]") if card.parent else None
            link = read_more
        if link is None:
            continue
        href = urljoin(list_url, link.get("href", ""))
        if not href.startswith(list_url):
            continue
        cards.append(
            {
                "title": _normalize_space(match.group("title")),
                "source_url": href,
                "card_text": card_text,
            }
        )
    return cards


def _extract_okeeffe_location(meta_items: list[str]) -> str | None:
    for item in meta_items:
        if " NM" in item or item.lower() == "online":
            return item
    return None


def _extract_nmart_location(article_text: str) -> str | None:
    date_match = _find_first_date_match(article_text)
    time_match = TIME_RANGE_RE.search(article_text)
    if date_match is None or time_match is None or time_match.end() <= date_match.end():
        return None
    tail = article_text[time_match.end():].strip(" |,")
    if not tail:
        return None
    split_match = NMART_LOCATION_SPLIT_RE.search(tail)
    location = tail[:split_match.start()] if split_match else tail
    location = _normalize_space(location)
    return location or None


def _extract_nmart_description(article_text: str, *, title: str, location_text: str | None) -> str | None:
    text = article_text
    text = text.replace("Upcoming Event", "", 1).strip()
    if text.startswith(title):
        text = text[len(title):].strip()
    date_match = _find_first_date_match(text)
    time_match = TIME_RANGE_RE.search(text)
    end_index = time_match.end() if time_match is not None else (date_match.end() if date_match is not None else 0)
    text = text[end_index:].strip()
    if location_text and text.startswith(location_text):
        text = text[len(location_text):].strip()
    text = text.split("Directions to", 1)[0].strip()
    return text or None


def _extract_sitesf_date_text(soup: BeautifulSoup | None) -> str | None:
    if soup is None:
        return None
    return _text_or_none(soup.select_one("[class*=EventComponent_contentMetadataDate]"))


def _extract_sitesf_place_text(soup: BeautifulSoup | None) -> str | None:
    if soup is None:
        return None
    return _text_or_none(soup.select_one("[class*=EventComponent_contentMetadataPlace]"))


def _extract_sitesf_description(soup: BeautifulSoup | None) -> str | None:
    if soup is None:
        return None
    main = soup.find("main")
    main_text = _normalize_space(_text_or_none(main))
    title = _normalize_space(_text_or_none(soup.find("h1")) or "")
    date_text = _normalize_space(_extract_sitesf_date_text(soup))
    place_text = _normalize_space(_extract_sitesf_place_text(soup))
    marker = f"BACK TO EVENTS {title}"
    if marker and marker in main_text:
        main_text = main_text.split(marker, 1)[1].strip()
    if main_text.startswith("UPCOMING"):
        main_text = main_text[len("UPCOMING"):].strip(" ,")
    if date_text and date_text in main_text:
        main_text = main_text.split(date_text, 1)[1].strip(" ,")
    if place_text and place_text in main_text:
        main_text = main_text.split(place_text, 1)[1].strip(" ,")
    for stopper in (
        " FREE!",
        " RSVP",
        " Artists ",
        " Curators ",
        " Related ",
        " Guided by artists, rooted in New Mexico",
    ):
        if stopper in main_text:
            main_text = main_text.split(stopper, 1)[0].strip()
    return main_text or None


def _should_keep_event(*, title: str, text: str | None) -> bool:
    normalized = f" {_normalize_space(_combine_text(title, text)).lower()} "
    has_include = _has_any_pattern(normalized, INCLUDE_PATTERNS) or " first friday" in normalized or " final friday" in normalized
    has_reject = _has_any_pattern(normalized, HARD_REJECT_PATTERNS)
    if has_reject and not has_include:
        return False
    if " first friday " in normalized and (" admission " in normalized or " free from " in normalized):
        return False
    if " annual benefit " in normalized:
        return False
    if " member preview " in normalized:
        return False
    if " opening celebration " in normalized:
        return False
    if " recital " in normalized:
        return False
    return has_include


def _infer_activity_type(title: str, text: str | None) -> str:
    normalized = f" {_normalize_space(_combine_text(title, text)).lower()} "
    if " first friday" in normalized:
        return "talk"
    if _has_any_pattern(
        normalized,
        (
            " accordion books ",
            " class ",
            " classes ",
            " create with ",
            " family art making ",
            " family day ",
            " final friday ",
            " fine art friday ",
            " paint ",
            " painting ",
            " printmaking ",
            " still life ",
            " watercolor ",
            " workshop ",
            " workshops ",
        ),
    ):
        return "workshop"
    if _has_any_pattern(
        normalized,
        (
            " conversation ",
            " first friday ",
            " lecture ",
            " lectures ",
            " talk ",
            " talks ",
        ),
    ):
        return "talk"
    return "activity"


def _parse_okeeffe_date(text: str | None) -> date | None:
    if not text:
        return None
    return _parse_named_date(text, infer_year=True)


def _parse_named_date(text: str | None, *, infer_year: bool = False) -> date | None:
    if not text:
        return None
    for pattern in (DATE_WITH_YEAR_RE, DAY_MONTH_YEAR_RE):
        match = pattern.search(text)
        if match:
            month = MONTH_LOOKUP.get(match.group("month")[:3].lower())
            if month is None:
                continue
            return date(int(match.group("year")), month, int(match.group("day")))

    match = DATE_WITHOUT_YEAR_RE.search(text)
    if match:
        month = MONTH_LOOKUP.get(match.group("month")[:3].lower())
        if month is None:
            return None
        day = int(match.group("day"))
        year = _infer_event_year(month=month, day=day) if infer_year else datetime.now(NM_ZONEINFO).year
        return date(year, month, day)

    return None


def _infer_event_year(*, month: int, day: int) -> int:
    today = datetime.now(NM_ZONEINFO).date()
    candidate = date(today.year, month, day)
    if candidate < today and (today - candidate).days > 180:
        return today.year + 1
    return today.year


def _extract_time_range_from_text(text: str | None) -> tuple[str | None, str | None]:
    if not text:
        return None, None
    match = TIME_RANGE_RE.search(text)
    if match:
        start_raw = match.group("start")
        end_raw = match.group("end")
        end = _normalize_time_meridiem(end_raw)
        start = _normalize_time_meridiem(start_raw)
        if not re.search(r"\b(?:am|pm)\b", start_raw, re.IGNORECASE) and end is not None:
            end_meridiem_match = re.search(r"\b(AM|PM)\b", end)
            if end_meridiem_match:
                candidate = _normalize_time_meridiem(f"{start_raw} {end_meridiem_match.group(1)}")
                if candidate is not None:
                    start_time = _parse_clock_time(candidate)
                    end_time = _parse_clock_time(end)
                    if start_time is not None and end_time is not None and start_time > end_time:
                        flipped = "AM" if end_meridiem_match.group(1) == "PM" else "PM"
                        candidate = _normalize_time_meridiem(f"{start_raw} {flipped}")
                    start = candidate
        return start, end
    match = TIME_SINGLE_RE.search(text)
    if match:
        return _normalize_time_meridiem(match.group("time")), None
    return None, None


def _normalize_time_meridiem(value: str | None, *, fallback: str | None = None) -> str | None:
    if not value:
        return None
    normalized = _normalize_space(value).replace(".", "")
    if re.search(r"\b(?:am|pm)\b", normalized, re.IGNORECASE):
        return normalized.upper()
    if fallback:
        fallback_match = re.search(r"\b(am|pm)\b", fallback.replace(".", ""), re.IGNORECASE)
        if fallback_match:
            return f"{normalized} {fallback_match.group(1).upper()}"
    return normalized.upper()


def _build_local_datetime(event_date: date, time_text: str | None) -> datetime | None:
    if event_date is None:
        return None
    if not time_text:
        return datetime.combine(event_date, time.min, tzinfo=NM_ZONEINFO)
    parsed_time = _parse_clock_time(time_text)
    if parsed_time is None:
        return None
    return datetime.combine(event_date, parsed_time, tzinfo=NM_ZONEINFO)


def _parse_clock_time(value: str) -> time | None:
    normalized = _normalize_space(value).replace(".", "").upper()
    if re.fullmatch(r"\d{1,2}\s+[AP]M", normalized):
        normalized = normalized.replace(" ", ":00 ", 1)
    elif re.fullmatch(r"\d{1,2}[AP]M", normalized):
        normalized = re.sub(r"^(\d{1,2})([AP]M)$", r"\1:00 \2", normalized)
    elif re.fullmatch(r"\d{1,2}:\d{2}[AP]M", normalized):
        normalized = re.sub(r"^(\d{1,2}:\d{2})([AP]M)$", r"\1 \2", normalized)
    elif re.fullmatch(r"\d{1,2}\s*:\s*\d{2}\s+[AP]M", normalized):
        normalized = re.sub(r"\s*:\s*", ":", normalized)

    for fmt in ("%I:%M %p", "%I %p"):
        try:
            return datetime.strptime(normalized, fmt).time()
        except ValueError:
            continue
    return None


def _parse_age_range(text: str | None) -> tuple[int | None, int | None]:
    if not text:
        return None, None
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = AGE_PLUS_RE.search(text)
    if match:
        return int(match.group(1)), None
    return None, None


def _has_registration_text(text: str | None) -> bool | None:
    if not text:
        return None
    normalized = f" {_normalize_space(text).lower()} "
    if " no registration required " in normalized:
        return False
    if _has_any_pattern(normalized, REGISTRATION_PATTERNS):
        return True
    return None


def _find_first_date_match(text: str | None) -> re.Match[str] | None:
    if not text:
        return None
    for pattern in (DATE_WITH_YEAR_RE, DAY_MONTH_YEAR_RE, DATE_WITHOUT_YEAR_RE):
        match = pattern.search(text)
        if match:
            return match
    return None


def _has_any_pattern(text: str | None, patterns: tuple[str, ...]) -> bool:
    if not text:
        return False
    normalized = f" {text.lower()} "
    return any(pattern in normalized for pattern in patterns)


def _combine_text(*parts: str | None) -> str:
    return " ".join(_normalize_space(part) for part in parts if _normalize_space(part))


def _text_or_none(node) -> str | None:
    if node is None:
        return None
    return node.get_text(" ", strip=True)


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())
