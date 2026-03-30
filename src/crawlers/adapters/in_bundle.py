from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
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
from src.crawlers.adapters.oh_common import NY_TIMEZONE as IN_TIMEZONE
from src.crawlers.adapters.oh_common import clean_html_fragment
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

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
    " art making ",
    " art-making ",
    " artmaking ",
    " class ",
    " classes ",
    " conversation ",
    " craft ",
    " crafting ",
    " creative ",
    " design ",
    " discussion ",
    " draw ",
    " drawing ",
    " drop in ",
    " drop-in ",
    " family ",
    " hands on ",
    " hands-on ",
    " insight ",
    " lab ",
    " lecture ",
    " lectures ",
    " make your own ",
    " make ",
    " open studio ",
    " spring break ",
    " talk ",
    " talks ",
    " teen ",
    " teens ",
    " toddler ",
    " workshop ",
    " workshops ",
    " youth ",
)

WEAK_INCLUDE_PATTERNS = (
    " art ",
    " artist ",
    " family ",
    " kid ",
    " kids ",
    " teen ",
    " teens ",
    " youth ",
)

HARD_REJECT_PATTERNS = (
    " artwords ",
    " art fair ",
    " block party ",
    " camp ",
    " camps ",
    " concert ",
    " concerts ",
    " dance ",
    " dinner ",
    " ekphrastic ",
    " fair ",
    " film ",
    " films ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " jazz ",
    " lawn games ",
    " meditation ",
    " mindfulness ",
    " music ",
    " opening day ",
    " open house ",
    " orchestra ",
    " performance ",
    " performances ",
    " poet ",
    " poets ",
    " poem ",
    " poetry ",
    " reading ",
    " reception ",
    " rivalry ",
    " spoken word ",
    " storytime ",
    " story time ",
    " tai chi ",
    " tv ",
    " writing ",
    " yoga ",
    " dj ",
)

SOFT_REJECT_PATTERNS = (
    " admission ",
    " exhibition ",
    " exhibitions ",
    " first thursday ",
    " first thursdays ",
    " tour ",
    " tours ",
)

DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
    " no registration required ",
    " no prior experience is necessary ",
    " no experience is necessary ",
)

REGISTRATION_PATTERNS = (
    " registration ",
    " register ",
    " reserve ",
    " rsvp ",
    " sign up ",
    " ticket ",
    " tickets ",
)

FWMOA_DATE_TIME_RE = re.compile(
    r"(?P<date>(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2})"
    r"\s*\|\s*(?P<time>[^<]{1,40}?)\b(?:RSVP|SHARE|ADD TO CALENDAR|$)",
    re.IGNORECASE,
)
FWMOA_MULTI_DAY_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2})(?:\s*[-–]\s*(?P<end_day>\d{1,2}))?,\s*(?P<year>20\d{2})",
    re.IGNORECASE,
)
RACLIN_TITLE_RE = re.compile(
    r"(?P<weekday>[A-Za-z]{3})\s+(?P<month>[A-Za-z]{3})\s+(?P<day>\d{1,2}),\s+(?P<year>20\d{2})\s*(?P<time>.+)",
    re.IGNORECASE,
)
SOUTHBEND_DAY_MONTH_RE = re.compile(r"^(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]{3,9})$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class InVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    discovery_mode: str
    default_location: str
    discovery_url: str | None = None


IN_VENUES: tuple[InVenueConfig, ...] = (
    InVenueConfig(
        slug="eiteljorg",
        source_name="in_eiteljorg_events",
        venue_name="Eiteljorg Museum of American Indians and Western Art",
        city="Indianapolis",
        state="IN",
        list_url="https://eiteljorg.org/events/",
        discovery_mode="eiteljorg_jsonld",
        default_location="Eiteljorg Museum, Indianapolis, IN",
    ),
    InVenueConfig(
        slug="eskenazi",
        source_name="in_eskenazi_events",
        venue_name="Sidney and Lois Eskenazi Museum of Art",
        city="Bloomington",
        state="IN",
        list_url="https://artmuseum.indiana.edu/news-events/calendar/index.html",
        discovery_mode="eskenazi_listing",
        default_location="Sidney and Lois Eskenazi Museum of Art, Bloomington, IN",
    ),
    InVenueConfig(
        slug="fwmoa",
        source_name="in_fwmoa_events",
        venue_name="Fort Wayne Museum of Art",
        city="Fort Wayne",
        state="IN",
        list_url="https://fwmoa.org/events/",
        discovery_mode="fwmoa_detail_pages",
        default_location="Fort Wayne Museum of Art, Fort Wayne, IN",
    ),
    InVenueConfig(
        slug="raclin",
        source_name="in_raclin_murphy_events",
        venue_name="Raclin Murphy Museum of Art",
        city="Notre Dame",
        state="IN",
        list_url="https://raclinmurphymuseum.nd.edu/visit/events/",
        discovery_mode="raclin_listing",
        default_location="Raclin Murphy Museum of Art, Notre Dame, IN",
    ),
    InVenueConfig(
        slug="south_bend",
        source_name="in_south_bend_museum_of_art_events",
        venue_name="South Bend Museum of Art",
        city="South Bend",
        state="IN",
        list_url="https://southbendart.org/events/",
        discovery_mode="southbend_mec_listing",
        default_location="South Bend Museum of Art, South Bend, IN",
        discovery_url="https://southbendart.org/all-events/",
    ),
)

IN_VENUES_BY_SLUG = {venue.slug: venue for venue in IN_VENUES}


class InBundleAdapter(BaseSourceAdapter):
    source_name = "in_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_in_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_in_bundle_payload/parse_in_events from the script runner.")


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
            timezone_id=IN_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def load_in_bundle_payload(
    *,
    venues: list[InVenueConfig] | tuple[InVenueConfig, ...] | None = None,
    max_detail_pages: int = 60,
) -> dict[str, dict]:
    selected = list(venues) if venues is not None else list(IN_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.discovery_mode in {"eiteljorg_jsonld", "eskenazi_listing", "raclin_listing"}:
                    payload_by_slug[venue.slug] = {
                        "html": await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
                    }
                elif venue.discovery_mode == "fwmoa_detail_pages":
                    list_html = await fetch_html(venue.list_url, client=client, use_playwright_fallback=True)
                    detail_urls = _extract_detail_urls(
                        list_html,
                        list_url=venue.list_url,
                        path_fragment="/event/",
                    )[:max_detail_pages]
                    payload_by_slug[venue.slug] = {
                        "list_html": list_html,
                        "detail_html_by_url": await _fetch_many_html(detail_urls, client=client),
                    }
                elif venue.discovery_mode == "southbend_mec_listing":
                    discovery_url = venue.discovery_url or venue.list_url
                    html = await fetch_html(discovery_url, client=client, use_playwright_fallback=True)
                    payload_by_slug[venue.slug] = {"html": html}
                else:
                    raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[in-bundle-fetch] venue={venue.slug} failed: {exc}")

    return {"payload_by_slug": payload_by_slug, "errors_by_slug": errors_by_slug}


def parse_in_events(payload: dict, *, venue: InVenueConfig) -> list[ExtractedActivity]:
    if venue.discovery_mode == "eiteljorg_jsonld":
        rows = _parse_eiteljorg_events(payload, venue=venue)
    elif venue.discovery_mode == "eskenazi_listing":
        rows = _parse_eskenazi_events(payload, venue=venue)
    elif venue.discovery_mode == "fwmoa_detail_pages":
        rows = _parse_fwmoa_events(payload, venue=venue)
    elif venue.discovery_mode == "raclin_listing":
        rows = _parse_raclin_events(payload, venue=venue)
    elif venue.discovery_mode == "southbend_mec_listing":
        rows = _parse_southbend_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(IN_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


def get_in_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in IN_VENUES:
        for url in (venue.list_url, venue.discovery_url):
            if not url:
                continue
            parsed = urlparse(url)
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(dict.fromkeys(prefixes))


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


def _parse_eiteljorg_events(payload: dict, *, venue: InVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("html") or ""
    rows: list[ExtractedActivity] = []

    for event_obj in _extract_event_objects_from_ldjson(html):
        source_url = _normalize_url(event_obj.get("url"), venue.list_url)
        title = normalize_space(event_obj.get("name"))
        description = clean_html_fragment(unescape(str(event_obj.get("description") or "")))
        start_at = _parse_iso_datetime_value(event_obj.get("startDate"))
        end_at = _parse_iso_datetime_value(event_obj.get("endDate"))
        price_text = _extract_offer_text(event_obj.get("offers"))
        price_amount = _extract_offer_amount(event_obj.get("offers"))
        location_text = _extract_location_name(event_obj.get("location")) or venue.default_location

        if not title or not source_url or start_at is None:
            continue
        if _is_unavailable_event(title=title, description=description, event_obj=event_obj):
            continue
        if not _should_keep_event(title=title, description=description):
            continue

        full_description = join_non_empty(
            [
                description,
                f"Price: {price_text}" if price_text else None,
            ]
        )
        text_blob = " ".join(part for part in [title, full_description or ""] if part)
        age_min, age_max = parse_age_range(text_blob)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=full_description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=_contains_any(text_blob, DROP_IN_PATTERNS),
                registration_required=_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=IN_TIMEZONE,
                **price_classification_kwargs_from_amount(price_amount, text=price_text or full_description),
            )
        )

    return rows


def _parse_eskenazi_events(payload: dict, *, venue: InVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("html") or ""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    today = datetime.now(ZoneInfo(IN_TIMEZONE)).date()

    for article in soup.select("article.event.feed-item"):
        title = normalize_space(_node_text(article.select_one("[itemprop='name']")))
        source_url = _normalize_url(
            article.select_one("a[itemprop='url'][href]").get("href") if article.select_one("a[itemprop='url'][href]") else None,
            venue.list_url,
        )
        date_text = normalize_space(_node_text(article.select_one(".meta.date")))
        time_text = normalize_space(_node_text(article.select_one(".meta.time")))
        description = normalize_space(_node_text(article.select_one("[itemprop='description']"))).removesuffix(" Read more")
        location_text = normalize_space(_node_text(article.select_one("[itemprop='location'] [itemprop='name']"))) or venue.default_location
        parsed = _parse_listing_datetime(date_text=date_text, time_text=time_text, today=today)

        if not title or not source_url or parsed is None:
            continue
        if not _should_keep_event(title=title, description=description):
            continue

        start_at, end_at = parsed
        text_blob = " ".join(part for part in [title, description] if part)
        age_min, age_max = parse_age_range(text_blob)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=_contains_any(text_blob, DROP_IN_PATTERNS),
                registration_required=_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=IN_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )

    return rows


def _parse_fwmoa_events(payload: dict, *, venue: InVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    today = datetime.now(ZoneInfo(IN_TIMEZONE)).date()

    for source_url, html in (payload.get("detail_html_by_url") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        title = _clean_fwmoa_title(
            _first_non_empty(
                _node_text(soup.find("h1")),
                _extract_meta_content(soup, "og:title"),
                _node_text(soup.title),
            )
        )
        body_text = normalize_space(soup.get_text(" ", strip=True))
        if not title:
            continue

        parsed = _parse_fwmoa_datetime(body_text, today=today)
        if parsed is None:
            continue

        description = _extract_fwmoa_description(body_text, title=title)
        if not _should_keep_event(title=title, description=description):
            continue

        start_at, end_at = parsed
        text_blob = " ".join(part for part in [title, description] if part)
        age_min, age_max = parse_age_range(text_blob)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=_contains_any(text_blob, DROP_IN_PATTERNS),
                registration_required=_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=IN_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )

    return rows


def _parse_raclin_events(payload: dict, *, venue: InVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("html") or ""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []

    for article in soup.select("article.article.snippet.event"):
        link = article.select_one("a.card-link[href]")
        start_node = article.select_one("time[property='startDate'][datetime]")
        if link is None or start_node is None:
            continue

        title = normalize_space(_node_text(article.select_one("[property='name']")))
        source_url = _normalize_url(link.get("href"), venue.list_url)
        description = normalize_space(
            article.select_one("meta[property='description']").get("content")
            if article.select_one("meta[property='description']")
            else ""
        )
        location_text = normalize_space(_node_text(article.select_one("[property='location'] [property='name address']"))) or venue.default_location
        parsed = _parse_raclin_datetime(article)
        if parsed is None:
            start_at = _parse_iso_datetime_value(start_node.get("datetime"))
            end_at = _parse_iso_datetime_value(
                article.select_one("time[property='endDate'][datetime]").get("datetime")
                if article.select_one("time[property='endDate'][datetime]")
                else None
            )
        else:
            start_at, end_at = parsed

        if not title or not source_url or start_at is None:
            continue
        if not _should_keep_event(title=title, description=description):
            continue

        text_blob = " ".join(part for part in [title, description] if part)
        age_min, age_max = parse_age_range(text_blob)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=_contains_any(text_blob, DROP_IN_PATTERNS),
                registration_required=_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=IN_TIMEZONE,
                **price_classification_kwargs(description),
            )
        )

    return rows


def _parse_southbend_events(payload: dict, *, venue: InVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("html") or ""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    today = datetime.now(ZoneInfo(IN_TIMEZONE)).date()
    event_obj_by_url = {
        _normalize_url(event_obj.get("url"), venue.list_url): event_obj
        for event_obj in _extract_event_objects_from_ldjson(html)
        if _normalize_url(event_obj.get("url"), venue.list_url)
    }

    for article in soup.select("article.mec-event-article"):
        link = article.select_one(".mec-event-title a[href]")
        if link is None:
            continue

        source_url = _normalize_url(link.get("href"), venue.list_url)
        title = normalize_space(_node_text(link))
        description = normalize_space(_node_text(article.select_one(".mec-event-description")))
        event_obj = event_obj_by_url.get(source_url)
        parsed = _parse_southbend_visible_datetime(article, today=today)
        start_at = parsed[0] if parsed is not None else None
        end_at = parsed[1] if parsed is not None else None
        price_text = _extract_offer_text(event_obj.get("offers") if event_obj else None)
        price_amount = _extract_offer_amount(event_obj.get("offers") if event_obj else None)
        location_text = _extract_location_name(event_obj.get("location") if event_obj else None)
        if not location_text:
            location_text = normalize_space(_node_text(article.select_one(".mec-venue-details"))) or venue.default_location

        if start_at is None:
            continue

        if not title or not source_url:
            continue
        if not _should_keep_event(title=title, description=description):
            continue

        full_description = join_non_empty(
            [
                description,
                f"Price: {price_text}" if price_text else None,
            ]
        )
        text_blob = " ".join(part for part in [title, full_description or ""] if part)
        age_min, age_max = parse_age_range(text_blob)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=full_description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=_contains_any(text_blob, DROP_IN_PATTERNS),
                registration_required=_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=IN_TIMEZONE,
                **price_classification_kwargs_from_amount(price_amount, text=price_text or full_description),
            )
        )

    return rows


def _extract_detail_urls(html: str, *, list_url: str, path_fragment: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        absolute_url = _normalize_url(anchor.get("href"), list_url)
        if not absolute_url or path_fragment not in urlparse(absolute_url).path:
            continue
        if absolute_url in seen or absolute_url.rstrip("/") == list_url.rstrip("/"):
            continue
        seen.add(absolute_url)
        urls.append(absolute_url)
    return urls


def _extract_event_objects_from_ldjson(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = _strip_json_wrappers(script.get_text() or "")
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        _collect_event_objects(data, rows)
    return rows


def _collect_event_objects(value: object, rows: list[dict]) -> None:
    if isinstance(value, dict):
        value_type = value.get("@type")
        if value_type == "Event" or (isinstance(value_type, list) and "Event" in value_type):
            rows.append(value)
        for nested in value.values():
            _collect_event_objects(nested, rows)
    elif isinstance(value, list):
        for nested in value:
            _collect_event_objects(nested, rows)


def _strip_json_wrappers(text: str) -> str:
    stripped = text.strip()
    stripped = stripped.removeprefix("/*<![CDATA[*/").removesuffix("/*]]>*/").strip()
    return stripped


def _parse_iso_datetime_value(value: object) -> datetime | None:
    if not value:
        return None
    try:
        return parse_iso_datetime(str(value), timezone_name=IN_TIMEZONE)
    except ValueError:
        return None


def _parse_listing_datetime(
    *,
    date_text: str,
    time_text: str | None,
    today: date,
) -> tuple[datetime, datetime | None] | None:
    normalized_date = normalize_space(date_text)
    parsed_date = parse_date_text(normalized_date)
    if parsed_date is None:
        parsed_date = _parse_date_without_year(normalized_date, today=today)
    if parsed_date is None:
        return None
    return parse_time_range(base_date=parsed_date, time_text=_normalize_time_text(time_text))


def _parse_date_without_year(text: str, *, today: date) -> date | None:
    normalized = normalize_space(text)
    if not normalized:
        return None

    for fmt in ("%A, %B %d", "%A, %b %d", "%B %d", "%b %d"):
        try:
            parsed = datetime.strptime(normalized, fmt).date().replace(year=today.year)
            if parsed < today.replace(month=1, day=1):
                return parsed
            return parsed
        except ValueError:
            continue

    month_day_match = SOUTHBEND_DAY_MONTH_RE.match(normalized)
    if month_day_match is not None:
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                return datetime.strptime(
                    f"{month_day_match.group('day')} {month_day_match.group('month')} {today.year}",
                    fmt,
                ).date()
            except ValueError:
                continue

    return None


def _parse_southbend_visible_datetime(article: BeautifulSoup, *, today: date) -> tuple[datetime, datetime | None] | None:
    date_text = normalize_space(_node_text(article.select_one(".mec-start-date-label")))
    start_text = normalize_space(_node_text(article.select_one(".mec-start-time")))
    end_text = normalize_space(_node_text(article.select_one(".mec-end-time")))
    if not date_text:
        return None
    parsed_date = _parse_date_without_year(date_text, today=today)
    if parsed_date is None:
        return None
    time_text = start_text
    if end_text:
        time_text = f"{start_text} - {end_text}"
    return parse_time_range(base_date=parsed_date, time_text=_normalize_time_text(time_text))


def _extract_fwmoa_description(text: str, *, title: str) -> str:
    body = text
    if "ADD TO CALENDAR" in body:
        body = body.split("ADD TO CALENDAR", 1)[1]
    elif title in body:
        body = body.split(title, 1)[1]

    for marker in ("Upcoming Events", "Back to Top", "311 E Main Street"):
        if marker in body:
            body = body.split(marker, 1)[0]

    body = body.replace("RSVP", " ").replace("SHARE", " ").replace("ADD TO CALENDAR", " ")
    return normalize_space(body)


def _parse_fwmoa_datetime(body_text: str, *, today: date) -> tuple[datetime, datetime | None] | None:
    match = FWMOA_DATE_TIME_RE.search(body_text)
    if match is not None:
        return _parse_listing_datetime(
            date_text=f"{match.group('date')}, {today.year}",
            time_text=match.group("time"),
            today=today,
        )

    multi_day_match = FWMOA_MULTI_DAY_RE.search(body_text)
    if multi_day_match is None:
        return None

    start_date = parse_date_text(
        f"{multi_day_match.group('month')} {multi_day_match.group('day')}, {multi_day_match.group('year')}"
    )
    if start_date is None:
        return None

    start_at, _ = parse_time_range(base_date=start_date, time_text=None)
    end_at = None
    if multi_day_match.group("end_day"):
        end_date = parse_date_text(
            f"{multi_day_match.group('month')} {multi_day_match.group('end_day')}, {multi_day_match.group('year')}"
        )
        if end_date is not None:
            _, end_at = parse_time_range(base_date=end_date, time_text="11:59 pm")
    return start_at, end_at


def _parse_raclin_datetime(article: BeautifulSoup) -> tuple[datetime, datetime | None] | None:
    title_attr = normalize_space(article.select_one(".event-time").get("title") if article.select_one(".event-time") else "")
    match = RACLIN_TITLE_RE.match(title_attr)
    if match is None:
        return None
    base_date = parse_date_text(f"{match.group('month')} {match.group('day')}, {match.group('year')}")
    if base_date is None:
        return None
    return parse_time_range(base_date=base_date, time_text=_normalize_time_text(match.group("time")))


def _clean_fwmoa_title(text: str | None) -> str:
    value = normalize_space(text)
    for suffix in (" - FWMoA", " FWMoA"):
        if value.endswith(suffix):
            value = value[: -len(suffix)].strip()
    return value


def _extract_offer_text(offers: object) -> str | None:
    if isinstance(offers, dict):
        price = normalize_space(str(offers.get("price") or offers.get("name") or ""))
        currency = normalize_space(str(offers.get("priceCurrency") or ""))
        if price and currency:
            return f"{currency} {price}"
        return price or None
    if isinstance(offers, list):
        values = [_extract_offer_text(item) for item in offers]
        values = [value for value in values if value]
        return ", ".join(values) or None
    return None


def _extract_offer_amount(offers: object) -> Decimal | None:
    if isinstance(offers, dict):
        raw_price = offers.get("price")
        if raw_price in (None, ""):
            return None
        try:
            return Decimal(str(raw_price))
        except Exception:
            return None
    if isinstance(offers, list):
        for offer in offers:
            value = _extract_offer_amount(offer)
            if value is not None:
                return value
    return None


def _extract_location_name(location: object) -> str | None:
    if location is None:
        return None
    if isinstance(location, dict):
        name = normalize_space(location.get("name"))
        if name:
            return name
        address = location.get("address")
        if isinstance(address, dict):
            parts = [
                normalize_space(address.get("streetAddress")),
                normalize_space(address.get("addressLocality")),
                normalize_space(address.get("addressRegion")),
            ]
            joined = ", ".join(part for part in parts if part)
            return joined or None
        return None
    if isinstance(location, list):
        for item in location:
            value = _extract_location_name(item)
            if value:
                return value
    return normalize_space(str(location)) or None


def _extract_meta_content(soup: BeautifulSoup, key: str) -> str | None:
    for attr in ("property", "name"):
        node = soup.find("meta", attrs={attr: key})
        if node and node.get("content"):
            return normalize_space(node.get("content"))
    return None


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        normalized = normalize_space(value)
        if normalized:
            return normalized
    return None


def _node_text(node) -> str:
    if node is None:
        return ""
    return node.get_text(" ", strip=True)


def _normalize_url(url: object, base_url: str) -> str | None:
    normalized = normalize_space(str(url or ""))
    if not normalized:
        return None
    return urljoin(base_url, normalized)


def _normalize_time_text(value: str | None) -> str | None:
    if not value:
        return None
    normalized = normalize_space(value)
    normalized = normalized.replace("a.m.", "am").replace("p.m.", "pm")
    normalized = normalized.replace("A.M.", "AM").replace("P.M.", "PM")
    return normalized


def _searchable_blob(text: str | None) -> str:
    normalized = normalize_space(unescape(text or "")).lower()
    return f" {normalized} " if normalized else " "


def _contains_any(text: str | None, patterns: tuple[str, ...]) -> bool:
    blob = _searchable_blob(text)
    return any(pattern in blob for pattern in patterns)


def _should_keep_event(*, title: str, description: str | None) -> bool:
    title_blob = _searchable_blob(title)
    token_blob = _searchable_blob(" ".join(part for part in [title, description or ""] if part))

    if any(pattern in token_blob for pattern in HARD_REJECT_PATTERNS):
        return False

    if " tour " in token_blob or " tours " in token_blob:
        if not any(
            pattern in token_blob
            for pattern in (" craft ", " crafting ", " workshop ", " class ", " art making ", " make your own ")
        ):
            return False

    if any(pattern in token_blob for pattern in SOFT_REJECT_PATTERNS):
        return any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)

    if any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS):
        return True

    return any(pattern in title_blob or pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS)


def _infer_activity_type(text: str | None) -> str | None:
    blob = _searchable_blob(text)
    if " workshop " in blob or " workshops " in blob:
        return "workshop"
    if " class " in blob or " classes " in blob or " studio " in blob:
        return "class"
    if any(pattern in blob for pattern in (" lecture ", " lectures ", " talk ", " talks ", " discussion ", " conversation ")):
        return "lecture"
    return "activity"


def _registration_required(text: str | None) -> bool | None:
    blob = _searchable_blob(text)
    if any(pattern in blob for pattern in DROP_IN_PATTERNS):
        return False
    if any(pattern in blob for pattern in REGISTRATION_PATTERNS):
        return True
    return None


def _is_unavailable_event(*, title: str, description: str | None, event_obj: dict | None = None) -> bool:
    blob = _searchable_blob(" ".join(part for part in [title, description or ""] if part))
    if any(pattern in blob for pattern in (" postponed ", " canceled ", " cancelled ", " sold out ")):
        return True
    event_status = normalize_space((event_obj or {}).get("eventStatus"))
    return event_status.endswith("EventCancelled") or event_status.endswith("EventPostponed")


def _looks_like_bot_challenge(html: str) -> bool:
    return any(marker in html for marker in BOT_CHALLENGE_MARKERS)
