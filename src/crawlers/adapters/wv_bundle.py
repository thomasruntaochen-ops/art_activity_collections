from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from html import unescape
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

import httpx
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

WV_TIMEZONE = "America/New_York"
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
WVU_WIDGET_URL = (
    "https://cal.wvu.edu/widget/view?schools=wvu&days=90&num=50&tags=art+museum"
    "&experience=inperson&container=localist-widget-61083755&template=dsv3-vertical-mini"
)
MONTH_ABBREVIATIONS = {
    "jan": "Jan",
    "feb": "Feb",
    "mar": "Mar",
    "apr": "Apr",
    "jun": "Jun",
    "jul": "Jul",
    "aug": "Aug",
    "sep": "Sep",
    "sept": "Sep",
    "oct": "Oct",
    "nov": "Nov",
    "dec": "Dec",
}
WEEKDAY_NAMES = {
    "monday": 0,
    "mondays": 0,
    "tuesday": 1,
    "tuesdays": 1,
    "wednesday": 2,
    "wednesdays": 2,
    "thursday": 3,
    "thursdays": 3,
    "friday": 4,
    "fridays": 4,
    "saturday": 5,
    "saturdays": 5,
    "sunday": 6,
    "sundays": 6,
}
TITLE_DATE_RE = re.compile(r"\bon (?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\b", re.IGNORECASE)
DATE_WITH_SUFFIX_RE = re.compile(
    r"(?P<date>[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4})",
    re.IGNORECASE,
)
TIME_RANGE_WITH_FROM_RE = re.compile(
    r"\bfrom\s+(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?\s*"
    r"(?:-|–|—|to)\s*\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
TIME_SINGLE_WITH_AT_RE = re.compile(
    r"\bat\s+(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
LOCALIST_START_RE = re.compile(r'"startDate":"(?P<value>[^"]+)"')
LOCALIST_END_RE = re.compile(r'"endDate":"(?P<value>[^"]+)"')
HMOA_INCLUDE_PATTERNS = (
    " art activity ",
    " art workshop ",
    " class ",
    " classes ",
    " discussion ",
    " family program ",
    " gallery talk ",
    " kidsart ",
    " lecture ",
    " lectures ",
    " scholar ",
    " studio ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
HMOA_REJECT_PATTERNS = (
    " camp ",
    " camps ",
    " closing at ",
    " concert ",
    " concerts ",
    " exhibition ",
    " exhibitions ",
    " free tuesday ",
    " gala ",
    " music ",
    " performance ",
    " performances ",
    " reception ",
    " tour ",
    " tours ",
)
WVU_REJECT_PATTERNS = (
    " admission ",
    " closing reception ",
    " concert ",
    " exhibition ",
    " gala ",
    " music ",
    " opening reception ",
    " reception ",
    " screening ",
    " tour ",
)
STIFEL_SCHEDULE_SEGMENT_RE = re.compile(
    r"(?P<weekday>Monday|Mondays|Tuesday|Tuesdays|Wednesday|Wednesdays|Thursday|Thursdays|"
    r"Friday|Fridays|Saturday|Saturdays|Sunday|Sundays)\s*,?\s*"
    r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?\s*(?:-|–|—|to)\s*"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
STIFEL_WEEKS_RE = re.compile(r"(?P<count>\d+)\s+weeks?", re.IGNORECASE)
STIFEL_BEGINS_RE = re.compile(
    r"(?:(?:Mon|Tue|Tues|Wed|Thu|Thur|Thurs|Fri|Sat|Sun)\w*\.?,?\s+)?"
    r"(?P<month>[A-Za-z]+)\.?\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?",
    re.IGNORECASE,
)
STIFEL_AGE_UP_RE = re.compile(r"\bages?\s*:?\s*(?P<age>\d{1,2})\s*(?:\+|&\s*up|and up)\b", re.IGNORECASE)
AGE_UP_TEXT_RE = re.compile(r"\b(?P<age>\d{1,2})\s*(?:\+|&\s*up|and up)\b", re.IGNORECASE)
KINDERGARTEN_GRADE_RE = re.compile(r"kindergarten through fifth grade", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class WvVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    mode: str
    source_prefixes: tuple[str, ...]


WV_VENUES: tuple[WvVenueConfig, ...] = (
    WvVenueConfig(
        slug="wvu",
        source_name="wv_art_museum_of_wvu_events",
        venue_name="Art Museum of WVU",
        city="Morgantown",
        state="WV",
        list_url="https://artmuseum.wvu.edu/events",
        default_location="Art Museum of WVU, Morgantown, WV",
        mode="wvu_localist",
        source_prefixes=("https://cal.wvu.edu/event/",),
    ),
    WvVenueConfig(
        slug="hmoa",
        source_name="wv_huntington_museum_of_art_events",
        venue_name="Huntington Museum of Art",
        city="Huntington",
        state="WV",
        list_url="https://hmoa.org/upcoming/",
        default_location="Huntington Museum of Art, Huntington, WV",
        mode="hmoa_playwright",
        source_prefixes=("https://hmoa.org/2026/", "https://hmoa.org/2027/"),
    ),
    WvVenueConfig(
        slug="stifel",
        source_name="wv_stifel_fine_arts_center_events",
        venue_name="Stifel Fine Arts Center",
        city="Wheeling",
        state="WV",
        list_url="https://oionline.com/classes/art/",
        default_location="Stifel Fine Arts Center, Wheeling, WV",
        mode="stifel_classes",
        source_prefixes=(
            "https://oionline.com/classes/art/",
            "https://oionline.app.neoncrm.com/event",
            "https://oionline.app.neoncrm.com/eventReg.jsp",
        ),
    ),
)

WV_VENUES_BY_SLUG = {venue.slug: venue for venue in WV_VENUES}


class WvBundleAdapter(BaseSourceAdapter):
    source_name = "wv_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_wv_bundle_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_wv_bundle_payload/parse_wv_events from the script runner.")


def get_wv_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in WV_VENUES:
        prefixes.extend(venue.source_prefixes)
    return tuple(dict.fromkeys(prefixes))


async def fetch_html(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    referer: str | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    headers = dict(DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer

    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers)

    last_exception: Exception | None = None
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(url, headers=headers)
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


async def fetch_html_playwright(
    url: str,
    *,
    timeout_ms: int = 120000,
    wait_ms: int = 12000,
) -> str:
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
            timezone_id=WV_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(wait_ms)
        html = await page.content()
        await browser.close()
        return html


async def fetch_pages_playwright(
    urls: list[str],
    *,
    timeout_ms: int = 120000,
    wait_ms: int = 12000,
) -> dict[str, str]:
    if not urls:
        return {}
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    pages: dict[str, str] = {}
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, args=list(PLAYWRIGHT_LAUNCH_ARGS))
        context = await browser.new_context(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            locale="en-US",
            timezone_id=WV_TIMEZONE,
        )
        for url in urls:
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(wait_ms)
            pages[url] = await page.content()
            await page.close()
        await context.close()
        await browser.close()
    return pages


async def _fetch_detail_pages(
    *,
    urls: list[str],
    client: httpx.AsyncClient,
    label: str,
) -> dict[str, str]:
    if not urls:
        return {}

    async def _fetch_one(url: str) -> tuple[str, str | None]:
        try:
            return url, await fetch_html(url, client=client)
        except Exception as exc:
            print(f"[wv-bundle] {label} detail fetch failed url={url}: {exc}")
            return url, None

    pages: dict[str, str] = {}
    for url, html in await asyncio.gather(*(_fetch_one(url) for url in urls)):
        if html:
            pages[url] = html
    return pages


async def load_wv_bundle_payload(
    *,
    venues: list[WvVenueConfig] | None = None,
) -> dict[str, dict]:
    selected = venues or list(WV_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.mode == "wvu_localist":
                    payload_by_slug[venue.slug] = await _load_wvu_payload(venue, client=client)
                elif venue.mode == "hmoa_playwright":
                    payload_by_slug[venue.slug] = await _load_hmoa_payload(venue)
                elif venue.mode == "stifel_classes":
                    payload_by_slug[venue.slug] = {
                        "list_html": await fetch_html(venue.list_url, client=client),
                    }
                else:
                    payload_by_slug[venue.slug] = {}
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)

    return {
        "payload_by_slug": payload_by_slug,
        "errors_by_slug": errors_by_slug,
    }


async def _load_wvu_payload(
    venue: WvVenueConfig,
    *,
    client: httpx.AsyncClient,
) -> dict[str, object]:
    raw_widget = await fetch_html(WVU_WIDGET_URL, client=client, referer=venue.list_url)
    widget_html = _decode_wvu_widget_html(raw_widget)
    soup = BeautifulSoup(widget_html, "html.parser")
    detail_urls = sorted(
        {
            _canonicalize_url(anchor["href"])
            for anchor in soup.select("a[href]")
            if anchor.get("href")
        }
    )
    detail_pages = await _fetch_detail_pages(urls=detail_urls, client=client, label=venue.slug)
    return {
        "widget_html": widget_html,
        "detail_pages": detail_pages,
    }


async def _load_hmoa_payload(venue: WvVenueConfig) -> dict[str, object]:
    list_html = await fetch_html_playwright(venue.list_url)
    soup = BeautifulSoup(list_html, "html.parser")
    detail_urls: list[str] = []
    for card in soup.select(".post-card"):
        heading = card.select_one("h3")
        anchor = card.select_one("a[href]")
        if heading is None or anchor is None or not anchor.get("href"):
            continue
        title = normalize_space(heading.get_text(" ", strip=True))
        if not _should_fetch_hmoa_detail(title):
            continue
        detail_urls.append(_canonicalize_url(anchor["href"]))
    detail_urls = sorted(dict.fromkeys(detail_urls))
    detail_pages = await fetch_pages_playwright(detail_urls)
    return {
        "list_html": list_html,
        "detail_pages": detail_pages,
    }


def parse_wv_events(
    payload: dict,
    *,
    venue: WvVenueConfig,
) -> list[ExtractedActivity]:
    if venue.slug == "wvu":
        return _parse_wvu_events(payload, venue=venue)
    if venue.slug == "hmoa":
        return _parse_hmoa_events(payload, venue=venue)
    if venue.slug == "stifel":
        return _parse_stifel_events(payload, venue=venue)
    return []


def _parse_wvu_events(payload: dict, *, venue: WvVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("widget_html") or "", "html.parser")
    detail_pages = payload.get("detail_pages") or {}
    activities: list[ExtractedActivity] = []
    today = date.today()

    for item in soup.select("li.row"):
        anchor = item.select_one("a[href]")
        if anchor is None:
            continue

        title = normalize_space(anchor.get_text(" ", strip=True))
        source_url = _canonicalize_url(anchor["href"])
        detail_html = detail_pages.get(source_url, "")
        description = _extract_meta_description(detail_html)
        if not _should_include_wvu(title=title, description=description):
            continue

        location_text = _extract_wvu_location_text(item) or venue.default_location
        start_at, end_at = _extract_wvu_datetimes(item=item, detail_html=detail_html, source_url=source_url)
        if start_at is None or start_at.date() < today:
            continue

        body_text = join_non_empty([description, location_text])
        age_min, age_max = _parse_extended_age_range(body_text)
        registration_required = _has_registration_marker(body_text)
        drop_in = _contains_phrase(body_text, "drop in") or _contains_phrase(body_text, "drop-in")
        activity_type = _infer_wv_activity_type(title, description)

        activities.append(
            ExtractedActivity(
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
                drop_in=drop_in or None,
                registration_required=registration_required,
                start_at=start_at,
                end_at=end_at,
                timezone=WV_TIMEZONE,
                **price_classification_kwargs(body_text),
            )
        )

    return activities


def _parse_hmoa_events(payload: dict, *, venue: WvVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    detail_pages = payload.get("detail_pages") or {}
    activities: list[ExtractedActivity] = []
    today = date.today()

    for card in soup.select(".post-card"):
        anchor = card.select_one("a[href]")
        heading = card.select_one("h3")
        if anchor is None or heading is None:
            continue

        title = normalize_space(heading.get_text(" ", strip=True))
        source_url = _canonicalize_url(anchor["href"])
        detail_html = detail_pages.get(source_url, "")
        description = _extract_meta_description(detail_html)
        if description is None and _contains_phrase(title, "saturday kidsart"):
            description = (
                "Saturday KidsArt is an art activity designed for children in "
                "kindergarten through fifth grade."
            )
        detail_text = description or _extract_main_text(detail_html)
        card_text = normalize_space(card.get_text(" ", strip=True))
        combined = join_non_empty([title, description, card_text])
        if not _should_include_hmoa(combined):
            continue

        event_date = _extract_hmoa_date(title=title, card_text=card_text, detail_text=combined)
        if event_date is None or event_date < today:
            continue

        time_text = _extract_hmoa_time(combined)
        start_at, end_at = parse_time_range(base_date=event_date, time_text=time_text)
        age_min, age_max = _parse_extended_age_range(combined)
        registration_required = _has_registration_marker(combined)
        drop_in = _contains_phrase(combined, "drop in") or _contains_phrase(combined, "drop-in")
        activity_type = _infer_wv_activity_type(title, description or detail_text)

        activities.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or detail_text or None,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=activity_type,
                age_min=age_min,
                age_max=age_max,
                drop_in=drop_in or None,
                registration_required=registration_required,
                start_at=start_at,
                end_at=end_at,
                timezone=WV_TIMEZONE,
                **price_classification_kwargs(combined),
            )
        )

    return activities


def _parse_stifel_events(payload: dict, *, venue: WvVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    page_text = soup.get_text(" ", strip=True)
    session_year = _extract_page_year(page_text)
    today = date.today()
    activities: list[ExtractedActivity] = []

    for accordion in soup.select("div.wpb_accordion"):
        summary_block = accordion.find_previous_sibling("div", class_="wpb_text_column")
        if summary_block is None:
            continue

        detail_text = normalize_space(accordion.get_text(" ", strip=True))
        title, description = _extract_stifel_summary(summary_block)
        if not title:
            continue
        if _contains_phrase(title, "sold out") or _contains_phrase(detail_text, "sold out"):
            continue

        register_link = accordion.select_one("a[href*='neoncrm.com/event']")
        source_url = register_link.get("href") if register_link else venue.list_url
        source_url = _canonicalize_url(source_url)

        age_text = _match_group(r"Ages:\s*(?P<value>.*?)(?:Meets:|Begins:|Duration:|Instructor)", detail_text)
        meets_text = _match_group(r"Meets:\s*(?P<value>.*?)(?:Begins:|Duration:|Instructor)", detail_text)
        begins_text = _match_group(r"Begins:\s*(?P<value>.*?)(?:Duration:|Instructor)", detail_text)
        duration_text = _match_group(r"Duration:\s*(?P<value>.*?)(?:Instructor|\$|Register)", detail_text)
        price_text = _match_group(r"(?P<value>\$\d+(?:/\$\d+)?[^R]*?)(?:Register|$)", detail_text)

        start_date = _parse_stifel_start_date(begins_text or "", session_year=session_year)
        schedule_segments = _parse_stifel_schedule_segments(meets_text or "")
        duration_weeks = _parse_duration_weeks(duration_text)
        occurrence_date, occurrence_time = _pick_stifel_occurrence(
            start_date=start_date,
            schedule_segments=schedule_segments,
            duration_weeks=duration_weeks,
            today=today,
        )
        if occurrence_date is None:
            continue

        start_at, end_at = parse_time_range(base_date=occurrence_date, time_text=occurrence_time)
        if end_at is None and start_at.date() < today:
            continue

        body_text = join_non_empty([description, detail_text, price_text])
        age_min, age_max = _parse_extended_age_range(age_text or body_text)
        activity_type = "workshop" if _contains_phrase(title, "workshop") else "class"

        activities.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=activity_type,
                age_min=age_min,
                age_max=age_max,
                drop_in=None,
                registration_required=bool(register_link),
                start_at=start_at,
                end_at=end_at,
                timezone=WV_TIMEZONE,
                **price_classification_kwargs(price_text or body_text),
            )
        )

    return activities


def _decode_wvu_widget_html(raw_widget: str) -> str:
    match = re.search(r'innerHTML="(?P<html>.*)";\s*}\s*catch', raw_widget, re.DOTALL)
    if match is None:
        raise RuntimeError("Unable to decode WVU Localist widget HTML.")
    return json.loads(f'"{match.group("html")}"')


def _canonicalize_url(url: str) -> str:
    parsed = urlsplit(url)
    query = ""
    if "neoncrm.com" in parsed.netloc and "event=" in parsed.query:
        query = "&".join(
            part
            for part in parsed.query.split("&")
            if part.startswith("event=")
        )
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _extract_meta_description(html: str) -> str | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    candidates = [
        soup.find("meta", attrs={"property": "og:description"}),
        soup.find("meta", attrs={"name": "description"}),
    ]
    for meta in candidates:
        if meta is None:
            continue
        content = normalize_space(unescape(meta.get("content") or ""))
        if "performing security verification" in content.lower():
            continue
        if content:
            content = re.sub(
                r",?\s*powered by Localist.*$",
                "",
                content,
                flags=re.IGNORECASE,
            ).strip()
            return re.sub(r"\s*\[\u2026\]\s*$", "", content).strip()
    return _extract_main_text(html)


def _extract_main_text(html: str) -> str | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    main = (
        soup.select_one(".entry-content")
        or soup.select_one("article")
        or soup.select_one("main")
        or soup.body
    )
    if main is None:
        return None
    return normalize_space(main.get_text(" ", strip=True)) or None


def _extract_wvu_location_text(item: BeautifulSoup) -> str | None:
    info = item.select_one("p.small")
    if info is None:
        return None
    raw = normalize_space(info.get_text("\n", strip=True))
    parts = [part for part in [normalize_space(line) for line in raw.split("\n")] if part]
    if len(parts) >= 2:
        return parts[1]
    return None


def _extract_wvu_datetimes(
    *,
    item: BeautifulSoup,
    detail_html: str,
    source_url: str,
) -> tuple[datetime | None, datetime | None]:
    start_match = LOCALIST_START_RE.search(detail_html or "")
    if start_match:
        start_at = parse_iso_datetime(start_match.group("value"), timezone_name=WV_TIMEZONE)
        end_match = LOCALIST_END_RE.search(detail_html or "")
        end_value = end_match.group("value") if end_match else None
        end_at = parse_iso_datetime(end_value, timezone_name=WV_TIMEZONE) if end_value and "T" in end_value else None
        return start_at, end_at

    month_text = normalize_space(item.select_one("p").get_text(" ", strip=True) if item.select_one("p") else "")
    day_text = normalize_space(
        item.select_one("div[id^='aria__date--']").get_text(" ", strip=True)
        if item.select_one("div[id^='aria__date--']")
        else ""
    )
    info = normalize_space(item.select_one("p.small").get_text(" ", strip=True) if item.select_one("p.small") else "")
    time_match = re.search(r"\b\d{1,2}:\d{2}\s*(?:AM|PM)\b", info, re.IGNORECASE)
    if not month_text or not day_text:
        return None, None
    year = datetime.now().year
    fallback_date = parse_date_text(f"{month_text} {day_text}, {year}")
    if fallback_date is None:
        print(f"[wv-bundle] unable to derive WVU date for {source_url}")
        return None, None
    return parse_time_range(base_date=fallback_date, time_text=time_match.group(0) if time_match else None)


def _extract_hmoa_date(*, title: str, card_text: str, detail_text: str | None) -> date | None:
    for candidate in (title, card_text, detail_text or ""):
        title_match = TITLE_DATE_RE.search(candidate)
        if title_match:
            parsed = parse_date_text(title_match.group("date"))
            if parsed is not None:
                return parsed

        suffix_match = DATE_WITH_SUFFIX_RE.search(candidate)
        if suffix_match:
            parsed = parse_date_text(_clean_date_suffixes(suffix_match.group("date")))
            if parsed is not None:
                return parsed
    return None


def _extract_hmoa_time(text: str | None) -> str | None:
    blob = normalize_space(text)
    if not blob:
        return None
    range_match = TIME_RANGE_WITH_FROM_RE.search(blob)
    if range_match:
        return range_match.group("time")
    single_match = TIME_SINGLE_WITH_AT_RE.search(blob)
    if single_match:
        return single_match.group("time")
    if "saturday kidsart" in blob.lower():
        return "1 to 3 p.m."
    return None


def _should_include_wvu(*, title: str, description: str | None) -> bool:
    blob = f" {normalize_space(join_non_empty([title, description]) or '').lower()} "
    if " lunchtime looks " in blob or " with dr. " in blob:
        return True
    return not any(pattern in blob for pattern in WVU_REJECT_PATTERNS)


def _should_include_hmoa(text: str | None) -> bool:
    blob = f" {normalize_space(text).lower()} "
    include_hit = any(pattern in blob for pattern in HMOA_INCLUDE_PATTERNS)
    reject_hit = any(pattern in blob for pattern in HMOA_REJECT_PATTERNS)
    return include_hit and not reject_hit


def _should_fetch_hmoa_detail(title: str) -> bool:
    blob = f" {normalize_space(title).lower()} "
    if any(pattern in blob for pattern in HMOA_REJECT_PATTERNS):
        return False
    return any(pattern in blob for pattern in HMOA_INCLUDE_PATTERNS)


def _infer_wv_activity_type(title: str, description: str | None) -> str:
    blob = f" {normalize_space(join_non_empty([title, description]) or '').lower()} "
    if any(token in blob for token in (" kidsart ", " art activity ", " screenprinting ", " wire wrapping ", " craft ")):
        return "workshop"
    if " once upon a time " in blob:
        return "activity"
    return infer_activity_type(title, description)


def _parse_extended_age_range(text: str | None) -> tuple[int | None, int | None]:
    age_min, age_max = parse_age_range(text)
    if age_min is not None or age_max is not None:
        return age_min, age_max

    blob = normalize_space(text).lower()
    if not blob:
        return None, None

    up_match = STIFEL_AGE_UP_RE.search(blob)
    if up_match:
        return int(up_match.group("age")), None
    generic_up_match = AGE_UP_TEXT_RE.search(blob)
    if generic_up_match:
        return int(generic_up_match.group("age")), None

    if KINDERGARTEN_GRADE_RE.search(blob):
        return 5, 11

    return None, None


def _has_registration_marker(text: str | None) -> bool | None:
    blob = f" {normalize_space(text).lower()} "
    if not blob.strip():
        return None
    if any(token in blob for token in (" register ", " registration ", " reserve ", " ticket ", " tickets ", " rsvp ")):
        return True
    return None


def _contains_phrase(text: str | None, needle: str) -> bool:
    blob = normalize_space(text).lower()
    return needle.lower() in blob


def _extract_page_year(page_text: str) -> int:
    match = re.search(r"\b20\d{2}\b", page_text)
    return int(match.group(0)) if match else datetime.now().year


def _extract_stifel_summary(summary_block: BeautifulSoup) -> tuple[str | None, str | None]:
    title_node = summary_block.select_one("strong") or summary_block.select_one("span")
    title = normalize_space(title_node.get_text(" ", strip=True) if title_node else "")
    full_text = normalize_space(summary_block.get_text(" ", strip=True))
    if not full_text:
        return None, None
    if title and full_text.startswith(title):
        description = normalize_space(full_text[len(title) :])
    else:
        description = full_text
    return title or None, description or None


def _match_group(pattern: str, text: str) -> str | None:
    match = re.search(pattern, text, re.IGNORECASE)
    if match is None:
        return None
    return normalize_space(match.group("value"))


def _parse_stifel_start_date(text: str, *, session_year: int) -> date | None:
    match = STIFEL_BEGINS_RE.search(text)
    if match is None:
        return None
    month_key = match.group("month").lower().rstrip(".")
    month = MONTH_ABBREVIATIONS.get(month_key, match.group("month").title())
    return parse_date_text(f"{month} {match.group('day')}, {session_year}")


def _parse_duration_weeks(text: str | None) -> int | None:
    if text is None:
        return None
    if "year round" in text.lower():
        return None
    match = STIFEL_WEEKS_RE.search(text)
    return int(match.group("count")) if match else None


def _parse_stifel_schedule_segments(text: str) -> list[tuple[int, str]]:
    segments: list[tuple[int, str]] = []
    for match in STIFEL_SCHEDULE_SEGMENT_RE.finditer(text):
        weekday = WEEKDAY_NAMES.get(match.group("weekday").lower())
        if weekday is None:
            continue
        segments.append((weekday, normalize_space(match.group("time"))))
    return segments


def _pick_stifel_occurrence(
    *,
    start_date: date | None,
    schedule_segments: list[tuple[int, str]],
    duration_weeks: int | None,
    today: date,
) -> tuple[date | None, str | None]:
    if start_date is None:
        return None, None

    if not schedule_segments:
        return start_date, None

    last_date = start_date + timedelta(weeks=max(duration_weeks - 1, 0)) if duration_weeks else None
    candidates: list[tuple[date, str]] = []
    reference = max(today, start_date)

    for weekday, time_text in schedule_segments:
        candidate = reference + timedelta(days=(weekday - reference.weekday()) % 7)
        if candidate < start_date:
            candidate += timedelta(days=7)
        if last_date is not None and candidate > last_date:
            continue
        candidates.append((candidate, time_text))

    if not candidates:
        fallback_date = start_date
        fallback_time = schedule_segments[0][1]
        if last_date is not None and fallback_date > last_date:
            return None, None
        return fallback_date, fallback_time

    return min(candidates, key=lambda item: item[0])


def _clean_date_suffixes(text: str) -> str:
    return re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", normalize_space(text), flags=re.IGNORECASE)
