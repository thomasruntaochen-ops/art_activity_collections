from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from datetime import timezone
from urllib.parse import parse_qs
from urllib.parse import urljoin
from urllib.parse import urlsplit

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import clean_html_fragment
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

MT_TIMEZONE = "America/Denver"
MT_ZONEINFO = ZoneInfo(MT_TIMEZONE)

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

HOLTER_DISCOVERY_URLS = (
    "https://holtermuseum.org/programs/creative-kids/",
    "https://holtermuseum.org/programs/teen/",
)
HOLTER_SKIP_SLUGS = {"scholarship-application"}

MONTH_TOKEN_RE = re.compile(
    r"(?P<month>Jan(?:uary)?\.?|Feb(?:ruary)?\.?|Mar(?:ch)?\.?|Apr(?:il)?\.?|May|"
    r"Jun(?:e)?\.?|Jul(?:y)?\.?|Aug(?:ust)?\.?|Sep(?:t(?:ember)?)?\.?|Oct(?:ober)?\.?|"
    r"Nov(?:ember)?\.?|Dec(?:ember)?\.?)\s+"
    r"(?P<day>\d{1,2})(?:st|nd|rd|th)?",
    re.IGNORECASE,
)
HOLTER_RANGE_RE = re.compile(
    r"(?P<start_month>Jan(?:uary)?\.?|Feb(?:ruary)?\.?|Mar(?:ch)?\.?|Apr(?:il)?\.?|May|"
    r"Jun(?:e)?\.?|Jul(?:y)?\.?|Aug(?:ust)?\.?|Sep(?:t(?:ember)?)?\.?|Oct(?:ober)?\.?|"
    r"Nov(?:ember)?\.?|Dec(?:ember)?\.?)\s+"
    r"(?P<start_day>\d{1,2})(?:st|nd|rd|th)?\s*[-–—]\s*"
    r"(?:(?P<end_month>Jan(?:uary)?\.?|Feb(?:ruary)?\.?|Mar(?:ch)?\.?|Apr(?:il)?\.?|May|"
    r"Jun(?:e)?\.?|Jul(?:y)?\.?|Aug(?:ust)?\.?|Sep(?:t(?:ember)?)?\.?|Oct(?:ober)?\.?|"
    r"Nov(?:ember)?\.?|Dec(?:ember)?\.?)\s+)?"
    r"(?P<end_day>\d{1,2})(?:st|nd|rd|th)?",
    re.IGNORECASE,
)
HOLTER_WEEKDAY_RE = re.compile(
    r"\b(?P<weekday>Monday|Mondays|Tuesday|Tuesdays|Wednesday|Wednesdays|Thursday|Thursdays|"
    r"Friday|Fridays|Saturday|Saturdays|Sunday|Sundays)\b",
    re.IGNORECASE,
)
TIME_RANGE_TEXT_RE = re.compile(
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?"
    r"\s*(?:-|–|—|to)\s*"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
GOOGLE_DATES_RE = re.compile(r"^(?P<start>[^/]+)/(?P<end>.+)$")

MONTH_NAME_TO_NUMBER = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
WEEKDAY_NAME_TO_INDEX = {
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

INCLUDE_MARKERS = (
    " art activity ",
    " art adventure ",
    " art in the moment ",
    " art project ",
    " artspark ",
    " artist talk ",
    " class ",
    " classes ",
    " drawing ",
    " figure drawing ",
    " open studio ",
    " paint ",
    " painting ",
    " presentation ",
    " printmaking ",
    " studio ",
    " symposium ",
    " talk ",
    " talks ",
    " teen council ",
    " visual journaling ",
    " workshop ",
    " workshops ",
)
EXCLUDE_MARKERS = (
    " admission ",
    " celebration ",
    " exhibit ",
    " exhibition ",
    " free museum day ",
    " fundraiser ",
    " gala ",
    " give big ",
    " museum closed ",
    " open house ",
    " reception ",
    " scavenger hunt ",
    " slow art day ",
    " write along ",
    " write-along ",
    " write-alongs ",
    " writing ",
    " yoga ",
)
EXCLUDE_OVERRIDE_MARKERS = (
    " artist talk ",
    " guest lecture ",
    " lecture ",
    " presentation ",
    " talk ",
    " workshop ",
)
SOLD_OUT_RE = re.compile(r"\b(?:sold out|waitlist)\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class MtVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    mode: str
    source_prefixes: tuple[str, ...]


MT_VENUES: tuple[MtVenueConfig, ...] = (
    MtVenueConfig(
        slug="bozeman",
        source_name="mt_bozeman_art_museum_events",
        venue_name="Bozeman Art Museum",
        city="Bozeman",
        state="MT",
        list_url="https://bozemanartmuseum.org/events/",
        default_location="Bozeman Art Museum, Bozeman, MT",
        mode="bozeman_playwright",
        source_prefixes=(
            "https://bozemanartmuseum.org/calendar/events-calendar/",
            "https://bozemanartmuseum.org/",
        ),
    ),
    MtVenueConfig(
        slug="holter",
        source_name="mt_holter_museum_of_art_events",
        venue_name="Holter Museum of Art",
        city="Helena",
        state="MT",
        list_url="https://holtermuseum.org/events/",
        default_location="Holter Museum of Art, Helena, MT",
        mode="holter_programs",
        source_prefixes=(
            "https://holtermuseum.org/education/",
            "https://holtermuseum.org/programs/creative-kids/",
            "https://holtermuseum.org/programs/teen/",
        ),
    ),
    MtVenueConfig(
        slug="missoula",
        source_name="mt_missoula_art_museum_events",
        venue_name="Missoula Art Museum",
        city="Missoula",
        state="MT",
        list_url="https://www.missoulaartmuseum.org/events",
        default_location="Missoula Art Museum, Missoula, MT",
        mode="squarespace_events",
        source_prefixes=("https://www.missoulaartmuseum.org/events/",),
    ),
    MtVenueConfig(
        slug="paris_gibson",
        source_name="mt_paris_gibson_square_museum_of_art_events",
        venue_name="Paris Gibson Square Museum of Art",
        city="Great Falls",
        state="MT",
        list_url="https://www.the-square.org/events",
        default_location="Paris Gibson Square Museum of Art, Great Falls, MT",
        mode="squarespace_events",
        source_prefixes=("https://www.the-square.org/events/",),
    ),
    MtVenueConfig(
        slug="yellowstone",
        source_name="mt_yellowstone_art_museum_events",
        venue_name="Yellowstone Art Museum",
        city="Billings",
        state="MT",
        list_url="https://www.artmuseum.org/events-calendar/",
        default_location="Yellowstone Art Museum, Billings, MT",
        mode="yellowstone_schema",
        source_prefixes=("https://www.artmuseum.org/events/",),
    ),
)

MT_VENUES_BY_SLUG = {venue.slug: venue for venue in MT_VENUES}


class MtBundleAdapter(BaseSourceAdapter):
    source_name = "mt_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_mt_bundle_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_mt_bundle_payload/parse_mt_events from the script runner.")


def get_mt_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in MT_VENUES:
        prefixes.extend(venue.source_prefixes)
    return tuple(dict.fromkeys(prefixes))


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


async def fetch_html_playwright(
    url: str,
    *,
    timeout_ms: int = 90000,
    wait_ms: int = 6000,
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
            timezone_id=MT_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(wait_ms)
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
                print(f"[mt-bundle] {label} detail fetch failed url={url}: {exc}")
                return url, None

    pages: dict[str, str] = {}
    for url, html in await asyncio.gather(*(_fetch_one(url) for url in sorted(urls))):
        if html:
            pages[url] = html
    return pages


async def load_mt_bundle_payload(
    *,
    venues: list[MtVenueConfig] | None = None,
) -> dict[str, dict]:
    selected = venues or list(MT_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.mode == "squarespace_events":
                    payload_by_slug[venue.slug] = {"list_html": await fetch_html(venue.list_url, client=client)}
                elif venue.mode == "yellowstone_schema":
                    payload_by_slug[venue.slug] = {"list_html": await fetch_html(venue.list_url, client=client)}
                elif venue.mode == "holter_programs":
                    payload_by_slug[venue.slug] = await _load_holter_payload(client=client)
                elif venue.mode == "bozeman_playwright":
                    payload_by_slug[venue.slug] = {"list_html": await _load_bozeman_html(venue=venue, client=client)}
                else:
                    payload_by_slug[venue.slug] = {}
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)

    return {
        "payload_by_slug": payload_by_slug,
        "errors_by_slug": errors_by_slug,
    }


def parse_mt_events(payload: dict, *, venue: MtVenueConfig) -> list[ExtractedActivity]:
    if venue.mode == "squarespace_events":
        rows = _parse_squarespace_events(payload, venue=venue)
    elif venue.mode == "yellowstone_schema":
        rows = _parse_yellowstone_events(payload, venue=venue)
    elif venue.mode == "holter_programs":
        rows = _parse_holter_events(payload, venue=venue)
    elif venue.mode == "bozeman_playwright":
        rows = _parse_bozeman_events(payload, venue=venue)
    else:
        rows = []

    today = datetime.now(MT_ZONEINFO).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < today:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


async def _load_bozeman_html(*, venue: MtVenueConfig, client: httpx.AsyncClient) -> str:
    html = await fetch_html(venue.list_url, client=client)
    if _looks_like_bot_block(html) or "simcal-event" not in html:
        html = await fetch_html_playwright("https://bozemanartmuseum.org/calendar/events-calendar/")
    if "simcal-event" not in html:
        raise RuntimeError("Bozeman calendar did not expose Simple Calendar event rows.")
    return html


async def _load_holter_payload(*, client: httpx.AsyncClient) -> dict:
    list_pages: dict[str, str] = {}
    detail_urls: set[str] = set()

    for url in HOLTER_DISCOVERY_URLS:
        html = await fetch_html(url, client=client)
        list_pages[url] = html
        detail_urls.update(_extract_holter_detail_urls(html, base_url=url))

    detail_pages = await _fetch_detail_pages(
        urls=detail_urls,
        client=client,
        label="holter",
    )
    return {
        "list_pages": list_pages,
        "detail_pages": detail_pages,
    }


def _parse_squarespace_events(payload: dict, *, venue: MtVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("list_html") or ""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []

    for item in soup.select(".eventlist-event"):
        title = normalize_space(_text_from_node(item.select_one(".eventlist-title-link")))
        if not title or SOLD_OUT_RE.search(title):
            continue

        detail_url = urljoin(venue.list_url, item.select_one(".eventlist-title-link").get("href", ""))
        description = clean_html_fragment(_html_from_node(item.select_one(".eventlist-description, .eventlist-excerpt")))
        categories = [
            normalize_space(link.get_text(" ", strip=True))
            for link in item.select(".eventlist-cats a")
            if normalize_space(link.get_text(" ", strip=True))
        ]
        if not _should_include_event(title=title, description=description, categories=categories):
            continue

        start_at, end_at = _parse_squarespace_datetimes(item)
        if start_at is None:
            continue

        text_blob = join_non_empty([title, description, " | ".join(categories)])
        age_min, age_max = parse_age_range(text_blob)
        price_kwargs = price_classification_kwargs(text_blob)
        location_text = normalize_space(_text_from_node(item.select_one(".eventlist-meta-address"))) or venue.default_location
        location_text = re.sub(r"\s*\(map\)$", "", location_text, flags=re.IGNORECASE)

        rows.append(
            ExtractedActivity(
                source_url=detail_url,
                title=title,
                description=join_non_empty(
                    [
                        description,
                        f"Categories: {', '.join(categories)}" if categories else None,
                    ]
                ),
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description),
                age_min=age_min,
                age_max=age_max,
                drop_in=_blob_contains(text_blob, "drop-in", "drop in"),
                registration_required=_blob_contains(text_blob, "register", "registration"),
                start_at=start_at,
                end_at=end_at,
                timezone=MT_TIMEZONE,
                **price_kwargs,
            )
        )

    return rows


def _parse_yellowstone_events(payload: dict, *, venue: MtVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("list_html") or ""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []

    for item in soup.select('[itemtype="http://schema.org/Event"], [itemtype="https://schema.org/Event"]'):
        title = normalize_space(_content_or_text(item.select_one('[itemprop="name"]')))
        if not title:
            continue

        description = normalize_space(_content_or_text(item.select_one('[itemprop="description"]')))
        if not _should_include_event(title=title, description=description, categories=()):
            continue

        start_value = normalize_space(item.select_one('[itemprop="startDate"]').get("content", ""))
        if not start_value:
            continue
        end_value = normalize_space(_node_attr(item.select_one('[itemprop="endDate"]'), "content"))
        start_at = parse_iso_datetime(start_value, timezone_name=MT_TIMEZONE)
        end_at = parse_iso_datetime(end_value, timezone_name=MT_TIMEZONE) if end_value else None

        source_url = normalize_space(_node_attr(item.select_one('[itemprop="url"]'), "content")) or venue.list_url
        location_text = (
            normalize_space(_node_attr(item.select_one('[itemprop="location"] [itemprop="name"]'), "content"))
            or venue.default_location
        )
        text_blob = join_non_empty([title, description])
        age_min, age_max = parse_age_range(text_blob)
        price_kwargs = price_classification_kwargs(text_blob)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description),
                age_min=age_min,
                age_max=age_max,
                drop_in=_blob_contains(text_blob, "drop-in", "drop in"),
                registration_required=_blob_contains(text_blob, "register", "registration", "apply"),
                start_at=start_at,
                end_at=end_at,
                timezone=MT_TIMEZONE,
                **price_kwargs,
            )
        )

    return rows


def _parse_bozeman_events(payload: dict, *, venue: MtVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("list_html") or ""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []

    for item in soup.select(".simcal-event"):
        title = normalize_space(
            _text_from_node(item.select_one('.simcal-event-title[itemprop="name"]'))
            or _text_from_node(item.select_one(".simcal-event-title"))
        )
        if not title:
            continue

        description = clean_html_fragment(_html_from_node(item.select_one(".simcal-event-description")))
        if not _should_include_event(title=title, description=description, categories=()):
            continue

        start_value = normalize_space(_node_attr(item.select_one('[itemprop="startDate"]'), "content"))
        if not start_value:
            continue
        end_value = normalize_space(_node_attr(item.select_one('[itemprop="endDate"]'), "content"))
        start_at = parse_iso_datetime(start_value, timezone_name=MT_TIMEZONE)
        end_at = parse_iso_datetime(end_value, timezone_name=MT_TIMEZONE) if end_value else None
        detail_link = item.select_one(".simcal-event-description a[href]")
        source_url = urljoin(venue.list_url, detail_link.get("href", "")) if detail_link else venue.list_url
        location_text = normalize_space(_text_from_node(item.select_one(".simcal-event-address"))) or venue.default_location
        text_blob = join_non_empty([title, description])
        age_min, age_max = parse_age_range(text_blob)
        price_kwargs = price_classification_kwargs(text_blob)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description),
                age_min=age_min,
                age_max=age_max,
                drop_in=_blob_contains(text_blob, "drop-in", "drop in"),
                registration_required=_blob_contains(text_blob, "register", "registration"),
                start_at=start_at,
                end_at=end_at,
                timezone=MT_TIMEZONE,
                **price_kwargs,
            )
        )

    return rows


def _parse_holter_events(payload: dict, *, venue: MtVenueConfig) -> list[ExtractedActivity]:
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []
    today = datetime.now(MT_ZONEINFO).date()

    for detail_url, html in sorted(detail_pages.items()):
        lines = _extract_holter_intro_lines(html)
        if not lines:
            continue

        title = lines[0]
        description = _build_holter_description(lines[1:])
        if not _should_include_event(title=title, description=description, categories=()):
            continue

        occurrence_dates = _extract_holter_occurrence_dates(lines, reference_date=today)
        if not occurrence_dates:
            continue

        time_text = _extract_holter_time_text(lines)
        text_blob = join_non_empty([title, " | ".join(lines[1:])])
        age_min, age_max = parse_age_range(text_blob)
        price_kwargs = price_classification_kwargs(text_blob)

        for occurrence_date in occurrence_dates:
            start_at, end_at = _apply_time_text(occurrence_date, time_text=time_text)
            rows.append(
                ExtractedActivity(
                    source_url=detail_url,
                    title=title,
                    description=description,
                    venue_name=venue.venue_name,
                    location_text=venue.default_location,
                    city=venue.city,
                    state=venue.state,
                    activity_type=_infer_activity_type(title=title, description=description),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=_blob_contains(text_blob, "drop-in", "drop in"),
                    registration_required=_blob_contains(
                        text_blob,
                        "register",
                        "registration",
                        "preregistration",
                    ),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=MT_TIMEZONE,
                    **price_kwargs,
                )
            )

    return rows


def _parse_squarespace_datetimes(item) -> tuple[datetime | None, datetime | None]:
    google_link = item.select_one(".eventlist-meta-export-google[href]")
    if google_link is not None:
        parsed = _parse_google_calendar_dates(google_link.get("href", ""))
        if parsed is not None:
            return parsed

    date_nodes = item.select(".eventlist-meta-date .event-date[datetime]")
    if not date_nodes:
        return None, None

    start_date = datetime.fromisoformat(date_nodes[0].get("datetime")).date()
    time_text = normalize_space(_text_from_node(item.select_one(".eventlist-meta-time")))
    start_at, end_at = parse_time_range(base_date=start_date, time_text=time_text)

    if len(date_nodes) > 1:
        end_date = datetime.fromisoformat(date_nodes[-1].get("datetime")).date()
        end_time_text = normalize_space(_text_from_node(item.select(".eventlist-meta-time")[-1]))
        _, end_at = parse_time_range(base_date=end_date, time_text=end_time_text)

    return start_at, end_at


def _parse_google_calendar_dates(url: str) -> tuple[datetime | None, datetime | None] | None:
    query = parse_qs(urlsplit(url).query)
    dates_value = normalize_space((query.get("dates") or [""])[0])
    if not dates_value:
        return None

    match = GOOGLE_DATES_RE.match(dates_value)
    if not match:
        return None

    return (
        _parse_google_calendar_value(match.group("start")),
        _parse_google_calendar_value(match.group("end")),
    )


def _parse_google_calendar_value(value: str) -> datetime | None:
    value = normalize_space(value)
    if not value:
        return None
    if value.endswith("Z"):
        return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    if "T" in value:
        return datetime.strptime(value, "%Y%m%dT%H%M%S")
    return datetime.strptime(value, "%Y%m%d")


def _extract_holter_detail_urls(html: str, *, base_url: str) -> set[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: set[str] = set()
    for link in soup.select('a[href*="/education/"]'):
        href = normalize_space(link.get("href"))
        if not href:
            continue
        full_url = urljoin(base_url, href)
        slug = full_url.rstrip("/").rsplit("/", 1)[-1]
        if slug in HOLTER_SKIP_SLUGS:
            continue
        urls.add(full_url)
    return urls


def _extract_holter_intro_lines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    title = normalize_space(_text_from_node(soup.select_one("h1")))
    if not title:
        return []

    article = soup.select_one("article") or soup
    all_lines = [normalize_space(text) for text in article.stripped_strings if normalize_space(text)]
    start_idx = 0
    for index, line in enumerate(all_lines):
        if line == title:
            start_idx = index
            break

    out: list[str] = []
    for line in all_lines[start_idx:]:
        if line in {"Register Now", "Instagram"}:
            break
        out.append(line)

    return out


def _build_holter_description(lines: list[str]) -> str | None:
    cleaned: list[str] = []
    for line in lines:
        if not line or line in {"Price:", "Members:"}:
            continue
        if line.startswith("$"):
            continue
        cleaned.append(line)
    return join_non_empty([" ".join(cleaned)]) if cleaned else None


def _extract_holter_occurrence_dates(lines: list[str], *, reference_date: date) -> list[date]:
    joined = " | ".join(lines)
    range_match = HOLTER_RANGE_RE.search(joined)
    if range_match:
        start_date = _build_month_day_date(
            range_match.group("start_month"),
            range_match.group("start_day"),
            year=reference_date.year,
        )
        end_date = _build_month_day_date(
            range_match.group("end_month") or range_match.group("start_month"),
            range_match.group("end_day"),
            year=reference_date.year,
        )
        if start_date is None or end_date is None or end_date < start_date:
            return []

        weekday = _extract_holter_weekday(lines)
        exceptions = _extract_holter_exception_dates(joined, year=reference_date.year)
        if weekday is not None:
            current = start_date + timedelta(days=(weekday - start_date.weekday()) % 7)
            values: list[date] = []
            while current <= end_date:
                if current >= reference_date and current not in exceptions:
                    values.append(current)
                current += timedelta(days=7)
            return values

        return [start_date] if start_date >= reference_date else []

    explicit_dates: list[date] = []
    for line in lines[1:]:
        if HOLTER_RANGE_RE.search(line):
            continue
        if "There is no class on" in line:
            continue
        explicit_dates.extend(_extract_month_day_dates(line, year=reference_date.year))

    unique_dates = sorted({value for value in explicit_dates if value >= reference_date})
    return unique_dates


def _extract_holter_weekday(lines: list[str]) -> int | None:
    for line in lines:
        match = HOLTER_WEEKDAY_RE.search(line)
        if not match:
            continue
        return WEEKDAY_NAME_TO_INDEX.get(match.group("weekday").lower())
    return None


def _extract_holter_exception_dates(text: str, *, year: int) -> set[date]:
    if "There is no class on" not in text:
        return set()
    exceptions: set[date] = set()
    for chunk in re.findall(r"There is no class on([^|]+)", text, flags=re.IGNORECASE):
        exceptions.update(_extract_month_day_dates(chunk, year=year))
    return exceptions


def _extract_holter_time_text(lines: list[str]) -> str | None:
    for line in lines:
        match = TIME_RANGE_TEXT_RE.search(line)
        if match:
            return match.group(0)
    return None


def _apply_time_text(activity_date: date, *, time_text: str | None) -> tuple[datetime, datetime | None]:
    start_at, end_at = parse_time_range(base_date=activity_date, time_text=time_text)
    if start_at is None:
        return datetime.combine(activity_date, time.min), None
    return start_at, end_at


def _extract_month_day_dates(text: str, *, year: int) -> list[date]:
    values: list[date] = []
    for match in MONTH_TOKEN_RE.finditer(text):
        built = _build_month_day_date(match.group("month"), match.group("day"), year=year)
        if built is not None:
            values.append(built)
    return values


def _build_month_day_date(month_text: str | None, day_text: str | None, *, year: int) -> date | None:
    if not month_text or not day_text:
        return None
    month_key = normalize_space(month_text).lower().rstrip(".")
    month_number = MONTH_NAME_TO_NUMBER.get(month_key)
    if month_number is None:
        return None
    try:
        return date(year, month_number, int(day_text))
    except ValueError:
        return None


def _should_include_event(*, title: str, description: str | None, categories: list[str] | tuple[str, ...]) -> bool:
    title_blob = f" {normalize_space(title).lower()} "
    description_blob = f" {normalize_space(description).lower()} "
    categories_blob = f" {' '.join(normalize_space(value).lower() for value in categories)} "
    blob = " ".join([title_blob, description_blob, categories_blob])

    if "(copy)" in title_blob:
        return False
    if " camp " in title_blob or " camps " in title_blob:
        return False
    if " camp " in categories_blob or " camps " in categories_blob:
        return False
    if title_blob.startswith(" last day ") or " museum closed " in title_blob:
        return False

    has_include = any(marker in blob for marker in INCLUDE_MARKERS)
    has_exclude = any(marker in blob for marker in EXCLUDE_MARKERS)
    has_exclude_override = any(marker in blob for marker in EXCLUDE_OVERRIDE_MARKERS)

    if " reception " in blob and not has_exclude_override:
        return False
    if " exhibition " in blob and not has_include:
        return False
    if " exhibit " in blob and not has_include:
        return False
    if has_exclude and not has_exclude_override:
        return False
    if " dungeons and dragons " in blob or " games " in blob:
        return False

    return has_include


def _infer_activity_type(*, title: str, description: str | None) -> str | None:
    blob = f" {normalize_space(title).lower()} {normalize_space(description).lower()} "
    if any(marker in blob for marker in (" talk ", " lecture ", " presentation ")):
        return "talk"
    if " workshop " in blob or " workshops " in blob:
        return "workshop"
    if " class " in blob or " classes " in blob:
        return "class"
    if " studio " in blob:
        return "studio"
    if any(marker in blob for marker in (" drawing ", " painting ", " printmaking ", " symposium ")):
        return "workshop"
    if " activity " in blob:
        return "activity"
    return None


def _looks_like_bot_block(html: str) -> bool:
    blob = html.lower()
    return "cloudflare" in blob and (
        "attention required!" in blob
        or "sorry, you have been blocked" in blob
        or "unable to access wpdns.site" in blob
    )


def _text_from_node(node) -> str:
    if node is None:
        return ""
    return normalize_space(node.get_text(" ", strip=True))


def _html_from_node(node) -> str:
    if node is None:
        return ""
    return str(node)


def _node_attr(node, name: str) -> str:
    if node is None:
        return ""
    return normalize_space(node.get(name, ""))


def _content_or_text(node) -> str:
    if node is None:
        return ""
    return normalize_space(node.get("content") or node.get_text(" ", strip=True))


def _blob_contains(blob: str | None, *needles: str) -> bool | None:
    normalized = f" {normalize_space(blob).lower()} "
    if not normalized.strip():
        return None
    return any(f" {needle.lower()} " in normalized for needle in needles)
