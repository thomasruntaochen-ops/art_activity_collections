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
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

NV_TIMEZONE = "America/Los_Angeles"
NV_ZONEINFO = ZoneInfo(NV_TIMEZONE)

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

DATE_WITH_YEAR_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\.?\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})",
    re.IGNORECASE,
)
TIME_RANGE_SPLIT_RE = re.compile(r"\s*(?:-|–|—|to)\s*", re.IGNORECASE)
TIME_TOKEN_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?|am|pm)?",
    re.IGNORECASE,
)
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

BARRICK_INCLUDE_PATTERNS = (
    " workshop ",
    " class ",
    " classes ",
    " activity ",
    " activities ",
    " lab ",
    " labs ",
    " talk ",
    " talks ",
    " lecture ",
    " lectures ",
    " conversation ",
    " conversations ",
)
BARRICK_ART_CONTEXT_PATTERNS = (
    " art ",
    " artist ",
    " artists ",
    " museum ",
    " gallery ",
    " galleries ",
    " visual ",
    " creative ",
    " print ",
    " sculpture ",
    " painting ",
    " drawing ",
    " slow art ",
    " sunprint ",
)
BARRICK_REJECT_PATTERNS = (
    " exhibition ",
    " exhibitions ",
    " fundraiser ",
    " gala ",
    " music ",
    " performance ",
    " performances ",
    " reception ",
    " screening ",
    " film ",
    " films ",
    " tour ",
    " tours ",
)

NEVADA_ART_SPECIAL_INCLUDE_PATTERNS = (
    " hands on second saturday ",
    " hands-on art activities ",
    " early childhood education encounters ",
)
NEVADA_ART_INCLUDE_PATTERNS = (
    " activity ",
    " activities ",
    " art afternoon ",
    " artist talk ",
    " conversation ",
    " conversations ",
    " discussion ",
    " discussions ",
    " educator evening ",
    " education encounters ",
    " hands on ",
    " hands-on ",
    " introduction to ",
    " lab ",
    " lecture ",
    " lectures ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
NEVADA_ART_REJECT_PATTERNS = (
    " book club ",
    " dance ",
    " early closure ",
    " exhibition ",
    " exhibitions ",
    " film ",
    " fundraiser ",
    " gala ",
    " guided tour ",
    " guided tours ",
    " hours of operation ",
    " jazz ",
    " member events ",
    " members premiere ",
    " movie ",
    " music ",
    " party ",
    " performance ",
    " performances ",
    " reception ",
    " summit ",
    " symposium ",
    " tour ",
    " tours ",
    " vendor ",
    " yoga ",
)


@dataclass(frozen=True, slots=True)
class NvVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    timezone: str
    list_url: str
    default_location: str
    source_prefixes: tuple[str, ...]


NV_VENUES: tuple[NvVenueConfig, ...] = (
    NvVenueConfig(
        slug="barrick",
        source_name="nv_barrick_events",
        venue_name="Marjorie Barrick Museum of Art",
        city="Las Vegas",
        state="NV",
        timezone=NV_TIMEZONE,
        list_url="https://www.unlv.edu/barrickmuseum/events",
        default_location="Marjorie Barrick Museum of Art, Las Vegas, NV",
        source_prefixes=("https://www.unlv.edu/event/",),
    ),
    NvVenueConfig(
        slug="nevada_art",
        source_name="nv_nevada_art_events",
        venue_name="Nevada Museum of Art",
        city="Reno",
        state="NV",
        timezone=NV_TIMEZONE,
        list_url="https://www.nevadaart.org/calendar/",
        default_location="Nevada Museum of Art, Reno, NV",
        source_prefixes=("https://www.nevadaart.org/event/",),
    ),
)

NV_VENUES_BY_SLUG = {venue.slug: venue for venue in NV_VENUES}


class NvBundleAdapter(BaseSourceAdapter):
    source_name = "nv_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_nv_bundle_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_nv_bundle_payload/parse_nv_events from the script runner.")


def get_nv_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in NV_VENUES:
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
            timezone_id=NV_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(8000)
        html = await page.content()
        await browser.close()
        return html


async def fetch_html_resilient(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
) -> str:
    try:
        return await fetch_html(url, client=client)
    except Exception as exc:
        if async_playwright is None:
            raise RuntimeError(f"Unable to fetch HTML and Playwright is unavailable: {url}") from exc
        print(f"[nv-bundle] HTTP fetch failed for {url}; retrying with Playwright: {exc}")
        return await fetch_html_playwright(url)


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
                return url, await fetch_html_resilient(url, client=client)
            except Exception as exc:
                print(f"[nv-bundle] {label} detail fetch failed url={url}: {exc}")
                return url, None

    pages: dict[str, str] = {}
    for url, html in await asyncio.gather(*(_fetch_one(url) for url in sorted(urls))):
        if html:
            pages[url] = html
    return pages


async def load_nv_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
) -> dict[str, dict]:
    selected = [NV_VENUES_BY_SLUG[slug] for slug in venue_slugs] if venue_slugs else list(NV_VENUES)
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            list_html = await fetch_html_resilient(venue.list_url, client=client)
            if venue.slug == "barrick":
                detail_urls = _collect_barrick_detail_urls(list_html, venue=venue)
                detail_pages = await _fetch_detail_pages(urls=detail_urls, client=client, label=venue.slug)
                payload[venue.slug] = {
                    "list_html": list_html,
                    "detail_pages": detail_pages,
                }
            else:
                payload[venue.slug] = {"list_html": list_html}

    return payload


def parse_nv_events(payload: dict, *, venue: NvVenueConfig) -> list[ExtractedActivity]:
    if venue.slug == "barrick":
        return _parse_barrick_events(payload, venue=venue)
    if venue.slug == "nevada_art":
        return _parse_nevada_art_events(payload, venue=venue)
    return []


def _collect_barrick_detail_urls(list_html: str, *, venue: NvVenueConfig) -> set[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    urls: set[str] = set()
    for anchor in soup.select(".click-region-link a[href*='/event/']"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        urls.add(urljoin(venue.list_url, href.split("?", 1)[0]))
    return urls


def _parse_barrick_events(payload: dict, *, venue: NvVenueConfig) -> list[ExtractedActivity]:
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for url in sorted(detail_pages):
        row = _parse_barrick_detail_page(url=url, html=detail_pages[url], venue=venue)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    return rows


def _parse_barrick_detail_page(
    *,
    url: str,
    html: str,
    venue: NvVenueConfig,
) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = _normalize_space(soup.select_one(".event-node h2") and soup.select_one(".event-node h2").get_text(" ", strip=True))
    if not title:
        title = _normalize_space(soup.title.get_text(" ", strip=True).split("|", 1)[0] if soup.title else "")
    if not title:
        return None

    description = _normalize_space(_extract_text(soup.select_one(".field--name-body")))
    if not _is_barrick_candidate(title=title, description=description):
        return None

    date_text = _normalize_space(_extract_text(soup.select_one(".event-dates .event-first-date")))
    if not date_text:
        return None
    start_at, end_at = _parse_date_time_range(date_text, timezone=venue.timezone)
    if start_at is None:
        return None

    campus_location = _normalize_space(_extract_text(soup.select_one(".event-location .field--item")))
    room_location = _normalize_space(_extract_text(soup.select_one(".event-location-number .field--item")))
    location_parts = [part for part in (room_location, campus_location, f"{venue.city}, {venue.state}") if part]
    location_text = ", ".join(dict.fromkeys(location_parts)) if location_parts else venue.default_location

    text_blob = " ".join(part for part in (title, description, location_text) if part)
    row_kwargs = price_classification_kwargs(text_blob, default_is_free=None)

    return ExtractedActivity(
        source_url=url,
        title=title,
        description=description,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=_resolve_activity_type(title=title, description=description),
        age_min=None,
        age_max=None,
        drop_in=_has_any_pattern(text_blob, (" drop in ", " drop-in ")),
        registration_required=_infer_registration_required(text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=venue.timezone,
        **row_kwargs,
    )


def _parse_nevada_art_events(payload: dict, *, venue: NvVenueConfig) -> list[ExtractedActivity]:
    list_html = str(payload.get("list_html") or "")
    if not list_html:
        return []

    soup = BeautifulSoup(list_html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in soup.select(".event-item"):
        detail_box = item.select_one(".event-item__details")
        content_box = item.select_one(".event-item__content")
        if detail_box is None or content_box is None:
            continue

        headline_link = content_box.select_one(".event-item__headline a[href]")
        if headline_link is None:
            continue

        title = _normalize_space(headline_link.get_text(" ", strip=True))
        source_url = _normalize_space(headline_link.get("href"))
        if not title or not source_url:
            continue

        detail_parts = [_normalize_space(part) for part in detail_box.stripped_strings]
        detail_parts = [part for part in detail_parts if part]
        if len(detail_parts) < 3:
            continue

        category = detail_parts[0]
        date_text = detail_parts[2]
        time_text = detail_parts[3] if len(detail_parts) > 3 else None

        start_at, end_at = _parse_date_time_range(
            f"{date_text}, {time_text}" if time_text else date_text,
            timezone=venue.timezone,
        )
        if start_at is None:
            continue

        description = _normalize_space(_extract_text(content_box.select_one(".event-item__description")))
        if not description:
            content_strings = [_normalize_space(part) for part in content_box.stripped_strings]
            content_strings = [part for part in content_strings if part and part not in {title, "More Info"}]
            description = content_strings[0] if content_strings else None

        if not _is_nevada_art_candidate(title=title, description=description, category=category):
            continue

        button_texts = [
            _normalize_space(button.get_text(" ", strip=True))
            for button in item.select(".event-item__buttons a, .event-item__buttons span, .event-item__cta a, .event-item__cta span")
            if _normalize_space(button.get_text(" ", strip=True))
        ]
        text_blob = " ".join(part for part in (category, title, description, " ".join(button_texts)) if part)
        row_kwargs = price_classification_kwargs(text_blob, default_is_free=None)

        row = ExtractedActivity(
            source_url=source_url,
            title=title,
            description=_build_nevada_art_description(description=description, category=category),
            venue_name=venue.venue_name,
            location_text=venue.default_location,
            city=venue.city,
            state=venue.state,
            activity_type=_resolve_activity_type(title=title, description=f"{category} {description or ''}"),
            age_min=None,
            age_max=None,
            drop_in=_has_any_pattern(text_blob, (" drop in ", " drop-in ", " hands on ", " hands-on ")),
            registration_required=_infer_nevada_art_registration(button_texts=button_texts, text_blob=text_blob),
            start_at=start_at,
            end_at=end_at,
            timezone=venue.timezone,
            **row_kwargs,
        )

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    return rows


def _build_nevada_art_description(*, description: str | None, category: str | None) -> str | None:
    parts = []
    if description:
        parts.append(description)
    if category:
        parts.append(f"Category: {category}")
    return " | ".join(parts) if parts else None


def _is_barrick_candidate(*, title: str, description: str | None) -> bool:
    title_text = _normalized_blob(title)
    text = _normalized_blob(title, description)
    if _has_any_pattern(title_text, (" workshop ", " workshops ", " class ", " classes ", " activity ", " activities ", " lab ", " labs ")):
        return True
    if _has_any_pattern(title_text, BARRICK_REJECT_PATTERNS):
        return False
    if _has_any_pattern(text, (" talk ", " talks ", " lecture ", " lectures ", " conversation ", " conversations ")):
        return _has_any_pattern(text, BARRICK_ART_CONTEXT_PATTERNS)
    return False


def _is_nevada_art_candidate(*, title: str, description: str | None, category: str | None) -> bool:
    text = _normalized_blob(category, title, description)
    if _has_any_pattern(text, NEVADA_ART_SPECIAL_INCLUDE_PATTERNS):
        return True
    if _has_any_pattern(text, NEVADA_ART_REJECT_PATTERNS):
        return False
    return _has_any_pattern(text, NEVADA_ART_INCLUDE_PATTERNS)


def _resolve_activity_type(*, title: str, description: str | None) -> str | None:
    text = _normalized_blob(title, description)
    if _has_any_pattern(text, (" talk ", " talks ", " lecture ", " lectures ", " conversation ", " conversations ")):
        return "talk"
    if _has_any_pattern(text, (" workshop ", " workshops ", " class ", " classes ", " lab ", " labs ")):
        return "workshop"
    if _has_any_pattern(text, (" activity ", " activities ", " hands on ", " hands-on ")):
        return "activity"
    return "activity"


def _infer_registration_required(text: str) -> bool | None:
    if "no reservation is required" in text.lower() or "registration not required" in text.lower():
        return False
    if _has_any_pattern(text, (" register ", " registration ", " reserve ", " reservation ", " rsvp ", " sold out ")):
        return True
    return None


def _infer_nevada_art_registration(*, button_texts: list[str], text_blob: str) -> bool | None:
    lowered_buttons = " ".join(button_texts).lower()
    if "sold out" in lowered_buttons:
        return True
    if "register" in lowered_buttons:
        return True
    return _infer_registration_required(text_blob)


def _parse_date_time_range(
    raw_text: str,
    *,
    timezone: str,
) -> tuple[datetime | None, datetime | None]:
    cleaned = _normalize_space(raw_text.replace("–", "-").replace("—", "-"))
    if not cleaned:
        return None, None

    match = DATE_WITH_YEAR_RE.search(cleaned)
    if match is None:
        return None, None

    month_key = match.group("month").lower().rstrip(".")
    month = MONTH_LOOKUP.get(month_key)
    if month is None:
        return None, None

    event_date = date(int(match.group("year")), month, int(match.group("day")))
    remainder = cleaned[match.end() :].strip(" ,")
    if not remainder:
        start_time = datetime.combine(event_date, time.min, tzinfo=ZoneInfo(timezone))
        return start_time, None

    start_clock, end_clock = _parse_time_range(remainder)
    if start_clock is None:
        start_time = datetime.combine(event_date, time.min, tzinfo=ZoneInfo(timezone))
        return start_time, None

    start_at = datetime.combine(event_date, start_clock, tzinfo=ZoneInfo(timezone))
    end_at = datetime.combine(event_date, end_clock, tzinfo=ZoneInfo(timezone)) if end_clock else None
    return start_at, end_at


def _parse_time_range(raw_text: str) -> tuple[time | None, time | None]:
    cleaned = _normalize_space(raw_text.replace("\u00a0", " "))
    parts = [part for part in TIME_RANGE_SPLIT_RE.split(cleaned) if part]
    if not parts:
        return None, None
    if len(parts) == 1:
        parsed = _parse_time_token(parts[0], fallback_meridiem=None)
        return (parsed, None) if parsed is not None else (None, None)

    end_part = parts[-1]
    end_meridiem = _extract_meridiem(end_part)
    start_part = parts[0]
    start_meridiem = _extract_meridiem(start_part) or end_meridiem

    start_time = _parse_time_token(start_part, fallback_meridiem=start_meridiem)
    end_time = _parse_time_token(end_part, fallback_meridiem=end_meridiem or start_meridiem)
    return start_time, end_time


def _parse_time_token(raw_text: str, *, fallback_meridiem: str | None) -> time | None:
    cleaned = _normalize_space(raw_text).lower().replace(".", "")
    if cleaned == "noon":
        return time(hour=12, minute=0)
    if cleaned == "midnight":
        return time(hour=0, minute=0)

    match = TIME_TOKEN_RE.fullmatch(cleaned)
    if match is None:
        return None

    hour = int(match.group("hour"))
    minute = int(match.group("minute") or 0)
    meridiem = (match.group("meridiem") or fallback_meridiem or "").lower().replace(".", "")
    if meridiem not in {"am", "pm"}:
        return None

    if meridiem == "am":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12

    return time(hour=hour, minute=minute)


def _extract_meridiem(raw_text: str) -> str | None:
    lowered = raw_text.lower()
    if "am" in lowered:
        return "am"
    if "pm" in lowered:
        return "pm"
    if "noon" in lowered:
        return "pm"
    if "midnight" in lowered:
        return "am"
    return None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def _extract_text(node) -> str:
    if node is None:
        return ""
    return _normalize_space(node.get_text(" ", strip=True))


def _normalized_blob(*parts: str | None) -> str:
    joined = " ".join(_normalize_space(part) for part in parts if _normalize_space(part))
    normalized = NON_ALNUM_RE.sub(" ", joined.lower())
    return f" {' '.join(normalized.split())} "


def _has_any_pattern(text: str, patterns: tuple[str, ...]) -> bool:
    return any(pattern in text.lower() for pattern in patterns)
