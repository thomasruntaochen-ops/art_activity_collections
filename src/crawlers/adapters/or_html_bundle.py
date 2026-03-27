from __future__ import annotations

import asyncio
import json
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
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

OR_TIMEZONE = "America/Los_Angeles"

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

DATE_WITH_YEAR_RE = re.compile(r"([A-Za-z]{3,9})\s+(\d{1,2}),\s+(\d{4})", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?)?\s*(?:-|–|—|to)\s*(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*([AaPp]\.?[Mm]\.?)\b", re.IGNORECASE)
NO_OVERRIDE_EXCLUDES = (
    " camp ",
    " camps ",
    " film ",
    " films ",
    " fundraiser ",
    " fundraising ",
    " information session ",
    " open house ",
    " reception ",
    " storytime ",
    " tour ",
    " tours ",
)
INCLUDE_MARKERS = (
    " artmaking ",
    " artist talk ",
    " class ",
    " classes ",
    " club ",
    " community ",
    " creative aging ",
    " discussion ",
    " drawing ",
    " hands on ",
    " hands-on ",
    " kids ",
    " lecture ",
    " meet up ",
    " meet-up ",
    " scavenger hunt ",
    " symposium ",
    " talk ",
    " workshop ",
    " workshops ",
    " youth ",
)


@dataclass(frozen=True, slots=True)
class OrHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    discovery_mode: str


OR_HTML_VENUES: tuple[OrHtmlVenueConfig, ...] = (
    OrHtmlVenueConfig(
        slug="jsma_uo",
        source_name="or_jordan_schnitzer_uo_events",
        venue_name="Jordan Schnitzer Museum of Art",
        city="Eugene",
        state="OR",
        list_url="https://jsma.uoregon.edu/events",
        default_location="Jordan Schnitzer Museum of Art, Eugene, OR",
        discovery_mode="jsma",
    ),
    OrHtmlVenueConfig(
        slug="jsma_psu",
        source_name="or_jordan_schnitzer_psu_events",
        venue_name="Jordan Schnitzer Museum of Art at Portland State University",
        city="Portland",
        state="OR",
        list_url="https://www.pdx.edu/museum-of-art/calendar/month",
        default_location="Jordan Schnitzer Museum of Art at PSU, Portland, OR",
        discovery_mode="psu",
    ),
)

OR_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in OR_HTML_VENUES}


@dataclass(frozen=True, slots=True)
class OrHtmlEventStub:
    source_url: str
    title: str
    start_at: datetime
    end_at: datetime | None


async def fetch_html(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
    use_playwright_fallback: bool = True,
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
                text = response.text
                if _looks_like_bot_challenge(text) and use_playwright_fallback:
                    return await fetch_html_playwright(url)
                return text

            if response.status_code in (403, 429) and use_playwright_fallback:
                return await fetch_html_playwright(url)

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if use_playwright_fallback and async_playwright is not None:
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
            timezone_id=OR_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def load_or_html_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
) -> dict[str, dict]:
    selected = (
        [OR_HTML_VENUES_BY_SLUG[slug] for slug in venue_slugs]
        if venue_slugs
        else list(OR_HTML_VENUES)
    )
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            list_html = await fetch_html(venue.list_url, client=client)
            if venue.discovery_mode == "jsma":
                stubs = _extract_jsma_stubs(list_html, list_url=venue.list_url)
            else:
                stubs = _extract_psu_stubs(list_html, list_url=venue.list_url)

            detail_urls = [stub.source_url for stub in stubs]
            detail_pages: dict[str, str] = {}
            if detail_urls:
                detail_htmls = await asyncio.gather(*(fetch_html(url, client=client) for url in detail_urls))
                detail_pages = {url: html for url, html in zip(detail_urls, detail_htmls, strict=True)}

            payload[venue.slug] = {
                "list_html": list_html,
                "stubs": stubs,
                "detail_pages": detail_pages,
            }

    return payload


def parse_or_html_events(payload: dict, *, venue: OrHtmlVenueConfig) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(OR_TIMEZONE)).date()
    stubs: list[OrHtmlEventStub] = payload.get("stubs") or []
    detail_pages: dict[str, str] = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for stub in stubs:
        detail_html = detail_pages.get(stub.source_url)
        if not detail_html:
            continue
        if venue.discovery_mode == "jsma":
            row = _build_jsma_row(stub=stub, html=detail_html, venue=venue)
        else:
            row = _build_psu_row(stub=stub, html=detail_html, venue=venue)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class OrHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "or_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_or_html_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_or_html_bundle_payload/parse_or_html_events from script runner.")


def _extract_jsma_stubs(list_html: str, *, list_url: str) -> list[OrHtmlEventStub]:
    soup = BeautifulSoup(list_html, "html.parser")
    stubs: list[OrHtmlEventStub] = []
    seen: set[tuple[str, str, datetime]] = set()

    for row in soup.select(".views-row"):
        anchor = row.find("a", href=True)
        if anchor is None:
            continue
        source_url = urljoin(list_url, anchor.get("href") or "")
        if "/events/" not in source_url and "/learn/studio-programs/" not in source_url:
            continue

        title = _normalize_space(anchor.get_text(" ", strip=True))
        row_text = _normalize_space(row.get_text(" ", strip=True))
        date_match = DATE_WITH_YEAR_RE.search(row_text)
        if date_match is None:
            continue

        event_date = _parse_month_day_year(date_match.group(0))
        if event_date is None:
            continue

        time_match = TIME_RANGE_RE.search(row_text)
        if time_match:
            start_clock, end_clock = _parse_time_range_match(time_match)
            start_at = datetime.combine(event_date, start_clock)
            end_at = datetime.combine(event_date, end_clock) if end_clock is not None else None
        else:
            single_time = TIME_SINGLE_RE.search(row_text)
            if single_time:
                start_at = datetime.combine(event_date, _parse_time_components(single_time.group(1), single_time.group(2), single_time.group(3)))
            else:
                start_at = datetime.combine(event_date, time.min)
            end_at = None

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        stubs.append(
            OrHtmlEventStub(
                source_url=source_url,
                title=title,
                start_at=start_at,
                end_at=end_at,
            )
        )

    return stubs


def _extract_psu_stubs(list_html: str, *, list_url: str) -> list[OrHtmlEventStub]:
    soup = BeautifulSoup(list_html, "html.parser")
    settings_tag = soup.select_one('script[data-drupal-selector="drupal-settings-json"]')
    if settings_tag is None:
        return []

    try:
        settings = json.loads(settings_tag.get_text())
    except json.JSONDecodeError:
        return []

    raw_payload = (
        (((settings.get("pdxd8_content_gen") or {}).get("calendar_event_json")))
        or "[]"
    )
    try:
        events = json.loads(raw_payload)
    except json.JSONDecodeError:
        return []

    stubs: list[OrHtmlEventStub] = []
    seen: set[tuple[str, str, datetime]] = set()
    for event_obj in events:
        title = _normalize_space(event_obj.get("title"))
        source_url = urljoin(list_url, _normalize_space(event_obj.get("url")))
        start_at = _parse_datetime(event_obj.get("start"))
        if not title or not source_url or start_at is None:
            continue
        end_at = _parse_datetime(event_obj.get("end"))
        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        stubs.append(
            OrHtmlEventStub(
                source_url=source_url,
                title=title,
                start_at=start_at,
                end_at=end_at,
            )
        )

    return stubs


def _build_jsma_row(*, stub: OrHtmlEventStub, html: str, venue: OrHtmlVenueConfig) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    detail_wrap = soup.select_one(".exhibition-detail-wrap") or soup.select_one(".node__content")
    if detail_wrap is None:
        return None

    title = _normalize_space((detail_wrap.select_one(".exhibition-title") or soup.find("h1")).get_text(" ", strip=True))
    if not title:
        return None

    category = _normalize_space(
        (detail_wrap.select_one("p.has-red-color") or detail_wrap.select_one("p.text-transform-uppercase")).get_text(" ", strip=True)
        if (detail_wrap.select_one("p.has-red-color") or detail_wrap.select_one("p.text-transform-uppercase"))
        else ""
    )
    description = _extract_jsma_description(detail_wrap)
    cost_text = _normalize_price_text(_normalize_space(
        (detail_wrap.select_one(".field--name-field-cost") or detail_wrap.select_one(".cost")).get_text(" ", strip=True)
        if (detail_wrap.select_one(".field--name-field-cost") or detail_wrap.select_one(".cost"))
        else ""
    ))

    token_blob = _searchable_blob(" ".join([title, category, description or "", cost_text, stub.source_url]))
    if not _should_include_html_event(token_blob=token_blob):
        return None

    location_text = "Online" if " virtual " in token_blob else venue.default_location

    return ExtractedActivity(
        source_url=stub.source_url,
        title=title,
        description=_join_non_empty([description, f"Program: {category}" if category else None, f"Price: {cost_text}" if cost_text else None]),
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(token_blob),
        age_min=None,
        age_max=None,
        drop_in=(" drop in " in token_blob or " drop-in " in token_blob),
        registration_required=(
            " register " in token_blob
            or " registration " in token_blob
            or " reservations are required " in token_blob
            or " reserve a spot " in token_blob
        ),
        start_at=stub.start_at,
        end_at=stub.end_at,
        timezone=OR_TIMEZONE,
        **price_classification_kwargs(cost_text or description),
    )


def _build_psu_row(*, stub: OrHtmlEventStub, html: str, venue: OrHtmlVenueConfig) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = _normalize_space((soup.find("h1") or soup.title).get_text(" ", strip=True))
    if not title:
        return None

    field_items = [_normalize_space(node.get_text(" ", strip=True)) for node in soup.select(".field__item")]
    field_items = [item for item in field_items if item]

    location_text = field_items[0] if len(field_items) >= 1 else venue.default_location
    cost_text = _normalize_price_text(field_items[1] if len(field_items) >= 2 else "")
    description = field_items[-1] if len(field_items) >= 5 else ""
    if not description:
        node_text = _normalize_space((soup.select_one(".node__content") or soup.body).get_text(" ", strip=True))
        description = _extract_psu_description_from_node_text(node_text)

    token_blob = _searchable_blob(" ".join([title, description, cost_text]))
    if not _should_include_html_event(token_blob=token_blob):
        return None

    if " virtual " in token_blob:
        location_text = "Online"

    return ExtractedActivity(
        source_url=stub.source_url,
        title=title,
        description=_join_non_empty([description or None, f"Price: {cost_text}" if cost_text else None]),
        venue_name=venue.venue_name,
        location_text=location_text or venue.default_location,
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(token_blob),
        age_min=None,
        age_max=None,
        drop_in=(" drop in " in token_blob or " drop-in " in token_blob),
        registration_required=(" reserve tickets " in token_blob or " registration " in token_blob or " reserve " in token_blob),
        start_at=stub.start_at,
        end_at=stub.end_at,
        timezone=OR_TIMEZONE,
        **price_classification_kwargs(cost_text or description),
    )


def _extract_jsma_description(detail_wrap: BeautifulSoup) -> str:
    parts: list[str] = []
    for paragraph in detail_wrap.find_all("p", recursive=False):
        text = _normalize_space(paragraph.get_text(" ", strip=True))
        classes = paragraph.get("class") or []
        if not text:
            continue
        if "has-red-color" in classes:
            continue
        if "text-transform-uppercase" in classes:
            continue
        if "has-small-font-size" in classes:
            continue
        parts.append(text)
    if parts:
        return " ".join(parts)

    seen: set[str] = set()
    for paragraph in detail_wrap.select("p"):
        text = _normalize_space(paragraph.get_text(" ", strip=True))
        classes = paragraph.get("class") or []
        if not text or text in seen:
            continue
        if len(text) < 40:
            continue
        if "has-red-color" in classes:
            continue
        if "text-transform-uppercase" in classes:
            continue
        if "has-small-font-size" in classes:
            continue
        if text.startswith("Website link:"):
            continue
        if "Get Directions" in text or "Subscribe to our e-newsletter" in text:
            continue
        seen.add(text)
        parts.append(text)
        if len(parts) >= 2:
            break
    return " ".join(parts)


def _extract_psu_description_from_node_text(node_text: str) -> str:
    if " Yahoo! Calendar " in node_text:
        return node_text.split(" Yahoo! Calendar ", 1)[1].strip()
    if " Outlook Online " in node_text:
        return node_text.split(" Outlook Online ", 1)[1].strip()
    return node_text


def _should_include_html_event(*, token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in NO_OVERRIDE_EXCLUDES):
        return False
    if not any(pattern in token_blob for pattern in INCLUDE_MARKERS):
        return False
    return True


def _infer_activity_type(token_blob: str) -> str:
    if any(marker in token_blob for marker in (" lecture ", " talk ", " discussion ", " symposium ", " conversation ")):
        return "lecture"
    return "workshop"


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None)
    except ValueError:
        return None


def _parse_month_day_year(value: str) -> date | None:
    text = _normalize_space(value)
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_time_range_match(match: re.Match[str]) -> tuple[time, time | None]:
    start_time = _parse_time_components(match.group(1), match.group(2), match.group(3) or match.group(5))
    end_time = _parse_time_components(match.group(4), match.group(5), match.group(6))
    return start_time, end_time


def _parse_time_components(hour_text: str, minute_text: str | None, meridiem_text: str | None) -> time:
    hour = int(hour_text)
    minute = int(minute_text or "0")
    meridiem = (meridiem_text or "").lower().replace(".", "")

    if meridiem.startswith("p") and hour != 12:
        hour += 12
    elif meridiem.startswith("a") and hour == 12:
        hour = 0

    return time(hour=hour, minute=minute)


def _looks_like_bot_challenge(html: str) -> bool:
    lowered = html.lower()
    return (
        "just a moment" in lowered
        or "cf-browser-verification" in lowered
        or "__cf_chl_tk" in lowered
        or "cloudflare" in lowered and "challenge" in lowered
    )


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _normalize_price_text(value: str) -> str:
    return value.replace("$Free", "Free")


def _join_non_empty(parts: list[str | None]) -> str | None:
    cleaned = [part for part in parts if part]
    if not cleaned:
        return None
    return " | ".join(cleaned)


def get_or_html_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in OR_HTML_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)
