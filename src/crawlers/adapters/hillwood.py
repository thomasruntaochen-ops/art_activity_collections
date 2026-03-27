from __future__ import annotations

import asyncio
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    from playwright.async_api import BrowserContext
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    BrowserContext = None
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

HILLWOOD_EVENTS_URL = "https://hillwoodmuseum.org/events"
HILLWOOD_TIMEZONE = "America/New_York"
HILLWOOD_VENUE_NAME = "Hillwood Estate, Museum & Gardens"
HILLWOOD_CITY = "Washington"
HILLWOOD_STATE = "DC"
HILLWOOD_DEFAULT_LOCATION = "4155 Linnean Avenue NW, Washington, DC 20008"
HILLWOOD_MAX_PAGES = 5

PLAYWRIGHT_LAUNCH_ARGS = (
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
)
PLAYWRIGHT_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'platform', {get: () => 'MacIntel'});
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
"""

DATE_RE = re.compile(
    r"^(?P<dow>[A-Za-z]{3,9},\s+)?(?P<date>[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})$",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)\s*(?:-|–|—|to)\s*(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)", re.IGNORECASE)
PAGE_COUNT_RE = re.compile(r"Page\s+(\d+)", re.IGNORECASE)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
AGE_YOUNGER_RE = re.compile(
    r"\bages?\s*(\d{1,2})\s*(?:and younger|or younger|and under|or under)\b",
    re.IGNORECASE,
)

INCLUSION_MARKERS = (
    " art-making ",
    " hands-on ",
    " hands on ",
    " make ",
    " preschool ",
    " preschooler ",
    " family ",
    " families ",
    " workshop ",
    " workshops ",
    " activity ",
    " activities ",
    " craft ",
    " crafts ",
    " create ",
    " creative ",
)
STRONG_EXCLUSION_MARKERS = (
    " bird walk ",
    " bird walks ",
    " film ",
    " films ",
    " guided tour ",
    " guided tours ",
    " lecture ",
    " lectures ",
    " movie ",
    " movie night ",
    " night ",
    " open house ",
    " performance ",
    " performances ",
    " reception ",
    " shop pop up ",
    " shopping ",
    " trivia ",
    " tour ",
    " tours ",
    " writing ",
    " yoga ",
    " concert ",
    " concerts ",
    " cocktails ",
    " garden walk ",
    " forest bathing ",
)
YOUTH_MARKERS = (
    " family ",
    " families ",
    " kid ",
    " kids ",
    " preschool ",
    " preschooler ",
    " child ",
    " children ",
    " teen ",
    " teens ",
)


@dataclass(slots=True)
class HillwoodListingEntry:
    list_url: str
    source_url: str
    title: str
    date_text: str
    time_text: str | None
    teaser_text: str | None
    cost_text: str | None
    part_of_text: str | None
    extra_text: str | None


@dataclass(slots=True)
class HillwoodDetailFields:
    description: str | None
    age_min: int | None
    age_max: int | None
    location_text: str | None
    start_at: datetime | None
    end_at: datetime | None


class HillwoodEventsAdapter(BaseSourceAdapter):
    source_name = "hillwood_events"

    def __init__(self, url: str = HILLWOOD_EVENTS_URL, *, max_pages: int = HILLWOOD_MAX_PAGES):
        self.url = url
        self.max_pages = max_pages

    async def fetch(self) -> list[str]:
        payload = await load_hillwood_events_payload(url=self.url, max_pages=self.max_pages)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_hillwood_events_payload(json.loads(payload))


async def load_hillwood_events_payload(
    *,
    url: str = HILLWOOD_EVENTS_URL,
    max_pages: int = HILLWOOD_MAX_PAGES,
    timeout_ms: int = 90000,
) -> dict:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    listing_pages: dict[str, str] = {}
    detail_pages: dict[str, str] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, args=list(PLAYWRIGHT_LAUNCH_ARGS))
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id=HILLWOOD_TIMEZONE,
            viewport={"width": 1440, "height": 1800},
            screen={"width": 1440, "height": 1800},
            color_scheme="light",
        )
        await context.add_init_script(PLAYWRIGHT_INIT_SCRIPT)

        try:
            for page_number in range(max_pages):
                page_url = url if page_number == 0 else f"{url}?page={page_number}"
                print(f"[hillwood-fetch] page={page_number} url={page_url}")
                html = await _fetch_hillwood_page_playwright(
                    context=context,
                    url=page_url,
                    ready_selector=".node--view-mode-event-right-list, .views-row",
                    timeout_ms=timeout_ms,
                )
                listing_pages[page_url] = html

            detail_urls = _collect_detail_urls(listing_pages)
            print(f"[hillwood-fetch] unique_detail_urls={len(detail_urls)}")
            for index, detail_url in enumerate(detail_urls, start=1):
                print(f"[hillwood-fetch] detail {index}/{len(detail_urls)} url={detail_url}")
                detail_pages[detail_url] = await _fetch_hillwood_page_playwright(
                    context=context,
                    url=detail_url,
                    ready_selector="main h1, h1",
                    timeout_ms=timeout_ms,
                )
        finally:
            await context.close()
            await browser.close()

    return {
        "listing_pages": listing_pages,
        "detail_pages": detail_pages,
    }


def parse_hillwood_events_payload(payload: dict) -> list[ExtractedActivity]:
    listing_pages = payload.get("listing_pages") or {}
    detail_pages = payload.get("detail_pages") or {}

    entries_by_url: dict[str, HillwoodListingEntry] = {}
    for list_url, html in sorted(listing_pages.items()):
        for entry in _parse_listing_entries(html=html, list_url=list_url):
            entries_by_url.setdefault(entry.source_url, entry)

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, entry in sorted(entries_by_url.items()):
        detail = _parse_detail_html(detail_pages.get(source_url, ""), entry=entry)
        if not _should_include_event(entry=entry, detail=detail):
            continue

        start_at, end_at = detail.start_at, detail.end_at
        if start_at is None:
            start_at, end_at = _parse_listing_datetime(entry.date_text, entry.time_text)
        if start_at is None:
            continue

        description_parts = [part for part in [detail.description, entry.teaser_text, entry.extra_text] if part]
        if entry.part_of_text:
            description_parts.append(f"Part of: {entry.part_of_text}")
        description = " | ".join(description_parts) if description_parts else None

        age_min, age_max = detail.age_min, detail.age_max
        if age_min is None and age_max is None:
            age_min, age_max = _parse_age_range(" ".join(filter(None, [entry.title, description, entry.part_of_text])))

        pricing_text = " ".join(filter(None, [entry.cost_text, description, entry.part_of_text]))
        text_blob = _normalized_blob(entry.title, description, entry.teaser_text, entry.part_of_text, entry.cost_text)

        key = (source_url, entry.title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=entry.title,
                description=description,
                venue_name=HILLWOOD_VENUE_NAME,
                location_text=detail.location_text or HILLWOOD_DEFAULT_LOCATION,
                city=HILLWOOD_CITY,
                state=HILLWOOD_STATE,
                activity_type=_infer_activity_type(entry.title, description),
                age_min=age_min,
                age_max=age_max,
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=HILLWOOD_TIMEZONE,
                **price_classification_kwargs(pricing_text, default_is_free=None),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


async def _fetch_hillwood_page_playwright(
    *,
    context: BrowserContext,
    url: str,
    ready_selector: str,
    timeout_ms: int,
) -> str:
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
        await page.wait_for_selector(ready_selector, state="attached", timeout=timeout_ms)
        await page.wait_for_timeout(800)
        return await page.content()
    finally:
        await page.close()


def _collect_detail_urls(listing_pages: dict[str, str]) -> list[str]:
    detail_urls: list[str] = []
    seen: set[str] = set()
    for list_url, html in sorted(listing_pages.items()):
        for entry in _parse_listing_entries(html=html, list_url=list_url):
            if entry.source_url in seen:
                continue
            seen.add(entry.source_url)
            detail_urls.append(entry.source_url)
    return detail_urls


def _parse_listing_entries(*, html: str, list_url: str) -> list[HillwoodListingEntry]:
    soup = BeautifulSoup(html, "html.parser")
    entries: list[HillwoodListingEntry] = []
    seen_urls: set[str] = set()

    for node in soup.select(".node--view-mode-event-right-list"):
        row = node.find_parent("div", class_="views-row") or node
        title_node = node.select_one(".field--name-node-title a")
        if title_node is None:
            continue

        source_url = urljoin(list_url, title_node.get("href") or "")
        if not source_url or source_url in seen_urls:
            continue
        seen_urls.add(source_url)

        title = _normalize_space(title_node.get_text(" ", strip=True))
        teaser_text = _extract_block_text(node.select_one(".field--name-field-teaser-text"))
        cost_text = _extract_block_text(row.select_one(".field--name-field-cost-summary"))
        part_of_text = _extract_block_text(row.select_one(".field--name-field-event-type .field--item"))
        extra_text = _extract_block_text(row.select_one(".button.green a")) if row.select_one(".button.green a") else None

        if not title:
            continue

        date_text = _extract_block_text(row.select_one(".floatleft .date")) or ""
        time_text = None
        time_match = _extract_time_from_row(row)
        if time_match:
            time_text = time_match

        entries.append(
            HillwoodListingEntry(
                list_url=list_url,
                source_url=source_url,
                title=title,
                date_text=date_text,
                time_text=time_text,
                teaser_text=teaser_text,
                cost_text=cost_text,
                part_of_text=part_of_text,
                extra_text=extra_text,
            )
        )

    return entries


def _parse_detail_html(html: str, *, entry: HillwoodListingEntry) -> HillwoodDetailFields:
    if not html:
        return HillwoodDetailFields(
            description=None,
            age_min=None,
            age_max=None,
            location_text=None,
            start_at=None,
            end_at=None,
        )

    soup = BeautifulSoup(html, "html.parser")
    body_text = _extract_detail_body_text(soup)
    age_min, age_max = _parse_age_range(body_text)
    location_text = _extract_detail_location(soup)
    start_at, end_at = _extract_detail_datetimes(soup)
    description = body_text or None

    return HillwoodDetailFields(
        description=description,
        age_min=age_min,
        age_max=age_max,
        location_text=location_text,
        start_at=start_at,
        end_at=end_at,
    )


def _should_include_event(*, entry: HillwoodListingEntry, detail: HillwoodDetailFields) -> bool:
    combined = _normalized_blob(
        entry.title,
        entry.teaser_text,
        entry.part_of_text,
        entry.cost_text,
        detail.description,
    )

    has_youth_context = any(marker in combined for marker in YOUTH_MARKERS)
    if not has_youth_context:
        return False

    if any(marker in combined for marker in STRONG_EXCLUSION_MARKERS):
        return False

    if "preschool" in combined:
        return True
    if "family" in combined and any(marker in combined for marker in INCLUSION_MARKERS):
        return True
    if "workshop" in combined and "writing" not in combined:
        return True
    return False


def _infer_activity_type(title: str, description: str | None) -> str:
    combined = _normalized_blob(title, description)
    if "workshop" in combined:
        return "workshop"
    if "class" in combined:
        return "class"
    if "preschool" in combined or "family" in combined or "activity" in combined:
        return "activity"
    return "activity"


def _infer_drop_in(text: str) -> bool | None:
    if "drop-in" in text or "drop in" in text:
        return True
    return False


def _infer_registration_required(text: str) -> bool | None:
    if "registration required" in text or "purchase" in text:
        return True
    if "registration encouraged" in text or "included in suggested donation" in text:
        return False
    return None


def _parse_age_range(text: str | None) -> tuple[int | None, int | None]:
    normalized = _normalize_space(text or "")
    if not normalized:
        return None, None

    range_match = AGE_RANGE_RE.search(normalized)
    if range_match:
        age_min = int(range_match.group(1))
        age_max = int(range_match.group(2))
        return (age_min, age_max) if age_min <= age_max else (age_max, age_min)

    younger_match = AGE_YOUNGER_RE.search(normalized)
    if younger_match:
        return None, int(younger_match.group(1))

    plus_match = AGE_PLUS_RE.search(normalized)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def _parse_listing_datetime(date_text: str, time_text: str | None) -> tuple[datetime | None, datetime | None]:
    match = DATE_RE.match(_normalize_space(date_text))
    if not match:
        return None, None

    base = datetime.strptime(match.group("date"), "%B %d, %Y")
    if not time_text:
        return base.replace(hour=0, minute=0, second=0, microsecond=0), None

    time_match = TIME_RANGE_RE.search(time_text)
    if time_match:
        start = _parse_time_token(time_match.group("start"))
        end = _parse_time_token(time_match.group("end"))
        if start is None:
            return None, None
        start_at = base.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
        end_at = None
        if end is not None:
            end_at = base.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(time_text)
    if single_match:
        parsed = _parse_time_token(single_match.group("time"))
        if parsed is None:
            return None, None
        return base.replace(hour=parsed.hour, minute=parsed.minute, second=0, microsecond=0), None

    return base.replace(hour=0, minute=0, second=0, microsecond=0), None


def _parse_time_token(token: str):
    cleaned = token.lower().replace(".", "").replace(" ", "")
    for fmt in ("%I:%M%p", "%I%p"):
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def _extract_time_from_row(row) -> str | None:
    date_block = row.select_one(".floatleft .date") or row.select_one(".date")
    if date_block is None:
        return None
    value = _normalize_space(date_block.get_text(" ", strip=True))
    if not value:
        return None
    # If the date block includes a line break separated date + time, the part after
    # the date is useful for time parsing. Otherwise fall back to the whole value.
    parts = value.split()
    if len(parts) >= 2 and ":" in value:
        return value.replace(parts[0] + " ", "", 1)
    return None


def _extract_detail_location(soup: BeautifulSoup) -> str | None:
    body_text = _extract_detail_body_text(soup)
    if not body_text:
        return None
    for marker in ("Visitor Center Conservatory", "Kogod Courtyard", "Hillwood"):
        if marker.lower() in body_text.lower():
            return f"{marker}, {HILLWOOD_DEFAULT_LOCATION}"
    return HILLWOOD_DEFAULT_LOCATION


def _extract_detail_body_text(soup: BeautifulSoup) -> str | None:
    candidates = []
    for selector in (
        ".node--view-mode-full .field--name-body",
        ".node--type-event.node--view-mode-full .field--name-body",
        ".node--type-event .field--name-body",
        "main",
    ):
        node = soup.select_one(selector)
        if node is None:
            continue
        text = _normalize_space(unescape(node.get_text("\n", strip=True)))
        if text:
            candidates.append(text)
    return max(candidates, key=len) if candidates else None


def _extract_detail_datetimes(soup: BeautifulSoup) -> tuple[datetime | None, datetime | None]:
    field = soup.select_one(".node--view-mode-full .field--name-field-date-and-time")
    if field is None:
        field = soup.select_one(".field--name-field-date-and-time")
    if field is None:
        return None, None

    datetimes: list[datetime] = []
    seen: set[str] = set()
    for time_node in field.select("time[datetime]"):
        raw = _normalize_space(time_node.get("datetime") or "")
        if not raw or raw in seen:
            continue
        try:
            parsed = parse_iso_datetime(raw, timezone_name=HILLWOOD_TIMEZONE)
        except ValueError:
            continue
        seen.add(raw)
        datetimes.append(parsed)
        if len(datetimes) >= 2:
            break

    if not datetimes:
        return None, None
    if len(datetimes) == 1:
        return datetimes[0], None
    return datetimes[0], datetimes[1]


def _extract_block_text(node) -> str | None:
    if node is None:
        return None
    text = _normalize_space(unescape(node.get_text(" ", strip=True)))
    return text or None


def _normalized_blob(*parts: str | None) -> str:
    return f" {' '.join(_normalize_space(part or '') for part in parts if part).lower()} "


def _normalize_space(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())
