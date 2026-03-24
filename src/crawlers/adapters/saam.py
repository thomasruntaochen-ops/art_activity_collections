from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from urllib.parse import urlencode
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    from playwright.async_api import BrowserContext
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    BrowserContext = None
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

SAAM_SEARCH_BASE_URL = "https://americanart.si.edu/search/events"
SAAM_TIMEZONE = "America/New_York"
SAAM_VENUE_NAME = "Smithsonian American Art Museum"
SAAM_CITY = "Washington"
SAAM_STATE = "DC"
SAAM_DEFAULT_LOCATION = "Smithsonian American Art Museum, Washington, DC"
SAAM_KIDS_FAMILIES_CATEGORY_ID = "2616"
SAAM_LOCATION_ID = "1558"
SAAM_SOURCE_NAME = "saam_family_events"
SAAM_RESULTS_PER_PAGE = 16
SAAM_MAX_PAGE_GUARD = 6

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

RESULT_COUNT_RE = re.compile(r"Showing\s+(\d+)\s+results?", re.IGNORECASE)
LISTING_DATETIME_RE = re.compile(
    r"^(?:[A-Za-z]+,\s+)?(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4}),\s+"
    r"(?P<times>.+?)(?:\s+(?:EDT|EST))?$",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)"
    r"\s*(?:-|–|—|to)\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))", re.IGNORECASE)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
AGE_UNDER_RE = re.compile(
    r"\bages?\s*(\d{1,2})\s*(?:and\s+younger|and\s+under|or\s+younger|or\s+under)\b",
    re.IGNORECASE,
)

ACTIVITY_MARKERS = (
    " activity ",
    " activities ",
    " art-making ",
    " art making ",
    " craft ",
    " crafts ",
    " create ",
    " drawing ",
    " draw ",
    " hands-on ",
    " hands on ",
    " lab ",
    " make ",
    " making ",
    " scavenger hunt ",
    " sketch ",
    " studio ",
    " workshop ",
    " workshops ",
)
TALK_MARKERS = (
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " discussion ",
    " discussions ",
    " lecture ",
    " lectures ",
    " talk ",
    " talks ",
)
STRONG_EXCLUSION_MARKERS = (
    " film ",
    " films ",
    " meditation ",
    " music ",
    " performance ",
    " performances ",
    " poetry ",
    " reading ",
    " storytime ",
    " story time ",
    " tour ",
    " tours ",
    " yoga ",
)
YOUTH_CONTEXT_MARKERS = (
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


@dataclass(slots=True)
class SaamListingEntry:
    list_url: str
    source_url: str
    title: str
    display_time: str
    building_text: str | None
    cost_text: str | None
    action_url: str | None
    featured: bool


@dataclass(slots=True)
class SaamDetailFields:
    description: str | None
    cost_text: str | None
    event_location: str | None
    categories: tuple[str, ...]
    series: tuple[str, ...]


class SaamEventsAdapter(BaseSourceAdapter):
    source_name = SAAM_SOURCE_NAME

    async def fetch(self) -> list[str]:
        payload = await load_saam_events_payload()
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_saam_events_payload(json.loads(payload))


def build_saam_search_url(*, page: int | None = None) -> str:
    params: list[tuple[str, str]] = [
        ("content_type", "event"),
        ("event_categories[]", SAAM_KIDS_FAMILIES_CATEGORY_ID),
        ("locations[]", SAAM_LOCATION_ID),
    ]
    if page is not None and page > 1:
        params.append(("page", str(page)))
    return f"{SAAM_SEARCH_BASE_URL}?{urlencode(params)}"


SAAM_FILTERED_SEARCH_URL = build_saam_search_url()


async def load_saam_events_payload(
    *,
    timeout_ms: int = 90000,
    max_pages: int = SAAM_MAX_PAGE_GUARD,
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
            timezone_id=SAAM_TIMEZONE,
            viewport={"width": 1365, "height": 900},
            screen={"width": 1365, "height": 900},
            color_scheme="light",
        )
        await context.add_init_script(PLAYWRIGHT_INIT_SCRIPT)

        try:
            first_url = build_saam_search_url()
            first_html = await _fetch_saam_page_playwright(
                context=context,
                url=first_url,
                ready_selector="#results .azalea-event-teaser, #results .azalea-text-sm",
                timeout_ms=timeout_ms,
            )
            listing_pages[first_url] = first_html

            first_entries, total_results = _parse_listing_entries(first_html, list_url=first_url)
            page_count = _compute_page_count(
                total_results=total_results,
                first_page_count=len(first_entries),
                max_pages=max_pages,
            )
            print(
                "[saam-fetch] "
                f"first_page_entries={len(first_entries)} total_results={total_results} page_count={page_count}"
            )

            for page_number in range(2, page_count + 1):
                page_url = build_saam_search_url(page=page_number)
                print(f"[saam-fetch] listing page={page_number} url={page_url}")
                listing_pages[page_url] = await _fetch_saam_page_playwright(
                    context=context,
                    url=page_url,
                    ready_selector="#results .azalea-event-teaser, #results .azalea-text-sm",
                    timeout_ms=timeout_ms,
                )

            detail_urls = _collect_detail_urls(listing_pages)
            print(f"[saam-fetch] unique_detail_urls={len(detail_urls)}")
            for index, detail_url in enumerate(detail_urls, start=1):
                print(f"[saam-fetch] detail {index}/{len(detail_urls)} url={detail_url}")
                detail_pages[detail_url] = await _fetch_saam_page_playwright(
                    context=context,
                    url=detail_url,
                    ready_selector="main h1",
                    timeout_ms=timeout_ms,
                )
        finally:
            await context.close()
            await browser.close()

    return {
        "listing_pages": listing_pages,
        "detail_pages": detail_pages,
    }


def parse_saam_events_payload(payload: dict) -> list[ExtractedActivity]:
    listing_pages = payload.get("listing_pages") or {}
    detail_pages = payload.get("detail_pages") or {}

    entries_by_url: dict[str, SaamListingEntry] = {}
    for list_url, html in sorted(listing_pages.items()):
        for entry in _parse_listing_entries(html, list_url=list_url)[0]:
            entries_by_url.setdefault(entry.source_url, entry)

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, entry in sorted(entries_by_url.items()):
        detail = _parse_detail_html(detail_pages.get(source_url, ""))
        if not _should_include_event(entry=entry, detail=detail):
            continue

        start_at, end_at = _parse_listing_datetime(entry.display_time)
        if start_at is None:
            continue

        description = detail.description
        categories_blob = " | ".join(detail.categories)
        series_blob = " | ".join(detail.series)
        extra_parts = []
        if categories_blob:
            extra_parts.append(f"Categories: {categories_blob}")
        if series_blob:
            extra_parts.append(f"Series: {series_blob}")
        if detail.event_location:
            extra_parts.append(f"Event location: {detail.event_location}")
        full_description = _join_description(description, extra_parts)

        age_min, age_max = _parse_age_range(" ".join(filter(None, [entry.title, full_description])))
        pricing_text = detail.cost_text or entry.cost_text or full_description
        combined_text = _normalized_blob(
            entry.title,
            description,
            detail.cost_text,
            entry.cost_text,
            categories_blob,
            series_blob,
        )

        key = (source_url, entry.title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=entry.title,
                description=full_description,
                venue_name=SAAM_VENUE_NAME,
                location_text=_build_location_text(detail.event_location),
                city=SAAM_CITY,
                state=SAAM_STATE,
                activity_type=_infer_activity_type(entry.title, full_description),
                age_min=age_min,
                age_max=age_max,
                drop_in=_infer_drop_in(combined_text),
                registration_required=_infer_registration_required(combined_text),
                start_at=start_at,
                end_at=end_at,
                timezone=SAAM_TIMEZONE,
                **price_classification_kwargs(pricing_text, default_is_free=None),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


async def _fetch_saam_page_playwright(
    *,
    context: BrowserContext,
    url: str,
    ready_selector: str,
    timeout_ms: int,
) -> str:
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_selector(ready_selector, timeout=timeout_ms)
        await page.wait_for_timeout(700)
        return await page.content()
    finally:
        await page.close()


def _compute_page_count(*, total_results: int | None, first_page_count: int, max_pages: int) -> int:
    if first_page_count <= 0:
        return 1
    if total_results is None or total_results <= first_page_count:
        return 1
    estimated = math.ceil(total_results / max(first_page_count, SAAM_RESULTS_PER_PAGE))
    return max(1, min(max_pages, estimated))


def _collect_detail_urls(listing_pages: dict[str, str]) -> list[str]:
    detail_urls: list[str] = []
    seen: set[str] = set()
    for list_url, html in sorted(listing_pages.items()):
        for entry in _parse_listing_entries(html, list_url=list_url)[0]:
            if entry.source_url in seen:
                continue
            seen.add(entry.source_url)
            detail_urls.append(entry.source_url)
    return detail_urls


def _parse_listing_entries(html: str, *, list_url: str) -> tuple[list[SaamListingEntry], int | None]:
    soup = BeautifulSoup(html, "html.parser")
    entries: list[SaamListingEntry] = []
    seen_urls: set[str] = set()

    for teaser in soup.select("#results .azalea-event-teaser, .azalea-event-teaser"):
        title_node = teaser.select_one("a.azalea-heading-level-4.text-black[rel='bookmark']")
        if title_node is None:
            continue

        source_href = title_node.get("href")
        source_url = urljoin(list_url, source_href or "")
        if not source_url or source_url in seen_urls:
            continue
        seen_urls.add(source_url)

        title = _normalize_space(title_node.get_text(" ", strip=True))
        display_node = teaser.select_one(".azalea-heading-level-6")
        display_time = _normalize_space(display_node.get_text(" ", strip=True) if display_node else "")
        building_link = teaser.select_one('a[href*="/visit/"]')
        building_text = _normalize_space(building_link.get_text(" ", strip=True) if building_link else "")
        cost_nodes = teaser.select(".azalea-text-sm.text-tertiary-500")
        cost_text = _normalize_space(cost_nodes[-1].get_text(" ", strip=True) if cost_nodes else "")
        action_node = teaser.select_one(".azalea-push-button[href]")
        action_url = urljoin(list_url, action_node.get("href") or "") if action_node else None
        featured = teaser.select_one(".text-secondary-700") is not None

        if not title or not display_time:
            continue
        if building_text and SAAM_VENUE_NAME.lower() not in building_text.lower():
            continue

        entries.append(
            SaamListingEntry(
                list_url=list_url,
                source_url=source_url,
                title=title,
                display_time=display_time,
                building_text=building_text or None,
                cost_text=cost_text or None,
                action_url=action_url,
                featured=featured,
            )
        )

    result_count_match = RESULT_COUNT_RE.search(_normalize_space(soup.get_text(" ", strip=True)))
    total_results = int(result_count_match.group(1)) if result_count_match else None
    return entries, total_results


def _parse_detail_html(html: str) -> SaamDetailFields:
    if not html:
        return SaamDetailFields(
            description=None,
            cost_text=None,
            event_location=None,
            categories=(),
            series=(),
        )

    soup = BeautifulSoup(html, "html.parser")
    tombstone = _extract_tombstone_fields(soup)
    event_payload = _extract_event_json_ld(soup)

    description = _normalize_space(unescape(str(event_payload.get("description") or ""))) or None
    if not description:
        description = _extract_detail_description_fallback(soup)

    return SaamDetailFields(
        description=description,
        cost_text=tombstone.get("cost"),
        event_location=tombstone.get("event location"),
        categories=tombstone.get("categories_list", ()),
        series=tombstone.get("part of series_list", ()),
    )


def _extract_tombstone_fields(soup: BeautifulSoup) -> dict[str, str | tuple[str, ...]]:
    extracted: dict[str, str | tuple[str, ...]] = {}
    for row in soup.select("dl.tombstone > div"):
        dt = row.find("dt")
        dd = row.find("dd")
        if dt is None or dd is None:
            continue
        key = _normalize_space(dt.get_text(" ", strip=True)).lower()
        text_value = _normalize_space(dd.get_text(" ", strip=True))
        if text_value:
            extracted[key] = text_value
        list_values = tuple(
            dict.fromkeys(
                _normalize_space(node.get_text(" ", strip=True))
                for node in dd.select("li, a")
                if _normalize_space(node.get_text(" ", strip=True))
            )
        )
        if list_values:
            extracted[f"{key}_list"] = list_values
    return extracted


def _extract_event_json_ld(soup: BeautifulSoup) -> dict:
    for script in soup.select('script[type="application/ld+json"]'):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        candidates = data if isinstance(data, list) else [data]
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate.get("@type") == "Event":
                return candidate
    return {}


def _extract_detail_description_fallback(soup: BeautifulSoup) -> str | None:
    candidates: list[str] = []
    for node in soup.select("main .azalea-text-md"):
        text = _normalize_space(node.get_text(" ", strip=True))
        if len(text) >= 80:
            candidates.append(text)
    if not candidates:
        return None
    return max(candidates, key=len)


def _parse_listing_datetime(value: str) -> tuple[datetime | None, datetime | None]:
    normalized = _normalize_space(value)
    match = LISTING_DATETIME_RE.match(normalized)
    if not match:
        return None, None

    base_day = datetime.strptime(match.group("date"), "%B %d, %Y")
    times_text = match.group("times")

    range_match = TIME_RANGE_RE.search(times_text)
    if range_match:
        end_meridiem = _extract_meridiem(range_match.group("end"))
        start_time = _parse_time_token(range_match.group("start"), fallback_meridiem=end_meridiem)
        end_time = _parse_time_token(range_match.group("end"))
        if start_time is None:
            return None, None
        start_at = base_day.replace(
            hour=start_time.hour,
            minute=start_time.minute,
            second=0,
            microsecond=0,
        )
        end_at = None
        if end_time is not None:
            end_at = base_day.replace(
                hour=end_time.hour,
                minute=end_time.minute,
                second=0,
                microsecond=0,
            )
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(times_text)
    if single_match is None:
        return base_day.replace(hour=0, minute=0, second=0, microsecond=0), None

    start_time = _parse_time_token(single_match.group("time"))
    if start_time is None:
        return None, None
    start_at = base_day.replace(
        hour=start_time.hour,
        minute=start_time.minute,
        second=0,
        microsecond=0,
    )
    return start_at, None


def _parse_time_token(token: str, *, fallback_meridiem: str | None = None):
    cleaned = token.lower().replace(".", "").replace(" ", "")
    meridiem = _extract_meridiem(cleaned) or fallback_meridiem
    if meridiem is None:
        return None

    numeric = cleaned
    if numeric.endswith("am") or numeric.endswith("pm"):
        numeric = numeric[:-2]
    for fmt in ("%I:%M%p", "%I%p"):
        try:
            return datetime.strptime(f"{numeric}{meridiem}", fmt)
        except ValueError:
            continue
    return None


def _extract_meridiem(token: str) -> str | None:
    lowered = token.lower()
    if lowered.endswith("am"):
        return "am"
    if lowered.endswith("pm"):
        return "pm"
    return None


def _should_include_event(*, entry: SaamListingEntry, detail: SaamDetailFields) -> bool:
    categories_blob = " | ".join(detail.categories)
    series_blob = " | ".join(detail.series)
    combined = _normalized_blob(entry.title, detail.description, categories_blob, series_blob, entry.cost_text)

    has_youth_context = (
        "kids & families" in categories_blob.lower()
        or "family programs" in series_blob.lower()
        or "kids and families" in series_blob.lower()
        or any(marker in combined for marker in YOUTH_CONTEXT_MARKERS)
    )
    if not has_youth_context:
        return False

    has_activity = any(marker in combined for marker in ACTIVITY_MARKERS)
    has_talk = any(marker in combined for marker in TALK_MARKERS)
    has_exclusion = any(marker in combined for marker in STRONG_EXCLUSION_MARKERS)

    if has_activity or has_talk:
        return True
    if "family celebration" in combined:
        return True
    if has_exclusion:
        return False
    return True


def _infer_activity_type(title: str, description: str | None) -> str:
    combined = _normalized_blob(title, description)
    if " workshop " in combined or " lab " in combined or " class " in combined:
        return "workshop"
    if any(marker in combined for marker in TALK_MARKERS):
        return "talk"
    return "activity"


def _infer_drop_in(text: str) -> bool | None:
    if " drop-in " in text or " drop in " in text:
        return True
    return False


def _infer_registration_required(text: str) -> bool | None:
    if " registration required " in text or " tickets required " in text:
        return True
    if " sold out " in text:
        return True
    if " registration encouraged " in text or " registration recommended " in text:
        return False
    if " registration not required " in text:
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

    under_match = AGE_UNDER_RE.search(normalized)
    if under_match:
        return None, int(under_match.group(1))

    plus_match = AGE_PLUS_RE.search(normalized)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def _build_location_text(event_location: str | None) -> str:
    if event_location:
        return f"{event_location}, {SAAM_DEFAULT_LOCATION}"
    return SAAM_DEFAULT_LOCATION


def _join_description(description: str | None, extra_parts: list[str]) -> str | None:
    pieces = []
    if description:
        pieces.append(description)
    pieces.extend(part for part in extra_parts if part)
    if not pieces:
        return None
    return " | ".join(pieces)


def _normalized_blob(*parts: str | None) -> str:
    return f" {' '.join(_normalize_space(part or '') for part in parts if part).lower()} "


def _normalize_space(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())
