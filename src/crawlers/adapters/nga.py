from __future__ import annotations

import asyncio
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urljoin
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from bs4 import BeautifulSoup

try:
    from playwright.async_api import BrowserContext
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    BrowserContext = None
    async_playwright = None

from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

NGA_CALENDAR_BASE_URL = "https://www.nga.gov/calendar?type[103021]=103021&type[103041]=103041&tab=all"
NGA_KIDS_CALENDAR_URL = (
    "https://www.nga.gov/calendar?type[103021]=103021&type[103041]=103041"
    "&audience[103001]=103001&tab=all"
)
NGA_TEENS_CALENDAR_URL = (
    "https://www.nga.gov/calendar?type[103021]=103021&type[103041]=103041"
    "&audience[103006]=103006&tab=all"
)

NGA_TIMEZONE = "America/New_York"
NGA_VENUE_NAME = "National Gallery of Art"
NGA_CITY = "Washington"
NGA_STATE = "DC"
NGA_DEFAULT_LOCATION = "Washington, DC"
NGA_REQUEST_DELAY_SECONDS = 0.35
NGA_LISTING_READY_SELECTOR = ".c-calendar__result-count, .views-row"
NGA_DETAIL_READY_SELECTOR = "section.c-event-header, h1.c-event-header__title"
NGA_MAX_FETCH_ATTEMPTS = 3
NGA_MAX_PAGE_GUARD = 12

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

CHALLENGE_MARKERS = (
    "performing security verification",
    "just a moment...",
    "this website uses a security service to protect against malicious bots",
    "ray id:",
)

EXCLUDED_MARKERS = (
    " admission ",
    " admissions ",
    " band ",
    " book launch ",
    " camp ",
    " camps ",
    " concert ",
    " concerts ",
    " dinner ",
    " exhibition ",
    " exhibitions ",
    " film ",
    " films ",
    " fundraising ",
    " free night ",
    " guided tour ",
    " guided tours ",
    " jazz ",
    " meditation ",
    " mindfulness ",
    " music ",
    " open house ",
    " orchestra ",
    " performance ",
    " performances ",
    " poem ",
    " poetry ",
    " pop up library ",
    " reading ",
    " reception ",
    " registration desk ",
    " storytime ",
    " story time ",
    " tai chi ",
    " tour ",
    " tours ",
    " tv ",
    " writing ",
    " yoga ",
)
YOUTH_MARKERS = (
    " child ",
    " children ",
    " families ",
    " family ",
    " kid friendly ",
    " kids ",
    " teen ",
    " teens ",
    " youth ",
)
HANDS_ON_MARKERS = (
    " art-making ",
    " art making ",
    " create ",
    " hands on ",
    " hands-on ",
    " playtime ",
    " sketchbook ",
    " studio ",
    " workshop ",
    " workshops ",
)
TALK_MARKERS = (
    " conversation ",
    " conversations ",
    " discussion ",
    " discussions ",
    " lecture ",
    " lectures ",
    " talk ",
    " talks ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
RESULT_COUNT_RE = re.compile(r"(\d+)\s+results\s+found", re.IGNORECASE)

NGA_AUDIENCE_URLS = {
    "kids": NGA_KIDS_CALENDAR_URL,
    "teens": NGA_TEENS_CALENDAR_URL,
}


@dataclass(slots=True)
class NgaListingEntry:
    audience: str
    list_url: str
    source_url: str
    title: str
    category: str | None
    location_text: str | None
    start_at: datetime | None
    end_at: datetime | None
    registration_required: bool | None
    tags: tuple[str, ...]


@dataclass(slots=True)
class NgaCalendarPayload:
    audience: str
    listing_entries: list[NgaListingEntry]
    detail_pages: dict[str, str]


async def load_nga_calendar_payload(
    *,
    audience: str = "both",
    timeout_ms: int = 90000,
) -> NgaCalendarPayload:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    selected_audiences = _normalize_audience(audience)
    listing_entries_by_url: dict[str, NgaListingEntry] = {}
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
            timezone_id=NGA_TIMEZONE,
            viewport={"width": 1365, "height": 900},
            screen={"width": 1365, "height": 900},
            color_scheme="light",
        )
        await context.add_init_script(PLAYWRIGHT_INIT_SCRIPT)

        try:
            for audience_name in selected_audiences:
                base_url = NGA_AUDIENCE_URLS[audience_name]
                first_page_url = _with_page_number(base_url, 0)
                print(f"[nga-fetch] audience={audience_name} page=1")
                first_html = await fetch_nga_page_playwright(
                    context=context,
                    url=first_page_url,
                    ready_selector=NGA_LISTING_READY_SELECTOR,
                    timeout_ms=timeout_ms,
                )

                page_count = _discover_listing_page_count(first_html)
                page_count = min(page_count, NGA_MAX_PAGE_GUARD)
                print(f"[nga-fetch] audience={audience_name} discovered_pages={page_count}")

                for page_index in range(page_count):
                    page_url = _with_page_number(base_url, page_index)
                    if page_index == 0:
                        html = first_html
                    else:
                        print(f"[nga-fetch] audience={audience_name} page={page_index + 1}")
                        html = await fetch_nga_page_playwright(
                            context=context,
                            url=page_url,
                            ready_selector=NGA_LISTING_READY_SELECTOR,
                            timeout_ms=timeout_ms,
                        )
                    for entry in _parse_listing_entries(html=html, list_url=page_url, audience=audience_name):
                        listing_entries_by_url.setdefault(entry.source_url, entry)

                await asyncio.sleep(NGA_REQUEST_DELAY_SECONDS)

            detail_urls = [
                source_url
                for source_url, entry in listing_entries_by_url.items()
                if _should_include_event(
                    title=entry.title,
                    category=entry.category,
                    description=None,
                    tags=entry.tags,
                )
            ]
            print(f"[nga-fetch] unique_detail_urls={len(detail_urls)}")
            for index, detail_url in enumerate(detail_urls, start=1):
                print(f"[nga-fetch] detail {index}/{len(detail_urls)}")
                try:
                    detail_pages[detail_url] = await fetch_nga_page_playwright(
                        context=context,
                        url=detail_url,
                        ready_selector=NGA_DETAIL_READY_SELECTOR,
                        timeout_ms=timeout_ms,
                    )
                except Exception as exc:
                    print(f"[nga-fetch] detail fetch failed url={detail_url}: {exc}")
                await asyncio.sleep(NGA_REQUEST_DELAY_SECONDS)
        finally:
            await context.close()
            await browser.close()

    return NgaCalendarPayload(
        audience=audience,
        listing_entries=list(listing_entries_by_url.values()),
        detail_pages=detail_pages,
    )


async def fetch_nga_page_playwright(
    *,
    context: BrowserContext,
    url: str,
    ready_selector: str,
    timeout_ms: int = 90000,
    max_attempts: int = NGA_MAX_FETCH_ATTEMPTS,
) -> str:
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        page = await context.new_page()
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.mouse.move(220 + attempt * 15, 180 + attempt * 20)
            try:
                await page.wait_for_selector(ready_selector, timeout=20000)
            except Exception:
                pass
            await page.wait_for_timeout(2000)

            title = _normalize_space(await page.title())
            body_text = _normalize_space(await page.locator("body").inner_text())
            if _looks_like_challenge(title=title, body_text=body_text):
                raise RuntimeError(f"Cloudflare challenge remained active for {url}")

            status = response.status if response is not None else None
            if status is not None and status >= 400:
                raise RuntimeError(f"Unexpected status={status} for {url}")

            return await page.content()
        except Exception as exc:
            last_error = exc
            if attempt < max_attempts:
                await asyncio.sleep(float(attempt))
                continue
        finally:
            await page.close()

    if last_error is not None:
        raise RuntimeError(f"Unable to fetch NGA page: {url}") from last_error
    raise RuntimeError(f"Unable to fetch NGA page: {url}")


def parse_nga_calendar_payload(payload: NgaCalendarPayload) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for listing_entry in payload.listing_entries:
        detail_html = payload.detail_pages.get(listing_entry.source_url)
        item = _build_row_from_detail_html(listing_entry=listing_entry, detail_html=detail_html)
        if item is None:
            item = _build_row_from_listing(listing_entry)
        if item is None:
            continue

        key = (item.source_url, item.title, item.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(item)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _build_row_from_detail_html(
    listing_entry: NgaListingEntry,
    detail_html: str | None,
) -> ExtractedActivity | None:
    if not detail_html:
        return None

    soup = BeautifulSoup(detail_html, "html.parser")
    header = soup.select_one("section.c-event-header")
    event_obj = _extract_event_json_ld(soup)

    title = _normalize_space(
        _text_of(header.select_one("h1.c-event-header__title") if header else None)
        or (event_obj.get("name") if isinstance(event_obj, dict) else None)
        or listing_entry.title
    )
    category = _normalize_space(
        _text_of(header.select_one("span.c-event-header__eyebrow") if header else None)
        or listing_entry.category
    )
    detail_tags = _extract_tag_texts(header)
    tags = tuple(detail_tags) if detail_tags else listing_entry.tags
    description = _extract_detail_description(header, soup)
    header_location = _normalize_space(
        _text_of(header.select_one(".c-event-header__location-text") if header else None)
    )
    detail_location = header_location or listing_entry.location_text
    registration_required = _detail_registration_required(header)
    if registration_required is None:
        registration_required = listing_entry.registration_required

    start_at = _parse_datetime_value(event_obj.get("startDate") if isinstance(event_obj, dict) else None)
    end_at = _parse_datetime_value(event_obj.get("endDate") if isinstance(event_obj, dict) else None)
    if start_at is None:
        start_at = listing_entry.start_at
    if end_at is None:
        end_at = listing_entry.end_at
    if start_at is None:
        return None

    if not _should_include_event(
        title=title,
        category=category,
        description=description,
        tags=tags,
    ):
        return None

    description_parts: list[str] = []
    if description:
        description_parts.append(description)
    if detail_location:
        description_parts.append(f"Location: {detail_location}")
    if tags:
        description_parts.append(f"Tags: {', '.join(tags)}")
    full_description = " | ".join(description_parts) if description_parts else None
    age_min, age_max = _parse_age_range(" ".join(filter(None, [title, description])))
    price_amount = _extract_offer_amount(event_obj)
    activity_type = _infer_activity_type(category=category, title=title, description=description)
    searchable = _searchable_blob(title, category, description, tags)

    return ExtractedActivity(
        source_url=listing_entry.source_url,
        title=title,
        description=full_description,
        venue_name=NGA_VENUE_NAME,
        location_text=NGA_DEFAULT_LOCATION,
        city=NGA_CITY,
        state=NGA_STATE,
        activity_type=activity_type,
        age_min=age_min,
        age_max=age_max,
        drop_in=_infer_drop_in(searchable),
        registration_required=registration_required,
        start_at=start_at,
        end_at=end_at,
        timezone=NGA_TIMEZONE,
        **price_classification_kwargs_from_amount(
            price_amount,
            text=full_description,
            default_is_free=True,
        ),
    )


def _build_row_from_listing(listing_entry: NgaListingEntry) -> ExtractedActivity | None:
    if listing_entry.start_at is None:
        return None

    if not _should_include_event(
        title=listing_entry.title,
        category=listing_entry.category,
        description=None,
        tags=listing_entry.tags,
    ):
        return None

    description_parts: list[str] = []
    if listing_entry.location_text:
        description_parts.append(f"Location: {listing_entry.location_text}")
    if listing_entry.tags:
        description_parts.append(f"Tags: {', '.join(listing_entry.tags)}")
    full_description = " | ".join(description_parts) if description_parts else None

    return ExtractedActivity(
        source_url=listing_entry.source_url,
        title=listing_entry.title,
        description=full_description,
        venue_name=NGA_VENUE_NAME,
        location_text=NGA_DEFAULT_LOCATION,
        city=NGA_CITY,
        state=NGA_STATE,
        activity_type=_infer_activity_type(
            category=listing_entry.category,
            title=listing_entry.title,
            description=full_description,
        ),
        age_min=None,
        age_max=None,
        drop_in=_infer_drop_in(_searchable_blob(listing_entry.title, listing_entry.category, full_description, listing_entry.tags)),
        registration_required=listing_entry.registration_required,
        start_at=listing_entry.start_at,
        end_at=listing_entry.end_at,
        timezone=NGA_TIMEZONE,
        free_verification_status="inferred",
        is_free=True,
    )


def _normalize_audience(audience: str) -> list[str]:
    normalized = _normalize_space(audience).lower()
    if normalized == "both":
        return ["kids", "teens"]
    if normalized in NGA_AUDIENCE_URLS:
        return [normalized]
    raise ValueError(f"Unsupported audience: {audience}")


def _discover_listing_page_count(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    result_text = _normalize_space(_text_of(soup.select_one(".c-calendar__result-count")))
    match = RESULT_COUNT_RE.search(result_text)
    total_results = int(match.group(1)) if match else 0
    page_size = len(soup.select("div.views-row"))
    if total_results <= 0 or page_size <= 0:
        return 1
    return max(1, math.ceil(total_results / page_size))


def _parse_listing_entries(
    *,
    html: str,
    list_url: str,
    audience: str,
) -> list[NgaListingEntry]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[NgaListingEntry] = []

    for view_row in soup.select("div.views-row"):
        for card in view_row.select("div.c-event-list-item--image"):
            title_link = card.select_one("a.c-event-list-item__title[href]")
            if title_link is None:
                continue

            href = _normalize_space(title_link.get("href"))
            if not href or "evd=" not in href:
                continue

            title = _normalize_space(
                _text_of(title_link.select_one(".u-hover-list__title-line"))
                or _text_of(title_link.select_one("h4"))
                or _text_of(title_link.select_one("h5"))
                or _text_of(title_link)
            )
            category = _normalize_space(_text_of(title_link.select_one(".f-text--eyebrow")))

            location_text: str | None = None
            for paragraph in title_link.select("p.f-text--para-s"):
                paragraph_text = _normalize_space(_text_of(paragraph))
                if paragraph_text:
                    location_text = paragraph_text
                    break

            time_values = [
                _parse_datetime_value(node.get("datetime"))
                for node in card.select(".c-event-header__event-date time[datetime]")
            ]
            start_at = time_values[0] if time_values else None
            end_at = time_values[1] if len(time_values) > 1 else None
            registration_text = _normalize_space(_text_of(card.select_one(".c-event-list-item__registration")))
            tags = tuple(_extract_tag_texts(card))

            rows.append(
                NgaListingEntry(
                    audience=audience,
                    list_url=list_url,
                    source_url=urljoin(list_url, href),
                    title=title,
                    category=category,
                    location_text=location_text,
                    start_at=start_at,
                    end_at=end_at,
                    registration_required=("registration required" in registration_text.lower() if registration_text else None),
                    tags=tags,
                )
            )

    return rows


def _extract_event_json_ld(soup: BeautifulSoup) -> dict:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = (script.string or script.get_text() or "").strip()
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        for event_obj in _iter_event_objects(data):
            return event_obj
    return {}


def _iter_event_objects(value: object) -> list[dict]:
    stack = [value]
    events: list[dict] = []

    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            current_type = current.get("@type")
            if current_type == "Event" or (
                isinstance(current_type, list) and "Event" in current_type
            ):
                events.append(current)
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)

    return events


def _extract_detail_description(header: object, soup: BeautifulSoup) -> str | None:
    header_text = _normalize_space(_text_of(header.select_one(".c-event-header__description")) if header else None)
    if header_text:
        return header_text

    meta_description = soup.select_one('meta[name="description"]')
    meta_text = _normalize_space(meta_description.get("content") if meta_description else None)
    if meta_text:
        return meta_text

    event_obj = _extract_event_json_ld(soup)
    if isinstance(event_obj, dict):
        description = _normalize_space(event_obj.get("description"))
        if description:
            return description

    return None


def _extract_offer_amount(event_obj: dict | None) -> Decimal | None:
    if not isinstance(event_obj, dict):
        return None

    offers = event_obj.get("offers")
    if isinstance(offers, list):
        for offer in offers:
            amount = _coerce_decimal(offer.get("price") if isinstance(offer, dict) else None)
            if amount is not None:
                return amount
        return None

    if isinstance(offers, dict):
        return _coerce_decimal(offers.get("price"))

    return None


def _extract_tag_texts(node: object) -> list[str]:
    if node is None:
        return []

    seen: set[str] = set()
    tags: list[str] = []
    for item in node.select(".c-tag-list li"):
        tag_text = _normalize_space(_text_of(item))
        if not tag_text:
            continue
        if tag_text in seen:
            continue
        seen.add(tag_text)
        tags.append(tag_text)
    return tags


def _detail_registration_required(header: object) -> bool | None:
    if header is None:
        return None

    registration_node = header.select_one(".c-event-register__description")
    if registration_node is not None:
        registration_text = _searchable_blob(_text_of(registration_node))
        if " registration is not required " in registration_text:
            return False
        if " registration required " in registration_text:
            return True

    header_text = _searchable_blob(_text_of(header))
    if " registration is not required " in header_text:
        return False
    if " registration required " in header_text:
        return True
    return None


def _should_include_event(
    *,
    title: str | None,
    category: str | None,
    description: str | None,
    tags: tuple[str, ...] | list[str] | None,
) -> bool:
    blob = _searchable_blob(title, category, description, tags or ())
    if not blob:
        return False

    has_youth_focus = any(marker in blob for marker in YOUTH_MARKERS)
    if not has_youth_focus:
        return False

    if any(marker in blob for marker in EXCLUDED_MARKERS):
        return False

    normalized_category = _normalize_space(category).lower()
    if normalized_category in {"hands-on activities", "talks & conversations"}:
        return True

    if any(marker in blob for marker in HANDS_ON_MARKERS):
        return True
    if any(marker in blob for marker in TALK_MARKERS):
        return True

    return False


def _infer_activity_type(
    *,
    category: str | None,
    title: str | None,
    description: str | None,
) -> str:
    normalized_category = _normalize_space(category).lower()
    blob = _searchable_blob(category, title, description)

    if normalized_category == "talks & conversations" or any(marker in blob for marker in TALK_MARKERS):
        return "talk"
    if " playtime " in blob:
        return "activity"
    if normalized_category == "hands-on activities" or any(marker in blob for marker in HANDS_ON_MARKERS):
        return "workshop"
    return "activity"


def _infer_drop_in(searchable_blob: str) -> bool | None:
    if " registration is not required " in searchable_blob:
        return True
    if " drop-in " in searchable_blob or " drop in " in searchable_blob:
        return True
    return None


def _parse_age_range(text: str | None) -> tuple[int | None, int | None]:
    normalized = _normalize_space(text)
    if not normalized:
        return (None, None)

    match = AGE_RANGE_RE.search(normalized)
    if match:
        return (int(match.group(1)), int(match.group(2)))

    match = AGE_PLUS_RE.search(normalized)
    if match:
        return (int(match.group(1)), None)

    return (None, None)


def _parse_datetime_value(value: object) -> datetime | None:
    if not value:
        return None
    text = _normalize_space(str(value))
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _coerce_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = _normalize_space(str(value))
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _with_page_number(base_url: str, page_number: int) -> str:
    parsed = urlsplit(base_url)
    query_pairs = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key != "page"]
    query_pairs.append(("page", str(page_number)))
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query_pairs),
            parsed.fragment,
        )
    )


def _looks_like_challenge(*, title: str, body_text: str) -> bool:
    challenge_blob = _searchable_blob(title, body_text[:1200])
    return any(marker in challenge_blob for marker in CHALLENGE_MARKERS)


def _searchable_blob(*parts: object) -> str:
    values: list[str] = []
    for part in parts:
        if part is None:
            continue
        if isinstance(part, (tuple, list, set)):
            values.extend(_normalize_space(str(item)) for item in part if item is not None)
        else:
            values.append(_normalize_space(str(part)))
    normalized = " ".join(value for value in values if value)
    if not normalized:
        return ""
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized.lower()).strip()
    return f" {normalized} " if normalized else ""


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def _text_of(node: object) -> str:
    if node is None:
        return ""
    if hasattr(node, "get_text"):
        return _normalize_space(node.get_text(" ", strip=True))
    return _normalize_space(node)
