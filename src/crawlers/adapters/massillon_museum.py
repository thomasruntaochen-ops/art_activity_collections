import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import httpx

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import DEFAULT_HEADERS
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - environment-specific dependency
    async_playwright = None


MASSILLON_EVENTS_URL = "https://www.massillonmuseum.org/home/programs/massillon-museum-events-calendar"
MASSILLON_COLLECTION_URL = "https://collections.humanitix.com/massmutickets?widget=collection"
MASSILLON_VENUE_NAME = "Massillon Museum"
MASSILLON_CITY = "Massillon"
MASSILLON_STATE = "OH"
MASSILLON_LOCATION = "Massillon Museum, Massillon, OH"
LISTING_ITEM_RE = re.compile(
    r"^(?P<weekday>[A-Za-z]{3}),\s+"
    r"(?P<month>[A-Za-z]{3})\s+(?P<day>\d{1,2}),\s+"
    r"(?P<time>.+?)\s+EDT\s+"
    r"(?:(?:\+\s+\d+\s+dates)\s+)?"
    r"(?P<title>.+?)\s+Massillon Museum,",
    re.IGNORECASE,
)
WORD_AGE_RANGE_RE = re.compile(
    r"\bages?\s+(?P<min_word>[a-z]+|\d{1,2})\s+(?:through|to|-|–)\s+(?P<max_word>[a-z]+|\d{1,2})\b",
    re.IGNORECASE,
)
AGE_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
}
EXPLICIT_INCLUDE_MARKERS = (
    "story and art time",
    "do the mu",
    "ceramics",
    "cartooning",
    "floral resin bookmark",
    "littles lunch and learn",
    "sensory lab",
    "tactile tales",
    "wheel throwing",
    "water-soluble oil",
    "tots 'n' pots",
)
EXPLICIT_EXCLUDE_MARKERS = (
    "yoga",
    "writing",
    "history",
    "podcast",
    "pure potentiality",
    "concert",
)
PAID_CLASS_TITLE_MARKERS = (
    "ceramics open studio",
    "introduction to ceramics",
    "water-soluble oil",
    "wheel throwing",
    "floral resin bookmark",
    "cartooning workshop",
)


async def load_massillon_museum_payload() -> dict:
    collection_url = MASSILLON_COLLECTION_URL
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is required for Massillon Museum parsing. Install crawler extras and chromium first."
        )

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(MASSILLON_EVENTS_URL, wait_until="networkidle", timeout=120000)

        iframe = page.locator('iframe[src*="humanitix.com"]')
        if await iframe.count():
            discovered = await iframe.first.get_attribute("src")
            if discovered:
                collection_url = discovered

        await browser.close()

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        collection_html = await fetch_html(collection_url, referer=MASSILLON_EVENTS_URL, client=client)
        listing_items = _extract_listing_items(collection_html)
        detail_pages: dict[str, str] = {}
        for detail_url in listing_items:
            detail_pages[detail_url] = await fetch_html(detail_url, referer=collection_url, client=client)

    return {
        "collection_url": collection_url,
        "listing_items": listing_items,
        "detail_pages": detail_pages,
    }


def parse_massillon_museum_payload(payload: dict) -> list[ExtractedActivity]:
    listing_items = payload.get("listing_items") or {}
    detail_pages = payload.get("detail_pages") or {}
    current_year = datetime.now(ZoneInfo(NY_TIMEZONE)).year
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for detail_url, html in detail_pages.items():
        row = _build_row(
            html,
            detail_url=detail_url,
            listing_item=listing_items.get(detail_url),
            current_year=current_year,
            today=today,
        )
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class MassillonMuseumAdapter(BaseSourceAdapter):
    source_name = "massillon_museum_events"

    async def fetch(self) -> list[str]:
        payload = await load_massillon_museum_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_massillon_museum_payload(json.loads(payload))


def _extract_listing_items(collection_html: str) -> dict[str, dict]:
    soup = BeautifulSoup(collection_html, "html.parser")
    items: dict[str, dict] = _extract_json_ld_listing_items(soup)

    for anchor in soup.select('a[href^="https://events.humanitix.com/"]'):
        href = normalize_space(anchor.get("href"))
        text = normalize_space(anchor.get_text(" ", strip=True))
        if not href or "/tickets" in href or not text:
            continue
        match = LISTING_ITEM_RE.match(text)
        if match is None:
            continue
        title = normalize_space(match.group("title"))
        if not _listing_title_is_candidate(title):
            continue
        items[href] = {
            "title": title,
            "month_text": match.group("month"),
            "day_text": match.group("day"),
            "time_text": normalize_space(match.group("time")),
        }

    return items


def _extract_json_ld_listing_items(soup: BeautifulSoup) -> dict[str, dict]:
    items: dict[str, dict] = {}
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = normalize_space(script.get_text())
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue

        for event_obj in _iter_schema_events(data):
            href = normalize_space(event_obj.get("url"))
            title = normalize_space(event_obj.get("name"))
            if not href or not title or "/tickets" in href:
                continue
            if not _listing_title_is_candidate(title):
                continue
            items[href] = {
                "title": title,
                "start_iso": normalize_space(event_obj.get("startDate")),
                "end_iso": normalize_space(event_obj.get("endDate")),
                "description": _html_to_text(event_obj.get("description")),
                "location_text": _extract_schema_location_text(event_obj.get("location")),
            }
    return items


def _iter_schema_events(value: object) -> list[dict]:
    events: list[dict] = []
    if isinstance(value, dict):
        if value.get("@type") == "Event":
            events.append(value)
        for child in value.values():
            events.extend(_iter_schema_events(child))
    elif isinstance(value, list):
        for child in value:
            events.extend(_iter_schema_events(child))
    return events


def _build_row(
    html: str,
    *,
    detail_url: str,
    listing_item: dict | None,
    current_year: int,
    today,
) -> ExtractedActivity | None:
    if listing_item is None:
        return None

    soup = BeautifulSoup(html, "html.parser")
    title = normalize_space(
        (soup.find("meta", attrs={"property": "og:title"}) or {}).get("content")
        or (soup.find("title").get_text(" ", strip=True) if soup.find("title") else listing_item.get("title"))
    )
    description_text = normalize_space(
        (soup.find("meta", attrs={"property": "og:description"}) or {}).get("content")
        or (soup.find("meta", attrs={"name": "description"}) or {}).get("content")
        or listing_item.get("description")
        or ""
    )
    if not _listing_title_is_candidate(title) and not should_include_event(title=title, description=description_text):
        return None

    start_at, end_at = _parse_listing_item_datetimes(listing_item, current_year=current_year)
    if start_at is None or start_at.date() < today:
        return None

    body_text = normalize_space(soup.get_text(" ", strip=True))
    text_blob = join_non_empty([title, description_text, body_text])
    age_min, age_max = _parse_massillon_age_range(text_blob)
    full_description = join_non_empty([description_text])
    lowered_meta = normalize_space(
        (soup.find("meta", attrs={"name": "description"}) or {}).get("content")
        or description_text
    ).lower()
    activity_type = infer_activity_type(title, full_description)

    return ExtractedActivity(
        source_url=detail_url,
        title=title,
        description=full_description,
        venue_name=MASSILLON_VENUE_NAME,
        location_text=normalize_space(listing_item.get("location_text")) or MASSILLON_LOCATION,
        city=MASSILLON_CITY,
        state=MASSILLON_STATE,
        activity_type=activity_type,
        age_min=age_min,
        age_max=age_max,
        drop_in=False,
        registration_required=(
            "no registration required" not in lowered_meta
            and "no registration." not in lowered_meta
            and "no registration" not in lowered_meta
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        audience_segment=_infer_massillon_audience(
            title=title,
            description=description_text,
            source_url=detail_url,
            age_min=age_min,
            age_max=age_max,
            activity_type=activity_type,
        ),
        **_price_kwargs_for_massillon(title=title, text=text_blob),
    )


def _parse_listing_item_datetimes(
    listing_item: dict,
    *,
    current_year: int,
) -> tuple[datetime | None, datetime | None]:
    start_iso = normalize_space(listing_item.get("start_iso"))
    if start_iso:
        start_at = _parse_humanitix_datetime(start_iso)
        end_at = _parse_humanitix_datetime(listing_item.get("end_iso"))
        return start_at, end_at

    month_text = normalize_space(listing_item.get("month_text"))
    day_text = normalize_space(listing_item.get("day_text"))
    try:
        base_date = datetime.strptime(f"{month_text} {day_text} {current_year}", "%b %d %Y").date()
    except ValueError:
        return None, None
    return parse_time_range(base_date=base_date, time_text=listing_item.get("time_text"))


def _parse_humanitix_datetime(value: str | None) -> datetime | None:
    text = normalize_space(value)
    if not text:
        return None
    if re.search(r"[+-]\d{4}$", text):
        text = f"{text[:-2]}:{text[-2:]}"
    try:
        return parse_iso_datetime(text, timezone_name=NY_TIMEZONE)
    except ValueError:
        return None


def _extract_schema_location_text(location: object) -> str | None:
    if not isinstance(location, dict):
        return None
    name = normalize_space(location.get("name"))
    address = location.get("address")
    if isinstance(address, dict):
        address_text = join_non_empty(
            [
                normalize_space(address.get("streetAddress")),
                normalize_space(address.get("addressLocality")),
                normalize_space(address.get("addressRegion")),
                normalize_space(address.get("postalCode")),
            ]
        )
    else:
        address_text = normalize_space(address)
    return join_non_empty([name, address_text])


def _html_to_text(value: str | None) -> str | None:
    text = normalize_space(value)
    if not text:
        return None
    return normalize_space(BeautifulSoup(text, "html.parser").get_text(" ", strip=True))


def _listing_title_is_candidate(title: str) -> bool:
    title_text = normalize_space(title)
    if not title_text:
        return False
    lowered = title_text.lower()
    if any(marker in lowered for marker in EXPLICIT_EXCLUDE_MARKERS):
        return False
    if any(marker in lowered for marker in EXPLICIT_INCLUDE_MARKERS):
        return True
    return should_include_event(title=title_text)


def _parse_massillon_age_range(text: str | None) -> tuple[int | None, int | None]:
    age_min, age_max = parse_age_range(text)
    if age_min is not None or age_max is not None:
        return age_min, age_max

    match = WORD_AGE_RANGE_RE.search(text or "")
    if match is None:
        return None, None
    return _parse_age_word(match.group("min_word")), _parse_age_word(match.group("max_word"))


def _parse_age_word(value: str | None) -> int | None:
    normalized = normalize_space(value).lower()
    if normalized.isdigit():
        return int(normalized)
    return AGE_WORDS.get(normalized)


def _infer_massillon_audience(
    *,
    title: str | None,
    description: str | None,
    source_url: str | None,
    age_min: int | None,
    age_max: int | None,
    activity_type: str | None,
) -> str:
    blob = f" {normalize_space(join_non_empty([title, description, source_url])).lower()} "
    if " do the mu " in blob:
        return "all_ages"
    if any(marker in blob for marker in (" tots 'n' pots ", " littles lunch ", " tactile tales ", " sensory lab ")):
        return "kids"

    inferred = infer_audience_segment(
        title=title,
        description=description,
        source_url=source_url,
        age_min=age_min,
        age_max=age_max,
    )
    if inferred != "unknown":
        return inferred
    if activity_type in {"class", "workshop", "activity"}:
        return "adults"
    return "unknown"


def _price_kwargs_for_massillon(*, title: str | None, text: str | None) -> dict[str, bool | None | str]:
    title_blob = f" {normalize_space(title).lower()} "
    full_blob = f" {normalize_space(text).lower()} "
    kwargs = price_classification_kwargs(text, default_is_free=True)
    if kwargs["is_free"] is not None:
        return kwargs

    if any(marker in title_blob for marker in PAID_CLASS_TITLE_MARKERS) and " free " not in full_blob:
        return {"is_free": False, "free_verification_status": "inferred"}

    return {"is_free": True, "free_verification_status": "inferred"}
