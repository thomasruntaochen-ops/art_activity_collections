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
EXPLICIT_INCLUDE_MARKERS = (
    "story and art time",
    "do the mu",
    "ceramics",
    "wheel throwing",
    "water-soluble oil",
    "tots 'n' pots",
)
EXPLICIT_EXCLUDE_MARKERS = (
    "yoga",
    "writing",
    "history",
    "pure potentiality",
    "concert",
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
    items: dict[str, dict] = {}

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
        or ""
    )
    if not _listing_title_is_candidate(title) and not should_include_event(title=title, description=description_text):
        return None

    month_text = normalize_space(listing_item.get("month_text"))
    day_text = normalize_space(listing_item.get("day_text"))
    try:
        base_date = datetime.strptime(f"{month_text} {day_text} {current_year}", "%b %d %Y").date()
    except ValueError:
        return None
    start_at, end_at = parse_time_range(base_date=base_date, time_text=listing_item.get("time_text"))
    if start_at is None or start_at.date() < today:
        return None

    age_min, age_max = parse_age_range(join_non_empty([title, description_text]))
    full_description = join_non_empty([description_text])
    lowered_meta = normalize_space(
        (soup.find("meta", attrs={"name": "description"}) or {}).get("content")
    ).lower()

    return ExtractedActivity(
        source_url=detail_url,
        title=title,
        description=full_description,
        venue_name=MASSILLON_VENUE_NAME,
        location_text=MASSILLON_LOCATION,
        city=MASSILLON_CITY,
        state=MASSILLON_STATE,
        activity_type=infer_activity_type(title, full_description),
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
        **price_classification_kwargs(full_description),
    )


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
