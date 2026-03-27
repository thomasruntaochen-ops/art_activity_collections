import json
import re
from datetime import date
from datetime import datetime

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - environment-specific dependency
    async_playwright = None


BUTLER_EVENTS_URL = "https://butlerart.com/programs/"
BUTLER_VENUE_NAME = "The Butler Institute of American Art"
BUTLER_CITY = "Youngstown"
BUTLER_STATE = "OH"
BUTLER_LOCATION = "The Butler Institute of American Art, Youngstown, OH"
TIME_RANGE_RE = re.compile(r"TIME\s+(.+?)(?:LOCATION|More Information|Registration|AMERICA 250)", re.IGNORECASE)
AGE_RE = re.compile(r"ages?\s+([0-9]{1,2})(?:\s*-\s*([0-9]{1,2}))?", re.IGNORECASE)


async def load_butler_institute_of_american_art_payload() -> dict:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is required for Butler Institute parsing. Install crawler extras and chromium first."
        )

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(BUTLER_EVENTS_URL, wait_until="networkidle", timeout=120000)

        articles = page.locator(".mec-event-article")
        item_count = await articles.count()
        items: list[dict] = []
        for index in range(item_count):
            article = articles.nth(index)
            title = normalize_space(await article.locator(".mec-event-title").inner_text())
            href = await article.locator(".mec-event-title a").get_attribute("href")
            date_text = normalize_space(await article.locator(".mec-event-date").inner_text())
            location_text = normalize_space(await article.locator(".mec-grid-event-location").inner_text())

            detail_text = None
            if href and _title_is_candidate(title):
                await page.goto(href, wait_until="networkidle", timeout=120000)
                detail_text = normalize_space(await page.locator("body").inner_text())
                await page.goto(BUTLER_EVENTS_URL, wait_until="networkidle", timeout=120000)
                articles = page.locator(".mec-event-article")

            items.append(
                {
                    "title": title,
                    "source_url": href,
                    "date_text": date_text,
                    "location_text": location_text,
                    "detail_text": detail_text,
                }
            )

        await browser.close()
        return {"items": items}


def parse_butler_institute_of_american_art_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    today = datetime.now().date()

    for item in payload.get("items", []):
        row = _build_row(item, today=today)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class ButlerInstituteOfAmericanArtAdapter(BaseSourceAdapter):
    source_name = "butler_institute_of_american_art_events"

    async def fetch(self) -> list[str]:
        payload = await load_butler_institute_of_american_art_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_butler_institute_of_american_art_payload(json.loads(payload))


def _build_row(item: dict, *, today: date) -> ExtractedActivity | None:
    title = normalize_space(item.get("title"))
    detail_text = _clean_detail_text(title=title, raw_text=item.get("detail_text"))
    detail_lower = detail_text.lower()
    explicit_include = any(
        marker in detail_lower
        for marker in ("art activity", "art-making", "art making", "artful fun")
    )
    if not (should_include_event(title=title, description=detail_text, category=None) or explicit_include):
        return None
    if any(marker in title.lower() for marker in ("film", "music", "meditation", "sensory", "senior art & learn")):
        return None

    base_date = datetime.strptime(normalize_space(item["date_text"]), "%d %B %Y").date()
    time_text = _extract_time_text(detail_text)
    start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
    if start_at is None or start_at.date() < today:
        return None

    age_min = None
    age_max = None
    age_match = AGE_RE.search(detail_text or "")
    if age_match:
        age_min = int(age_match.group(1))
        age_max = int(age_match.group(2)) if age_match.group(2) else None

    full_description = join_non_empty([detail_text])
    return ExtractedActivity(
        source_url=item["source_url"],
        title=title,
        description=full_description,
        venue_name=BUTLER_VENUE_NAME,
        location_text=normalize_space(item.get("location_text")) or BUTLER_LOCATION,
        city=BUTLER_CITY,
        state=BUTLER_STATE,
        activity_type=infer_activity_type(title, full_description),
        age_min=age_min,
        age_max=age_max,
        drop_in=False,
        registration_required="register" in (detail_text or "").lower(),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **price_classification_kwargs(full_description),
    )


def _title_is_candidate(title: str) -> bool:
    lowered = title.lower()
    return any(
        marker in lowered
        for marker in ("art classes", "stroller art", "young friends", "family days", "art")
    )


def _extract_time_text(detail_text: str | None) -> str | None:
    if not detail_text:
        return None
    match = TIME_RANGE_RE.search(detail_text)
    if match is None:
        return None
    value = normalize_space(match.group(1))
    return None if value.lower() == "all day" else value


def _clean_detail_text(*, title: str, raw_text: str | None) -> str:
    text = normalize_space(raw_text)
    if not text:
        return ""

    start_index = text.lower().find(title.lower())
    if start_index >= 0:
        text = text[start_index:]

    stop_markers = (
        "AMERICA 250 PANEL DISCUSSION",
        "Phone: (330) 743-1107",
        "Menu Home About Exhibitions",
        "Disclaimer: The Butler Institute of American Art aims",
    )
    for marker in stop_markers:
        stop_index = text.find(marker)
        if stop_index >= 0:
            text = text[:stop_index]
            break

    return normalize_space(text)
