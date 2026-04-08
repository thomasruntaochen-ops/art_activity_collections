import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - environment-specific dependency
    async_playwright = None

LASM_CALENDAR_URL = "https://www.lasm.org/calendar/"
CT_TIMEZONE = "America/Chicago"
LASM_VENUE_NAME = "Louisiana Art & Science Museum"
LASM_CITY = "Baton Rouge"
LASM_STATE = "LA"
LASM_LOCATION = "Louisiana Art & Science Museum, Baton Rouge, LA"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " activities ",
    " art making ",
    " class ",
    " classes ",
    " conversation ",
    " discussion ",
    " hands on ",
    " hands-on ",
    " kids lab ",
    " lab ",
    " lecture ",
    " lectures ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
WEAK_INCLUDE_PATTERNS = (
    " child ",
    " children ",
    " family ",
    " families ",
    " kid ",
    " kids ",
    " youth ",
)
NON_OVERRIDE_REJECT_PATTERNS = (
    " camp ",
    " camps ",
    " yoga ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " concert ",
    " concerts ",
    " film ",
    " films ",
    " planetarium ",
    " reception ",
    " show ",
    " shows ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
    " on-site the day ",
)
REGISTER_PATTERNS = (
    " preregister ",
    " pre-register ",
    " register ",
    " registration ",
)


async def fetch_lasm_calendar_page(
    url: str = LASM_CALENDAR_URL,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                if async_playwright is not None and attempt == max_attempts:
                    return await _fetch_lasm_page_playwright(url)
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            if response.status_code < 400:
                return response.text

            if response.status_code in (403, 429) and async_playwright is not None:
                return await _fetch_lasm_page_playwright(url)
            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch LASM calendar page") from last_exception
    raise RuntimeError("Unable to fetch LASM calendar page after retries")


async def _fetch_lasm_page_playwright(url: str) -> str:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is required for LASM fallback parsing. "
            "Install crawler extras and chromium first."
        )
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=120000)
        try:
            await page.wait_for_selector("article.mec-event-article", timeout=20000)
        except Exception:
            await page.wait_for_timeout(3000)
        html = await page.content()
        await browser.close()
    return html


class LasmCalendarAdapter(BaseSourceAdapter):
    source_name = "lasm_calendar"

    def __init__(self, url: str = LASM_CALENDAR_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_lasm_calendar_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_lasm_events_html(payload, list_url=self.url)


def parse_lasm_events_html(
    html: str,
    *,
    list_url: str,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    today = datetime.now(ZoneInfo(CT_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for section in soup.select(".mec-calendar-events-sec[data-mec-cell]"):
        cell = _normalize_space(section.get("data-mec-cell"))
        if len(cell) != 8 or not cell.isdigit():
            continue
        base_date = datetime.strptime(cell, "%Y%m%d").date()
        for article in section.select("article.mec-event-article"):
            row = _build_row(article, base_date=base_date, list_url=list_url)
            if row is None:
                continue
            if row.start_at.date() < today:
                continue
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _build_row(article: BeautifulSoup, *, base_date, list_url: str) -> ExtractedActivity | None:
    anchor = article.select_one(".mec-event-title a[href]")
    if anchor is None:
        return None

    title = _normalize_space(anchor.get_text(" ", strip=True))
    description = _normalize_space(
        article.select_one(".event_calendar_description").get_text(" ", strip=True)
        if article.select_one(".event_calendar_description")
        else ""
    )
    category_text = _normalize_space(
        article.select_one(".event_calendar_category").get_text(" ", strip=True)
        if article.select_one(".event_calendar_category")
        else ""
    )
    location_text = _normalize_space(
        article.select_one(".mec-event-loc-place").get_text(" ", strip=True)
        if article.select_one(".mec-event-loc-place")
        else ""
    ) or LASM_LOCATION
    time_text = _normalize_space(
        article.select_one(".mec-event-time").get_text(" ", strip=True)
        if article.select_one(".mec-event-time")
        else ""
    )
    start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
    if start_at is None:
        return None

    source_url = urljoin(list_url, anchor.get("href") or "")
    token_blob = _searchable_blob(" ".join([title, description, category_text, location_text]))
    title_blob = _searchable_blob(title)
    if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
        return None

    full_description = _join_non_empty(
        [
            description,
            category_text and f"Category: {category_text}",
            location_text and f"Location: {location_text}",
        ]
    )
    age_min, age_max = parse_age_range(" ".join(part for part in [title, description] if part))

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=LASM_VENUE_NAME,
        location_text=location_text,
        city=LASM_CITY,
        state=LASM_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=(
            not any(pattern in token_blob for pattern in DROP_IN_PATTERNS)
            and any(pattern in token_blob for pattern in REGISTER_PATTERNS)
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=CT_TIMEZONE,
        **price_classification_kwargs(description),
    )


def _should_keep_event(*, title_blob: str, token_blob: str) -> bool:
    if any(pattern in title_blob for pattern in NON_OVERRIDE_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in NON_OVERRIDE_REJECT_PATTERNS):
        return False

    strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    weak_include = any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS)
    if not strong_include and not weak_include:
        return False

    if any(pattern in title_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_include:
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_include:
        return False

    return True


def _infer_activity_type(token_blob: str) -> str:
    if any(pattern in token_blob for pattern in (" conversation ", " discussion ", " lecture ", " talk ")):
        return "lecture"
    return "workshop"


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _join_non_empty(parts: list[str | None]) -> str | None:
    cleaned = [_normalize_space(part) for part in parts if _normalize_space(part)]
    if not cleaned:
        return None
    return " | ".join(cleaned)


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())
