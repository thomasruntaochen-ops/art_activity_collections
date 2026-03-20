import re
from datetime import datetime
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

CLARK_EVENTS_URL = "https://events.clarkart.edu/"
CLARK_TIMEZONE = "America/New_York"
CLARK_VENUE_NAME = "The Clark Art Institute"
CLARK_CITY = "Williamstown"
CLARK_STATE = "MA"
CLARK_DEFAULT_LOCATION = "The Clark Art Institute, 225 South Street, Williamstown, MA 01267"
CLARK_DEFAULT_SCROLL_ROUNDS = 8

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CLARK_EVENTS_URL,
}

CHALLENGE_MARKERS = (
    "Just a moment...",
    "Enable JavaScript and cookies to continue",
    "managed challenge",
)
LISTING_MARKERS = (
    "Clark Events",
    'id="event_list"',
    'id="available_results"',
    '<div class="details">',
)
EXCLUDED_TYPES = {
    "Film",
    "Music",
    "Performing Arts",
}
EXCLUDED_TITLE_MARKERS = (
    "concert",
    "screening",
    "soundscape",
)

SINGLE_DATE_RE = re.compile(r"^(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})$")
DATE_RANGE_RE = re.compile(
    r"^(?P<start_month>[A-Za-z]+)\s+(?P<start_day>\d{1,2})\s*[–-]\s*"
    r"(?:(?P<end_month>[A-Za-z]+)\s+)?(?P<end_day>\d{1,2}),\s*(?P<year>\d{4})$"
)


def build_clark_events_url() -> str:
    return CLARK_EVENTS_URL


async def load_clark_events_payload(
    *,
    scroll_rounds: int = CLARK_DEFAULT_SCROLL_ROUNDS,
    timeout_ms: int = 90000,
) -> dict:
    html = await _load_clark_events_html(
        url=CLARK_EVENTS_URL,
        scroll_rounds=scroll_rounds,
        timeout_ms=timeout_ms,
    )
    return {
        "pages": [
            {
                "url": CLARK_EVENTS_URL,
                "html": html,
            }
        ]
    }


async def _load_clark_events_html(
    *,
    url: str,
    scroll_rounds: int,
    timeout_ms: int,
) -> str:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install extras and browsers: "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="en-US",
            user_agent=DEFAULT_HEADERS["User-Agent"],
        )
        try:
            return await fetch_clark_page_playwright(
                context=context,
                url=url,
                scroll_rounds=scroll_rounds,
                timeout_ms=timeout_ms,
            )
        finally:
            await context.close()
            await browser.close()


async def fetch_clark_page_playwright(
    *,
    context: BrowserContext,
    url: str,
    scroll_rounds: int = CLARK_DEFAULT_SCROLL_ROUNDS,
    timeout_ms: int = 90000,
    max_attempts: int = 2,
) -> str:
    for attempt in range(1, max_attempts + 1):
        page = await context.new_page()
        try:
            print(f"[clark-fetch] Playwright attempt {attempt}/{max_attempts}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            for wait_ms in (5000, 8000):
                await page.wait_for_timeout(wait_ms)
                html = await page.content()
                if _looks_like_clark_events_html(html):
                    break
            else:
                html = await page.content()

            if not _looks_like_clark_events_html(html):
                continue

            stable_rounds = 0
            previous_count = -1
            target_count = await _target_results_count(page)
            for _ in range(max(scroll_rounds, 1)):
                current_count = await page.locator("#event_list > div").count()
                if target_count is not None and current_count >= target_count:
                    break
                if current_count == previous_count:
                    stable_rounds += 1
                    if stable_rounds >= 2:
                        break
                else:
                    stable_rounds = 0
                previous_count = current_count
                await page.mouse.wheel(0, 7000)
                await page.wait_for_timeout(3500)

            html = await page.content()
            if _looks_like_clark_events_html(html):
                return html
        finally:
            await page.close()

    raise RuntimeError(
        "Unable to load Clark events HTML via Playwright; "
        "page still resembled a Cloudflare challenge or lacked event listing markup."
    )


async def _target_results_count(page) -> int | None:
    locator = page.locator("#available_results")
    if await locator.count() < 1:
        return None
    value = (await locator.text_content() or "").strip()
    if not value.isdigit():
        return None
    return int(value)


def parse_clark_events_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for page in payload.get("pages", []):
        for item in parse_clark_events_html(page["html"], list_url=page["url"]):
            key = (item.source_url, item.title, item.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(item)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def parse_clark_events_html(
    html: str,
    *,
    list_url: str = CLARK_EVENTS_URL,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []

    for card in soup.select("#event_list > div"):
        detail_link = card.select_one("div.details a[href]")
        if detail_link is None:
            continue

        type_node = card.select_one("div.details div.type")
        title_node = card.select_one("div.details div.title")
        subtitle_node = card.select_one("div.details div.subtitle")
        date_node = card.select_one("div.details div.date")

        type_label = _normalize_space(type_node.get_text(" ", strip=True) if type_node else "")
        title = _normalize_space(title_node.get_text(" ", strip=True) if title_node else "")
        subtitle = _normalize_space(subtitle_node.get_text(" ", strip=True) if subtitle_node else "")
        date_text = _normalize_space(date_node.get_text(" ", strip=True) if date_node else "")
        if not title or not date_text or not type_label:
            continue
        if type_label in EXCLUDED_TYPES:
            continue

        combined_title = title
        if subtitle and subtitle.lower() not in title.lower():
            combined_title = f"{title}: {subtitle}"

        token_blob = " ".join([type_label, combined_title, date_text]).lower()
        if any(marker in token_blob for marker in EXCLUDED_TITLE_MARKERS):
            continue

        start_at = _parse_start_at(date_text)
        if start_at is None:
            continue

        description_parts = [
            f"Type: {type_label}",
            f"Listing date: {date_text}",
        ]
        if subtitle:
            description_parts.insert(0, f"Subtitle: {subtitle}")
        description = " | ".join(description_parts)

        rows.append(
            ExtractedActivity(
                source_url=urljoin(list_url, detail_link.get("href", "").strip()),
                title=combined_title,
                description=description,
                venue_name=CLARK_VENUE_NAME,
                location_text=CLARK_DEFAULT_LOCATION,
                city=CLARK_CITY,
                state=CLARK_STATE,
                activity_type=_infer_activity_type(type_label=type_label, title=combined_title),
                age_min=None,
                age_max=None,
                drop_in=True if "free sunday" in token_blob or "drop" in token_blob else None,
                registration_required=None,
                start_at=start_at,
                end_at=None,
                timezone=CLARK_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=None),
            )
        )

    return rows


class ClarkEventsAdapter(BaseSourceAdapter):
    source_name = "clark_events"

    def __init__(self, url: str = CLARK_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await _load_clark_events_html(
            url=self.url,
            scroll_rounds=CLARK_DEFAULT_SCROLL_ROUNDS,
            timeout_ms=90000,
        )
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_clark_events_html(payload, list_url=self.url)


def _infer_activity_type(*, type_label: str, title: str) -> str:
    combined = f"{type_label} {title}".lower()
    if any(keyword in combined for keyword in ("lecture", "talk", "forum", "scholarly")):
        return "lecture"
    return "workshop"


def _parse_start_at(date_text: str) -> datetime | None:
    single_match = SINGLE_DATE_RE.match(date_text)
    if single_match is not None:
        return datetime(
            int(single_match.group("year")),
            _month_number(single_match.group("month")),
            int(single_match.group("day")),
        )

    range_match = DATE_RANGE_RE.match(date_text)
    if range_match is not None:
        return datetime(
            int(range_match.group("year")),
            _month_number(range_match.group("start_month")),
            int(range_match.group("start_day")),
        )

    return None


def _month_number(name: str) -> int:
    return datetime.strptime(name, "%B").month


def _looks_like_clark_events_html(html: str) -> bool:
    return all(marker in html for marker in LISTING_MARKERS) and not _looks_like_cloudflare_challenge(html)


def _looks_like_cloudflare_challenge(html: str) -> bool:
    return any(marker in html for marker in CHALLENGE_MARKERS)


def _normalize_space(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned or None
