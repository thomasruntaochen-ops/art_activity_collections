import asyncio
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin

import httpx
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
INCLUDED_TYPES = {
    "Community Program",
    "Family Program",
    "Lecture",
    "Nature Program",
    "Special Event",
}
EXCLUDED_TITLE_MARKERS = (
    "concert",
    "screening",
    "soundscape",
)
EXCLUDED_SPECIAL_EVENT_TITLE_MARKERS = (
    "graduate program",
    "reception",
)

CLARK_DETAIL_CONCURRENCY = 5

# Listing cards carry only a date; the clock time lives in div.time on the detail
# page, as either a range ("1-4 pm", "10:30 am-12 pm") or a single start ("5:30 pm").
DETAIL_TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>[ap]\.?m\.?)?"
    r"\s*[\u2013\u2014-]\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>[ap]\.?m\.?)",
    re.IGNORECASE,
)
DETAIL_TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>[ap]\.?m\.?)",
    re.IGNORECASE,
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
    pages = [
        {
            "url": CLARK_EVENTS_URL,
            "html": html,
        }
    ]
    # Re-run the listing parse here so only events that survive the filters cost a
    # detail request, and so the filter logic stays defined in one place.
    detail_urls = sorted({row.source_url for row in parse_clark_events_html(html, list_url=CLARK_EVENTS_URL)})
    return {
        "pages": pages,
        "detail_html_by_url": await _fetch_clark_detail_html(detail_urls),
    }


async def _fetch_clark_detail_html(urls: list[str]) -> dict[str, str]:
    if not urls:
        return {}

    results: dict[str, str] = {}
    semaphore = asyncio.Semaphore(CLARK_DETAIL_CONCURRENCY)

    async with httpx.AsyncClient(timeout=30.0, headers=DEFAULT_HEADERS, follow_redirects=True) as client:
        async def fetch_one(url: str) -> None:
            async with semaphore:
                try:
                    response = await client.get(url)
                except httpx.HTTPError as exc:
                    print(f"[clark] detail fetch failed ({url}): {exc}")
                    return
                if response.status_code != 200:
                    print(f"[clark] detail fetch failed ({url}): HTTP {response.status_code}")
                    return
                if any(marker in response.text for marker in CHALLENGE_MARKERS):
                    print(f"[clark] detail fetch blocked by challenge ({url}); keeping date-only start.")
                    return
                results[url] = response.text

        await asyncio.gather(*(fetch_one(url) for url in urls))

    missing = len(urls) - len(results)
    if missing:
        print(f"[clark] WARNING: {missing}/{len(urls)} detail pages unavailable; those rows keep a midnight start.")
    return results


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
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for page in payload.get("pages", []):
        for item in parse_clark_events_html(page["html"], list_url=page["url"]):
            _apply_detail_time(item, detail_html_by_url.get(item.source_url))
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
        if type_label not in INCLUDED_TYPES:
            continue

        combined_title = title
        if subtitle and subtitle.lower() not in title.lower():
            combined_title = f"{title}: {subtitle}"

        if type_label == "Special Event" and any(
            marker in combined_title.lower() for marker in EXCLUDED_SPECIAL_EVENT_TITLE_MARKERS
        ):
            continue

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
                audience_segment=_infer_audience_segment(type_label=type_label),
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


def _infer_audience_segment(*, type_label: str) -> str:
    if type_label == "Family Program":
        return "kids"
    if type_label == "Community Program":
        return "all_ages"
    return "adults"


def _apply_detail_time(row: ExtractedActivity, detail_html: str | None) -> None:
    """Refine a date-only start with the clock time from the event's detail page."""
    if not detail_html:
        return

    time_node = BeautifulSoup(detail_html, "html.parser").select_one("div.time")
    if time_node is None:
        return

    parsed = _parse_detail_time_text(_normalize_space(time_node.get_text(" ", strip=True)))
    if parsed is None:
        return

    start_time, end_time = parsed
    row.start_at = row.start_at.replace(hour=start_time[0], minute=start_time[1])
    if end_time is not None:
        end_at = row.start_at.replace(hour=end_time[0], minute=end_time[1])
        if end_at < row.start_at:
            # An evening event that runs past midnight ends on the following day.
            end_at += timedelta(days=1)
        row.end_at = end_at


def _parse_detail_time_text(
    time_text: str,
) -> tuple[tuple[int, int], tuple[int, int] | None] | None:
    if not time_text:
        return None

    range_match = DETAIL_TIME_RANGE_RE.search(time_text)
    if range_match is not None:
        end_meridiem = range_match.group("end_meridiem")
        # "1-4 pm" states the meridiem once, at the end.
        start_meridiem = range_match.group("start_meridiem") or end_meridiem
        start = _to_24_hour(
            range_match.group("start_hour"), range_match.group("start_minute"), start_meridiem
        )
        end = _to_24_hour(
            range_match.group("end_hour"), range_match.group("end_minute"), end_meridiem
        )
        if start is None:
            return None
        return start, end

    single_match = DETAIL_TIME_SINGLE_RE.search(time_text)
    if single_match is not None:
        start = _to_24_hour(
            single_match.group("hour"), single_match.group("minute"), single_match.group("meridiem")
        )
        if start is not None:
            return start, None

    return None


def _to_24_hour(hour: str | None, minute: str | None, meridiem: str | None) -> tuple[int, int] | None:
    if hour is None or meridiem is None:
        return None
    hour_value = int(hour)
    minute_value = int(minute or "0")
    if not (1 <= hour_value <= 12) or not (0 <= minute_value <= 59):
        return None
    if meridiem.lower().replace(".", "").startswith("p") and hour_value != 12:
        hour_value += 12
    if meridiem.lower().replace(".", "").startswith("a") and hour_value == 12:
        hour_value = 0
    return hour_value, minute_value


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
