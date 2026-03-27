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
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

WADSWORTH_EVENTS_URL = "https://www.thewadsworth.org/visit/events/"
WADSWORTH_TIMEZONE = "America/New_York"
WADSWORTH_VENUE_NAME = "Wadsworth Atheneum Museum of Art"
WADSWORTH_CITY = "Hartford"
WADSWORTH_STATE = "CT"
WADSWORTH_DEFAULT_LOCATION = "Hartford, CT"
WADSWORTH_DEFAULT_PAGE_LIMIT = 1

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": WADSWORTH_EVENTS_URL,
}

CHALLENGE_MARKERS = (
    "Just a moment...",
    "Enable JavaScript and cookies to continue",
    "managed challenge",
)
DATE_HREF_RE = re.compile(r"exact_date~(?P<month>\d{1,2})-(?P<day>\d{1,2})-(?P<year>\d{4})")
TIME_SINGLE_RE = re.compile(
    r"@\s*(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>am|pm)",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

INCLUSION_MARKERS = (
    "talk",
    "lecture",
    "class",
    "workshop",
    "conversation",
    "art-making",
    "art making",
    "studio",
    "drop-in",
    "drop in",
)
YOUTH_ACTIVITY_MARKERS = (
    "family",
    "families",
    "kids",
    "children",
    "teens",
    "all ages",
)
STRONG_EXCLUSION_MARKERS = (
    "film",
    "screening",
    "performance",
    "concert",
    "music",
    "jazz",
    "orchestra",
    "band",
    "poetry",
    "poem",
    "storytime",
    "story time",
    "yoga",
    "meditation",
    "mindfulness",
    "camp",
    "member morning",
    "members only",
    "opening reception",
    "reception",
    "dinner",
    "tour",
    "tours",
)


def build_wadsworth_agenda_urls(*, page_limit: int = WADSWORTH_DEFAULT_PAGE_LIMIT) -> list[str]:
    page_total = max(page_limit, 1)
    urls = [WADSWORTH_EVENTS_URL]
    for page_offset in range(1, page_total):
        urls.append(
            f"{WADSWORTH_EVENTS_URL}action~agenda/page_offset~{page_offset}/request_format~html/"
        )
    return urls


async def load_wadsworth_events_payload(
    *,
    page_limit: int = WADSWORTH_DEFAULT_PAGE_LIMIT,
    timeout_ms: int = 90000,
) -> dict:
    urls = build_wadsworth_agenda_urls(page_limit=page_limit)
    pages: list[dict[str, str]] = []

    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install extras and browsers: "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            locale="en-US",
        )
        try:
            for index, url in enumerate(urls):
                try:
                    html = await fetch_wadsworth_page_playwright(
                        context=context,
                        url=url,
                        timeout_ms=timeout_ms,
                    )
                except Exception:
                    if index == 0:
                        raise
                    print(f"[wadsworth-fetch] stopping after page {index - 1}; unable to load {url}")
                    break
                pages.append({"url": url, "html": html})
                if not _looks_like_wadsworth_events_html(html):
                    if index == 0:
                        raise RuntimeError(
                            "Unable to load Wadsworth events HTML via Playwright; "
                            "root page lacked agenda markup."
                        )
                    break
        finally:
            await context.close()
            await browser.close()

    return {"pages": pages}


async def fetch_wadsworth_page_playwright(
    *,
    context: BrowserContext,
    url: str,
    timeout_ms: int = 90000,
    max_attempts: int = 2,
) -> str:
    for attempt in range(1, max_attempts + 1):
        page = await context.new_page()
        try:
            print(f"[wadsworth-fetch] Playwright attempt {attempt}/{max_attempts}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            for wait_ms in (6000, 10000):
                await page.wait_for_timeout(wait_ms)
                html = await page.content()
                if _looks_like_wadsworth_events_html(html):
                    return html
                if not _looks_like_cloudflare_challenge(html):
                    break
        finally:
            await page.close()

    raise RuntimeError(
        "Unable to load Wadsworth events HTML via Playwright; "
        "page still resembled a Cloudflare challenge or lacked agenda markup."
    )


def parse_wadsworth_events_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for page in payload.get("pages", []):
        for item in parse_wadsworth_events_html(page["html"], list_url=page["url"]):
            key = (item.source_url, item.title, item.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(item)

    return rows


def parse_wadsworth_events_html(
    html: str,
    *,
    list_url: str = WADSWORTH_EVENTS_URL,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []

    for date_node in soup.select("div.ai1ec-agenda-view div.ai1ec-date"):
        date_value = _parse_date_value(date_node)
        if date_value is None:
            continue

        for event_node in date_node.select("div.ai1ec-event"):
            title_node = event_node.select_one("span.ai1ec-event-title")
            if title_node is None:
                continue
            title = _normalize_space(title_node.get_text(" ", strip=True))
            if not title or is_irrelevant_item_text(title):
                continue

            description_node = event_node.select_one("div.ai1ec-event-description")
            description = _extract_description(description_node)
            text_blob = " ".join([title, description or ""]).lower()
            if not _should_include_event(title=title, description=description):
                continue

            source_anchor = event_node.select_one("a.ai1ec-load-event[href]")
            source_href = source_anchor.get("href", "").strip() if source_anchor is not None else ""
            if not source_href:
                continue
            source_url = urljoin(list_url, source_href)

            time_node = event_node.select_one("div.ai1ec-event-time")
            start_at = _parse_start_at(
                date_value=date_value,
                time_text=_normalize_space(time_node.get_text(" ", strip=True) if time_node else ""),
            )
            if start_at is None:
                continue

            end_at = _parse_end_at(event_node.get("data-end"))
            age_min, age_max = _parse_age_range(" ".join([title, description or ""]))

            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=description,
                    venue_name=WADSWORTH_VENUE_NAME,
                    location_text=WADSWORTH_DEFAULT_LOCATION,
                    city=WADSWORTH_CITY,
                    state=WADSWORTH_STATE,
                    activity_type=_infer_activity_type(title=title, description=description),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                    registration_required=_registration_required(text_blob),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=WADSWORTH_TIMEZONE,
                    **price_classification_kwargs(
                        _pricing_text(
                            title=title,
                            description=description,
                            ticket_url=event_node.get("data-ticket-url", ""),
                        ),
                        default_is_free=None,
                    ),
                )
            )

    return rows


class WadsworthEventsAdapter(BaseSourceAdapter):
    source_name = "wadsworth_events"

    async def fetch(self) -> list[str]:
        payload = await load_wadsworth_events_payload(page_limit=1)
        return [page["html"] for page in payload["pages"]]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_wadsworth_events_html(payload, list_url=WADSWORTH_EVENTS_URL)


def _looks_like_cloudflare_challenge(html: str) -> bool:
    normalized = html.lower()
    return any(marker.lower() in normalized for marker in CHALLENGE_MARKERS)


def _looks_like_wadsworth_events_html(html: str) -> bool:
    normalized = html.lower()
    return "ai1ec-agenda-view" in normalized and "ai1ec-event-title" in normalized


def _parse_date_value(date_node) -> datetime | None:
    anchor = date_node.select_one("a.ai1ec-date-title[href]")
    if anchor is None:
        return None

    href = anchor.get("href", "").strip()
    match = DATE_HREF_RE.search(href)
    if match is None:
        return None

    return datetime(
        year=int(match.group("year")),
        month=int(match.group("month")),
        day=int(match.group("day")),
    )


def _parse_start_at(*, date_value: datetime, time_text: str) -> datetime | None:
    match = TIME_SINGLE_RE.search(time_text)
    if match is None:
        return date_value

    return date_value.replace(
        hour=_to_24_hour(int(match.group("hour")), match.group("meridiem")),
        minute=int(match.group("minute") or 0),
    )


def _parse_end_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return parse_iso_datetime(value, timezone_name=WADSWORTH_TIMEZONE)
    except ValueError:
        return None


def _extract_description(description_node) -> str | None:
    if description_node is None:
        return None

    parts = [
        _normalize_space(paragraph.get_text(" ", strip=True))
        for paragraph in description_node.select("p")
    ]
    parts = [part for part in parts if part]
    if parts:
        return " | ".join(parts)

    text = _normalize_space(description_node.get_text(" ", strip=True))
    return text or None


def _should_include_event(*, title: str, description: str | None) -> bool:
    normalized_title = title.lower()
    normalized_description = (description or "").lower()
    combined = f"{normalized_title} {normalized_description}"

    if any(_contains_marker(normalized_title, marker) for marker in STRONG_EXCLUSION_MARKERS):
        return False

    if any(_contains_marker(combined, marker) for marker in INCLUSION_MARKERS):
        return True

    return any(_contains_marker(combined, marker) for marker in YOUTH_ACTIVITY_MARKERS)


def _infer_activity_type(*, title: str, description: str | None) -> str:
    combined = f"{title} {description or ''}".lower()
    if "lecture" in combined:
        return "lecture"
    if "talk" in combined:
        return "talk"
    if "class" in combined:
        return "class"
    if "workshop" in combined:
        return "workshop"
    if "conversation" in combined:
        return "conversation"
    return "activity"


def _registration_required(text_blob: str) -> bool | None:
    if "registration required" in text_blob:
        return True
    if "registration encouraged" in text_blob or "register now" in text_blob:
        return False
    if "no registration" in text_blob or "registration not required" in text_blob:
        return False
    if "register" in text_blob or "registration" in text_blob:
        return True
    return None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    age_range_match = AGE_RANGE_RE.search(text)
    if age_range_match is not None:
        return int(age_range_match.group(1)), int(age_range_match.group(2))

    age_plus_match = AGE_PLUS_RE.search(text)
    if age_plus_match is not None:
        return int(age_plus_match.group(1)), None

    return None, None


def _to_24_hour(hour: int, meridiem: str | None) -> int:
    normalized = (meridiem or "").lower()
    if normalized == "am":
        return 0 if hour == 12 else hour
    if normalized == "pm":
        return 12 if hour == 12 else hour + 12
    return hour


def _normalize_space(value: str) -> str:
    return " ".join(value.split())


def _contains_marker(text: str, marker: str) -> bool:
    return re.search(rf"\b{re.escape(marker)}\b", text) is not None


def _pricing_text(*, title: str, description: str | None, ticket_url: str) -> str:
    combined = " ".join([title, description or "", ticket_url])
    return re.sub(r"[^\w$]+", " ", combined)
