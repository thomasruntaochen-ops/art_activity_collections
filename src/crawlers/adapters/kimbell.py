import asyncio
import json
import re
from datetime import date
from datetime import datetime
from datetime import timedelta
from urllib.parse import parse_qs
from urllib.parse import urljoin
from urllib.parse import urlparse

from bs4 import BeautifulSoup

try:
    from playwright.async_api import BrowserContext
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    BrowserContext = None
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

KIMBELL_EVENTS_URL = "https://kimbellart.org/calendar"
KIMBELL_TIMEZONE = "America/Chicago"
KIMBELL_VENUE_NAME = "Kimbell Art Museum"
KIMBELL_CITY = "Fort Worth"
KIMBELL_STATE = "TX"
KIMBELL_LOCATION = "3333 Camp Bowie Blvd, Fort Worth, TX"
KIMBELL_WINDOW_DAYS = 120

KIMBELL_INCLUDE_KEYWORDS = (
    "art-making",
    "children",
    "class",
    "family",
    "families",
    "kids",
    "pages",
    "sketch",
    "sketching",
    "studio",
    "workshop",
)
KIMBELL_EXCLUDE_KEYWORDS = (
    "casual friday",
    "coffee and culture",
    "exhibition highlights",
    "happy hour",
    "members-only",
    "members only",
    "public tour",
    "sale",
    "special exhibition tour",
    "university evening",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_YOUNGER_RE = re.compile(r"\bages?\s*(\d{1,2})\s*and\s*younger\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)
DETAIL_DATETIME_RE = re.compile(
    r"(?P<weekday>[A-Za-z]+),\s+(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+(?P<year>\d{4}),\s+"
    r"(?P<start>\d{1,2}:\d{2}\s*(?:am|pm))"
    r"(?:\s*[–-]\s*(?P<end>\d{1,2}:\d{2}\s*(?:am|pm)))?",
    re.IGNORECASE,
)
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
    "just a moment...",
    "enable javascript and cookies to continue",
    "managed challenge",
)


def build_kimbell_calendar_url(*, now: datetime | None = None, window_days: int = KIMBELL_WINDOW_DAYS) -> str:
    reference = now or datetime.now()
    start_day = reference.date()
    end_day = start_day + timedelta(days=window_days)
    return (
        f"{KIMBELL_EVENTS_URL}?start={_format_kimbell_date(start_day)}"
        f"&end={_format_kimbell_date(end_day)}&filter=month"
    )


async def load_kimbell_payload(base_url: str = KIMBELL_EVENTS_URL) -> dict[str, object]:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    listing_url = base_url if "start=" in base_url else build_kimbell_calendar_url()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, args=list(PLAYWRIGHT_LAUNCH_ARGS))
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id=KIMBELL_TIMEZONE,
            viewport={"width": 1365, "height": 900},
            screen={"width": 1365, "height": 900},
        )
        await context.add_init_script(PLAYWRIGHT_INIT_SCRIPT)

        try:
            listing_html = await _fetch_kimbell_page_playwright(
                context=context,
                url=listing_url,
                wait_ms=10000,
            )
            detail_pages: dict[str, str] = {}
            for entry in _discover_kimbell_listing_entries(listing_html, list_url=listing_url):
                if not _should_keep_kimbell_event(
                    title=entry["title"],
                    subtitle=entry.get("subtitle"),
                    detail_text=None,
                ):
                    continue
                try:
                    await asyncio.sleep(1.0)
                    detail_pages[entry["source_url"]] = await _fetch_kimbell_page_playwright(
                        context=context,
                        url=entry["source_url"],
                        wait_ms=7000,
                    )
                except Exception as exc:
                    print(f"[kimbell-fetch] detail fetch skipped for {entry['source_url']}: {exc}")
            return {
                "listing_url": listing_url,
                "listing_html": listing_html,
                "detail_pages": detail_pages,
            }
        finally:
            await context.close()
            await browser.close()


async def _fetch_kimbell_page_playwright(
    *,
    context: BrowserContext,
    url: str,
    wait_ms: int,
    timeout_ms: int = 90000,
    max_attempts: int = 3,
) -> str:
    for attempt in range(1, max_attempts + 1):
        page = await context.new_page()
        try:
            print(f"[kimbell-fetch] attempt {attempt}/{max_attempts}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(wait_ms)
            html = await page.content()
            title = (await page.title()).strip()
            if _looks_like_challenge_page(html=html, title=title):
                if attempt == max_attempts:
                    raise RuntimeError(f"challenge page returned for {url}")
                await asyncio.sleep(2.0 * attempt)
                continue
            return html
        finally:
            await page.close()

    raise RuntimeError(f"Unable to fetch Kimbell page: {url}")


class KimbellEventsAdapter(BaseSourceAdapter):
    source_name = "kimbell_events"

    def __init__(self, url: str = KIMBELL_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_kimbell_payload(self.url)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_kimbell_payload(data)


def parse_kimbell_payload(payload: dict[str, object]) -> list[ExtractedActivity]:
    listing_html = payload.get("listing_html")
    listing_url = str(payload.get("listing_url") or KIMBELL_EVENTS_URL)
    detail_pages = payload.get("detail_pages") or {}
    if not isinstance(listing_html, str) or not isinstance(detail_pages, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for entry in _discover_kimbell_listing_entries(listing_html, list_url=listing_url):
        detail_html = detail_pages.get(entry["source_url"])
        detail = _parse_kimbell_detail_page(detail_html) if isinstance(detail_html, str) else {}

        description_parts: list[str] = []
        subtitle = entry.get("subtitle")
        if subtitle:
            description_parts.append(subtitle)
        summary = detail.get("summary")
        if isinstance(summary, str) and summary:
            description_parts.append(summary)
        location_name = detail.get("location_name")
        if isinstance(location_name, str) and location_name:
            description_parts.append(f"Location: {location_name}")
        description = " | ".join(dict.fromkeys(part for part in description_parts if part)) or None

        text_blob = " ".join(
            [
                entry["title"],
                subtitle or "",
                description or "",
            ]
        )
        if not _should_keep_kimbell_event(
            title=entry["title"],
            subtitle=subtitle,
            detail_text=text_blob,
        ):
            continue

        start_at = detail.get("start_at")
        if not isinstance(start_at, datetime):
            start_at = _parse_kimbell_url_start(entry["source_url"])
        if start_at is None:
            continue

        end_at = detail.get("end_at") if isinstance(detail.get("end_at"), datetime) else None
        age_min, age_max = _extract_age_range(description or "")
        is_free, free_status = infer_price_classification(
            " ".join(
                part
                for part in [
                    description or "",
                    str(detail.get("price_text") or ""),
                ]
                if part
            ),
            default_is_free=None,
        )
        normalized_blob = f" {text_blob.lower()} "
        registration_required = any(
            marker in normalized_blob
            for marker in (" register ", " registration ", " tickets ", " sign-up ", " sign up ")
        )
        location_text = KIMBELL_LOCATION
        if isinstance(location_name, str) and location_name:
            location_text = f"{location_name}, {KIMBELL_LOCATION}"

        item = ExtractedActivity(
            source_url=entry["source_url"],
            title=entry["title"],
            description=description,
            venue_name=KIMBELL_VENUE_NAME,
            location_text=location_text,
            city=KIMBELL_CITY,
            state=KIMBELL_STATE,
            activity_type=_infer_kimbell_activity_type(entry["title"], description),
            age_min=age_min,
            age_max=age_max,
            drop_in=("drop-in" in normalized_blob or "drop in" in normalized_blob),
            registration_required=registration_required,
            start_at=start_at,
            end_at=end_at,
            timezone=KIMBELL_TIMEZONE,
            is_free=is_free,
            free_verification_status=free_status,
        )
        key = (item.source_url, item.title, item.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(item)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _discover_kimbell_listing_entries(html: str, *, list_url: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(html, "html.parser")
    entries: list[dict[str, str | None]] = []
    seen: set[str] = set()

    for anchor in soup.select('a[href*="/event/"][href*="date="]'):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        source_url = urljoin(list_url, href)
        if source_url in seen:
            continue

        lines = [
            _normalize_space(line)
            for line in anchor.get_text("\n").splitlines()
            if _normalize_space(line) and _normalize_space(line) != ":"
        ]
        if not lines:
            continue
        title = lines[0]
        subtitle = lines[1] if len(lines) > 1 else None
        seen.add(source_url)
        entries.append(
            {
                "source_url": source_url,
                "title": title,
                "subtitle": subtitle,
            }
        )

    return entries


def _parse_kimbell_detail_page(html: str) -> dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("main") or soup
    lines = [_normalize_space(line) for line in main.get_text("\n").splitlines() if _normalize_space(line)]
    if not lines:
        return {}

    datetime_line = next((line for line in lines if DETAIL_DATETIME_RE.search(line)), None)
    start_at: datetime | None = None
    end_at: datetime | None = None
    if datetime_line:
        start_at, end_at = _parse_kimbell_datetime_line(datetime_line)

    location_name = next(
        (
            line
            for line in lines
            if any(marker in line for marker in ("Building", "Pavilion", "Auditorium", "Studio"))
        ),
        None,
    )
    price_lines = [line for line in lines if line == "Free" or "$" in line or "ticket" in line.lower()]

    title = ""
    title_node = soup.select_one("h1")
    if title_node is not None:
        title = _normalize_space(title_node.get_text(" ", strip=True))

    skip_values = {
        title,
        datetime_line or "",
        location_name or "",
        "Add to Calendar",
        "Share:",
        "Plan Your Visit with Kids",
        "VISIT",
        "PARTICIPATE",
        "Accessibility",
        "Studio A",
        "Visiting with Kids",
    }
    summary_lines: list[str] = []
    for line in lines:
        if line in skip_values or line in price_lines:
            continue
        if line.startswith("To request an accessibility accommodation"):
            break
        if line.startswith("Plan Your Visit"):
            break
        if len(line) < 25:
            continue
        if line.isupper():
            continue
        summary_lines.append(line)
        if len(" ".join(summary_lines)) >= 500:
            break

    return {
        "start_at": start_at,
        "end_at": end_at,
        "location_name": location_name,
        "price_text": " | ".join(price_lines) or None,
        "summary": " ".join(summary_lines[:4]) or None,
    }


def _should_keep_kimbell_event(
    *,
    title: str,
    subtitle: str | None,
    detail_text: str | None,
) -> bool:
    normalized = f" {title} {subtitle or ''} {detail_text or ''} ".lower()
    if any(keyword in normalized for keyword in KIMBELL_EXCLUDE_KEYWORDS):
        return False
    if " tour " in normalized and " sketch" not in normalized:
        return False
    return any(keyword in normalized for keyword in KIMBELL_INCLUDE_KEYWORDS)


def _infer_kimbell_activity_type(title: str, description: str | None) -> str:
    normalized = f" {title} {description or ''} ".lower()
    if any(keyword in normalized for keyword in ("sketch", "studio", "workshop")):
        return "workshop"
    if "class" in normalized:
        return "class"
    return "activity"


def _parse_kimbell_url_start(source_url: str) -> datetime | None:
    parsed = urlparse(source_url)
    date_value = parse_qs(parsed.query).get("date", [None])[0]
    if not date_value:
        return None
    try:
        return datetime.fromisoformat(date_value)
    except ValueError:
        return None


def _parse_kimbell_datetime_line(value: str) -> tuple[datetime | None, datetime | None]:
    match = DETAIL_DATETIME_RE.search(value)
    if match is None:
        return None, None

    try:
        base_day = datetime.strptime(
            f"{match.group('month')} {match.group('day')} {match.group('year')}",
            "%B %d %Y",
        )
        start_time = datetime.strptime(match.group("start").upper(), "%I:%M %p")
        start_at = base_day.replace(hour=start_time.hour, minute=start_time.minute)
        end_group = match.group("end")
        if not end_group:
            return start_at, None
        end_time = datetime.strptime(end_group.upper(), "%I:%M %p")
        end_at = base_day.replace(hour=end_time.hour, minute=end_time.minute)
        return start_at, end_at
    except ValueError:
        return None, None


def _extract_age_range(text: str) -> tuple[int | None, int | None]:
    range_match = AGE_RANGE_RE.search(text)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))

    younger_match = AGE_YOUNGER_RE.search(text)
    if younger_match:
        return None, int(younger_match.group(1))

    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def _looks_like_challenge_page(*, html: str, title: str) -> bool:
    normalized = f"{title}\n{html}".lower()
    return any(marker in normalized for marker in CHALLENGE_MARKERS)


def _format_kimbell_date(value: date) -> str:
    return value.strftime("%b-%d-%Y").lower()


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())
