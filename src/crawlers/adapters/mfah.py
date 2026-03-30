import asyncio
import json
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
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

MFAH_EVENTS_URL = "https://www.mfah.org/events?tag=families&period=year"
MFAH_TIMEZONE = "America/Chicago"
MFAH_VENUE_NAME = "Museum of Fine Arts, Houston"
MFAH_CITY = "Houston"
MFAH_STATE = "TX"
MFAH_LOCATION = "1001 Bissonnet St, Houston, TX"

MFAH_INCLUDE_KEYWORDS = (
    "activity",
    "art-making",
    "art encounters",
    "artists",
    "class",
    "family",
    "families",
    "little artists",
    "playdate",
    "studio",
    "workshop",
    "zone",
)
MFAH_EXCLUDE_KEYWORDS = (
    "film",
    "films",
    "look-alike",
    "music",
    "performance",
    "performances",
    "poetry",
    "reading",
    "reception",
    "tour",
    "writing",
)
MFAH_URL_STAMP_RE = re.compile(r"/events/[^/]+/(?P<stamp>\d{12})")
MFAH_TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))\s*[—–-]\s*(?P<end>\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))"
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
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
    "attention required!",
    "sorry, you have been blocked",
    "just a moment...",
)


async def load_mfah_payload(base_url: str = MFAH_EVENTS_URL) -> dict[str, object]:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, args=list(PLAYWRIGHT_LAUNCH_ARGS))
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id=MFAH_TIMEZONE,
            viewport={"width": 1365, "height": 900},
            screen={"width": 1365, "height": 900},
        )
        await context.add_init_script(PLAYWRIGHT_INIT_SCRIPT)

        try:
            listing_html = await _fetch_mfah_page_playwright(
                context=context,
                url=base_url,
                wait_ms=7000,
            )
            detail_pages: dict[str, str] = {}
            for entry in _discover_mfah_listing_entries(listing_html, list_url=base_url):
                if not _should_keep_mfah_event(
                    title=entry["title"],
                    kind=entry["kind"],
                    detail_text=None,
                ):
                    continue
                try:
                    await asyncio.sleep(0.75)
                    detail_pages[entry["source_url"]] = await _fetch_mfah_page_playwright(
                        context=context,
                        url=entry["source_url"],
                        wait_ms=6000,
                    )
                except Exception as exc:
                    print(f"[mfah-fetch] detail fetch skipped for {entry['source_url']}: {exc}")
            return {
                "listing_url": base_url,
                "listing_html": listing_html,
                "detail_pages": detail_pages,
            }
        finally:
            await context.close()
            await browser.close()


async def _fetch_mfah_page_playwright(
    *,
    context: BrowserContext,
    url: str,
    wait_ms: int,
    timeout_ms: int = 90000,
    max_attempts: int = 2,
) -> str:
    for attempt in range(1, max_attempts + 1):
        page = await context.new_page()
        try:
            print(f"[mfah-fetch] attempt {attempt}/{max_attempts}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(wait_ms)
            html = await page.content()
            title = (await page.title()).strip()
            if _looks_like_mfah_challenge(html=html, title=title):
                if attempt == max_attempts:
                    raise RuntimeError(f"challenge page returned for {url}")
                await asyncio.sleep(2.0 * attempt)
                continue
            return html
        finally:
            await page.close()

    raise RuntimeError(f"Unable to fetch MFAH page: {url}")


class MfahEventsAdapter(BaseSourceAdapter):
    source_name = "mfah_events"

    def __init__(self, url: str = MFAH_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_mfah_payload(self.url)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_mfah_payload(data)


def parse_mfah_payload(payload: dict[str, object]) -> list[ExtractedActivity]:
    listing_html = payload.get("listing_html")
    listing_url = str(payload.get("listing_url") or MFAH_EVENTS_URL)
    detail_pages = payload.get("detail_pages") or {}
    if not isinstance(listing_html, str) or not isinstance(detail_pages, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for entry in _discover_mfah_listing_entries(listing_html, list_url=listing_url):
        detail_html = detail_pages.get(entry["source_url"])
        detail = _parse_mfah_detail_page(detail_html) if isinstance(detail_html, str) else {}

        description_parts: list[str] = []
        detail_summary = detail.get("summary")
        if isinstance(detail_summary, str) and detail_summary:
            description_parts.append(detail_summary)
        if entry["kind"]:
            description_parts.append(f"Type: {entry['kind']}")
        if isinstance(detail.get("location_name"), str) and detail.get("location_name"):
            description_parts.append(f"Location: {detail['location_name']}")
        description = " | ".join(dict.fromkeys(part for part in description_parts if part)) or None

        text_blob = " ".join(
            [
                entry["title"],
                entry["kind"] or "",
                description or "",
            ]
        )
        if not _should_keep_mfah_event(
            title=entry["title"],
            kind=entry["kind"],
            detail_text=text_blob,
        ):
            continue

        start_at = entry["start_at"]
        if not isinstance(start_at, datetime):
            continue

        end_at = detail.get("end_at") if isinstance(detail.get("end_at"), datetime) else entry.get("end_at")
        age_min, age_max = _extract_mfah_age_range(description or "")
        price_text = " ".join(
            part for part in [str(detail.get("price_text") or ""), description or ""] if part
        )
        is_free, free_status = infer_price_classification(price_text, default_is_free=None)
        normalized_blob = f" {text_blob.lower()} "
        registration_required = any(
            marker in normalized_blob
            for marker in (" get tickets ", " registration ", " register ", " tickets ")
        )

        location_name = detail.get("location_name") or entry.get("location_name")
        location_text = MFAH_LOCATION
        if isinstance(location_name, str) and location_name:
            location_text = f"{location_name}, {MFAH_LOCATION}"

        item = ExtractedActivity(
            source_url=entry["source_url"],
            title=entry["title"],
            description=description,
            venue_name=MFAH_VENUE_NAME,
            location_text=location_text,
            city=MFAH_CITY,
            state=MFAH_STATE,
            activity_type=_infer_mfah_activity_type(entry["title"], entry["kind"], description),
            age_min=age_min,
            age_max=age_max,
            drop_in=False,
            registration_required=registration_required,
            start_at=start_at,
            end_at=end_at,
            timezone=MFAH_TIMEZONE,
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


def _discover_mfah_listing_entries(html: str, *, list_url: str) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    entries: list[dict[str, object]] = []
    seen: set[str] = set()

    for card in soup.select("div.card"):
        link_node = card.select_one('a[href*="/events/"]')
        title_node = card.select_one("h3")
        kind_node = card.select_one("span.small")
        time_node = card.select_one("p.icon-time")
        location_node = card.select_one("p.icon-pin-white")
        if link_node is None or title_node is None:
            continue

        href = (link_node.get("href") or "").strip()
        if not href or href == "/events/films":
            continue

        source_url = urljoin(list_url, href)
        if source_url in seen:
            continue

        title = _normalize_space(title_node.get_text(" ", strip=True))
        if not title:
            continue

        start_at = _parse_mfah_url_start(source_url)
        end_at = _parse_mfah_end_time(start_at=start_at, time_text=_normalize_space(time_node.get_text(" ", strip=True)) if time_node else "")
        seen.add(source_url)
        entries.append(
            {
                "source_url": source_url,
                "title": title,
                "kind": _normalize_space(kind_node.get_text(" ", strip=True)) if kind_node else "",
                "location_name": _normalize_space(location_node.get_text(" ", strip=True)) if location_node else "",
                "start_at": start_at,
                "end_at": end_at,
            }
        )

    return entries


def _parse_mfah_detail_page(html: str) -> dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("main") or soup
    lines = [_normalize_space(line) for line in main.get_text("\n").splitlines() if _normalize_space(line)]
    if not lines:
        return {}

    location_name = None
    if "Location" in lines:
        location_index = lines.index("Location")
        if location_index + 1 < len(lines):
            location_name = lines[location_index + 1]

    price_lines: list[str] = []
    if "Tickets" in lines:
        start_index = lines.index("Tickets") + 1
        for line in lines[start_index:]:
            if line in {"Plan Your Visit", "Related Event"}:
                break
            price_lines.append(line)

    summary_lines: list[str] = []
    capture = False
    for line in lines:
        if line in {"ENGLISH", "ESPAÑOL"}:
            capture = True
            continue
        if line in {"Tickets", "Plan Your Visit", "Related Event"}:
            break
        if not capture:
            continue
        if len(line) < 25:
            continue
        summary_lines.append(line)
        if len(" ".join(summary_lines)) >= 600:
            break

    end_at = None
    time_ranges = [line for line in lines if MFAH_TIME_RANGE_RE.search(line)]
    if time_ranges:
        match = MFAH_TIME_RANGE_RE.search(time_ranges[0])
        if match:
            end_at = match.group("end")

    return {
        "location_name": location_name,
        "price_text": " | ".join(price_lines) or None,
        "summary": " ".join(summary_lines[:4]) or None,
        "end_at": end_at,
    }


def _should_keep_mfah_event(*, title: str, kind: str | None, detail_text: str | None) -> bool:
    normalized = f" {title} {kind or ''} {detail_text or ''} ".lower()
    if any(keyword in normalized for keyword in MFAH_EXCLUDE_KEYWORDS):
        return False
    return any(keyword in normalized for keyword in MFAH_INCLUDE_KEYWORDS)


def _infer_mfah_activity_type(title: str, kind: str | None, description: str | None) -> str:
    normalized = f" {title} {kind or ''} {description or ''} ".lower()
    if any(keyword in normalized for keyword in ("workshop", "studio", "art-making")):
        return "workshop"
    if "class" in normalized:
        return "class"
    if any(keyword in normalized for keyword in ("talk", "lecture", "conversation")):
        return "talk"
    return "activity"


def _parse_mfah_url_start(source_url: str) -> datetime | None:
    match = MFAH_URL_STAMP_RE.search(source_url)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group("stamp"), "%Y%m%d%H%M")
    except ValueError:
        return None


def _parse_mfah_end_time(*, start_at: datetime | None, time_text: str) -> datetime | None:
    if start_at is None:
        return None
    match = MFAH_TIME_RANGE_RE.search(time_text)
    if match is None:
        return None
    try:
        end_time = datetime.strptime(match.group("end").upper(), "%I:%M %p")
    except ValueError:
        try:
            end_time = datetime.strptime(match.group("end").upper(), "%I %p")
        except ValueError:
            return None
    return start_at.replace(hour=end_time.hour, minute=end_time.minute)


def _extract_mfah_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def _looks_like_mfah_challenge(*, html: str, title: str) -> bool:
    normalized = f"{title}\n{html}".lower()
    return any(marker in normalized for marker in CHALLENGE_MARKERS)


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())
