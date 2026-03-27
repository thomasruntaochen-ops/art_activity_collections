import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from html import unescape
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import httpx

try:
    from playwright.async_api import BrowserContext
    from playwright.async_api import Page
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    BrowserContext = None
    Page = None
    PlaywrightTimeoutError = Exception
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

NBMAA_EVENTS_URL = "https://secure.nbmaa.org/events"
NBMAA_API_PATH = "/api/products/productionseasons"
NBMAA_TIMEZONE = "America/New_York"
NBMAA_VENUE_NAME = "New Britain Museum of American Art"
NBMAA_CITY = "New Britain"
NBMAA_STATE = "CT"
NBMAA_DEFAULT_LOCATION = "New Britain, CT"
NBMAA_DEFAULT_DAY_WINDOW = 92
NBMAA_LISTING_WAIT_MS = 8000
NBMAA_DETAIL_WAIT_MS = 5000

CHALLENGE_MARKERS = (
    "pardon our interruption",
    "please stand by",
    "incapsula",
)
LISTING_MARKERS = (
    "tnew-event-listing",
    "events | new britain museum of american art",
    "tn-events-calendar-view-day",
    "api/products/productionseasons",
)
DETAIL_MARKERS = (
    "tn-event-detail__main-container",
    "tn-event-detail__title",
)

FORCE_INCLUDE_PHRASES = (
    "art explorers",
    "art start",
    "homeschool day",
)
INCLUSION_MARKERS = (
    "talk",
    "lecture",
    "class",
    "workshop",
    "lab",
    "conversation",
    "studio",
    "printmaking",
)
YOUTH_ACTIVITY_MARKERS = (
    "ages ",
    "teen",
    "youth",
    "kids",
    "children",
    "child",
    "caregiver",
    "caregivers",
    "homeschool",
)
STRONG_EXCLUSION_MARKERS = (
    "tour",
    "tours",
    "concert",
    "music",
    "worldbeat",
    "social",
    "socials",
    "paint and sip",
    "mixology",
    "community day",
    "celebration",
    "first friday",
    "access for all",
    "poetry",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?"
    r"\s*(?P<start_meridiem>a\.?m\.?|p\.?m\.?)?"
    r"\s*(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?"
    r"\s*(?P<end_meridiem>a\.?m\.?|p\.?m\.?)",
    re.IGNORECASE,
)


@dataclass(slots=True)
class NbmaaDetailFields:
    title: str | None
    description: str | None
    display_time: str | None


async def load_nbmaa_events_payload(
    *,
    start_date: str | date | None = None,
    day_window: int = NBMAA_DEFAULT_DAY_WINDOW,
    timeout_ms: int = 90000,
) -> dict:
    start_day = _coerce_start_date(start_date)
    end_day = start_day + timedelta(days=max(day_window, 0))

    productions = await _fetch_productions_http(
        start_day=start_day,
        end_day=end_day,
        timeout_ms=timeout_ms,
    )

    detail_pages: dict[str, str] = {}
    async with httpx.AsyncClient(timeout=timeout_ms / 1000, follow_redirects=True) as client:
        playwright_manager = None
        playwright_browser = None
        playwright_context = None
        try:
            for source_url in _candidate_detail_urls(productions):
                html = await _fetch_detail_page_http(
                    client=client,
                    url=source_url,
                )
                if html is None and async_playwright is not None:
                    if playwright_context is None:
                        playwright_manager = await async_playwright().__aenter__()
                        playwright_browser = await playwright_manager.chromium.launch(headless=True)
                        playwright_context = await playwright_browser.new_context(locale="en-US")
                    html = await _fetch_detail_page_html(
                        context=playwright_context,
                        url=source_url,
                        timeout_ms=timeout_ms,
                    )
                if html:
                    detail_pages[source_url] = html
        finally:
            if playwright_context is not None:
                await playwright_context.close()
            if playwright_browser is not None:
                await playwright_browser.close()
            if playwright_manager is not None:
                await playwright_manager.__aexit__(None, None, None)

    return {
        "listing_html": "",
        "productions": productions,
        "detail_pages": detail_pages,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
    }


def parse_nbmaa_events_payload(payload: dict) -> list[ExtractedActivity]:
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for production in payload.get("productions") or []:
        production_title = _normalize_space(str(production.get("productionTitle") or ""))

        for performance in production.get("performances") or []:
            if not performance.get("isPerformanceVisible"):
                continue

            source_url = _normalize_url(performance.get("actionUrl"))
            if not source_url:
                continue

            api_title = _best_api_title(performance=performance, production_title=production_title)
            detail = _parse_detail_html(detail_pages.get(source_url))
            title = detail.title or api_title
            if not title:
                continue

            description = detail.description
            combined_text = _normalize_space(
                " ".join(
                    filter(
                        None,
                        [
                            production_title,
                            title,
                            description,
                            str(performance.get("performanceStatusMessage") or ""),
                        ],
                    )
                )
            )
            if not _should_include_event(title=title, description=description, combined_text=combined_text):
                continue

            start_at = _parse_api_datetime(
                performance.get("iso8601DateString") or performance.get("performanceDate")
            )
            if start_at is None:
                continue

            end_at = _parse_end_at(
                text=" | ".join(filter(None, [detail.display_time, detail.description])),
                start_at=start_at,
            )

            age_min, age_max = _parse_age_range(" ".join(filter(None, [title, description])))
            text_blob = combined_text.lower()

            key = (source_url, title, start_at)
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=description,
                    venue_name=NBMAA_VENUE_NAME,
                    location_text=NBMAA_DEFAULT_LOCATION,
                    city=NBMAA_CITY,
                    state=NBMAA_STATE,
                    activity_type=_infer_activity_type(
                        title=" ".join(filter(None, [production_title, title])),
                        description=description,
                    ),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=_infer_drop_in(text_blob),
                    registration_required=_registration_required(text_blob),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=NBMAA_TIMEZONE,
                    **price_classification_kwargs(
                        _pricing_text(
                            title=title,
                            description=description,
                            performance_status=performance.get("performanceStatusMessage"),
                        ),
                        default_is_free=None,
                    ),
                )
            )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class NbmaaEventsAdapter(BaseSourceAdapter):
    source_name = "nbmaa_events"

    async def fetch(self) -> list[str]:
        payload = await load_nbmaa_events_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_nbmaa_events_payload(json.loads(payload))


async def _fetch_productions_http(*, start_day: date, end_day: date, timeout_ms: int) -> list[dict]:
    payload = {
        "productionSeasonIdFilter": [],
        "keywordIds": None,
        "startDate": f"{start_day.isoformat()}T00:00",
        "endDate": f"{end_day.isoformat()}T23:59",
        "keywords": [],
    }
    async with httpx.AsyncClient(timeout=timeout_ms / 1000, follow_redirects=True) as client:
        response = await client.post(
            urljoin(NBMAA_EVENTS_URL, NBMAA_API_PATH),
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        data = response.json()
    if not isinstance(data, list):
        raise RuntimeError("NBMAA production API returned an unexpected payload shape.")
    return data


async def _fetch_detail_page_http(
    *,
    client: httpx.AsyncClient,
    url: str,
    max_attempts: int = 2,
) -> str | None:
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[nbmaa-fetch] detail http attempt {attempt}/{max_attempts}: {url}")
            response = await client.get(url)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            print(f"[nbmaa-fetch] detail http warning for {url}: {exc}")
            continue

        html = response.text
        if not _looks_like_challenge_html(html):
            return html

    print(f"[nbmaa-fetch] detail page skipped after retries: {url}")
    return None


async def _load_listing_page(*, page: Page, timeout_ms: int) -> None:
    await page.goto(NBMAA_EVENTS_URL, wait_until="domcontentloaded", timeout=timeout_ms)
    await page.wait_for_timeout(NBMAA_LISTING_WAIT_MS)
    html = await page.content()
    title = (await page.title()).strip().lower()
    if _looks_like_challenge_html(html) or "pardon our interruption" in title:
        raise RuntimeError(
            "Unable to load NBMAA events listing via Playwright; page resembled a challenge "
            "or did not expose the event listing shell."
        )
    if "new britain museum of american art" not in title and not _looks_like_listing_html(html):
        raise RuntimeError(
            "Unable to confirm NBMAA events listing via Playwright; page title and markup "
            "did not match the expected event listing shell."
        )


async def _fetch_productions(*, page: Page, start_day: date, end_day: date) -> list[dict]:
    payload = {
        "productionSeasonIdFilter": [],
        "keywordIds": None,
        "startDate": f"{start_day.isoformat()}T00:00",
        "endDate": f"{end_day.isoformat()}T23:59",
        "keywords": [],
    }
    data = await page.evaluate(
        """
        async payload => {
            const response = await fetch('/api/products/productionseasons', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
                credentials: 'same-origin',
            });
            if (!response.ok) {
                throw new Error(`NBMAA production API status ${response.status}`);
            }
            return await response.json();
        }
        """,
        payload,
    )
    if not isinstance(data, list):
        raise RuntimeError("NBMAA production API returned an unexpected payload shape.")
    return data


def _candidate_detail_urls(productions: list[dict]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    for production in productions:
        production_title = _normalize_space(str(production.get("productionTitle") or ""))
        for performance in production.get("performances") or []:
            if not performance.get("isPerformanceVisible"):
                continue

            performance_title = _best_api_title(performance=performance, production_title=production_title)
            if not _could_be_qualifying_event(
                production_title=production_title,
                performance_title=performance_title,
            ):
                continue

            source_url = _normalize_url(performance.get("actionUrl"))
            if not source_url or source_url in seen:
                continue
            seen.add(source_url)
            urls.append(source_url)

    return urls


async def _fetch_detail_page_html(
    *,
    context: BrowserContext,
    url: str,
    timeout_ms: int,
    max_attempts: int = 2,
) -> str | None:
    for attempt in range(1, max_attempts + 1):
        page = await context.new_page()
        try:
            print(f"[nbmaa-fetch] detail attempt {attempt}/{max_attempts}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            try:
                await page.wait_for_selector(".tn-event-detail__main-container", timeout=NBMAA_DETAIL_WAIT_MS)
            except PlaywrightTimeoutError:
                await page.wait_for_timeout(NBMAA_DETAIL_WAIT_MS)

            html = await page.content()
            if not _looks_like_challenge_html(html) and _looks_like_detail_html(html):
                return html
        except Exception as exc:
            print(f"[nbmaa-fetch] detail warning for {url}: {exc}")
        finally:
            await page.close()

    print(f"[nbmaa-fetch] detail page skipped after retries: {url}")
    return None


def _parse_detail_html(html: str | None) -> NbmaaDetailFields:
    if not html:
        return NbmaaDetailFields(title=None, description=None, display_time=None)

    soup = BeautifulSoup(html, "html.parser")
    title = _extract_text(soup.select_one(".tn-event-detail__title"))
    description = _extract_description(soup.select_one(".tn-event-detail__description"))
    display_time = _extract_text(soup.select_one(".tn-event-detail__display-time"))
    return NbmaaDetailFields(
        title=title,
        description=description,
        display_time=display_time,
    )


def _extract_description(node) -> str | None:
    if node is None:
        return None
    lines = [
        _normalize_space(line)
        for line in node.get_text("\n", strip=True).splitlines()
        if _normalize_space(line)
    ]
    if not lines:
        return None

    deduped: list[str] = []
    seen: set[str] = set()
    for line in lines:
        lowered = line.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(line)
    return " | ".join(deduped)


def _extract_text(node) -> str | None:
    if node is None:
        return None
    text = _normalize_space(node.get_text(" ", strip=True))
    return text or None


def _best_api_title(*, performance: dict, production_title: str) -> str:
    title = (
        _normalize_space(_strip_html(str(performance.get("performanceSortTitle") or "")))
        or _normalize_space(_strip_html(str(performance.get("performanceTitle") or "")))
        or production_title
    )
    return title


def _should_include_event(*, title: str, description: str | None, combined_text: str) -> bool:
    normalized = combined_text.lower()

    if any(marker in normalized for marker in STRONG_EXCLUSION_MARKERS):
        return False

    if any(phrase in normalized for phrase in FORCE_INCLUDE_PHRASES):
        return True

    if any(_contains_marker(normalized, marker) for marker in INCLUSION_MARKERS):
        return True

    return any(marker in normalized for marker in YOUTH_ACTIVITY_MARKERS)


def _could_be_qualifying_event(*, production_title: str, performance_title: str) -> bool:
    normalized = _normalize_space(" ".join([production_title, performance_title])).lower()

    if any(marker in normalized for marker in STRONG_EXCLUSION_MARKERS):
        return False
    if any(phrase in normalized for phrase in FORCE_INCLUDE_PHRASES):
        return True
    if any(_contains_marker(normalized, marker) for marker in INCLUSION_MARKERS):
        return True
    return any(marker in normalized for marker in YOUTH_ACTIVITY_MARKERS)


def _infer_activity_type(*, title: str, description: str | None) -> str:
    combined = f"{title} {description or ''}".lower()
    if "lecture" in combined:
        return "lecture"
    if "talk" in combined:
        return "talk"
    if "class" in combined or "studio" in combined:
        return "class"
    if "workshop" in combined:
        return "workshop"
    if "conversation" in combined:
        return "conversation"
    return "activity"


def _registration_required(text_blob: str) -> bool | None:
    if "registration encouraged" in text_blob or "pre-registration is encouraged" in text_blob:
        return False
    if "registration not required" in text_blob or "no registration" in text_blob:
        return False
    if "walk-in tickets only" in text_blob or "walk in tickets only" in text_blob:
        return False
    if "registration required" in text_blob or "pre-registration required" in text_blob:
        return True
    if "register" in text_blob or "registration" in text_blob or "pre-registration" in text_blob:
        return True
    return None


def _infer_drop_in(text_blob: str) -> bool | None:
    if "drop-in" in text_blob or "drop in" in text_blob or "pop-up" in text_blob:
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


def _parse_api_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = value.strip()
    normalized = re.sub(r"\.(\d{6})\d([+-])", r".\1\2", normalized)
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return parse_iso_datetime(normalized, timezone_name=NBMAA_TIMEZONE)
    except ValueError:
        return None


def _parse_end_at(*, text: str, start_at: datetime) -> datetime | None:
    if not text:
        return None

    match = TIME_RANGE_RE.search(text)
    if match is None:
        return None

    start_meridiem = _normalize_meridiem(match.group("start_meridiem") or match.group("end_meridiem"))
    end_meridiem = _normalize_meridiem(match.group("end_meridiem"))
    start_hour = _to_24_hour(int(match.group("start_hour")), start_meridiem)
    end_hour = _to_24_hour(int(match.group("end_hour")), end_meridiem)
    start_minute = int(match.group("start_minute") or 0)
    end_minute = int(match.group("end_minute") or 0)

    if start_hour != start_at.hour or start_minute != start_at.minute:
        return None

    return start_at.replace(hour=end_hour, minute=end_minute)


def _pricing_text(*, title: str, description: str | None, performance_status: str | None) -> str:
    return _normalize_space(" ".join(filter(None, [title, description, performance_status])))


def _looks_like_challenge_html(html: str) -> bool:
    normalized = html.lower()
    return any(marker in normalized for marker in CHALLENGE_MARKERS)


def _looks_like_listing_html(html: str) -> bool:
    normalized = html.lower()
    return any(marker in normalized for marker in LISTING_MARKERS)


def _looks_like_detail_html(html: str) -> bool:
    normalized = html.lower()
    return any(marker in normalized for marker in DETAIL_MARKERS)


def _coerce_start_date(value: str | date | None) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        return date.fromisoformat(value.strip())
    return datetime.now().date()


def _normalize_url(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return urljoin(NBMAA_EVENTS_URL, text)


def _normalize_space(value: str) -> str:
    return " ".join(value.split())


def _strip_html(value: str) -> str:
    return BeautifulSoup(unescape(value), "html.parser").get_text(" ", strip=True)


def _contains_marker(text: str, marker: str) -> bool:
    return re.search(rf"\b{re.escape(marker)}\b", text) is not None


def _normalize_meridiem(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.lower().replace(".", "")
    if normalized == "am":
        return "am"
    if normalized == "pm":
        return "pm"
    return None


def _to_24_hour(hour: int, meridiem: str | None) -> int:
    if meridiem == "am":
        return 0 if hour == 12 else hour
    if meridiem == "pm":
        return 12 if hour == 12 else hour + 12
    return hour
