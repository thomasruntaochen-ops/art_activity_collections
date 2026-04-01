import asyncio
import json
import re
from datetime import date
from datetime import datetime
from datetime import timedelta
from html import unescape
from zoneinfo import ZoneInfo

import httpx

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

NELSON_ATKINS_EVENTS_URL = "https://cart.nelson-atkins.org/events?view=calendar+view=list&kid=11,165,216"
NELSON_ATKINS_API_URL = "https://cart.nelson-atkins.org/api/products/productionseasons"
NELSON_ATKINS_TIMEZONE = "America/Chicago"
NELSON_ATKINS_VENUE_NAME = "Nelson-Atkins Museum of Art"
NELSON_ATKINS_CITY = "Kansas City"
NELSON_ATKINS_STATE = "MO"
NELSON_ATKINS_DEFAULT_LOCATION = "4525 Oak Street, Kansas City, MO 64111"
NELSON_ATKINS_DEFAULT_DAY_WINDOW = 184
NELSON_ATKINS_KEYWORDS = [11, 165, 216]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://cart.nelson-atkins.org",
    "Referer": NELSON_ATKINS_EVENTS_URL,
}

CHALLENGE_MARKERS = (
    "pardon our interruption",
    "please stand by",
    "incapsula",
)
FORCE_INCLUDE_MARKERS = (
    " free weekend fun ",
    " sketch club ",
    " teen drop-in art making ",
    " teen drop in art making ",
    " teacher workshop ",
)
INCLUDED_PRODUCTION_MARKERS = (
    " adult programs ",
    " adult studio classes ",
    " free weekend fun ",
    " library programs ",
    " professional development ",
    " teen programs ",
    " youth ",
    " youth studio classes ",
)
INCLUSION_MARKERS = (
    " art making ",
    " class ",
    " classes ",
    " drop-in ",
    " drop in ",
    " maker studio ",
    " studio ",
    " workshop ",
    " workshops ",
)
STRONG_EXCLUSION_MARKERS = (
    " access program ",
    " access programs ",
    " access tour ",
    " access tours ",
    " admission ",
    " art course ",
    " book social ",
    " brunch ",
    " cinema ",
    " closed ",
    " closes early ",
    " closure ",
    " closures ",
    " concert ",
    " exhibition ",
    " exhibitions ",
    " film ",
    " low sensory ",
    " member opening ",
    " member openings ",
    " member viewing ",
    " mini golf ",
    " movie ",
    " open mic ",
    " reading party ",
    " storytelling ",
    " story telling ",
    " story time ",
    " storytime ",
    " tour ",
    " tours ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
PAREN_AGE_RANGE_RE = re.compile(r"\((\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\)")
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
SEASON_PREFIX_RE = re.compile(r"^fy\d+\s+", re.IGNORECASE)


async def load_nelson_atkins_payload(
    *,
    start_date: str | date | None = None,
    day_window: int = NELSON_ATKINS_DEFAULT_DAY_WINDOW,
    timeout_ms: int = 90000,
) -> dict:
    start_day = _coerce_start_date(start_date)
    end_day = start_day + timedelta(days=max(day_window, 0))

    try:
        productions = await _fetch_productions_http(
            start_day=start_day,
            end_day=end_day,
            timeout_ms=timeout_ms,
        )
    except Exception:
        if async_playwright is None:
            raise
        productions = await _fetch_productions_playwright(
            start_day=start_day,
            end_day=end_day,
            timeout_ms=timeout_ms,
        )

    return {
        "productions": productions,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
    }


def parse_nelson_atkins_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NELSON_ATKINS_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for production in payload.get("productions") or []:
        production_title = _normalize_space(_strip_html(str(production.get("productionTitle") or "")))
        production_description = _normalize_space(_strip_html(str(production.get("description") or "")))
        normalized_production_title = _normalized_production_title(production_title)

        for performance in production.get("performances") or []:
            if not performance.get("isPerformanceVisible"):
                continue

            source_url = _normalize_space(str(performance.get("actionUrl") or ""))
            title = _best_api_title(performance=performance, production_title=production_title)
            start_at = _parse_api_datetime(
                str(performance.get("iso8601DateString") or performance.get("performanceDate") or "")
            )
            if not source_url or not title or start_at is None:
                continue
            if start_at.date() < current_date:
                continue

            performance_status = _normalize_space(str(performance.get("performanceStatusMessage") or ""))
            text_blob = _searchable_blob(
                " ".join(
                    filter(
                        None,
                        [
                            production_title,
                            normalized_production_title,
                            production_description,
                            title,
                            performance_status,
                            str(performance.get("displayTime") or ""),
                        ],
                    )
                )
            )
            if not _should_include_event(
                normalized_production_title=normalized_production_title,
                title=title,
                text_blob=text_blob,
            ):
                continue

            key = (source_url, title, start_at)
            if key in seen:
                continue
            seen.add(key)

            description = _build_description(
                production_title=production_title,
                production_description=production_description,
                performance_status=performance_status,
            )
            age_min, age_max = _parse_age_range(" ".join(filter(None, [title, production_description])))

            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=description,
                    venue_name=NELSON_ATKINS_VENUE_NAME,
                    location_text=NELSON_ATKINS_DEFAULT_LOCATION,
                    city=NELSON_ATKINS_CITY,
                    state=NELSON_ATKINS_STATE,
                    activity_type=_infer_activity_type(
                        normalized_production_title=normalized_production_title,
                        title=title,
                        description=description,
                    ),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=_infer_drop_in(text_blob),
                    registration_required=_infer_registration_required(text_blob),
                    start_at=start_at,
                    end_at=None,
                    timezone=NELSON_ATKINS_TIMEZONE,
                    **price_classification_kwargs(
                        " ".join(filter(None, [title, production_title, production_description, performance_status])),
                        default_is_free=None,
                    ),
                )
            )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class NelsonAtkinsEventsAdapter(BaseSourceAdapter):
    source_name = "nelson_atkins_events"

    async def fetch(self) -> list[str]:
        payload = await load_nelson_atkins_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_nelson_atkins_payload(json.loads(payload))


async def _fetch_productions_http(
    *,
    start_day: date,
    end_day: date,
    timeout_ms: int,
    max_attempts: int = 3,
) -> list[dict]:
    request_payload = _build_request_payload(start_day=start_day, end_day=end_day)
    last_exception: Exception | None = None

    async with httpx.AsyncClient(
        timeout=timeout_ms / 1000,
        follow_redirects=True,
        headers=DEFAULT_HEADERS,
    ) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.post(NELSON_ATKINS_API_URL, json=request_payload)
                response.raise_for_status()
                return _parse_production_response(response)
            except Exception as exc:
                last_exception = exc
                if attempt < max_attempts:
                    await asyncio.sleep(2 ** (attempt - 1))

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Nelson-Atkins production feed over HTTP.") from last_exception
    raise RuntimeError("Unable to fetch Nelson-Atkins production feed over HTTP.")


async def _fetch_productions_playwright(
    *,
    start_day: date,
    end_day: date,
    timeout_ms: int,
) -> list[dict]:
    if async_playwright is None:  # pragma: no cover - optional dependency
        raise RuntimeError("Playwright is not installed, so Nelson-Atkins browser fallback is unavailable.")

    request_payload = _build_request_payload(start_day=start_day, end_day=end_day)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page(locale="en-US")
        try:
            await page.goto(NELSON_ATKINS_EVENTS_URL, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(3000)
            html = await page.content()
            if _looks_like_challenge_html(html):
                raise RuntimeError("Nelson-Atkins listing page returned challenge HTML in Playwright.")

            data = await page.evaluate(
                """
                async payload => {
                    const response = await fetch('/api/products/productionseasons', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json, text/plain, */*',
                        },
                        body: JSON.stringify(payload),
                        credentials: 'same-origin',
                    });
                    if (!response.ok) {
                        throw new Error(`Nelson-Atkins production API status ${response.status}`);
                    }
                    return await response.json();
                }
                """,
                request_payload,
            )
        finally:
            await page.close()
            await browser.close()

    if not isinstance(data, list):
        raise RuntimeError("Nelson-Atkins production API returned an unexpected payload shape in Playwright.")
    return data


def _build_request_payload(*, start_day: date, end_day: date) -> dict:
    return {
        "productionSeasonIdFilter": [],
        "keywordIds": None,
        "startDate": f"{start_day.isoformat()}T00:00",
        "endDate": f"{end_day.isoformat()}T23:59",
        "keywords": list(NELSON_ATKINS_KEYWORDS),
    }


def _parse_production_response(response: httpx.Response) -> list[dict]:
    content_type = (response.headers.get("content-type") or "").lower()
    if "json" not in content_type:
        if _looks_like_challenge_html(response.text):
            raise RuntimeError("Nelson-Atkins production feed returned challenge HTML.")
        raise RuntimeError(f"Unexpected Nelson-Atkins content type: {content_type or 'unknown'}")

    data = response.json()
    if not isinstance(data, list):
        raise RuntimeError("Nelson-Atkins production API returned an unexpected payload shape.")
    return data


def _should_include_event(*, normalized_production_title: str, title: str, text_blob: str) -> bool:
    if any(marker in text_blob for marker in STRONG_EXCLUSION_MARKERS):
        return False
    if any(marker in text_blob for marker in FORCE_INCLUDE_MARKERS):
        return True
    if f" {normalized_production_title} " in INCLUDED_PRODUCTION_MARKERS:
        return True
    if any(marker in text_blob for marker in INCLUSION_MARKERS):
        return True
    return False


def _build_description(
    *,
    production_title: str,
    production_description: str,
    performance_status: str,
) -> str | None:
    parts: list[str] = []
    if production_title:
        parts.append(f"Series: {production_title}")
    if production_description:
        parts.append(production_description)
    if performance_status:
        parts.append(f"Status: {performance_status}")
    return " | ".join(parts) if parts else None


def _best_api_title(*, performance: dict, production_title: str) -> str:
    return (
        _normalize_space(_strip_html(str(performance.get("performanceSortTitle") or "")))
        or _normalize_space(_strip_html(str(performance.get("performanceTitle") or "")))
        or production_title
    )


def _infer_activity_type(*, normalized_production_title: str, title: str, description: str | None) -> str:
    combined = _searchable_blob(" ".join(filter(None, [normalized_production_title, title, description or ""])))
    if " workshop " in combined or " teacher workshop " in combined:
        return "workshop"
    if " class " in combined or " classes " in combined or " studio " in combined:
        return "class"
    if " lecture " in combined:
        return "lecture"
    if " talk " in combined or " adult programs " in combined or " library programs " in combined:
        return "talk"
    return "activity"


def _infer_drop_in(text_blob: str) -> bool | None:
    if " drop in " in text_blob or " drop-in " in text_blob or " free weekend fun " in text_blob:
        return True
    return None


def _infer_registration_required(text_blob: str) -> bool | None:
    if " drop in " in text_blob or " drop-in " in text_blob or " free weekend fun " in text_blob:
        return False
    if any(
        marker in text_blob
        for marker in (
            " sold out ",
            " not on sale ",
            " not yet available ",
            " register ",
            " registration ",
            " ticket ",
            " tickets ",
        )
    ):
        return True
    return None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    for pattern in (AGE_RANGE_RE, PAREN_AGE_RANGE_RE):
        match = pattern.search(text)
        if match is not None:
            return int(match.group(1)), int(match.group(2))

    age_plus_match = AGE_PLUS_RE.search(text)
    if age_plus_match is not None:
        return int(age_plus_match.group(1)), None

    return None, None


def _parse_api_datetime(value: str) -> datetime | None:
    if not value:
        return None

    normalized = value.strip()
    normalized = re.sub(r"\.(\d{6})\d([+-])", r".\1\2", normalized)
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return parse_iso_datetime(normalized, timezone_name=NELSON_ATKINS_TIMEZONE)
    except ValueError:
        return None


def _normalized_production_title(value: str) -> str:
    normalized = SEASON_PREFIX_RE.sub("", value).strip().lower()
    return _normalize_space(normalized)


def _coerce_start_date(value: str | date | None) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        return date.fromisoformat(value.strip())
    return datetime.now().date()


def _looks_like_challenge_html(text: str) -> bool:
    normalized = text.lower()
    return any(marker in normalized for marker in CHALLENGE_MARKERS)


def _searchable_blob(value: str) -> str:
    normalized = _normalize_space(value).lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return f" {normalized.strip()} "


def _strip_html(value: str) -> str:
    text = re.sub(r"<br\\s*/?>", " ", value, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return unescape(text)


def _normalize_space(value: str) -> str:
    return " ".join(value.split())
