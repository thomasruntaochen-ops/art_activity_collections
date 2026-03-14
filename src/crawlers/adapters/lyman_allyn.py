import asyncio
import json
import re
from datetime import datetime
from datetime import time as datetime_time
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.types import ExtractedActivity

LYMAN_ALLYN_EVENTS_URL = "https://www.lymanallyn.org/events/"

NY_TIMEZONE = "America/New_York"
LYMAN_ALLYN_VENUE_NAME = "Lyman Allyn Art Museum"
LYMAN_ALLYN_CITY = "New London"
LYMAN_ALLYN_STATE = "CT"
LYMAN_ALLYN_DEFAULT_LOCATION = "New London, CT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": LYMAN_ALLYN_EVENTS_URL,
}

MONTH_NAME_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b",
    re.IGNORECASE,
)
TITLE_MONTH_YEAR_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b[^0-9]*(20\d{2})",
    re.IGNORECASE,
)
DESCRIPTION_MONTH_DAY_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\b",
    re.IGNORECASE,
)
DATE_LINE_RE = re.compile(
    r"(?:Date(?:\s*&\s*Time)?|When)\s*:\s*(?:[A-Za-z]+,\s*)?"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>am|pm)\b",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

INCLUDED_KEYWORDS = (
    "free first saturday",
    "science saturday",
    "workshop",
    "artmaking",
    "hands-on",
    "lecture",
    "talk",
    "conversation",
)
EXCLUDED_KEYWORDS = (
    "film",
    "gala",
    "open collections",
    "re/framed",
    "art after dark",
    "reception",
    "tour",
    "podcast",
)
PAID_MARKERS = (
    "$",
    "members $",
    "non-members",
    "general admission",
    "discount code",
    "tickets can be purchased",
)
FREE_MARKERS = (
    "cost: free",
    "admission is free",
    "free all day",
    "free and open to the public",
)


async def fetch_lyman_allyn_page(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS)

    last_exception: Exception | None = None
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            if response.status_code < 400:
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Lyman Allyn page") from last_exception
    raise RuntimeError("Unable to fetch Lyman Allyn page after retries")


async def load_lyman_allyn_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_lyman_allyn_page(LYMAN_ALLYN_EVENTS_URL, client=client)
        detail_urls = _extract_detail_urls(listing_html)
        detail_pages = await asyncio.gather(*(fetch_lyman_allyn_page(url, client=client) for url in detail_urls))

    return {
        "listing_url": LYMAN_ALLYN_EVENTS_URL,
        "listing_html": listing_html,
        "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
    }


def parse_lyman_allyn_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, html in payload["detail_pages"].items():
        detail = _parse_detail_page(html, source_url=source_url)
        if detail is None:
            continue
        if not _should_include_event(detail):
            continue

        start_at, end_at = _resolve_start_and_end(detail)
        if start_at is None:
            continue

        age_min, age_max = _parse_age_range(detail["description"] or "")
        text_blob = " ".join(
            filter(
                None,
                [detail["title"], detail["description"], detail["category"], detail["location_text"]],
            )
        ).lower()
        key = (source_url, detail["title"], start_at)
        if key in seen:
            continue
        seen.add(key)
        is_free, free_status = _classify_price(detail)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=detail["title"],
                description=detail["description"],
                venue_name=LYMAN_ALLYN_VENUE_NAME,
                location_text=detail["location_text"] or LYMAN_ALLYN_DEFAULT_LOCATION,
                city=LYMAN_ALLYN_CITY,
                state=LYMAN_ALLYN_STATE,
                activity_type=_infer_activity_type(detail),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob or "no registration" in text_blob),
                registration_required=(
                    ("register" in text_blob or "registration" in text_blob)
                    and "no registration" not in text_blob
                ),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class LymanAllynEventsAdapter(BaseSourceAdapter):
    source_name = "lyman_allyn_events"

    async def fetch(self) -> list[str]:
        html = await fetch_lyman_allyn_page(LYMAN_ALLYN_EVENTS_URL)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        detail_urls = _extract_detail_urls(payload)
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
            detail_pages = await asyncio.gather(*(fetch_lyman_allyn_page(url, client=client) for url in detail_urls))
        return parse_lyman_allyn_payload(
            {
                "listing_url": LYMAN_ALLYN_EVENTS_URL,
                "listing_html": payload,
                "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
            }
        )


def _extract_detail_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = (script.get_text() or "").strip()
        if '"@type": "Event"' not in script_text and '"@type":"Event"' not in script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        if not isinstance(data, dict) or data.get("@type") != "Event":
            continue

        url = str(data.get("url") or "").strip()
        if not url:
            continue
        normalized = urljoin(LYMAN_ALLYN_EVENTS_URL, url)
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)

    return urls


def _parse_detail_page(html: str, *, source_url: str) -> dict | None:
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    event_obj = _extract_event_object(soup)
    title = _normalize_space(
        str(
            (event_obj or {}).get("name")
            or _extract_heading_text(soup)
            or ""
        )
    )
    if not title:
        return None

    description = _normalize_space(str((event_obj or {}).get("description") or ""))
    if not description:
        description = _extract_description_text(soup)

    category = _extract_category(html)
    location_text = _normalize_space(
        str(((event_obj or {}).get("location") or {}).get("address") or "")
    )
    if not location_text:
        location_text = _extract_location_text(soup)

    start_at = _parse_iso_datetime((event_obj or {}).get("startDate"))
    end_at = _parse_iso_datetime((event_obj or {}).get("endDate"))

    return {
        "source_url": source_url,
        "title": title,
        "description": description or None,
        "category": category,
        "location_text": location_text or None,
        "start_at": start_at,
        "end_at": end_at,
    }


def _extract_event_object(soup: BeautifulSoup) -> dict | None:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = (script.get_text() or "").strip()
        if '"@type": "Event"' not in script_text and '"@type":"Event"' not in script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "Event":
            return data
    return None


def _extract_heading_text(soup: BeautifulSoup) -> str:
    heading = soup.select_one("h1")
    if heading is None:
        return ""
    return heading.get_text(" ", strip=True)


def _extract_description_text(soup: BeautifulSoup) -> str:
    lines: list[str] = []
    for node in soup.select(".mec-single-event-description p, .mec-event-content p, .elementor-widget-theme-post-content p"):
        line = _normalize_space(node.get_text(" ", strip=True))
        if line:
            lines.append(line)
    if lines:
        return " | ".join(lines)
    return ""


def _extract_category(html: str) -> str | None:
    match = re.search(r"/mec-category/([^/]+)/", html)
    if match is None:
        return None
    return match.group(1).replace("-", " ")


def _extract_location_text(soup: BeautifulSoup) -> str:
    text = soup.get_text("\n")
    for pattern in (
        r"(Lyman Allyn Art Museum[^.\n]*)",
        r"(New London, CT[^.\n]*)",
        r"(CT State Housatonic[^.\n]*)",
    ):
        match = re.search(pattern, text, re.IGNORECASE)
        if match is not None:
            return _normalize_space(match.group(1))
    return ""


def _should_include_event(detail: dict) -> bool:
    blob = " ".join(filter(None, [detail["title"], detail["description"], detail["category"]])).lower()
    if not blob:
        return False
    if any(keyword in blob for keyword in EXCLUDED_KEYWORDS):
        return False
    return any(keyword in blob for keyword in INCLUDED_KEYWORDS)


def _classify_price(detail: dict) -> tuple[bool | None, str]:
    description = (detail["description"] or "").lower()
    if any(marker in description for marker in FREE_MARKERS):
        return True, "confirmed"
    if any(marker in description for marker in PAID_MARKERS):
        return False, "confirmed"
    return None, "uncertain"


def _infer_activity_type(detail: dict) -> str:
    blob = " ".join(filter(None, [detail["title"], detail["description"], detail["category"]])).lower()
    if any(keyword in blob for keyword in ("lecture", "talk", "conversation")):
        return "talk"
    return "workshop"


def _resolve_start_and_end(detail: dict) -> tuple[datetime | None, datetime | None]:
    description = detail["description"] or ""
    candidate_start = detail["start_at"]
    candidate_end = detail["end_at"]
    year = _extract_year(detail["title"], candidate_start)

    explicit_date = _parse_explicit_description_date(description, year=year)
    if explicit_date is None:
        explicit_date = candidate_start

    if explicit_date is None:
        return None, None

    start_time, end_time = _parse_time_range(description)
    if start_time is None and candidate_start is not None and candidate_start.time() != datetime.min.time():
        start_time = candidate_start.time()
    if end_time is None and candidate_end is not None and candidate_end.time() != datetime.min.time():
        end_time = candidate_end.time()

    if start_time is None:
        start_at = explicit_date.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start_at = explicit_date.replace(
            hour=start_time.hour,
            minute=start_time.minute,
            second=0,
            microsecond=0,
        )

    end_at: datetime | None = None
    if end_time is not None:
        end_at = explicit_date.replace(
            hour=end_time.hour,
            minute=end_time.minute,
            second=0,
            microsecond=0,
        )

    return start_at, end_at


def _extract_year(title: str, candidate_start: datetime | None) -> int:
    match = TITLE_MONTH_YEAR_RE.search(title)
    if match is not None:
        return int(match.group(2))
    if candidate_start is not None:
        return candidate_start.year
    return datetime.now().year


def _parse_explicit_description_date(description: str, *, year: int) -> datetime | None:
    for regex in (DATE_LINE_RE, DESCRIPTION_MONTH_DAY_RE):
        match = regex.search(description)
        if match is None:
            continue
        month_name = match.group(1)
        day = int(match.group(2))
        month = datetime.strptime(month_name, "%B").month
        return datetime(year, month, day)
    return None


def _parse_time_range(description: str) -> tuple[datetime_time | None, datetime_time | None]:
    match = TIME_RANGE_RE.search(description)
    if match is not None:
        start_meridiem = (match.group("start_meridiem") or match.group("end_meridiem") or "").lower()
        end_meridiem = (match.group("end_meridiem") or start_meridiem).lower()
        start_hour = _to_24_hour(int(match.group("start_hour")), start_meridiem)
        end_hour = _to_24_hour(int(match.group("end_hour")), end_meridiem)
        start_minute = int(match.group("start_minute") or 0)
        end_minute = int(match.group("end_minute") or 0)
        return datetime(2000, 1, 1, start_hour, start_minute).time(), datetime(2000, 1, 1, end_hour, end_minute).time()

    match = TIME_SINGLE_RE.search(description)
    if match is not None:
        hour = _to_24_hour(int(match.group("hour")), match.group("meridiem").lower())
        minute = int(match.group("minute") or 0)
        return datetime(2000, 1, 1, hour, minute).time(), None

    return None, None


def _to_24_hour(hour: int, meridiem: str) -> int:
    meridiem = meridiem.lower()
    if meridiem == "pm" and hour != 12:
        return hour + 12
    if meridiem == "am" and hour == 12:
        return 0
    return hour


def _parse_age_range(description: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(description)
    if match is not None:
        return int(match.group(1)), int(match.group(2))
    match = AGE_PLUS_RE.search(description)
    if match is not None:
        return int(match.group(1)), None
    return None, None


def _parse_iso_datetime(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10:
        text = f"{text}T00:00:00"
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalize_space(value: str) -> str:
    return " ".join(value.split()).strip()
