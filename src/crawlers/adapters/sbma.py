import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

SBMA_EVENTS_URL = (
    "https://www.sbma.net/events"
    "?field_event_type_target_id%5B24%5D=24"
    "&field_event_type_target_id%5B21%5D=21"
    "&field_event_type_target_id%5B28%5D=28"
    "&field_event_type_target_id%5B29%5D=29"
)

LA_TIMEZONE = "America/Los_Angeles"
SBMA_VENUE_NAME = "Santa Barbara Museum of Art"
SBMA_CITY = "Santa Barbara"
SBMA_STATE = "CA"
SBMA_DEFAULT_LOCATION = "Santa Barbara, CA"

SINGLE_DAY_RE = re.compile(
    r"^(?P<weekday>[A-Za-z]+),\s+"
    r"(?P<month>[A-Za-z]+)\s+"
    r"(?P<day>\d{1,2}),\s+"
    r"(?P<year>\d{4})$"
)
RANGE_RE = re.compile(
    r"^(?P<start_month>[A-Za-z]+)\s+"
    r"(?P<start_day>\d{1,2})\s+[–-]\s+"
    r"(?:(?P<end_month>[A-Za-z]+)\s+)?"
    r"(?P<end_day>\d{1,2}),\s+"
    r"(?P<year>\d{4})$"
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>am|pm|AM|PM)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>am|pm|AM|PM)",
)
TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>am|pm|AM|PM)"
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)

HARD_EXCLUDED_KEYWORDS = (
    "ticket",
    "tickets",
    "registration",
    "camp",
    "free night",
    "fundraising",
    "admission",
    "exhibition",
    "film",
    "tv",
    "reading",
    "writing",
    "open house",
    "performance",
    "music",
    "concert",
)
SOFT_EXCLUDED_KEYWORDS = (
    "tour",
    "tours",
)
INCLUDED_KEYWORDS = (
    "talk",
    "lecture",
    "artist talk",
    "workshop",
    "class",
    "activity",
    "lab",
    "conversation",
    "sketch",
    "art party",
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.sbma.net/events",
}


async def fetch_sbma_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[sbma-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[sbma-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[sbma-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[sbma-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[sbma-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[sbma-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(f"[sbma-fetch] transient status={response.status_code}, retrying after {wait_seconds:.1f}s")
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch SBMA events page") from last_exception
    raise RuntimeError("Unable to fetch SBMA events page after retries")


async def fetch_sbma_events_pages(
    url: str = SBMA_EVENTS_URL,
    *,
    max_pages: int = 10,
) -> list[tuple[str, str]]:
    pages: list[tuple[str, str]] = []
    visited: set[str] = set()
    next_url: str | None = url

    while next_url and next_url not in visited and len(pages) < max_pages:
        visited.add(next_url)
        html = await fetch_sbma_events_page(next_url)
        pages.append((next_url, html))
        next_url = _extract_next_page_url(html, list_url=next_url)

    return pages


class SbmaEventsAdapter(BaseSourceAdapter):
    source_name = "sbma_events"

    def __init__(self, url: str = SBMA_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        pages = await fetch_sbma_events_pages(self.url)
        return [html for _, html in pages]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_sbma_events_pages([(self.url, payload)], list_url=self.url)


def parse_sbma_events_pages(
    pages: list[tuple[str, str]],
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    current_date = (now or datetime.now()).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for page_url, html in pages:
        soup = BeautifulSoup(html, "html.parser")
        for row in soup.select("div.view-events.view-display-id-page_1 div.views-row"):
            title_node = row.select_one(".views-field-title h2 a[href]")
            subtitle_node = row.select_one(".views-field-field-subtitle")
            schedule_node = row.select_one(".views-field-field-eve-datetime")
            type_nodes = row.select(".views-field-field-event-type a")
            cta_node = row.select_one(".views-field-field-url a[href]")

            if title_node is None or schedule_node is None:
                continue

            title = _normalize_text(title_node.get_text(" ", strip=True))
            subtitle = _normalize_text(subtitle_node.get_text(" ", strip=True) if subtitle_node else None)
            schedule_raw = schedule_node.get_text("\n", strip=True)
            schedule_text = _normalize_text(schedule_raw)
            categories = [_normalize_text(node.get("title") or node.get_text(" ", strip=True)) for node in type_nodes]
            categories = [value for value in categories if value]
            cta_text = _normalize_text(cta_node.get_text(" ", strip=True) if cta_node else None)
            if not title or not schedule_text:
                continue
            if is_irrelevant_item_text(title):
                continue

            if _should_exclude_event(
                title=title,
                subtitle=subtitle,
                categories=categories,
                cta_text=cta_text,
            ):
                continue
            if not _should_include_event(title=title, subtitle=subtitle, categories=categories):
                continue

            start_at, end_at = _parse_schedule(schedule_raw)
            if start_at is None or start_at.date() < current_date:
                continue

            source_url = urljoin(page_url, title_node["href"])
            age_min, age_max = _parse_age_range(subtitle)
            category_text = ", ".join(categories) if categories else None
            blob = " ".join([title, subtitle or "", category_text or "", schedule_text, cta_text or ""]).lower()
            description_parts = []
            if subtitle:
                description_parts.append(subtitle)
            if category_text:
                description_parts.append(f"Category: {category_text}")
            description_parts.append(f"Schedule: {schedule_text}")
            if cta_text:
                description_parts.append(f"CTA: {cta_text}")
            description = " | ".join(description_parts)

            key = (source_url, title, start_at)
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=description,
                    venue_name=SBMA_VENUE_NAME,
                    location_text=SBMA_DEFAULT_LOCATION,
                    city=SBMA_CITY,
                    state=SBMA_STATE,
                    activity_type=_infer_activity_type(title=title, subtitle=subtitle, categories=categories),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=("drop-in" in blob or "drop in" in blob),
                    registration_required=bool(
                        cta_text and any(keyword in cta_text.lower() for keyword in ("register", "ticket"))
                    ),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=LA_TIMEZONE,
                    free_verification_status=("confirmed" if "free" in blob else "inferred"),
                )
            )

    return rows


def _extract_next_page_url(html: str, *, list_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    anchor = soup.select_one("nav.pager--load-more a[href]")
    if anchor is None:
        return None
    return urljoin(list_url, anchor["href"])


def _should_exclude_event(
    *,
    title: str,
    subtitle: str | None,
    categories: list[str],
    cta_text: str | None,
) -> bool:
    blob = " ".join([title, subtitle or "", " ".join(categories)]).lower()
    if any(keyword in blob for keyword in HARD_EXCLUDED_KEYWORDS):
        return True
    if any(keyword in blob for keyword in SOFT_EXCLUDED_KEYWORDS) and not any(
        keyword in blob for keyword in INCLUDED_KEYWORDS
    ):
        return True
    return False


def _should_include_event(*, title: str, subtitle: str | None, categories: list[str]) -> bool:
    blob = " ".join([title, subtitle or "", " ".join(categories)]).lower()
    return any(keyword in blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(*, title: str, subtitle: str | None, categories: list[str]) -> str:
    blob = " ".join([title, subtitle or "", " ".join(categories)]).lower()
    if any(keyword in blob for keyword in ("talk", "lecture", "conversation")):
        return "talk"
    if any(keyword in blob for keyword in ("workshop", "class", "lab", "sketch", "art party")):
        return "workshop"
    return "activity"


def _parse_schedule(value: str) -> tuple[datetime | None, datetime | None]:
    lines = [line.strip() for line in value.splitlines() if line.strip()]
    if not lines:
        return None, None

    date_line = None
    time_line = None
    for line in lines:
        if re.search(r"\d{4}", line):
            date_line = line
        elif re.search(r"\d", line):
            time_line = line

    if date_line is None:
        return None, None

    base_day = _parse_date_line(date_line)
    if base_day is None:
        return None, None
    if time_line is None:
        return base_day.replace(hour=0, minute=0, second=0, microsecond=0), None

    return _parse_time_line(base_day=base_day, value=time_line)


def _parse_date_line(value: str) -> datetime | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None

    single_match = SINGLE_DAY_RE.match(normalized)
    if single_match:
        try:
            return datetime.strptime(
                f"{single_match.group('month')} {single_match.group('day')} {single_match.group('year')}",
                "%B %d %Y",
            )
        except ValueError:
            return None

    range_match = RANGE_RE.match(normalized)
    if range_match:
        start_month = range_match.group("start_month")
        start_day = range_match.group("start_day")
        year = range_match.group("year")
        try:
            return datetime.strptime(f"{start_month} {start_day} {year}", "%B %d %Y")
        except ValueError:
            return None

    return None


def _parse_time_line(*, base_day: datetime, value: str) -> tuple[datetime | None, datetime | None]:
    normalized = _normalize_text(value)
    if not normalized:
        return base_day.replace(hour=0, minute=0, second=0, microsecond=0), None

    range_match = TIME_RANGE_RE.search(normalized)
    if range_match:
        start_meridiem = range_match.group("start_meridiem") or range_match.group("end_meridiem")
        start_hour, start_minute = _to_24h(
            int(range_match.group("start_hour")),
            int(range_match.group("start_minute") or "0"),
            start_meridiem,
        )
        end_hour, end_minute = _to_24h(
            int(range_match.group("end_hour")),
            int(range_match.group("end_minute") or "0"),
            range_match.group("end_meridiem"),
        )
        return (
            base_day.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0),
            base_day.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0),
        )

    single_match = TIME_SINGLE_RE.search(normalized)
    if single_match is None:
        return base_day.replace(hour=0, minute=0, second=0, microsecond=0), None

    hour, minute = _to_24h(
        int(single_match.group("hour")),
        int(single_match.group("minute") or "0"),
        single_match.group("meridiem"),
    )
    return base_day.replace(hour=hour, minute=minute, second=0, microsecond=0), None


def _parse_age_range(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    match = AGE_RANGE_RE.search(value)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def _to_24h(hour: int, minute: int, meridiem: str | None) -> tuple[int, int]:
    normalized = (meridiem or "").strip().upper()
    if normalized == "AM":
        hour = 0 if hour == 12 else hour
    elif normalized == "PM":
        hour = hour if hour == 12 else hour + 12
    return hour, minute


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.split())
    return normalized or None
