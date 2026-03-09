import asyncio
import re
from datetime import date, datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

HAMMER_PROGRAMS_URL = (
    "https://hammer.ucla.edu/programs-events"
    "?category[5326]=5326&category[5346]=5346&category[5341]=5341&category[5331]=5331"
)

LA_TIMEZONE = "America/Los_Angeles"
HAMMER_VENUE_NAME = "Hammer Museum"
HAMMER_CITY = "Los Angeles"
HAMMER_STATE = "CA"
HAMMER_DEFAULT_LOCATION = "Los Angeles, CA"

OCCURRENCE_RE = re.compile(
    r"^(?P<weekday>[A-Za-z]{3})\s+"
    r"(?P<month>[A-Za-z]{3})\s+"
    r"(?P<day>\d{1,2})\s+"
    r"(?P<time>.+)$"
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>AM|PM)\s*"
    r"(?:-|--|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>AM|PM)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>AM|PM)",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

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
    "screening",
    "screenings",
)
SOFT_EXCLUDED_KEYWORDS = (
    "tour",
    "tours",
)
INCLUDED_KEYWORDS = (
    "talk",
    "class",
    "lecture",
    "activity",
    "workshop",
    "lab",
    "conversation",
    "art lab",
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://hammer.ucla.edu/programs-events",
}


async def fetch_hammer_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[hammer-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[hammer-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[hammer-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[hammer-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[hammer-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[hammer-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(f"[hammer-fetch] transient status={response.status_code}, retrying after {wait_seconds:.1f}s")
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Hammer programs page") from last_exception
    raise RuntimeError("Unable to fetch Hammer programs page after retries")


async def fetch_hammer_program_pages(
    url: str = HAMMER_PROGRAMS_URL,
    *,
    max_pages: int = 10,
) -> list[tuple[str, str]]:
    pages: list[tuple[str, str]] = []
    visited: set[str] = set()
    seen_payloads: set[str] = set()
    next_url: str | None = url

    while next_url and next_url not in visited and len(pages) < max_pages:
        visited.add(next_url)
        html = await fetch_hammer_page(next_url)
        if html in seen_payloads:
            break
        seen_payloads.add(html)
        pages.append((next_url, html))
        next_url = _extract_next_page_url(html, list_url=next_url)

    return pages


class HammerProgramsAdapter(BaseSourceAdapter):
    source_name = "hammer_programs"

    def __init__(self, url: str = HAMMER_PROGRAMS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        pages = await fetch_hammer_program_pages(self.url)
        return [html for _, html in pages]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_hammer_program_pages([(self.url, payload)], list_url=self.url)


def parse_hammer_program_pages(
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
        for card in soup.select("a.result-item.result-item--program[href]"):
            title_node = card.select_one("h3.result-item__title")
            excerpt_node = card.select_one("div.result-item__excerpt")
            occurrence_node = card.select_one("div.result-item__occurrence")
            category_nodes = card.select(".field__item.category")

            title = _normalize_text(title_node.get_text(" ", strip=True) if title_node else None)
            description = _normalize_text(excerpt_node.get_text(" ", strip=True) if excerpt_node else None)
            occurrence_text = _normalize_text(occurrence_node.get_text(" ", strip=True) if occurrence_node else None)
            categories = [_normalize_text(node.get_text(" ", strip=True)) for node in category_nodes]
            categories = [value for value in categories if value]
            if not title or not occurrence_text:
                continue
            if is_irrelevant_item_text(title):
                continue

            if _should_exclude_event(title=title, description=description, categories=categories):
                continue
            if not _should_include_event(title=title, description=description, categories=categories):
                continue

            start_at, end_at = _parse_occurrence(occurrence_text, current_date=current_date)
            if start_at is None:
                continue
            if start_at.date() < current_date:
                continue

            source_url = urljoin(page_url, card["href"])
            age_min, age_max = _parse_age_range(title=title, description=description)
            category_text = ", ".join(categories) if categories else None
            blob = " ".join([title, description or "", category_text or "", occurrence_text]).lower()
            description_parts = []
            if description:
                description_parts.append(description)
            if category_text:
                description_parts.append(f"Category: {category_text}")
            description_parts.append(f"Schedule: {occurrence_text}")
            full_description = " | ".join(description_parts)

            key = (source_url, title, start_at)
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=full_description,
                    venue_name=HAMMER_VENUE_NAME,
                    location_text=HAMMER_DEFAULT_LOCATION,
                    city=HAMMER_CITY,
                    state=HAMMER_STATE,
                    activity_type=_infer_activity_type(
                        title=title,
                        description=description,
                        categories=categories,
                    ),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=("drop-in" in blob or "drop in" in blob),
                    registration_required=("registration" in blob and "not required" not in blob),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=LA_TIMEZONE,
                    free_verification_status=("confirmed" if "free" in blob else "inferred"),
                )
            )

    return rows


def _extract_next_page_url(html: str, *, list_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    anchor = soup.select_one("li.pager__item--next a[href]")
    if anchor is None:
        return None
    return urljoin(list_url, anchor["href"])


def _should_exclude_event(*, title: str, description: str | None, categories: list[str]) -> bool:
    title_blob = title.lower()
    blob = " ".join([title, description or "", " ".join(categories)]).lower()
    if any(keyword in blob for keyword in HARD_EXCLUDED_KEYWORDS):
        return True
    if any(keyword in blob for keyword in SOFT_EXCLUDED_KEYWORDS) and not any(
        keyword in blob for keyword in INCLUDED_KEYWORDS
    ):
        return True
    if "opening" in title_blob and not any(keyword in title_blob for keyword in INCLUDED_KEYWORDS):
        return True
    return False


def _should_include_event(*, title: str, description: str | None, categories: list[str]) -> bool:
    blob = " ".join([title, description or "", " ".join(categories)]).lower()
    return any(keyword in blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(*, title: str, description: str | None, categories: list[str]) -> str:
    blob = " ".join([title, description or "", " ".join(categories)]).lower()
    if any(keyword in blob for keyword in ("talk", "lecture", "conversation")):
        return "talk"
    if any(keyword in blob for keyword in ("workshop", "class", "lab", "activity")):
        return "workshop"
    return "activity"


def _parse_occurrence(value: str, *, current_date: date) -> tuple[datetime | None, datetime | None]:
    match = OCCURRENCE_RE.match(_normalize_space(value))
    if not match:
        return None, None

    month_value = match.group("month")
    day_value = int(match.group("day"))
    base_day = _resolve_upcoming_date(month_value=month_value, day_value=day_value, current_date=current_date)
    if base_day is None:
        return None, None

    time_text = match.group("time")
    range_match = TIME_RANGE_RE.search(time_text)
    if range_match:
        start_hour, start_minute = _to_24h(
            int(range_match.group("start_hour")),
            int(range_match.group("start_minute") or "0"),
            range_match.group("start_meridiem"),
        )
        end_hour, end_minute = _to_24h(
            int(range_match.group("end_hour")),
            int(range_match.group("end_minute") or "0"),
            range_match.group("end_meridiem"),
        )
        return (
            datetime.combine(base_day, datetime.min.time()).replace(hour=start_hour, minute=start_minute),
            datetime.combine(base_day, datetime.min.time()).replace(hour=end_hour, minute=end_minute),
        )

    single_match = TIME_SINGLE_RE.search(time_text)
    if single_match is None:
        return datetime.combine(base_day, datetime.min.time()), None

    hour, minute = _to_24h(
        int(single_match.group("hour")),
        int(single_match.group("minute") or "0"),
        single_match.group("meridiem"),
    )
    return datetime.combine(base_day, datetime.min.time()).replace(hour=hour, minute=minute), None


def _resolve_upcoming_date(*, month_value: str, day_value: int, current_date: date) -> date | None:
    for year in (current_date.year, current_date.year + 1):
        try:
            candidate = datetime.strptime(f"{month_value} {day_value} {year}", "%b %d %Y").date()
        except ValueError:
            continue
        if candidate >= current_date:
            return candidate
    try:
        return datetime.strptime(f"{month_value} {day_value} {current_date.year}", "%b %d %Y").date()
    except ValueError:
        return None


def _parse_age_range(*, title: str, description: str | None) -> tuple[int | None, int | None]:
    blob = " ".join([title, description or ""])
    range_match = AGE_RANGE_RE.search(blob)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))

    plus_match = AGE_PLUS_RE.search(blob)
    if plus_match:
        return int(plus_match.group(1)), None

    if "kids" in blob.lower():
        return None, 12
    return None, None


def _to_24h(hour: int, minute: int, meridiem: str | None) -> tuple[int, int]:
    normalized = (meridiem or "").strip().upper()
    if normalized == "AM":
        hour = 0 if hour == 12 else hour
    elif normalized == "PM":
        hour = hour if hour == 12 else hour + 12
    return hour, minute


def _normalize_space(value: str) -> str:
    return " ".join(value.split())


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _normalize_space(value)
    return normalized or None
