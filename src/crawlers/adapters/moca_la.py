import asyncio
import html
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

MOCA_PROGRAMS_URL = "https://www.moca.org/programs?category=25"
MOCA_TOGETHER_THURSDAYS_URL = "https://www.moca.org/together-thursdays"

LA_TIMEZONE = "America/Los_Angeles"
MOCA_VENUE_NAME = "The Museum of Contemporary Art, Los Angeles"
MOCA_CITY = "Los Angeles"
MOCA_STATE = "CA"
MOCA_DEFAULT_LOCATION = "Los Angeles, CA"

DATE_TEXT_RE = re.compile(r"(?P<month>[A-Za-z]{3})\s+(?P<day>\d{1,2})(?:,\s*(?P<time>.+))?$")
TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>am|pm)",
    re.IGNORECASE,
)
EXCLUDED_PRIMARY_KEYWORDS = (
    "ticket",
    "tickets",
    "tour",
    "tours",
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
    "dj",
)
INCLUDED_KEYWORDS = (
    "talk",
    "lecture",
    "conversation",
    "class",
    "activity",
    "workshop",
    "lab",
    "family",
    "drawing",
    "studio",
    "maker",
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.moca.org/programs",
}


async def fetch_moca_programs_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[moca-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[moca-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[moca-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[moca-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[moca-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[moca-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[moca-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch MOCA programs page") from last_exception
    raise RuntimeError("Unable to fetch MOCA programs page after retries")


class MocaProgramsAdapter(BaseSourceAdapter):
    source_name = "moca_programs"

    def __init__(self, url: str = MOCA_PROGRAMS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_moca_programs_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_moca_programs_html(payload, list_url=self.url)


def parse_moca_programs_html(
    html: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    current = now or datetime.now()
    current_date = current.date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    page_year = _infer_page_year(soup=soup, list_url=list_url, fallback_year=current.year)

    for card in soup.select("div.container-min-height.block-off-white"):
        row = _build_row_from_card(card=card, list_url=list_url, current_date=current_date, page_year=page_year)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    return rows


def _build_row_from_card(
    *,
    card,
    list_url: str,
    current_date,
    page_year: int,
) -> ExtractedActivity | None:
    link = card.select_one("h3 a[href]") or card.select_one("article a[href]")
    if link is None:
        return None

    title = _normalize_text(link)
    if not title or is_irrelevant_item_text(title):
        return None

    category_blob = _normalize_text(card.select_one(".label-heading .label.gray"))
    description = _normalize_text(card.select_one(".copy-secondary"))
    location_name = _normalize_text(card.select_one("a.circle-link"))
    schedule_text = _extract_schedule_text(card)
    if not schedule_text:
        return None

    text_blob = " ".join(
        part for part in [title, category_blob or "", description or "", location_name or "", schedule_text] if part
    ).lower()
    if any(keyword in text_blob for keyword in EXCLUDED_PRIMARY_KEYWORDS):
        return None
    if not any(keyword in text_blob for keyword in INCLUDED_KEYWORDS):
        return None

    start_at = _parse_schedule(schedule_text, page_year=page_year)
    if start_at is None or start_at.date() < current_date:
        return None

    source_url = urljoin(list_url, link.get("href", "").strip())
    location_text = _location_text(location_name)
    full_description = _join_non_empty(
        [
            description,
            f"Category: {category_blob}" if category_blob else None,
            f"Schedule: {schedule_text}",
            f"Location: {location_name}" if location_name else None,
        ]
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=MOCA_VENUE_NAME,
        location_text=location_text,
        city=MOCA_CITY,
        state=MOCA_STATE,
        activity_type=_infer_activity_type(title=title, category_blob=category_blob, description=description),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=("registration" in text_blob and "not required" not in text_blob),
        start_at=start_at,
        end_at=None,
        timezone=LA_TIMEZONE,
        free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
    )


def _infer_page_year(*, soup: BeautifulSoup, list_url: str, fallback_year: int) -> int:
    match = re.search(r"/programs/(?P<year>\d{4})/\d{1,2}", list_url)
    if match is not None:
        return int(match.group("year"))

    for anchor in soup.select("a[href*='/programs/']"):
        href = anchor.get("href", "")
        match = re.search(r"/programs/(?P<year>\d{4})/\d{1,2}", href)
        if match is not None:
            return int(match.group("year"))

    return fallback_year


def _extract_schedule_text(card) -> str | None:
    node = card.select_one(".left .label") or card.select_one(".label-heading.fw .fr.label")
    return _normalize_text(node)


def _parse_schedule(value: str, *, page_year: int) -> datetime | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None

    date_match = DATE_TEXT_RE.search(normalized)
    if date_match is None:
        return None

    month = date_match.group("month")
    day = int(date_match.group("day"))
    time_text = (date_match.group("time") or "").strip()
    try:
        base = datetime.strptime(f"{month} {day} {page_year}", "%b %d %Y")
    except ValueError:
        return None

    if not time_text:
        return base.replace(hour=0, minute=0, second=0, microsecond=0)

    time_match = TIME_SINGLE_RE.search(time_text)
    if time_match is None:
        return None

    hour = int(time_match.group("hour"))
    minute = int(time_match.group("minute") or 0)
    meridiem = time_match.group("meridiem").lower()
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0

    return base.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _infer_activity_type(*, title: str, category_blob: str | None, description: str | None) -> str:
    text_blob = " ".join(part for part in [title, category_blob or "", description or ""] if part).lower()
    if any(keyword in text_blob for keyword in ("talk", "lecture", "conversation", "panel")):
        return "talk"
    return "workshop"


def _location_text(location_name: str | None) -> str:
    if not location_name:
        return MOCA_DEFAULT_LOCATION
    if location_name.lower().startswith("moca "):
        return f"{location_name}, Los Angeles, CA"
    return location_name


def _normalize_text(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "get_text"):
        value = value.get_text(" ", strip=True)
    return _normalize_space(str(value))


def _normalize_space(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(html.unescape(value).replace("\xa0", " ").split())
    return normalized or None


def _join_non_empty(parts: list[str | None]) -> str | None:
    kept = [part for part in parts if part]
    return " | ".join(kept) if kept else None
