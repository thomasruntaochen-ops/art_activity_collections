import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

NEWARK_EVENTS_URL = "https://newarkmuseumart.org/events/"

NY_TIMEZONE = "America/New_York"
NEWARK_VENUE_NAME = "The Newark Museum of Art"
NEWARK_CITY = "Newark"
NEWARK_STATE = "NJ"
NEWARK_DEFAULT_LOCATION = "Newark, NJ"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": NEWARK_EVENTS_URL,
}

DATE_AND_TIMES_RE = re.compile(
    r"^(?:[A-Za-z]+,\s+)?(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<times>.+)$"
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>a\.?m\.?|p\.?m\.?|am|pm)?\s*"
    r"(?:-|–|—|‑|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?|am|pm)\b",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

HARD_EXCLUDED_KEYWORDS = (
    "tour",
    "registration",
    "camp",
    "yoga",
    "sports",
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
    "storytime",
    "dinner",
    "reception",
    "jazz",
    "orchestra",
    "band",
    "tai chi",
    "poetry",
)
STRONG_INCLUDED_KEYWORDS = (
    "talk",
    "class",
    "lecture",
    "activity",
    "workshop",
    "lab",
    "conversation",
    "artmaking",
    "family",
    "teen",
    "junior museum",
    "educator",
)


async def fetch_newark_page(
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
        raise RuntimeError("Unable to fetch Newark Museum events page") from last_exception
    raise RuntimeError("Unable to fetch Newark Museum events page after retries")


async def load_newark_events_payload() -> dict:
    html = await fetch_newark_page(NEWARK_EVENTS_URL)
    return {
        "listing_url": NEWARK_EVENTS_URL,
        "html": html,
    }


def parse_newark_events_payload(payload: dict) -> list[ExtractedActivity]:
    list_url = str(payload.get("listing_url") or NEWARK_EVENTS_URL)
    html = str(payload.get("html") or "")
    return parse_newark_events_html(html, list_url=list_url)


class NewarkEventsAdapter(BaseSourceAdapter):
    source_name = "newark_events"

    def __init__(self, url: str = NEWARK_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_newark_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_newark_events_html(payload, list_url=self.url)


def parse_newark_events_html(html: str, *, list_url: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    today = datetime.now().date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in soup.select("div.item.event-item"):
        anchor = item.select_one("a[href]")
        title_node = item.select_one("h4")
        date_node = item.select_one("p.date-time")
        if anchor is None or title_node is None:
            continue

        source_url = urljoin(list_url, anchor.get("href", "").strip())
        title = _normalize_text(title_node.get_text(" ", strip=True))
        date_text = _normalize_text(date_node.get_text(" ", strip=True) if date_node else None)
        terms = _normalize_text(item.get("data-terms"))
        data_datetime = _normalize_text(item.get("data-datetime"))

        if not source_url or not title:
            continue
        if "planetarium shows" in title.lower():
            continue

        start_at = _parse_data_datetime(data_datetime)
        end_at = None
        if date_text:
            parsed_start, parsed_end = _parse_from_date_text(date_text)
            if start_at is None:
                start_at = parsed_start
            end_at = parsed_end
        if start_at is None:
            continue
        if start_at.date() < today:
            continue

        text_blob = " ".join(part for part in [title, terms or "", date_text or ""] if part).lower()
        if _has_any_keywords(text_blob, HARD_EXCLUDED_KEYWORDS):
            continue
        if not _has_any_keywords(text_blob, STRONG_INCLUDED_KEYWORDS):
            continue

        description = " | ".join(part for part in [f"Tags: {terms}" if terms else None, date_text] if part)
        age_min, age_max = _parse_age_range(" ".join(part for part in [title, description] if part))
        is_free, free_status = infer_price_classification(description)

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=NEWARK_VENUE_NAME,
                location_text=NEWARK_DEFAULT_LOCATION,
                city=NEWARK_CITY,
                state=NEWARK_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=(
                    "registration" in text_blob or "register" in text_blob or "ticket" in text_blob
                ) and "no registration" not in text_blob,
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_data_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) == 12:
        try:
            return datetime.strptime(digits, "%Y%m%d%H%M")
        except ValueError:
            return None
    if len(digits) == 11:
        date_part = digits[:8]
        hour = int(digits[8:9])
        minute = int(digits[9:11])
        try:
            base = datetime.strptime(date_part, "%Y%m%d")
        except ValueError:
            return None
        return base.replace(hour=hour, minute=minute)
    if len(digits) == 8:
        try:
            return datetime.strptime(digits, "%Y%m%d").replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            return None
    return None


def _parse_from_date_text(value: str) -> tuple[datetime | None, datetime | None]:
    normalized = (
        value.replace("\u202f", " ")
        .replace("\xa0", " ")
        .replace("A.M.", "AM")
        .replace("P.M.", "PM")
        .replace("a.m.", "am")
        .replace("p.m.", "pm")
    )
    match = DATE_AND_TIMES_RE.search(normalized)
    if not match:
        return None, None
    try:
        base = datetime.strptime(
            f"{match.group('month')} {match.group('day')} {datetime.now().year}",
            "%B %d %Y",
        ).replace(hour=0, minute=0, second=0, microsecond=0)
    except ValueError:
        return None, None
    time_blob = _normalize_text(match.group("times")) or ""
    range_match = TIME_RANGE_RE.search(time_blob)
    if range_match:
        start_meridiem = range_match.group("start_meridiem") or range_match.group("end_meridiem")
        start_hour, start_minute = _to_24h(
            int(range_match.group("start_hour")),
            int(range_match.group("start_minute") or 0),
            start_meridiem,
        )
        end_hour, end_minute = _to_24h(
            int(range_match.group("end_hour")),
            int(range_match.group("end_minute") or 0),
            range_match.group("end_meridiem"),
        )
        return (
            base.replace(hour=start_hour, minute=start_minute),
            base.replace(hour=end_hour, minute=end_minute),
        )

    single_match = TIME_SINGLE_RE.search(time_blob)
    if single_match:
        hour, minute = _to_24h(
            int(single_match.group("hour")),
            int(single_match.group("minute") or 0),
            single_match.group("meridiem"),
        )
        return base.replace(hour=hour, minute=minute), None
    return base, None


def _to_24h(hour: int, minute: int, meridiem: str | None) -> tuple[int, int]:
    normalized = (meridiem or "").replace(".", "").lower()
    if normalized == "pm" and hour != 12:
        hour += 12
    if normalized == "am" and hour == 12:
        hour = 0
    return hour, minute


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _infer_activity_type(text: str) -> str:
    if _has_any_keywords(text, ("talk", "lecture", "conversation")):
        return "talk"
    if _has_any_keywords(text, ("workshop", "class", "lab", "artmaking")):
        return "workshop"
    return "activity"


def _has_any_keywords(text: str, keywords: tuple[str, ...]) -> bool:
    return any(_contains_keyword(text, keyword) for keyword in keywords)


def _contains_keyword(text: str, keyword: str) -> bool:
    return bool(re.search(rf"\b{re.escape(keyword)}\b", text, re.IGNORECASE))


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.replace("\xa0", " ").replace("\u202f", " ").split())
    return normalized or None
