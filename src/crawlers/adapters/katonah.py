import asyncio
import json
import re
from datetime import datetime
from urllib.parse import parse_qs
from urllib.parse import urljoin
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.candidates import record_candidate_count
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

KATONAH_EVENTS_URL = "https://www.katonahmuseum.org/events/"
# The /events/ page renders its calendar client-side (Vue); the static HTML has
# no event markup at all. This is the endpoint that page POSTs to, one month at
# a time, returning JSON keyed by day-of-month.
KATONAH_EVENTS_API_URL = "https://www.katonahmuseum.org/events/controller.php"
KATONAH_MONTHS_AHEAD = 3

NY_TIMEZONE = "America/New_York"
KATONAH_VENUE_NAME = "Katonah Museum of Art"
KATONAH_CITY = "Katonah"
KATONAH_STATE = "NY"
KATONAH_DEFAULT_LOCATION = "Katonah, NY"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": KATONAH_EVENTS_URL,
}

DATE_RE = re.compile(r"^[A-Za-z]+,\s+([A-Za-z]+)\s+(\d{1,2})$")
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)

INCLUDE_KEYWORDS = (
    "activity",
    "art",
    "class",
    "conversation",
    "drop-in",
    "drop in",
    "drawing",
    "family",
    "kids",
    "lab",
    "lecture",
    "school",
    "studio",
    "teen",
    "workshop",
)
EXCLUDE_KEYWORDS = (
    "admission",
    "auction",
    "camp",
    "concert",
    "dinner",
    "exhibition",
    "film",
    "fundraiser",
    "fundraising",
    "gala",
    "music",
    "performance",
    "reception",
    "storytime",
    "tour",
    "yoga",
)


async def fetch_katonah_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
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

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Katonah events page") from last_exception
    raise RuntimeError("Unable to fetch Katonah events page after retries")


async def fetch_katonah_events_payload(
    *,
    months_ahead: int = KATONAH_MONTHS_AHEAD,
    now: datetime | None = None,
    client: httpx.AsyncClient | None = None,
) -> list[dict]:
    """Collect raw event dicts for the current month plus the next `months_ahead`."""
    current = (now or datetime.now(ZoneInfo(NY_TIMEZONE))).date()
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS)

    events: list[dict] = []
    try:
        for offset in range(months_ahead + 1):
            month = current.month + offset
            year = current.year + (month - 1) // 12
            month = (month - 1) % 12 + 1
            response = await client.post(
                KATONAH_EVENTS_API_URL,
                data={"month": month, "year": year},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("error"):
                continue
            # `events` is keyed by day-of-month, each value a list of events.
            for day_events in (payload.get("events") or {}).values():
                events.extend(day_events or [])
    finally:
        if owns_client:
            await client.aclose()

    return events


def parse_katonah_events_payload(
    events: list[dict],
    *,
    list_url: str = KATONAH_EVENTS_URL,
) -> list[ExtractedActivity]:
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    record_candidate_count(len(events))
    for event in events:
        if event.get("visibility") not in (None, "", "public"):
            continue
        if event.get("cancelled_at") or event.get("deleted_at"):
            continue

        title = _normalize_space(event.get("title"))
        start_at = _parse_api_datetime(event.get("start_date"))
        if not title or start_at is None or start_at.date() < today:
            continue

        end_at = _parse_api_datetime(event.get("end_date"))
        source_url = urljoin(list_url, f"?eid={event.get('id')}")

        description = _normalize_space(
            _strip_html(event.get("full_description") or event.get("short_description"))
        )
        cost = _normalize_space(event.get("event_cost"))
        # Filter on the title (plus subtitle) only. The API exposes full event
        # descriptions the old HTML listing never had, and nearly every one of
        # them mentions "admission"/"exhibition" in boilerplate, which would
        # exclude genuine family programmes like "Saturday Open Studio".
        title_blob = " ".join(
            part for part in [title, _normalize_space(event.get("subtitle"))] if part
        ).lower()
        if _should_exclude_event(title_blob):
            continue
        if not _should_include_event(title_blob):
            continue
        text_blob = " ".join(part for part in [title, description, cost] if part).lower()

        key = (_event_identity(source_url), title, start_at)
        if key in seen:
            continue
        seen.add(key)

        is_free, free_status = infer_price_classification(cost or text_blob)
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=KATONAH_VENUE_NAME,
                location_text=_normalize_space(
                    event.get("location_override") or event.get("location")
                )
                or KATONAH_DEFAULT_LOCATION,
                city=KATONAH_CITY,
                state=KATONAH_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=None,
                age_max=None,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=bool(_normalize_space(event.get("registration_link"))),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_api_datetime(value: str | None) -> datetime | None:
    text = _normalize_space(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _strip_html(value: str | None) -> str:
    if not value:
        return ""
    return BeautifulSoup(value, "html.parser").get_text(" ", strip=True)


class KatonahEventsAdapter(BaseSourceAdapter):
    source_name = "katonah_events"

    def __init__(self, url: str = KATONAH_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        events = await fetch_katonah_events_payload()
        return [json.dumps(events)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_katonah_events_payload(json.loads(payload), list_url=self.url)


def parse_katonah_events_html(html: str, *, list_url: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in soup.select("div.exhibition"):
        title_tag = card.select_one("h3 a")
        date_tag = card.select_one("span.date")
        time_tag = card.select_one("span.time")
        if title_tag is None or date_tag is None:
            continue

        title = _normalize_space(title_tag.get_text(" ", strip=True))
        if not title:
            continue

        source_url = urljoin(list_url, (title_tag.get("href") or "").strip())
        if not source_url:
            continue

        date_text = _normalize_space(date_tag.get_text(" ", strip=True))
        time_text = _normalize_space(time_tag.get_text(" ", strip=True)) if time_tag else ""
        if not date_text:
            continue

        start_at, end_at = _parse_start_end(date_text=date_text, time_text=time_text, now=today)
        if start_at is None:
            continue
        if start_at.date() < today:
            continue

        description_parts = [part for part in [date_text, time_text] if part]
        description = " | ".join(description_parts) if description_parts else None
        text_blob = " ".join([title, description or ""]).lower()
        if _should_exclude_event(text_blob):
            continue
        if not _should_include_event(text_blob):
            continue

        key = (_event_identity(source_url), title, start_at)
        if key in seen:
            continue
        seen.add(key)

        is_free, free_status = infer_price_classification(text_blob)
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=KATONAH_VENUE_NAME,
                location_text=KATONAH_DEFAULT_LOCATION,
                city=KATONAH_CITY,
                state=KATONAH_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=None,
                age_max=None,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=("registration" in text_blob and "no registration" not in text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_start_end(*, date_text: str, time_text: str, now) -> tuple[datetime | None, datetime | None]:
    date_match = DATE_RE.search(date_text)
    if not date_match:
        return None, None

    month_name = date_match.group(1)
    day_text = date_match.group(2)
    year = now.year
    try:
        date_value = datetime.strptime(f"{month_name} {day_text} {year}", "%B %d %Y").date()
    except ValueError:
        try:
            date_value = datetime.strptime(f"{month_name} {day_text} {year}", "%b %d %Y").date()
        except ValueError:
            return None, None

    if date_value < now and (now - date_value).days > 45:
        date_value = date_value.replace(year=date_value.year + 1)

    if not time_text:
        start_at = datetime.combine(date_value, datetime.min.time())
        return start_at, None

    range_match = TIME_RANGE_RE.search(time_text)
    if range_match:
        start_raw = _normalize_meridiem(range_match.group("start"))
        end_raw = _normalize_meridiem(range_match.group("end"))
        start_at = _parse_time(date_value=date_value, time_text=start_raw)
        end_at = _parse_time(date_value=date_value, time_text=end_raw)
        return start_at, end_at

    start_single = _parse_time(date_value=date_value, time_text=_normalize_meridiem(time_text))
    return start_single, None


def _parse_time(*, date_value, time_text: str) -> datetime | None:
    normalized = _normalize_space(time_text).lower().replace(".", "")
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            parsed_time = datetime.strptime(normalized.upper(), fmt).time()
            return datetime.combine(date_value, parsed_time)
        except ValueError:
            continue
    return None


def _normalize_meridiem(value: str) -> str:
    normalized = _normalize_space(value)
    normalized = normalized.replace("a.m.", "AM").replace("p.m.", "PM")
    normalized = normalized.replace("a.m", "AM").replace("p.m", "PM")
    normalized = normalized.replace("am", "AM").replace("pm", "PM")
    return normalized


def _event_identity(source_url: str) -> str:
    parsed = urlparse(source_url)
    event_id = parse_qs(parsed.query).get("eid", [None])[0]
    if event_id:
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?eid={event_id}"
    return source_url


def _should_include_event(text_blob: str) -> bool:
    return any(keyword in text_blob for keyword in INCLUDE_KEYWORDS)


def _should_exclude_event(text_blob: str) -> bool:
    return any(keyword in text_blob for keyword in EXCLUDE_KEYWORDS)


def _infer_activity_type(text_blob: str) -> str:
    if any(keyword in text_blob for keyword in ("talk", "lecture", "conversation")):
        return "lecture"
    return "workshop"


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())
