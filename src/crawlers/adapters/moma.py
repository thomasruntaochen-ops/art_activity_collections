import asyncio
import json
import re
from datetime import datetime
from urllib.parse import parse_qs
from urllib.parse import urlparse
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

MOMA_TEENS_CALENDAR_URL = "https://www.moma.org/calendar/?happening_filter=For+teens"
MOMA_KIDS_CALENDAR_URL = "https://www.moma.org/calendar/?happening_filter=For+kids"

NY_TIMEZONE = "America/New_York"
MOMA_VENUE_NAME = "MoMA"
MOMA_CITY = "New York"
MOMA_STATE = "NY"
MOMA_DEFAULT_LOCATION = "New York, NY"
TEENS_DEFAULT_AGE_MIN = 13
TEENS_DEFAULT_AGE_MAX = 17
KIDS_DEFAULT_AGE_MAX = 12

MOMA_EVENT_PATH_RE = re.compile(r"/calendar/events/\d+", re.IGNORECASE)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|\u2013|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)
DATE_TIME_RE = re.compile(
    r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})(?:[^\d]+(\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?|AM|PM)))?",
    re.IGNORECASE,
)
DATE_HEADING_RE = re.compile(
    r"^(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?),\s+([A-Za-z]{3,9})\s+(\d{1,2})$",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(\d{1,2}):(\d{2})\s*(?:(a\.?m\.?|p\.?m\.?|am|pm)\s*)?(?:-|–|—|to)\s*(\d{1,2}):(\d{2})\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"(\d{1,2}):(\d{2})\s*(a\.?m\.?|p\.?m\.?|am|pm)", re.IGNORECASE)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.moma.org/calendar/",
}


async def fetch_moma_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[moma-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[moma-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[moma-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[moma-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[moma-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[moma-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[moma-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch MoMA events page") from last_exception
    raise RuntimeError("Unable to fetch MoMA events page after retries")


class MoMATeensAdapter(BaseSourceAdapter):
    source_name = "moma_teens"

    def __init__(self, url: str = MOMA_TEENS_CALENDAR_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_moma_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_moma_events_html(payload, audience="teens", list_url=self.url)


class MoMAKidsAdapter(BaseSourceAdapter):
    source_name = "moma_kids"

    def __init__(self, url: str = MOMA_KIDS_CALENDAR_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_moma_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_moma_events_html(payload, audience="kids", list_url=self.url)


def parse_moma_events_html(
    html: str,
    *,
    audience: str,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    rows = _parse_from_json_payloads(html=html, audience=audience, list_url=list_url)
    if rows:
        return rows
    return _parse_from_dom_fallback(html=html, audience=audience, list_url=list_url, now=now)


def _parse_from_json_payloads(html: str, *, audience: str, list_url: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime | None]] = set()

    for script in soup.find_all("script"):
        script_text = script.string or script.get_text() or ""
        script_text = script_text.strip()
        if not script_text:
            continue

        candidates: list[object] = []
        if script.get("type") == "application/ld+json":
            candidates.append(script_text)
        elif script.get("id") == "__NEXT_DATA__":
            candidates.append(script_text)

        if not candidates:
            maybe_json = _extract_first_json_object(script_text)
            if maybe_json:
                candidates.append(maybe_json)

        for candidate in candidates:
            data = _safe_json_loads(candidate)
            if data is None:
                continue
            for event_obj in _iter_event_objects(data):
                item = _build_row_from_event_obj(event_obj=event_obj, audience=audience, list_url=list_url)
                if item is None:
                    continue
                key = (item.source_url, item.title, item.start_at)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(item)

    return rows


def _build_row_from_event_obj(
    *,
    event_obj: dict,
    audience: str,
    list_url: str,
) -> ExtractedActivity | None:
    title = str(event_obj.get("name") or event_obj.get("title") or "").strip()
    if not title or is_irrelevant_item_text(title):
        return None

    source_url = str(event_obj.get("url") or event_obj.get("@id") or list_url).strip()
    source_url = urljoin(list_url, source_url)

    start_at = _parse_datetime(event_obj.get("startDate") or event_obj.get("start_date"))
    if start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("endDate") or event_obj.get("end_date"))

    description_parts: list[str] = []
    description = _normalize_text(event_obj.get("description"))
    if description:
        description_parts.append(description)

    location = event_obj.get("location")
    location_name = _extract_location_name(location)
    if location_name:
        description_parts.append(f"Location: {location_name}")

    audience_blob = _normalize_text(event_obj.get("audience"))
    if audience_blob:
        description_parts.append(f"Audience: {audience_blob}")

    full_description = " | ".join(description_parts) if description_parts else None

    text_blob = " ".join(
        [
            title,
            full_description or "",
            _normalize_text(event_obj.get("eventAttendanceMode")) or "",
            _normalize_text(event_obj.get("offers")) or "",
        ]
    ).lower()
    age_min, age_max = _parse_age_range(title=title, description=full_description, audience=audience)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=MOMA_VENUE_NAME,
        location_text=MOMA_DEFAULT_LOCATION,
        city=MOMA_CITY,
        state=MOMA_STATE,
        activity_type="workshop",
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=("registration" in text_blob and "not required" not in text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        free_verification_status="inferred",
    )


def _parse_from_dom_fallback(
    html: str,
    *,
    audience: str,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    default_day = _extract_base_day_from_url(list_url=list_url, now=now)
    current_day = default_day

    for node in soup.find_all(["h2", "a"]):
        if node.name == "h2":
            heading_day = _parse_heading_day(node.get_text(" ", strip=True), base_day=default_day)
            if heading_day is not None:
                current_day = heading_day
            continue

        if node.name != "a":
            continue
        href = node.get("href", "")
        if not MOMA_EVENT_PATH_RE.search(href):
            continue

        source_url = urljoin(list_url, href)
        title, detail_lines = _extract_anchor_text_parts(node)
        if not title or is_irrelevant_item_text(title):
            continue

        description = " | ".join(detail_lines) if detail_lines else None
        start_at = _parse_start_datetime_for_day(current_day, title=title, detail_lines=detail_lines)
        if start_at is None:
            start_at = current_day
        age_min, age_max = _parse_age_range(title=title, description=description, audience=audience)

        text_blob = f"{title} {description or ''}".lower()
        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=MOMA_VENUE_NAME,
                location_text=MOMA_DEFAULT_LOCATION,
                city=MOMA_CITY,
                state=MOMA_STATE,
                activity_type="workshop",
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=("registration" in text_blob and "not required" not in text_blob),
                start_at=start_at,
                end_at=None,
                timezone=NY_TIMEZONE,
                free_verification_status="inferred",
            )
        )

    return rows


def _iter_event_objects(node: object):
    if isinstance(node, dict):
        node_type = node.get("@type")
        if _is_event_type(node_type):
            yield node
        elif _looks_like_event(node):
            yield node

        for value in node.values():
            yield from _iter_event_objects(value)
        return

    if isinstance(node, list):
        for item in node:
            yield from _iter_event_objects(item)


def _is_event_type(value: object) -> bool:
    if isinstance(value, str):
        return value.lower() == "event"
    if isinstance(value, list):
        return any(isinstance(v, str) and v.lower() == "event" for v in value)
    return False


def _looks_like_event(node: dict) -> bool:
    has_title = bool(node.get("name") or node.get("title"))
    has_time = bool(node.get("startDate") or node.get("start_date"))
    maybe_url = str(node.get("url") or node.get("@id") or "")
    return has_title and has_time and ("/calendar/events/" in maybe_url or bool(maybe_url))


def _extract_first_json_object(script_text: str) -> str | None:
    start = script_text.find("{")
    end = script_text.rfind("}")
    if start < 0 or end < start:
        return None
    return script_text[start : end + 1]


def _safe_json_loads(raw: object) -> object | None:
    if not isinstance(raw, str):
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass

    dt_match = DATE_TIME_RE.search(text)
    if not dt_match:
        return None

    date_part = dt_match.group(1)
    time_part = _normalize_meridiem(dt_match.group(2)) if dt_match.group(2) else None

    try:
        day = datetime.strptime(date_part, "%B %d, %Y")
    except ValueError:
        try:
            day = datetime.strptime(date_part, "%b %d, %Y")
        except ValueError:
            return None

    if not time_part:
        return day

    try:
        parsed_time = datetime.strptime(time_part, "%I:%M %p")
    except ValueError:
        return day

    return day.replace(hour=parsed_time.hour, minute=parsed_time.minute)


def _parse_datetime_from_text(text: str) -> datetime | None:
    return _parse_datetime(text)


def _extract_base_day_from_url(list_url: str, *, now: datetime | None = None) -> datetime:
    parsed = urlparse(list_url)
    date_values = parse_qs(parsed.query).get("date") or []
    if date_values:
        try:
            base = datetime.strptime(date_values[0], "%Y-%m-%d")
            return base.replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            pass
    current = now or datetime.now()
    return current.replace(hour=0, minute=0, second=0, microsecond=0)


def _parse_heading_day(text: str, *, base_day: datetime) -> datetime | None:
    normalized = _normalize_space(text)
    match = DATE_HEADING_RE.match(normalized)
    if not match:
        return None

    month = match.group(1)
    day = int(match.group(2))
    parsed = _parse_month_day_with_year(month=month, day=day, year=base_day.year)
    if parsed is None:
        return None

    # Handle year rollover windows around Dec/Jan listings.
    if (parsed - base_day).days < -180:
        parsed = parsed.replace(year=parsed.year + 1)
    elif (parsed - base_day).days > 180:
        parsed = parsed.replace(year=parsed.year - 1)
    return parsed


def _parse_month_day_with_year(*, month: str, day: int, year: int) -> datetime | None:
    for fmt in ("%b %d %Y", "%B %d %Y"):
        try:
            return datetime.strptime(f"{month} {day} {year}", fmt)
        except ValueError:
            continue
    return None


def _extract_anchor_text_parts(anchor) -> tuple[str, list[str]]:
    lines: list[str] = []
    for paragraph in anchor.find_all("p"):
        line = _normalize_space(paragraph.get_text(" ", strip=True))
        if line:
            lines.append(line)

    if not lines:
        raw_lines = [_normalize_space(v) for v in anchor.get_text("\n").splitlines()]
        lines = [line for line in raw_lines if line]

    if not lines:
        return "", []

    title = lines[0]
    details: list[str] = []
    for line in lines[1:]:
        if line == title:
            continue
        details.append(line)

    return title, details


def _parse_start_datetime_for_day(
    day: datetime,
    *,
    title: str,
    detail_lines: list[str],
) -> datetime | None:
    candidates = [title, *detail_lines]
    for text in candidates:
        time_parts = _parse_start_time_parts(text)
        if time_parts is None:
            continue
        hour, minute = time_parts
        return day.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return None


def _parse_start_time_parts(text: str) -> tuple[int, int] | None:
    normalized = _normalize_space(text)

    range_match = TIME_RANGE_RE.search(normalized)
    if range_match:
        start_hour = int(range_match.group(1))
        start_minute = int(range_match.group(2))
        start_meridiem = range_match.group(3) or range_match.group(6)
        if start_meridiem:
            return _to_24h(start_hour, start_minute, start_meridiem)

    single_match = TIME_SINGLE_RE.search(normalized)
    if single_match:
        hour = int(single_match.group(1))
        minute = int(single_match.group(2))
        return _to_24h(hour, minute, single_match.group(3))

    return None


def _to_24h(hour: int, minute: int, meridiem: str) -> tuple[int, int]:
    suffix = meridiem.lower().replace(".", "")
    if suffix == "pm" and hour != 12:
        hour += 12
    if suffix == "am" and hour == 12:
        hour = 0
    return hour, minute


def _normalize_space(text: str) -> str:
    if not text:
        return ""
    cleaned = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", cleaned).strip()


def _normalize_meridiem(value: str) -> str:
    return value.replace("a.m.", "AM").replace("p.m.", "PM").replace("a.m", "AM").replace("p.m", "PM").upper()


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, list):
        parts = [_normalize_text(v) for v in value]
        joined = ", ".join(p for p in parts if p)
        return joined or None
    if isinstance(value, dict):
        parts = [_normalize_text(v) for v in value.values()]
        joined = ", ".join(p for p in parts if p)
        return joined or None
    text = str(value).strip()
    return text or None


def _extract_location_name(location: object) -> str | None:
    if location is None:
        return None
    if isinstance(location, str):
        return location.strip() or None
    if isinstance(location, dict):
        name = _normalize_text(location.get("name"))
        if name:
            return name
        address = _normalize_text(location.get("address"))
        return address
    if isinstance(location, list):
        names = [_extract_location_name(v) for v in location]
        cleaned = [n for n in names if n]
        return ", ".join(cleaned) if cleaned else None
    return None


def _parse_age_range(
    *,
    title: str,
    description: str | None,
    audience: str,
) -> tuple[int | None, int | None]:
    blob = title if not description else f"{title} {description}"

    age_range_match = AGE_RANGE_RE.search(blob)
    if age_range_match:
        return int(age_range_match.group(1)), int(age_range_match.group(2))

    age_plus_match = AGE_PLUS_RE.search(blob)
    if age_plus_match:
        return int(age_plus_match.group(1)), None

    if audience.lower() == "teens":
        return TEENS_DEFAULT_AGE_MIN, TEENS_DEFAULT_AGE_MAX
    if audience.lower() == "kids":
        return None, KIDS_DEFAULT_AGE_MAX

    return None, None
