import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

MFA_PROGRAMS_URL_TEMPLATE = "https://www.mfa.org/programs?page={page}"
MFA_PAGE_START = 0
MFA_PAGE_END = 4

NY_TIMEZONE = "America/New_York"
MFA_VENUE_NAME = "Museum of Fine Arts, Boston"
MFA_CITY = "Boston"
MFA_STATE = "MA"
MFA_DEFAULT_LOCATION = "Boston, MA"
MFA_EVENT_PATH_RE = re.compile(r"/(?:event|programs)/(?!\?)[^\s?#]+", re.IGNORECASE)
GUIDED_TOUR_RE = re.compile(r"\bguided\s+tou?rs?\b", re.IGNORECASE)
UNAVAILABLE_TICKETS_RE = re.compile(r"\btickets?\s+no\s+longer\s+available\b", re.IGNORECASE)

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|\u2013|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)
DATE_TIME_RE = re.compile(
    r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})(?:[^\d]+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|AM|PM)))?",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(?:(a\.?m\.?|p\.?m\.?|am|pm)\s*)?(?:-|\u2013|\u2014|to)\s*(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)\b", re.IGNORECASE)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.mfa.org/programs",
}


def build_mfa_program_urls(*, start_page: int = MFA_PAGE_START, end_page: int = MFA_PAGE_END) -> list[str]:
    return [MFA_PROGRAMS_URL_TEMPLATE.format(page=page) for page in range(start_page, end_page + 1)]


async def fetch_mfa_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[mfa-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[mfa-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[mfa-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[mfa-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[mfa-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[mfa-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[mfa-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch MFA programs page") from last_exception
    raise RuntimeError("Unable to fetch MFA programs page after retries")


class MfaProgramsAdapter(BaseSourceAdapter):
    source_name = "mfa_programs"

    def __init__(self, url: str = MFA_PROGRAMS_URL_TEMPLATE.format(page=MFA_PAGE_START)):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_mfa_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_mfa_events_html(payload, list_url=self.url)


def parse_mfa_events_html(
    html: str,
    *,
    list_url: str,
) -> list[ExtractedActivity]:
    rows = _parse_from_json_payloads(html=html, list_url=list_url)
    if rows:
        return rows
    return _parse_from_dom_fallback(html=html, list_url=list_url)


def _parse_from_json_payloads(html: str, *, list_url: str) -> list[ExtractedActivity]:
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
                item = _build_row_from_event_obj(event_obj=event_obj, list_url=list_url)
                if item is None:
                    continue
                key = (item.source_url, item.title, item.start_at)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(item)

    return rows


def _build_row_from_event_obj(*, event_obj: dict, list_url: str) -> ExtractedActivity | None:
    title = str(event_obj.get("name") or event_obj.get("title") or event_obj.get("headline") or "").strip()
    if not title or is_irrelevant_item_text(title):
        return None

    source_url = str(event_obj.get("url") or event_obj.get("@id") or event_obj.get("path") or list_url).strip()
    source_url = urljoin(list_url, source_url)
    if "/event/" not in source_url and "/programs/" not in source_url:
        return None

    start_at = _parse_datetime(event_obj.get("startDate") or event_obj.get("start_date"))
    if start_at is None:
        start_at = _parse_datetime(event_obj.get("date") or event_obj.get("start"))
    if start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("endDate") or event_obj.get("end_date") or event_obj.get("end"))

    description_parts: list[str] = []
    description = _normalize_text(
        event_obj.get("description")
        or event_obj.get("summary")
        or event_obj.get("excerpt")
        or event_obj.get("dek")
    )
    if description:
        description_parts.append(description)

    location_name = _extract_location_name(event_obj.get("location"))
    if location_name:
        description_parts.append(f"Location: {location_name}")

    category_blob = _normalize_text(event_obj.get("category") or event_obj.get("keywords"))
    if category_blob:
        description_parts.append(f"Category: {category_blob}")

    full_description = " | ".join(description_parts) if description_parts else None
    if _should_exclude_event(title=title, description=full_description, category=category_blob):
        return None

    text_blob = " ".join([title, full_description or "", category_blob or ""]).lower()
    age_min, age_max = _parse_age_range(title=title, description=full_description)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=MFA_VENUE_NAME,
        location_text=MFA_DEFAULT_LOCATION,
        city=MFA_CITY,
        state=MFA_STATE,
        activity_type="workshop",
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=("registration" in text_blob and "not required" not in text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
    )


def _parse_from_dom_fallback(html: str, *, list_url: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    title_to_links: dict[str, list[str]] = {}
    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not MFA_EVENT_PATH_RE.search(href):
            continue
        title = _normalize_space(anchor.get_text(" ", strip=True))
        if not title:
            continue
        title_to_links.setdefault(title, []).append(urljoin(list_url, href))

    if title_to_links:
        line_rows = _parse_from_text_lines(soup=soup, title_to_links=title_to_links)
        if line_rows:
            return line_rows

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not MFA_EVENT_PATH_RE.search(href):
            continue

        source_url = urljoin(list_url, href)
        title = _normalize_space(anchor.get_text(" ", strip=True))
        if not title or is_irrelevant_item_text(title):
            continue

        container = anchor.find_parent(["article", "li", "section", "div"]) or anchor
        blob = _normalize_space(container.get_text(" ", strip=True))
        if not blob:
            continue

        if _should_exclude_event(title=title, description=blob, category=None):
            continue

        start_at = _parse_datetime(blob)
        if start_at is None:
            continue

        age_min, age_max = _parse_age_range(title=title, description=blob)
        description = blob if blob != title else None
        text_blob = f"{title} {blob}".lower()

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=MFA_VENUE_NAME,
                location_text=MFA_DEFAULT_LOCATION,
                city=MFA_CITY,
                state=MFA_STATE,
                activity_type="workshop",
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=("registration" in text_blob and "not required" not in text_blob),
                start_at=start_at,
                end_at=None,
                timezone=NY_TIMEZONE,
                free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
            )
        )

    return rows


def _iter_event_objects(node: object):
    if isinstance(node, dict):
        node_type = node.get("@type")
        if _is_event_type(node_type) or _looks_like_event(node):
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
    has_title = bool(node.get("name") or node.get("title") or node.get("headline"))
    has_time = bool(node.get("startDate") or node.get("start_date") or node.get("date"))
    maybe_url = str(node.get("url") or node.get("@id") or node.get("path") or "")
    return has_title and has_time and ("/event/" in maybe_url or "/programs/" in maybe_url or bool(maybe_url))


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

    if isinstance(value, dict):
        nested = value.get("startDate") or value.get("start") or value.get("date")
        if nested:
            return _parse_datetime(nested)
        return None

    if isinstance(value, list):
        for item in value:
            parsed = _parse_datetime(item)
            if parsed is not None:
                return parsed
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
        maybe_time = _parse_start_time_parts(text)
        if maybe_time is not None:
            hour, minute = maybe_time
            return day.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return day

    maybe_time = _parse_start_time_parts(time_part)
    if maybe_time is not None:
        hour, minute = maybe_time
        return day.replace(hour=hour, minute=minute, second=0, microsecond=0)

    try:
        parsed_time = datetime.strptime(time_part, "%I:%M %p")
        return day.replace(hour=parsed_time.hour, minute=parsed_time.minute)
    except ValueError:
        return day


def _parse_start_time_parts(text: str) -> tuple[int, int] | None:
    normalized = _normalize_space(text)

    range_match = TIME_RANGE_RE.search(normalized)
    if range_match:
        start_hour = int(range_match.group(1))
        start_minute = int(range_match.group(2) or 0)
        start_meridiem = range_match.group(3) or range_match.group(6)
        if start_meridiem:
            return _to_24h(start_hour, start_minute, start_meridiem)

    single_match = TIME_SINGLE_RE.search(normalized)
    if single_match:
        hour = int(single_match.group(1))
        minute = int(single_match.group(2) or 0)
        return _to_24h(hour, minute, single_match.group(3))

    return None


def _to_24h(hour: int, minute: int, meridiem: str) -> tuple[int, int]:
    suffix = meridiem.lower().replace(".", "")
    if suffix == "pm" and hour != 12:
        hour += 12
    if suffix == "am" and hour == 12:
        hour = 0
    return hour, minute


def _extract_location_name(location_obj: object) -> str | None:
    if isinstance(location_obj, str):
        return _normalize_text(location_obj)
    if isinstance(location_obj, dict):
        name = _normalize_text(location_obj.get("name"))
        if name:
            return name
        address = _normalize_text(location_obj.get("address"))
        if address:
            return address
    return None


def _parse_age_range(*, title: str, description: str | None) -> tuple[int | None, int | None]:
    blob = f"{title} {description or ''}"

    range_match = AGE_RANGE_RE.search(blob)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))

    plus_match = AGE_PLUS_RE.search(blob)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def _should_exclude_event(*, title: str, description: str | None, category: str | None) -> bool:
    blob = " ".join([title, description or "", category or ""])
    return bool(GUIDED_TOUR_RE.search(blob) or UNAVAILABLE_TICKETS_RE.search(blob))


def _normalize_space(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    text = _normalize_space(str(value))
    return text or None


def _normalize_meridiem(text: str | None) -> str | None:
    if not text:
        return None
    normalized = text.replace(".", "").upper()
    normalized = _normalize_space(normalized)
    if normalized.endswith("AM") or normalized.endswith("PM"):
        return normalized
    return text


def _parse_from_text_lines(
    *,
    soup: BeautifulSoup,
    title_to_links: dict[str, list[str]],
) -> list[ExtractedActivity]:
    lines = [_normalize_space(line) for line in soup.get_text("\n").splitlines()]
    lines = [line for line in lines if line]

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    generic_markers = {
        "In Person",
        "Tickets",
        "Sold Out",
        "Course",
        "Film",
        "Music",
        "Special Event",
        "Lecture",
    }

    i = 0
    while i < len(lines):
        title = lines[i]
        if title not in title_to_links:
            i += 1
            continue

        # Calendar rows typically have date/time immediately after title.
        date_line = lines[i + 1] if i + 1 < len(lines) else ""
        time_line = lines[i + 2] if i + 2 < len(lines) else ""
        start_at = _parse_datetime(f"{date_line} {time_line}".strip())
        if start_at is None:
            i += 1
            continue

        category_line = lines[i - 1] if i - 1 >= 0 else ""
        description = None
        if i + 3 < len(lines):
            candidate_desc = lines[i + 3]
            if (
                candidate_desc
                and candidate_desc not in generic_markers
                and candidate_desc not in title_to_links
                and _parse_datetime(candidate_desc) is None
            ):
                description = candidate_desc

        if _should_exclude_event(title=title, description=description, category=category_line):
            i += 1
            continue

        source_url = title_to_links[title].pop(0)
        if not title_to_links[title]:
            del title_to_links[title]

        age_min, age_max = _parse_age_range(title=title, description=description)
        text_blob = f"{category_line} {title} {description or ''}".lower()
        key = (source_url, title, start_at)
        if key in seen:
            i += 1
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=MFA_VENUE_NAME,
                location_text=MFA_DEFAULT_LOCATION,
                city=MFA_CITY,
                state=MFA_STATE,
                activity_type="workshop",
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=("registration" in text_blob and "not required" not in text_blob),
                start_at=start_at,
                end_at=None,
                timezone=NY_TIMEZONE,
                free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
            )
        )
        i += 1

    return rows
