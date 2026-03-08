import asyncio
import re
from datetime import date, datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from bs4 import NavigableString
from bs4 import Tag

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

BMOA_EVENTS_URL = "https://www.bmoa.org/events"

LA_TIMEZONE = "America/Los_Angeles"
BMOA_VENUE_NAME = "Bakersfield Museum of Art"
BMOA_CITY = "Bakersfield"
BMOA_STATE = "CA"
BMOA_DEFAULT_LOCATION = "Bakersfield, CA"

DATE_RANGE_RE = re.compile(
    r"^(?:(?P<start_weekday>[A-Za-z]+),\s+)?(?P<start_month>[A-Za-z]+)\s+(?P<start_day>\d{1,2})"
    r"(?:\s*-\s*(?:(?P<end_weekday>[A-Za-z]+),\s+)?(?P<end_month>[A-Za-z]+)\s+(?P<end_day>\d{1,2}))?$"
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>AM|PM)\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>AM|PM)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>AM|PM)",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PAREN_RANGE_RE = re.compile(r"\((?:ages?\s*)?(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\)", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bmoa.org/",
}

EXCLUDED_KEYWORDS = (
    "ticket",
    "tickets",
    "tour",
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
    "free admission",
    "extended hours",
    "silent auction",
    "formal dinner",
    "festival",
    "fundraiser",
)
INCLUDED_KEYWORDS = (
    "talk",
    "class",
    "lecture",
    "activity",
    "workshop",
    "lab",
    "conversation",
    "studio",
    "family day",
    "artists on artists",
    "art project",
)


async def fetch_bmoa_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[bmoa-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[bmoa-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[bmoa-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[bmoa-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[bmoa-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[bmoa-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[bmoa-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch BMoA events page") from last_exception
    raise RuntimeError("Unable to fetch BMoA events page after retries")


class BmoaEventsAdapter(BaseSourceAdapter):
    source_name = "bmoa_events"

    def __init__(self, url: str = BMOA_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_bmoa_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_bmoa_events_html(payload, list_url=self.url)


def parse_bmoa_events_html(
    html: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = (now or datetime.now()).date()

    for paragraph in soup.select("div.sqs-html-content p"):
        strong = paragraph.find("strong")
        if strong is None:
            continue

        date_text = _normalize_text(strong.get_text(" ", strip=True))
        if not date_text:
            continue

        parsed_dates = _parse_date_text(date_text, current_date=current_date)
        if parsed_dates is None:
            continue
        start_date, end_date = parsed_dates
        if (end_date or start_date) < current_date:
            continue

        for entry in _extract_paragraph_entries(paragraph):
            source_url = urljoin(list_url, entry["href"])
            title = entry["title"]
            detail_text = entry["detail_text"]
            if not title or is_irrelevant_item_text(title):
                continue

            if _should_exclude_event(title=title, description=detail_text):
                continue
            if not _should_include_event(title=title, description=detail_text):
                continue

            start_at, end_at, schedule_text = _build_datetimes(
                start_date=start_date,
                end_date=end_date,
                date_text=date_text,
                detail_text=detail_text,
            )
            if start_at is None:
                continue

            description_parts: list[str] = []
            cleaned_detail = _clean_description(detail_text)
            if cleaned_detail:
                description_parts.append(cleaned_detail)
            if schedule_text:
                description_parts.append(f"Schedule: {schedule_text}")
            description = " | ".join(description_parts) if description_parts else None

            if _is_near_duplicate(
                rows=rows,
                source_url=source_url,
                title=title,
                description=description,
                start_at=start_at,
            ):
                continue

            age_min, age_max = _parse_age_range(title=title, description=description)
            text_blob = " ".join([title, description or ""]).lower()
            key = (source_url, title, start_at)
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=description,
                    venue_name=BMOA_VENUE_NAME,
                    location_text=BMOA_DEFAULT_LOCATION,
                    city=BMOA_CITY,
                    state=BMOA_STATE,
                    activity_type=_infer_activity_type(title=title, description=description),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=("drop-in" in text_blob or "drop in" in text_blob or "all-ages art project" in text_blob),
                    registration_required=(
                        ("registration" in text_blob or "register" in text_blob)
                        and "closed" not in text_blob
                        and "no registration" not in text_blob
                    ),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=LA_TIMEZONE,
                    free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
                )
            )

    return rows


def _extract_paragraph_entries(paragraph: Tag) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    current: dict[str, str | list[str]] | None = None

    for child in paragraph.children:
        if isinstance(child, Tag) and child.name == "strong":
            continue
        if isinstance(child, Tag) and child.name == "a" and child.get("href"):
            if current is not None:
                entries.append(_finalize_entry(current))
            current = {
                "href": child["href"],
                "anchor_text": _normalize_text(child.get_text(" ", strip=True)) or "",
                "fragments": [],
            }
            continue

        if current is None:
            continue

        fragment = ""
        if isinstance(child, NavigableString):
            fragment = str(child)
        elif isinstance(child, Tag):
            fragment = child.get_text(" ", strip=True)

        normalized = _normalize_text(fragment)
        if normalized:
            current["fragments"].append(normalized)

    if current is not None:
        entries.append(_finalize_entry(current))

    return [entry for entry in entries if entry["title"]]


def _finalize_entry(raw_entry: dict[str, str | list[str]]) -> dict[str, str]:
    anchor_text = str(raw_entry["anchor_text"]).strip()
    fragments = [str(item).strip() for item in raw_entry["fragments"] if str(item).strip()]
    detail_text = _normalize_space(" ".join(fragments))

    title = anchor_text
    if detail_text.startswith(":"):
        first_segment = detail_text.split("|", 1)[0].strip()
        suffix = first_segment.lstrip(":").strip()
        if suffix:
            title = f"{title}: {suffix}"
        detail_text = detail_text[len(first_segment) :].lstrip(" |")

    return {
        "href": str(raw_entry["href"]),
        "title": _normalize_space(title),
        "detail_text": _normalize_space(detail_text),
    }


def _should_exclude_event(*, title: str, description: str | None) -> bool:
    blob = f"{title} {description or ''}".lower()
    if any(keyword in blob for keyword in INCLUDED_KEYWORDS):
        return False
    return any(keyword in blob for keyword in EXCLUDED_KEYWORDS)


def _should_include_event(*, title: str, description: str | None) -> bool:
    blob = f"{title} {description or ''}".lower()
    return any(keyword in blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(*, title: str, description: str | None) -> str:
    blob = f"{title} {description or ''}".lower()
    if any(keyword in blob for keyword in ("talk", "lecture", "conversation", "artists on artists")):
        return "talk"
    if any(keyword in blob for keyword in ("workshop", "studio", "class")):
        return "workshop"
    return "activity"


def _build_datetimes(
    *,
    start_date: date,
    end_date: date | None,
    date_text: str,
    detail_text: str | None,
) -> tuple[datetime | None, datetime | None, str]:
    normalized_detail = _normalize_space(detail_text)
    time_match = TIME_RANGE_RE.search(normalized_detail)
    if time_match is not None:
        start_hour, start_minute = _to_24h(
            int(time_match.group("start_hour")),
            int(time_match.group("start_minute") or 0),
            time_match.group("start_meridiem"),
        )
        end_hour, end_minute = _to_24h(
            int(time_match.group("end_hour")),
            int(time_match.group("end_minute") or 0),
            time_match.group("end_meridiem"),
        )
        schedule_text = f"{date_text} | {time_match.group(0)}"
        return (
            datetime(
                start_date.year,
                start_date.month,
                start_date.day,
                start_hour,
                start_minute,
            ),
            datetime(
                start_date.year,
                start_date.month,
                start_date.day,
                end_hour,
                end_minute,
            ),
            schedule_text,
        )

    single_match = TIME_SINGLE_RE.search(normalized_detail)
    if single_match is not None:
        hour, minute = _to_24h(
            int(single_match.group("hour")),
            int(single_match.group("minute") or 0),
            single_match.group("meridiem"),
        )
        schedule_text = f"{date_text} | {single_match.group(0)}"
        return (
            datetime(start_date.year, start_date.month, start_date.day, hour, minute),
            None,
            schedule_text,
        )

    schedule_text = date_text
    if normalized_detail and "times vary" in normalized_detail.lower():
        schedule_text = f"{date_text} | times vary"

    return (
        datetime(start_date.year, start_date.month, start_date.day),
        None,
        schedule_text,
    )


def _parse_date_text(value: str, *, current_date: date) -> tuple[date, date | None] | None:
    match = DATE_RANGE_RE.match(_normalize_space(value))
    if match is None:
        return None

    start_month = _month_number(match.group("start_month"))
    start_day = int(match.group("start_day"))
    if start_month is None:
        return None

    start_date = _resolve_event_date(
        month=start_month,
        day=start_day,
        current_date=current_date,
        prefer_future=False,
    )

    end_month_text = match.group("end_month")
    end_day_text = match.group("end_day")
    if not end_month_text or not end_day_text:
        return start_date, None

    end_month = _month_number(end_month_text)
    if end_month is None:
        return None

    end_day = int(end_day_text)
    end_year = start_date.year
    if end_month < start_month:
        end_year += 1

    try:
        end_date = date(end_year, end_month, end_day)
    except ValueError:
        return None

    return start_date, end_date


def _resolve_event_date(
    *,
    month: int,
    day: int,
    current_date: date,
    prefer_future: bool,
) -> date:
    candidates: list[date] = []
    for year in (current_date.year - 1, current_date.year, current_date.year + 1):
        try:
            candidates.append(date(year, month, day))
        except ValueError:
            continue

    if not candidates:
        raise ValueError("No valid event date candidates")

    if prefer_future:
        future_candidates = [candidate for candidate in candidates if candidate >= current_date]
        if future_candidates:
            return min(future_candidates)

    return min(candidates, key=lambda candidate: abs((candidate - current_date).days))


def _parse_age_range(*, title: str, description: str | None) -> tuple[int | None, int | None]:
    blob = " ".join([title, description or ""])
    for pattern in (AGE_RANGE_RE, AGE_PAREN_RANGE_RE):
        match = pattern.search(blob)
        if match:
            return int(match.group(1)), int(match.group(2))

    plus_match = AGE_PLUS_RE.search(blob)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def _is_near_duplicate(
    *,
    rows: list[ExtractedActivity],
    source_url: str,
    title: str,
    description: str | None,
    start_at: datetime,
) -> bool:
    description_key = _description_key(description)
    for row in rows:
        if row.source_url != source_url or row.title != title:
            continue
        if _description_key(row.description) != description_key:
            continue
        if abs((row.start_at.date() - start_at.date()).days) <= 3:
            return True
    return False


def _description_key(value: str | None) -> str:
    text = _normalize_space(value).lower()
    if not text:
        return ""
    return text.split("| schedule:", 1)[0].strip()


def _month_number(value: str | None) -> int | None:
    if not value:
        return None
    for fmt in ("%B", "%b"):
        try:
            return datetime.strptime(value, fmt).month
        except ValueError:
            continue
    return None


def _to_24h(hour: int, minute: int, meridiem: str | None) -> tuple[int, int]:
    normalized = (meridiem or "").strip().upper().replace(".", "")
    if normalized == "AM":
        if hour == 12:
            hour = 0
    elif normalized == "PM" and hour != 12:
        hour += 12
    return hour, minute


def _clean_description(value: str | None) -> str | None:
    text = _normalize_space(value)
    if not text:
        return None

    text = TIME_RANGE_RE.sub("", text)
    text = TIME_SINGLE_RE.sub("", text)
    text = re.sub(r"\|\s*\|", "|", text)
    text = text.strip(" |")
    return text or None


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = _normalize_space(value)
    return text or None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())
