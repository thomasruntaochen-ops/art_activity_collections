import asyncio
import json
import re
from datetime import datetime
from html import unescape
from urllib.parse import unquote
from urllib.parse import urljoin
from urllib.request import Request
from urllib.request import urlopen

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.types import ExtractedActivity

BLANTON_PROGRAMS_URL = "https://blantonmuseum.org/programs/"
BLANTON_TIMEZONE = "America/Chicago"
BLANTON_VENUE_NAME = "Blanton Museum of Art"
BLANTON_CITY = "Austin"
BLANTON_STATE = "TX"
BLANTON_LOCATION = "Austin, TX"

PROGRAM_LINK_RE = re.compile(
    r'href="(?P<href>https://blantonmuseum\.org/programs/[^"/?#]+/)"',
    re.IGNORECASE,
)
UPCOMING_BLOCK_RE = re.compile(
    r'<article class="upcoming-block events-block__item event-listing__event.*?>(?P<block>.*?)</article>',
    re.DOTALL,
)
TITLE_RE = re.compile(
    r'<h3 class="events-block__item-title[^"]*">\s*(?P<value>.*?)\s*</h3>',
    re.DOTALL,
)
MONTH_RE = re.compile(r'<span class="month">(?P<value>[A-Za-z]{3})</span>')
DAY_RE = re.compile(r'<span class="day">(?P<value>\d{1,2})</span>')
EVENT_TIME_RE = re.compile(
    r'<span class="events-block__item-text time">\s*(?P<value>.*?)</span>',
    re.DOTALL,
)
EVENT_EXCERPT_RE = re.compile(
    r'<div class="event-block__item-excerpt">(?P<value>.*?)</div>',
    re.DOTALL,
)
EVENT_DESCRIPTION_RE = re.compile(
    r'<div class="event-block__item-description">(?P<value>.*?)</div>',
    re.DOTALL,
)
CALENDAR_LINK_RE = re.compile(
    r'href="https://calendar\.google\.com/calendar/u/0/r/eventedit\?[^"]*?dates=(?P<dates>[^"&]+)',
    re.IGNORECASE,
)
SCHEDULE_ITEM_RE = re.compile(
    r'<div class="event-block__item-schedule__item">(?P<value>.*?)</div>\s*</div>',
    re.DOTALL,
)
SCHEDULE_TIME_RE = re.compile(
    r'<div class="event-block__item-schedule__time">\s*(?P<value>.*?)\s*</div>',
    re.DOTALL,
)
SCHEDULE_TITLE_RE = re.compile(
    r'<h5 class="event-block__item-schedule__title">\s*(?P<value>.*?)\s*</h5>',
    re.DOTALL,
)
SCHEDULE_LOCATION_RE = re.compile(
    r'<span class="event-block__item-schedule__location">\s*(?P<value>.*?)\s*</span>',
    re.DOTALL,
)
SCHEDULE_CONTENT_RE = re.compile(
    r'<span class="event-block__item-schedule__content">\s*(?P<value>.*?)\s*</span>',
    re.DOTALL,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_OLDER_RE = re.compile(
    r"\b(?:recommended for all visitors|ages?)\s*(\d{1,2})\s*(?:years?\s*)?(?:and|&)\s*older\b",
    re.IGNORECASE,
)
EXCLUDED_KEYWORDS = (
    "music",
    "concert",
    "choir",
    "instrumental",
    "performance",
    "tour",
    "dance lesson",
    "dance lessons",
    "live music",
    "ticket info",
    "tattoo",
    "meditation",
    "yoga",
    "discount",
    "all purchases",
)
YOUTH_KEYWORDS = (
    "family",
    "children",
    "child",
    "young visitors",
    "teen",
    "12 years & older",
    "12 years and older",
    "12+",
)
INCLUDED_KEYWORDS = (
    "art-making",
    "storytime",
    "looking",
    "workshop",
    "class",
    "activity",
    "lab",
    "conversation",
    "talk",
)
STRIP_TAGS_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://blantonmuseum.org/programs/",
}


async def fetch_blanton_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[blanton-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[blanton-fetch] attempt {attempt}/{max_attempts}: sending request")
            response = await asyncio.to_thread(_fetch_sync, url)
        except Exception as exc:
            last_exception = exc
            print(f"[blanton-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
            if attempt < max_attempts:
                wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(f"[blanton-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                await asyncio.sleep(wait_seconds)
                continue
            break

        print(f"[blanton-fetch] attempt {attempt}/{max_attempts}: status={response['status']}")
        if response["status"] < 400:
            print(f"[blanton-fetch] success on attempt {attempt}, bytes={len(response['text'])}")
            return response["text"]

        if response["status"] in (429, 500, 502, 503, 504) and attempt < max_attempts:
            retry_after = response["headers"].get("Retry-After")
            if retry_after and retry_after.isdigit():
                wait_seconds = float(retry_after)
            else:
                wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
            print(
                f"[blanton-fetch] transient status={response['status']}, "
                f"retrying after {wait_seconds:.1f}s"
            )
            await asyncio.sleep(wait_seconds)
            continue

        raise RuntimeError(f"Unexpected response status: {response['status']}")

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Blanton programs page") from last_exception
    raise RuntimeError("Unable to fetch Blanton programs page after retries")


async def load_blanton_payload(base_url: str = BLANTON_PROGRAMS_URL) -> dict[str, object]:
    listing_html = await fetch_blanton_page(base_url)
    detail_urls = _extract_program_detail_urls(listing_html, base_url=base_url)
    detail_pages_raw = await asyncio.gather(
        *(fetch_blanton_page(url) for url in detail_urls)
    )
    detail_pages = dict(zip(detail_urls, detail_pages_raw))
    return {
        "listing_html": listing_html,
        "detail_pages": detail_pages,
    }


class BlantonProgramsAdapter(BaseSourceAdapter):
    source_name = "blanton_programs"

    def __init__(self, url: str = BLANTON_PROGRAMS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_blanton_payload(self.url)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_blanton_payload(data)


def parse_blanton_payload(payload: dict[str, object]) -> list[ExtractedActivity]:
    detail_pages = payload.get("detail_pages") or {}
    if not isinstance(detail_pages, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, html in detail_pages.items():
        if not isinstance(source_url, str) or not isinstance(html, str):
            continue
        for block in UPCOMING_BLOCK_RE.finditer(html):
            block_html = block.group("block")
            parent_title = _extract_first(TITLE_RE, block_html)
            parent_excerpt = _extract_first(EVENT_EXCERPT_RE, block_html)
            parent_description = _extract_first(EVENT_DESCRIPTION_RE, block_html)
            parsed_dates = _parse_calendar_dates(block_html)
            if parsed_dates is None:
                parsed_dates = _parse_fallback_block_date(block_html)
            if parsed_dates is None:
                continue

            parent_start_at, parent_end_at = parsed_dates
            event_date = parent_start_at.date()
            schedule_items = list(SCHEDULE_ITEM_RE.finditer(block_html))

            if not schedule_items and _should_keep_parent_event(
                title=parent_title,
                excerpt=parent_excerpt,
                description=parent_description,
            ):
                description = " | ".join(
                    part for part in [parent_excerpt, parent_description] if part
                ) or None
                item = ExtractedActivity(
                    source_url=source_url,
                    title=parent_title,
                    description=description,
                    venue_name=BLANTON_VENUE_NAME,
                    location_text=BLANTON_LOCATION,
                    city=BLANTON_CITY,
                    state=BLANTON_STATE,
                    activity_type="activity",
                    age_min=None,
                    age_max=None,
                    drop_in=False,
                    registration_required=_requires_registration(description),
                    start_at=parent_start_at,
                    end_at=parent_end_at,
                    timezone=BLANTON_TIMEZONE,
                    free_verification_status=_free_status(description),
                )
                key = (item.source_url, item.title, item.start_at)
                if key not in seen:
                    seen.add(key)
                    rows.append(item)
                continue

            for item_match in schedule_items:
                schedule_html = item_match.group("value")
                schedule_title = _extract_first(SCHEDULE_TITLE_RE, schedule_html)
                schedule_time = _extract_first(SCHEDULE_TIME_RE, schedule_html)
                schedule_location = _extract_first(SCHEDULE_LOCATION_RE, schedule_html)
                schedule_content = _extract_first(SCHEDULE_CONTENT_RE, schedule_html)
                if not _should_keep_schedule_item(
                    title=schedule_title,
                    content=schedule_content,
                    parent_title=parent_title,
                ):
                    continue

                age_min, age_max = _extract_age_range(schedule_title, schedule_content)
                start_times = _parse_schedule_start_times(schedule_time, event_date=event_date)
                if not start_times:
                    start_times = [datetime(event_date.year, event_date.month, event_date.day)]

                description_parts = []
                if schedule_content:
                    description_parts.append(schedule_content)
                if schedule_location:
                    description_parts.append(f"Location: {schedule_location}")
                if schedule_time and not _time_is_precise(schedule_time):
                    description_parts.append(f"Schedule time: {schedule_time}")
                description = " | ".join(description_parts) if description_parts else None

                for start_at in start_times:
                    item = ExtractedActivity(
                        source_url=source_url,
                        title=schedule_title,
                        description=description,
                        venue_name=BLANTON_VENUE_NAME,
                        location_text=BLANTON_LOCATION,
                        city=BLANTON_CITY,
                        state=BLANTON_STATE,
                        activity_type=_infer_activity_type(schedule_title, schedule_content),
                        age_min=age_min,
                        age_max=age_max,
                        drop_in=("drop-in" in (description or "").lower() or "drop in" in (description or "").lower()),
                        registration_required=_requires_registration(description),
                        start_at=start_at,
                        end_at=None,
                        timezone=BLANTON_TIMEZONE,
                        free_verification_status=_free_status(description),
                    )
                    key = (item.source_url, item.title, item.start_at)
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(item)

    return rows


def _extract_program_detail_urls(listing_html: str, *, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for match in PROGRAM_LINK_RE.finditer(listing_html):
        href = urljoin(base_url, unescape(match.group("href")))
        slug = href.rstrip("/").split("/")[-1]
        if slug not in {"blanton-all-day", "blanton-live", "soundbreak"}:
            continue
        if href in seen:
            continue
        seen.add(href)
        urls.append(href)
    return urls


def _extract_first(pattern: re.Pattern[str], html: str) -> str:
    match = pattern.search(html)
    if not match:
        return ""
    return _normalize_html_text(match.group("value"))


def _normalize_html_text(value: str | None) -> str:
    if not value:
        return ""
    stripped = STRIP_TAGS_RE.sub(" ", value)
    return WHITESPACE_RE.sub(" ", unescape(stripped)).strip()


def _parse_calendar_dates(block_html: str) -> tuple[datetime, datetime | None] | None:
    match = CALENDAR_LINK_RE.search(block_html)
    if not match:
        return None
    value = unquote(unescape(match.group("dates")))
    if "/" not in value:
        return None
    start_raw, end_raw = value.split("/", 1)
    try:
        start_at = datetime.strptime(start_raw, "%Y%m%dT%H%M%S")
    except ValueError:
        return None

    end_at: datetime | None = None
    if end_raw:
        try:
            end_at = datetime.strptime(end_raw, "%Y%m%dT%H%M%S")
        except ValueError:
            end_at = None
    return start_at, end_at


def _parse_fallback_block_date(block_html: str) -> tuple[datetime, datetime | None] | None:
    month_value = _extract_first(MONTH_RE, block_html)
    day_value = _extract_first(DAY_RE, block_html)
    if not month_value or not day_value:
        return None
    try:
        month = datetime.strptime(month_value, "%b").month
    except ValueError:
        return None
    day = int(day_value)
    year = datetime.now().year
    start_at = datetime(year, month, day)
    return start_at, None


def _should_keep_parent_event(*, title: str, excerpt: str, description: str) -> bool:
    blob = " ".join([title, excerpt, description]).lower()
    if any(keyword in blob for keyword in EXCLUDED_KEYWORDS):
        return False
    if "family-friendly art activities" in blob:
        return True
    if any(keyword in blob for keyword in YOUTH_KEYWORDS):
        return True
    return AGE_RANGE_RE.search(blob) is not None or AGE_OLDER_RE.search(blob) is not None


def _should_keep_schedule_item(*, title: str, content: str, parent_title: str) -> bool:
    blob = " ".join([title, content, parent_title]).lower()
    normalized_title = title.strip().lower()
    if any(keyword in blob for keyword in EXCLUDED_KEYWORDS):
        return False
    if normalized_title in {
        "look & listen storytime",
        "looking together",
        "experience art",
    }:
        return True
    if normalized_title.startswith("art with an expert"):
        return False
    if normalized_title in {
        "additional activities",
        "curator’s choice tour",
        "curator's choice tour",
    }:
        return False
    if any(keyword in blob for keyword in YOUTH_KEYWORDS):
        return True
    if AGE_RANGE_RE.search(blob) is not None or AGE_OLDER_RE.search(blob) is not None:
        return True
    return any(keyword in blob for keyword in INCLUDED_KEYWORDS)


def _extract_age_range(title: str, description: str) -> tuple[int | None, int | None]:
    blob = " ".join([title, description])
    match = AGE_RANGE_RE.search(blob)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = AGE_OLDER_RE.search(blob)
    if match:
        return int(match.group(1)), None
    return None, None


def _parse_schedule_start_times(
    raw_time: str,
    *,
    event_date,
) -> list[datetime]:
    cleaned = raw_time.strip()
    if not cleaned or cleaned.lower() == "tbd":
        return []

    values: list[datetime] = []
    parts = re.split(r"\s*(?:&|/|,| and )\s*", cleaned)
    for part in parts:
        part_clean = part.strip().upper().replace(".", "")
        range_match = re.match(
            r"(?P<start>\d{1,2}(?::\d{2})?)\s*(?P<meridiem>AM|PM)\s*(?:-|–|TO)\s*(?P<end>\d{1,2}(?::\d{2})?)\s*(?P<end_meridiem>AM|PM)",
            part_clean,
        )
        if range_match:
            parsed = _parse_single_time_token(
                f"{range_match.group('start')} {range_match.group('meridiem')}",
                event_date=event_date,
            )
            if parsed is not None:
                values.append(parsed)
            continue

        parsed = _parse_single_time_token(part_clean, event_date=event_date)
        if parsed is not None:
            values.append(parsed)
    return values


def _parse_single_time_token(token: str, *, event_date) -> datetime | None:
    token = token.strip()
    if not token:
        return None
    token = re.sub(r"(?i)(\d)(am|pm)\b", r"\1 \2", token)
    fmt = "%I:%M %p" if ":" in token else "%I %p"
    try:
        parsed = datetime.strptime(token, fmt)
    except ValueError:
        return None
    return datetime(
        event_date.year,
        event_date.month,
        event_date.day,
        parsed.hour,
        parsed.minute,
    )


def _time_is_precise(raw_time: str) -> bool:
    return any(char.isdigit() for char in raw_time)


def _infer_activity_type(title: str, description: str) -> str:
    blob = " ".join([title, description]).lower()
    if "talk" in blob or "conversation" in blob or "looking together" in blob:
        return "talk"
    if "storytime" in blob:
        return "activity"
    if "workshop" in blob or "class" in blob or "lab" in blob:
        return "workshop"
    return "activity"


def _requires_registration(description: str | None) -> bool | None:
    text = (description or "").lower()
    if not text:
        return None
    if "registration" in text or "register" in text:
        return "not required" not in text
    return False


def _free_status(description: str | None) -> str:
    text = (description or "").lower()
    return "confirmed" if "free" in text else "inferred"


def _fetch_sync(url: str) -> dict[str, object]:
    request = Request(url, headers=DEFAULT_HEADERS)
    with urlopen(request, timeout=30) as response:
        return {
            "status": getattr(response, "status", response.getcode()),
            "headers": dict(response.headers.items()),
            "text": response.read().decode("utf-8", errors="replace"),
        }
