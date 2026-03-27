import asyncio
import re
from datetime import date
from datetime import datetime
from datetime import time
from html import unescape

import httpx
from bs4 import BeautifulSoup


NY_TIMEZONE = "America/New_York"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*"
    r"(?P<start_meridiem>a\.?m\.?|p\.?m\.?|am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*"
    r"(?P<end_meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?|am|pm)\b",
    re.IGNORECASE,
)
DATE_AT_TIME_RE = re.compile(
    r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})(?:\s+at\s+(?P<time>.+))?$",
    re.IGNORECASE,
)
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)

INCLUDE_MARKERS = (
    "activity",
    "activities",
    "art making",
    "art-making",
    "artmaking",
    "artist talk",
    "artist-led",
    "after school studio",
    "class",
    "collage",
    "conversation",
    "curator talk",
    "discussion",
    "drawing",
    "lab",
    "labs",
    "lecture",
    "lectures",
    "object talk",
    "paint",
    "painting",
    "printmaking",
    "studio",
    "talk",
    "talks",
    "watercolor",
    "workshop",
    "workshops",
)
EXCLUDE_MARKERS = (
    "admission",
    "band",
    "camp",
    "concert",
    "dinner",
    "exhibition opening",
    "film",
    "fundraiser",
    "fundraising",
    "jazz",
    "meditation",
    "mindfulness",
    "music",
    "open house",
    "orchestra",
    "performance",
    "poem",
    "poetry",
    "reading",
    "reception",
    "sports",
    "storytime",
    "tai chi",
    "tour",
    "tours",
    "tv",
    "writing workshop",
    "yoga",
)


async def fetch_html(
    url: str,
    *,
    referer: str | None = None,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    headers = dict(DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer

    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers)

    last_exception: Exception | None = None
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(url, headers=headers)
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
        raise RuntimeError(f"Unable to fetch page: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch page after retries: {url}")


def normalize_space(value: str | None) -> str:
    return " ".join(unescape(value or "").split())


def clean_html_fragment(value: str | None) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return normalize_space(soup.get_text(" ", strip=True))


def join_non_empty(parts: list[str | None]) -> str | None:
    cleaned = [normalize_space(part) for part in parts if normalize_space(part)]
    if not cleaned:
        return None
    return " | ".join(cleaned)


def parse_date_text(date_text: str) -> date | None:
    text = normalize_space(date_text)
    if not text:
        return None

    if ISO_DATE_RE.match(text):
        return datetime.strptime(text, "%Y-%m-%d").date()

    if ", " in text and text.split(", ", 1)[0].isalpha():
        for fmt in ("%A, %B %d, %Y", "%A, %b %d, %Y"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue

    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    match = DATE_AT_TIME_RE.match(text)
    if match:
        return parse_date_text(match.group("date"))

    return None


def parse_month_day_year(month_text: str, day_text: str, year: int) -> date | None:
    text = f"{normalize_space(month_text)} {normalize_space(day_text)}, {year}"
    return parse_date_text(text)


def parse_datetime_range(
    *,
    date_text: str,
    time_text: str | None,
) -> tuple[datetime | None, datetime | None]:
    base_date = parse_date_text(date_text)
    if base_date is None:
        return None, None
    return parse_time_range(base_date=base_date, time_text=time_text)


def parse_time_range(*, base_date: date, time_text: str | None) -> tuple[datetime | None, datetime | None]:
    if not normalize_space(time_text):
        midnight = datetime.combine(base_date, time(0, 0))
        return midnight, None

    text = normalize_space(time_text).replace("–", "-").replace("—", "-")

    range_match = TIME_RANGE_RE.search(text)
    if range_match:
        start_meridiem = _normalize_meridiem(
            range_match.group("start_meridiem") or range_match.group("end_meridiem")
        )
        end_meridiem = _normalize_meridiem(range_match.group("end_meridiem"))

        start_at = _build_time(
            base_date=base_date,
            hour_text=range_match.group("start_hour"),
            minute_text=range_match.group("start_minute"),
            meridiem=start_meridiem,
        )
        end_at = _build_time(
            base_date=base_date,
            hour_text=range_match.group("end_hour"),
            minute_text=range_match.group("end_minute"),
            meridiem=end_meridiem,
        )
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(text)
    if single_match:
        start_at = _build_time(
            base_date=base_date,
            hour_text=single_match.group("hour"),
            minute_text=single_match.group("minute"),
            meridiem=_normalize_meridiem(single_match.group("meridiem")),
        )
        return start_at, None

    midnight = datetime.combine(base_date, time(0, 0))
    return midnight, None


def parse_age_range(text: str | None) -> tuple[int | None, int | None]:
    blob = normalize_space(text)
    if not blob:
        return None, None

    range_match = AGE_RANGE_RE.search(blob)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))

    plus_match = AGE_PLUS_RE.search(blob)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def should_include_event(
    *,
    title: str,
    description: str | None = None,
    category: str | None = None,
) -> bool:
    title_blob = f" {normalize_space(title).lower()} "
    body_blob = f" {normalize_space(join_non_empty([description, category]) or '').lower()} "
    combined = f"{title_blob}{body_blob}"

    include_hit = any(_contains_marker(combined, marker) for marker in INCLUDE_MARKERS)
    exclude_hit = any(_contains_marker(title_blob, marker) for marker in EXCLUDE_MARKERS)
    strong_body_exclude_hit = any(_contains_marker(body_blob, marker) for marker in EXCLUDE_MARKERS)

    if "sold out" in combined:
        return False
    if exclude_hit and not include_hit:
        return False
    if strong_body_exclude_hit and not include_hit:
        return False
    return include_hit


def infer_activity_type(title: str, description: str | None = None, category: str | None = None) -> str:
    blob = " ".join(
        normalize_space(part).lower() for part in (title, description, category) if normalize_space(part)
    )
    if "lecture" in blob:
        return "lecture"
    if "talk" in blob or "conversation" in blob or "discussion" in blob:
        return "talk"
    if "class" in blob:
        return "class"
    if "workshop" in blob or "studio" in blob or "art making" in blob or "artmaking" in blob:
        return "workshop"
    return "activity"


def absolute_url(url: str, href: str | None) -> str:
    return httpx.URL(url).join(href or "").__str__()


def _normalize_meridiem(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.lower().replace(".", "")
    if normalized in {"am", "pm"}:
        return normalized
    return None


def _build_time(
    *,
    base_date: date,
    hour_text: str,
    minute_text: str | None,
    meridiem: str | None,
) -> datetime:
    hour = int(hour_text)
    minute = int(minute_text or 0)

    if meridiem == "pm" and hour != 12:
        hour += 12
    elif meridiem == "am" and hour == 12:
        hour = 0

    return datetime.combine(base_date, time(hour, minute))


def _contains_marker(blob: str, marker: str) -> bool:
    padded = f" {normalize_space(blob).lower()} "
    marker_text = normalize_space(marker).lower()
    return f" {marker_text} " in padded
