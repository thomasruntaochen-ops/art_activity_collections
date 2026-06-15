import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

OCMA_CALENDAR_URL = "https://langson.uci.edu/calendar/"

LA_TIMEZONE = "America/Los_Angeles"
OCMA_VENUE_NAME = "Orange County Museum of Art"
OCMA_CITY = "Costa Mesa"
OCMA_STATE = "CA"
OCMA_DEFAULT_LOCATION = "Costa Mesa, CA"

DATE_PREFIX_RE = re.compile(r"^(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4}),\s*(?P<time>.+)$")
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*"
    r"(?P<start_meridiem>a\.?m\.?|p\.?m\.?|AM|PM)?\s*(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*"
    r"(?P<end_meridiem>a\.?m\.?|p\.?m\.?|AM|PM)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?|AM|PM)",
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
    "Referer": "https://langson.uci.edu/",
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
    "happy hour",
    "member",
    "opening",
    "tv",
    "reading",
    "writing",
    "open house",
    "performance",
    "music",
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
    "play",
    "make",
)


async def fetch_ocma_calendar_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[ocma-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[ocma-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[ocma-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[ocma-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[ocma-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[ocma-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[ocma-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch OCMA calendar page") from last_exception
    raise RuntimeError("Unable to fetch OCMA calendar page after retries")


class OcmaCalendarAdapter(BaseSourceAdapter):
    source_name = "ocma_calendar"

    def __init__(self, url: str = OCMA_CALENDAR_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_ocma_calendar_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_ocma_events_html(payload, list_url=self.url)


def parse_ocma_events_html(
    html: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = (now or datetime.now()).date()

    for article in soup.select("#newcalendarcontent section.month article[id^='post-']"):
        anchor = article.find("a", href=True)
        title_node = article.find("h3")
        category_node = article.find("h5")
        datetime_node = article.find("h4")

        title = _normalize_text(title_node.get_text(" ", strip=True) if title_node else None)
        category = _normalize_text(category_node.get_text(" ", strip=True) if category_node else None)
        datetime_text = _normalize_text(datetime_node.get_text(" ", strip=True) if datetime_node else None)
        if not title or not datetime_text or not anchor:
            continue
        if is_irrelevant_item_text(title):
            continue

        if _should_exclude_event(title=title, category=category):
            continue
        if not _should_include_event(title=title, category=category):
            continue

        start_at, end_at = _parse_card_datetimes(datetime_text)
        if start_at is None:
            continue
        if start_at.date() < current_date:
            continue

        source_url = urljoin(list_url, anchor["href"])
        age_min, age_max = _parse_age_range(title=title, category=category)
        text_blob = " ".join([title, category or "", datetime_text]).lower()

        description_parts = []
        if category:
            description_parts.append(f"Category: {category}")
        description_parts.append(f"Schedule: {datetime_text}")
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
                venue_name=OCMA_VENUE_NAME,
                location_text=OCMA_DEFAULT_LOCATION,
                city=OCMA_CITY,
                state=OCMA_STATE,
                activity_type=_infer_activity_type(title=title, category=category),
                age_min=age_min,
                age_max=age_max,
                audience_segment=_infer_ocma_audience(
                    title=title,
                    category=category,
                    age_min=age_min,
                    age_max=age_max,
                ),
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=("registration" in text_blob and "not required" not in text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=LA_TIMEZONE,
                **_ocma_price_kwargs(text_blob),
            )
        )

    if rows:
        return rows

    return _parse_langson_calendar_events(soup=soup, list_url=list_url, current_date=current_date)


def _parse_langson_calendar_events(
    *,
    soup: BeautifulSoup,
    list_url: str,
    current_date,
) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in soup.select(".uabb-blog-posts-shadow"):
        title_link = card.select_one("h3.uabb-post-heading a[href]")
        if title_link is None:
            continue
        title = _normalize_text(title_link.get_text(" ", strip=True))
        source_url = urljoin(list_url, title_link.get("href", ""))
        lines = [_normalize_space(text) for text in card.get_text("\n").splitlines() if _normalize_space(text)]
        if not title or not source_url or title not in lines:
            continue
        title_index = lines.index(title)
        category = _normalize_text(lines[title_index - 1] if title_index > 0 else "")
        datetime_text = _normalize_text(lines[title_index + 1] if title_index + 1 < len(lines) else "")
        if not datetime_text:
            continue
        if is_irrelevant_item_text(title):
            continue
        if _should_exclude_event(title=title, category=category):
            continue
        if not _should_include_event(title=title, category=category):
            continue

        start_at, end_at = _parse_langson_datetimes(datetime_text)
        if start_at is None:
            continue
        last_date = end_at.date() if end_at is not None else start_at.date()
        if last_date < current_date:
            continue

        age_min, age_max = _parse_age_range(title=title, category=category)
        text_blob = " ".join([title, category or "", datetime_text]).lower()
        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=" | ".join(
                    part
                    for part in [
                        f"Category: {category}" if category else "",
                        f"Schedule: {datetime_text}",
                    ]
                    if part
                ),
                venue_name=OCMA_VENUE_NAME,
                location_text=OCMA_DEFAULT_LOCATION,
                city=OCMA_CITY,
                state=OCMA_STATE,
                activity_type=_infer_activity_type(title=title, category=category),
                age_min=age_min,
                age_max=age_max,
                audience_segment=_infer_ocma_audience(
                    title=title,
                    category=category,
                    age_min=age_min,
                    age_max=age_max,
                ),
                drop_in=("drop-in" in text_blob or "drop in" in text_blob or "make" in text_blob),
                registration_required=("registration" in text_blob and "not required" not in text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=LA_TIMEZONE,
                **_ocma_price_kwargs(text_blob),
            )
        )

    return rows


def _should_exclude_event(*, title: str, category: str | None) -> bool:
    blob = f"{title} {category or ''}".lower()
    if "past " in blob:
        return True
    return any(keyword in blob for keyword in EXCLUDED_KEYWORDS)


def _should_include_event(*, title: str, category: str | None) -> bool:
    blob = f"{title} {category or ''}".lower()
    if "family program" in blob or "public program" in blob:
        return True
    return any(keyword in blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(*, title: str, category: str | None) -> str:
    blob = f"{title} {category or ''}".lower()
    if any(keyword in blob for keyword in ("talk", "lecture", "conversation")):
        return "talk"
    if any(keyword in blob for keyword in ("workshop", "studio", "class", "play", "make")):
        return "workshop"
    return "activity"


def _parse_card_datetimes(value: str) -> tuple[datetime | None, datetime | None]:
    match = DATE_PREFIX_RE.match(_normalize_space(value))
    if not match:
        return None, None

    try:
        day = datetime.strptime(match.group("date"), "%B %d, %Y")
    except ValueError:
        return None, None

    time_text = match.group("time").replace("–", "-").replace("—", "-")
    range_match = TIME_RANGE_RE.search(time_text)
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
            day.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0),
            day.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0),
        )

    single_match = TIME_SINGLE_RE.search(time_text)
    if single_match:
        hour, minute = _to_24h(
            int(single_match.group("hour")),
            int(single_match.group("minute") or 0),
            single_match.group("meridiem"),
        )
        return day.replace(hour=hour, minute=minute, second=0, microsecond=0), None

    return day, None


def _parse_langson_datetimes(value: str) -> tuple[datetime | None, datetime | None]:
    text = _normalize_space(value).replace("–", "-").replace("—", "-")
    match = re.match(
        r"^(?P<month>[A-Za-z]+)\s+(?P<start_day>\d{1,2})"
        r"(?:\s*-\s*(?P<end_day>\d{1,2}))?,\s*(?P<year>\d{4}),\s*(?P<time>.+)$",
        text,
    )
    if not match:
        return _parse_card_datetimes(value)

    start_date_text = f"{match.group('month')} {match.group('start_day')}, {match.group('year')}"
    try:
        start_day = datetime.strptime(start_date_text, "%B %d, %Y")
    except ValueError:
        return None, None

    start_at, end_at = _parse_card_datetimes(f"{start_date_text}, {match.group('time')}")
    if start_at is None:
        return None, None

    end_day_text = match.group("end_day")
    if end_day_text and end_at is not None:
        try:
            end_day = datetime.strptime(
                f"{match.group('month')} {end_day_text}, {match.group('year')}",
                "%B %d, %Y",
            )
        except ValueError:
            return start_at, end_at
        end_at = end_day.replace(hour=end_at.hour, minute=end_at.minute, second=0, microsecond=0)

    return start_at, end_at


def _to_24h(hour: int, minute: int | None, meridiem: str | None) -> tuple[int, int]:
    minute = minute or 0
    if not meridiem:
        return hour, minute
    suffix = meridiem.lower().replace(".", "")
    if suffix == "pm" and hour != 12:
        hour += 12
    if suffix == "am" and hour == 12:
        hour = 0
    return hour, minute


def _parse_age_range(*, title: str, category: str | None) -> tuple[int | None, int | None]:
    blob = f"{title} {category or ''}"

    range_match = AGE_PAREN_RANGE_RE.search(blob) or AGE_RANGE_RE.search(blob)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))

    plus_match = AGE_PLUS_RE.search(blob)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def _infer_ocma_audience(
    *,
    title: str,
    category: str | None,
    age_min: int | None,
    age_max: int | None,
) -> str:
    blob = f" {title.lower()} {category.lower() if category else ''} "
    if title.lower().startswith("make:") or " make " in blob or " all ages " in blob:
        return "all_ages"
    if any(marker in blob for marker in (" family ", " youth ", " kids ", " children ")):
        return "kids"
    if any(marker in blob for marker in (" teen ", " teens ")):
        inferred = infer_audience_segment(title=title, category=category, age_min=age_min, age_max=age_max)
        return inferred if inferred != "unknown" else "teens"
    inferred = infer_audience_segment(title=title, category=category, age_min=age_min, age_max=age_max)
    if inferred != "unknown":
        return inferred
    if any(marker in blob for marker in (" public program ", " talk ", " lecture ", " conversation ", " workshop ")):
        return "adults"
    return "unknown"


def _ocma_price_kwargs(text: str | None) -> dict[str, bool | None | str]:
    return price_classification_kwargs(text, default_is_free=True)


def _normalize_space(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    text = _normalize_space(str(value))
    return text or None
