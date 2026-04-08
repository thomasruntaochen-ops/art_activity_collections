from __future__ import annotations

import asyncio
import re
from datetime import date
from datetime import datetime
from html import unescape
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

UNIVERSITY_OF_WYOMING_ART_MUSEUM_LIST_URL = "https://www.uwyo.edu/artmuseum/events/index.html"
UNIVERSITY_OF_WYOMING_ART_MUSEUM_RSS_URL = "https://www.trumba.com/calendars/art-museum-marketing.rss"
UNIVERSITY_OF_WYOMING_ART_MUSEUM_TIMEZONE = "America/Denver"
UNIVERSITY_OF_WYOMING_ART_MUSEUM_VENUE_NAME = "University of Wyoming Art Museum"
UNIVERSITY_OF_WYOMING_ART_MUSEUM_CITY = "Laramie"
UNIVERSITY_OF_WYOMING_ART_MUSEUM_STATE = "WY"
UNIVERSITY_OF_WYOMING_ART_MUSEUM_DEFAULT_LOCATION = "University of Wyoming Art Museum, Laramie, WY"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": UNIVERSITY_OF_WYOMING_ART_MUSEUM_LIST_URL,
}

INCLUDE_MARKERS = (
    " artist talk ",
    " talk ",
    " lecture ",
    " conversation ",
    " workshop ",
    " class ",
    " artmaking ",
    " seminar ",
    " lab ",
)
EXCLUDE_MARKERS = (
    " opening reception ",
    " reception ",
    " gala ",
    " fundraiser ",
    " fundraising ",
    " exhibition opening ",
    " exhibition ",
    " film ",
    " music ",
    " performance ",
    " tour ",
    " storytime ",
    " story time ",
    " admission ",
)
TITLE_EXCLUDE_MARKERS = (
    " reception ",
    " gala ",
    " exhibition ",
    " fundraiser ",
    " fundraising ",
)
DATE_LINE_RE = re.compile(
    r"^(?:[A-Za-z]+),\s+(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+(?P<year>\d{4}),\s+(?P<time>.+)$"
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?)\s*(?P<start_meridiem>a\.?m\.?|p\.?m\.?)?"
    r"\s*(?:-|–|—|to)\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?)\s*(?P<end_meridiem>a\.?m\.?|p\.?m\.?)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<time>\d{1,2}(?::\d{2})?)\s*(?P<meridiem>a\.?m\.?|p\.?m\.?)",
    re.IGNORECASE,
)
METADATA_LABELS = {
    "Venue",
    "Address",
    "Building and Room",
    "City",
    "State",
    "Country",
    "Event Type",
    "Other Event Type",
    "Event Speakers",
    "Is it open to the general public?",
    "Audience",
    "Online Event Registration",
    "Sponsoring Department/Unit",
    "Other Sponsoring Department/Unit",
    "Sponsor Website",
    "Is there a fee to attend?",
    "Fees To Attend",
    "Contact Name",
    "Contact Phone",
    "Contact Email",
    "Contact Department",
    "More info",
}


async def fetch_university_of_wyoming_art_museum_rss(
    url: str = UNIVERSITY_OF_WYOMING_ART_MUSEUM_RSS_URL,
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
        raise RuntimeError("Unable to fetch University of Wyoming Art Museum RSS feed") from last_exception
    raise RuntimeError("Unable to fetch University of Wyoming Art Museum RSS feed after retries")


async def load_university_of_wyoming_art_museum_payload() -> str:
    return await fetch_university_of_wyoming_art_museum_rss()


def parse_university_of_wyoming_art_museum_payload(
    rss_text: str,
    *,
    start_date: str | date | datetime | None = None,
) -> list[ExtractedActivity]:
    start_day = _coerce_start_date(start_date)
    xml_text = rss_text.lstrip("\ufeff").strip()
    if not xml_text:
        return []

    root = ET.fromstring(xml_text)
    channel = root.find("channel")
    if channel is None:
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in channel.findall("item"):
        row = _build_row_from_item(item)
        if row is None or row.start_at.date() < start_day:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class UniversityOfWyomingArtMuseumAdapter(BaseSourceAdapter):
    source_name = "university_of_wyoming_art_museum_events"

    async def fetch(self) -> list[str]:
        return [await load_university_of_wyoming_art_museum_payload()]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_university_of_wyoming_art_museum_payload(payload)


def _build_row_from_item(item: ET.Element) -> ExtractedActivity | None:
    title = _normalize_space(unescape(item.findtext("title") or ""))
    source_url = _normalize_space(item.findtext("link") or "")
    description_html = item.findtext("description") or ""
    description_parts = _split_description_parts(description_html)
    schedule_text = description_parts[0] if description_parts else ""
    detail_segments, metadata = _extract_detail_segments(description_parts[1:])
    description_text = _normalize_space(" ".join(detail_segments))
    category_text = ", ".join(
        _normalize_space(unescape(category.text or ""))
        for category in item.findall("category")
        if _normalize_space(unescape(category.text or ""))
    )

    if not title or not source_url:
        return None
    if not _should_include(title=title, description=description_text, category_text=category_text):
        return None

    start_at, end_at = _parse_schedule_text(schedule_text)
    if start_at is None:
        return None

    location_text = _join_non_empty(
        [
            metadata.get("Venue"),
            metadata.get("Address"),
        ]
    ) or UNIVERSITY_OF_WYOMING_ART_MUSEUM_DEFAULT_LOCATION

    full_description = _join_non_empty(
        [
            description_text,
            f"Categories: {category_text}" if category_text else None,
        ]
    )
    text_blob = _search_blob(" ".join(filter(None, [title, description_text, category_text])))

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=UNIVERSITY_OF_WYOMING_ART_MUSEUM_VENUE_NAME,
        location_text=location_text,
        city=UNIVERSITY_OF_WYOMING_ART_MUSEUM_CITY,
        state=UNIVERSITY_OF_WYOMING_ART_MUSEUM_STATE,
        activity_type=_infer_activity_type(title=title, description=description_text, category_text=category_text),
        age_min=None,
        age_max=None,
        drop_in=(" drop in " in text_blob or " open to everyone " in text_blob),
        registration_required=(
            " register " in text_blob
            or " registration " in text_blob
            or " reserve " in text_blob
            or " rsvp " in text_blob
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=UNIVERSITY_OF_WYOMING_ART_MUSEUM_TIMEZONE,
        **_price_kwargs(description_text=description_text, metadata=metadata),
    )


def _coerce_start_date(value: str | date | datetime | None) -> date:
    if value is None:
        return datetime.now(ZoneInfo(UNIVERSITY_OF_WYOMING_ART_MUSEUM_TIMEZONE)).date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _split_description_parts(description_html: str) -> list[str]:
    if not description_html:
        return []
    parts = re.split(r"<br\s*/?>", description_html, flags=re.IGNORECASE)
    normalized: list[str] = []
    for part in parts:
        text = _normalize_space(BeautifulSoup(unescape(part), "html.parser").get_text(" ", strip=True))
        if text:
            normalized.append(text)
    return normalized


def _extract_detail_segments(parts: list[str]) -> tuple[list[str], dict[str, str]]:
    detail_segments: list[str] = []
    metadata: dict[str, str] = {}
    for part in parts:
        match = re.match(r"^(?P<label>[^:]+):\s*(?P<value>.+)$", part)
        label = _normalize_space(match.group("label")) if match else ""
        if match and label in METADATA_LABELS:
            metadata[label] = _normalize_space(match.group("value"))
            continue
        detail_segments.append(part)
    return detail_segments, metadata


def _price_kwargs(*, description_text: str, metadata: dict[str, str]) -> dict[str, bool | None | str]:
    fee_value = _normalize_space(metadata.get("Is there a fee to attend?"))
    if fee_value.lower() == "no":
        return {"is_free": True, "free_verification_status": "confirmed"}
    if fee_value.lower() == "yes":
        return {"is_free": False, "free_verification_status": "confirmed"}
    return price_classification_kwargs(
        " ".join(filter(None, [description_text, fee_value, metadata.get("Fees To Attend")])),
        default_is_free=None,
    )


def _parse_schedule_text(schedule_text: str) -> tuple[datetime | None, datetime | None]:
    match = DATE_LINE_RE.match(_normalize_space(unescape(schedule_text)))
    if match is None:
        return None, None

    base_date = datetime.strptime(
        f"{match.group('month')} {match.group('day')} {match.group('year')}",
        "%B %d %Y",
    ).date()
    time_text = _normalize_space(match.group("time"))
    return _parse_time_range(base_date, time_text)


def _parse_time_range(base_date: date, time_text: str) -> tuple[datetime | None, datetime | None]:
    range_match = TIME_RANGE_RE.search(time_text)
    if range_match is not None:
        end_meridiem = _normalize_meridiem(range_match.group("end_meridiem"))
        start_meridiem = _normalize_meridiem(range_match.group("start_meridiem")) or end_meridiem
        start_at = _combine_date_and_time(base_date, range_match.group("start"), start_meridiem)
        end_at = _combine_date_and_time(base_date, range_match.group("end"), end_meridiem)
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(time_text)
    if single_match is None:
        return datetime.combine(base_date, datetime.min.time()), None

    start_at = _combine_date_and_time(
        base_date,
        single_match.group("time"),
        _normalize_meridiem(single_match.group("meridiem")),
    )
    return start_at, None


def _combine_date_and_time(base_date: date, time_value: str, meridiem: str | None) -> datetime | None:
    cleaned = _normalize_space(time_value).lower().replace(".", "")
    if meridiem is None:
        return None

    parts = cleaned.split(":", 1)
    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) == 2 else 0

    if meridiem == "pm" and hour != 12:
        hour += 12
    elif meridiem == "am" and hour == 12:
        hour = 0

    return datetime(base_date.year, base_date.month, base_date.day, hour, minute)


def _normalize_meridiem(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.lower().replace(".", "")
    if normalized in {"am", "pm"}:
        return normalized
    return None


def _should_include(*, title: str, description: str, category_text: str) -> bool:
    title_blob = _search_blob(title)
    full_blob = _search_blob(" ".join(filter(None, [title, description, category_text])))
    has_include = any(marker in full_blob for marker in INCLUDE_MARKERS)
    has_title_exclude = any(marker in title_blob for marker in TITLE_EXCLUDE_MARKERS)
    has_exclude = any(marker in full_blob for marker in EXCLUDE_MARKERS)

    if has_title_exclude:
        return False
    if has_include:
        return True
    return not has_exclude and False


def _infer_activity_type(*, title: str, description: str, category_text: str) -> str:
    blob = _search_blob(" ".join(filter(None, [title, description, category_text])))
    if any(marker in blob for marker in (" workshop ", " class ", " artmaking ", " lab ")):
        return "workshop"
    if any(marker in blob for marker in (" talk ", " lecture ", " conversation ", " seminar ")):
        return "talk"
    return "activity"


def _search_blob(value: str) -> str:
    return f" {_normalize_space(unescape(value)).lower()} "


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).split())


def _join_non_empty(parts: list[str | None]) -> str | None:
    cleaned = [_normalize_space(part) for part in parts if _normalize_space(part)]
    if not cleaned:
        return None
    return " | ".join(cleaned)
