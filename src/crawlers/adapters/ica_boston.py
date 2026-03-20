import asyncio
import json
import re
from datetime import date
from datetime import datetime
from datetime import time
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

ICA_BOSTON_CALENDAR_URL = "https://www.icaboston.org/calendar/"

NY_TIMEZONE = "America/New_York"
ICA_BOSTON_VENUE_NAME = "Institute of Contemporary Art / Boston"
ICA_BOSTON_CITY = "Boston"
ICA_BOSTON_STATE = "MA"
ICA_BOSTON_DEFAULT_LOCATION = "Institute of Contemporary Art / Boston, Boston, MA"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": ICA_BOSTON_CALENDAR_URL,
}

CARD_SELECTOR = "div.ds-1col.node.node-event.view-mode-related.clearfix"

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
DATE_TEXT_RE = re.compile(
    r"^(?:[A-Za-z]{3,9},\s+)?"
    r"(?P<month>[A-Za-z]{3,9})\s+"
    r"(?P<day>\d{1,2})"
    r"(?:,\s*(?P<year>\d{4}))?"
    r"(?:,\s*(?P<time_text>.+))?$"
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_UNDER_RE = re.compile(
    r"\b(?:ages?\s*)?(\d{1,2})\s*(?:and|or)\s+under\b|\bchildren\s+(\d{1,2})\s*(?:and|or)\s+under\b",
    re.IGNORECASE,
)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

EXCLUDED_CATEGORY_NAMES = {
    "Dance",
    "Film",
    "First Fridays",
    "ICA Live",
    "Music",
}
STRONG_INCLUDE_CATEGORY_NAMES = {
    "Art Making",
    "Talks",
}
SOFT_INCLUDE_CATEGORY_NAMES = {
    "ICA Kids",
    "ICA Teens",
    "Tours + Workshops",
}
STRONG_EXCLUDED_TITLE_PATTERNS = (
    "bank of america museums on us",
    "boston family days",
    "first fridays",
    "free thursday night",
    "gala",
    "luncheon",
    "opening reception",
    "showcase",
    "teen night",
)
INCLUDE_KEYWORDS = (
    "art-making",
    "art making",
    "art trek",
    "artist's voice",
    "artist’s voice",
    "class",
    "classes",
    "conversation",
    "creative",
    "create",
    "drawing",
    "educator",
    "family art day",
    "gallery talk",
    "hands-on",
    "hands on",
    "interactive",
    "lecture",
    "paint",
    "play date",
    "talk",
    "watercolor",
    "workshop",
    "workshops",
)
SOFT_INCLUDE_KEYWORDS = (
    "activities",
    "activity",
    "drop-in",
    "drop in",
    "family",
    "games",
)


async def fetch_ica_boston_page(
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
        raise RuntimeError("Unable to fetch ICA Boston calendar pages") from last_exception
    raise RuntimeError("Unable to fetch ICA Boston calendar pages after retries")


async def load_ica_boston_calendar_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_ica_boston_page(ICA_BOSTON_CALENDAR_URL, client=client)
        listings = _parse_listing_cards(listing_html, list_url=ICA_BOSTON_CALENDAR_URL)
        detail_urls = sorted({listing["source_url"] for listing in listings})
        detail_pages = await asyncio.gather(*(fetch_ica_boston_page(url, client=client) for url in detail_urls))

    return {
        "listing_url": ICA_BOSTON_CALENDAR_URL,
        "listings": listings,
        "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
    }


def parse_ica_boston_calendar_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    listings_by_url = {listing["source_url"]: listing for listing in payload["listings"]}
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, html in payload["detail_pages"].items():
        listing = listings_by_url.get(source_url)
        if listing is None:
            continue

        detail = _parse_detail_page(html, source_url=source_url)
        if detail is None:
            continue
        if not _should_include_event(listing=listing, detail=detail):
            continue

        description = _build_description(detail=detail, categories=listing["categories"])
        text_blob = " ".join(
            filter(
                None,
                [
                    listing["title"],
                    detail["title"],
                    description,
                    detail.get("price_text"),
                    detail.get("button_label"),
                    " ".join(listing["categories"]),
                ],
            )
        ).lower()
        age_min, age_max = _parse_age_range(description)
        location_text = detail.get("location_text") or ICA_BOSTON_DEFAULT_LOCATION
        is_free, free_status = _classify_price(
            detail=detail,
            categories=listing["categories"],
            description=description,
        )
        registration_required = _is_registration_required(
            detail=detail,
            text_blob=text_blob,
        )
        drop_in = any(
            marker in text_blob
            for marker in ("drop-in", "drop in", "first come, first served", "first-come, first-served")
        ) or "no pre-registration required" in (detail.get("price_text") or "").lower()

        date_entries = detail["date_entries"] or listing["date_entries"]
        for start_at, end_at in date_entries:
            if start_at.date() < current_date:
                continue

            key = (source_url, detail["title"], start_at)
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=detail["title"],
                    description=description,
                    venue_name=ICA_BOSTON_VENUE_NAME,
                    location_text=location_text,
                    city=ICA_BOSTON_CITY,
                    state=ICA_BOSTON_STATE,
                    activity_type=_infer_activity_type(
                        title=detail["title"],
                        categories=listing["categories"],
                        description=description,
                    ),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=drop_in,
                    registration_required=registration_required,
                    start_at=start_at,
                    end_at=end_at,
                    timezone=NY_TIMEZONE,
                    is_free=is_free,
                    free_verification_status=free_status,
                )
            )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class IcaBostonCalendarAdapter(BaseSourceAdapter):
    source_name = "ica_boston_calendar"

    async def fetch(self) -> list[str]:
        html = await fetch_ica_boston_page(ICA_BOSTON_CALENDAR_URL)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        listings = _parse_listing_cards(payload, list_url=ICA_BOSTON_CALENDAR_URL)
        detail_urls = sorted({listing["source_url"] for listing in listings})
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
            detail_pages = await asyncio.gather(*(fetch_ica_boston_page(url, client=client) for url in detail_urls))
        return parse_ica_boston_calendar_payload(
            {
                "listing_url": ICA_BOSTON_CALENDAR_URL,
                "listings": listings,
                "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
            }
        )


def _parse_listing_cards(html: str, *, list_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    reference_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    listings: list[dict] = []

    for card in soup.select(CARD_SELECTOR):
        title_link = card.select_one("h3.teaser-title a[href]")
        if title_link is None:
            continue

        title = _normalize_space(title_link.get_text(" ", strip=True))
        href = (title_link.get("href") or "").strip()
        if not title or not href:
            continue

        source_url = urljoin(list_url, href)
        categories = [
            _normalize_space(anchor.get_text(" ", strip=True))
            for anchor in card.select(".field-name-field-event-type a[rel='tag']")
        ]
        categories = [category for category in categories if category]
        date_entries = _parse_date_entries(_extract_date_display_texts(card), reference_date=reference_date)

        listings.append(
            {
                "title": title,
                "source_url": source_url,
                "categories": categories,
                "date_entries": date_entries,
            }
        )

    return listings


def _parse_detail_page(html: str, *, source_url: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    reference_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    title = _normalize_space(_node_text(soup.select_one("h1.page-title")))
    if not title:
        return None

    description = _clean_description_text(_node_text(soup.select_one(".entry-content")))
    price_text = _normalize_space(_node_text(soup.select_one(".field-name-field-ticket-price")))
    button_label = _normalize_space(_node_text(soup.select_one(".group-buttons a")))

    event_obj = _extract_event_object(soup)
    if not description:
        description = _normalize_space(str(event_obj.get("description") or ""))
    if not price_text:
        price_text = _extract_offer_price_text(event_obj.get("offers"))

    location_text = _extract_location_name(event_obj.get("location")) or ICA_BOSTON_DEFAULT_LOCATION

    date_entries = _parse_date_entries(_extract_date_display_texts(soup), reference_date=reference_date)
    if not date_entries:
        fallback = _parse_iso_datetime_pair(
            event_obj.get("startDate") or event_obj.get("start_date"),
            event_obj.get("endDate") or event_obj.get("end_date"),
        )
        if fallback is not None:
            date_entries = [fallback]

    return {
        "source_url": source_url,
        "title": title,
        "description": description or None,
        "price_text": price_text or None,
        "button_label": button_label or None,
        "location_text": location_text,
        "date_entries": date_entries,
    }


def _parse_date_entries(date_texts: list[str], *, reference_date: date) -> list[tuple[datetime, datetime | None]]:
    entries: list[tuple[datetime, datetime | None]] = []
    seen: set[tuple[datetime, datetime | None]] = set()

    for date_text in date_texts:
        parsed = _parse_date_text(date_text, reference_date=reference_date)
        if parsed is None or parsed in seen:
            continue
        seen.add(parsed)
        entries.append(parsed)

    return entries


def _parse_date_text(date_text: str, *, reference_date: date) -> tuple[datetime, datetime | None] | None:
    normalized = _normalize_space(date_text).replace("\xa0", " ")
    if not normalized:
        return None

    match = DATE_TEXT_RE.match(normalized)
    if match is None:
        return None

    month_number = _parse_month_number(match.group("month"))
    if month_number is None:
        return None

    day_number = int(match.group("day"))
    year_text = match.group("year")
    event_date = (
        date(int(year_text), month_number, day_number)
        if year_text
        else _infer_event_date(month=month_number, day=day_number, reference_date=reference_date)
    )

    start_time, end_time = _parse_time_text(match.group("time_text") or "")
    start_at = datetime.combine(event_date, start_time)
    end_at = datetime.combine(event_date, end_time) if end_time is not None else None
    return start_at, end_at


def _parse_month_number(month_text: str) -> int | None:
    cleaned = month_text.strip().rstrip(".")
    for fmt in ("%b", "%B"):
        try:
            return datetime.strptime(cleaned, fmt).month
        except ValueError:
            continue
    return None


def _infer_event_date(*, month: int, day: int, reference_date: date) -> date:
    candidate = date(reference_date.year, month, day)
    if (reference_date - candidate).days > 180:
        return date(reference_date.year + 1, month, day)
    return candidate


def _parse_time_text(time_text: str) -> tuple[time, time | None]:
    normalized = _normalize_space(time_text).replace(".", "").lower()
    if not normalized:
        return time(0, 0), None

    range_match = TIME_RANGE_RE.search(normalized)
    if range_match is not None:
        end_meridiem = range_match.group("end_meridiem")
        start_meridiem = range_match.group("start_meridiem") or end_meridiem
        start = _build_time(
            hour_text=range_match.group("start_hour"),
            minute_text=range_match.group("start_minute"),
            meridiem_text=start_meridiem,
        )
        end = _build_time(
            hour_text=range_match.group("end_hour"),
            minute_text=range_match.group("end_minute"),
            meridiem_text=end_meridiem,
        )
        return start, end

    single_match = TIME_SINGLE_RE.search(normalized)
    if single_match is not None:
        start = _build_time(
            hour_text=single_match.group("hour"),
            minute_text=single_match.group("minute"),
            meridiem_text=single_match.group("meridiem"),
        )
        return start, None

    return time(0, 0), None


def _build_time(*, hour_text: str, minute_text: str | None, meridiem_text: str) -> time:
    hour = int(hour_text)
    minute = int(minute_text or "0")
    meridiem = meridiem_text.lower().replace(".", "")
    if meridiem == "am":
        if hour == 12:
            hour = 0
    elif meridiem == "pm" and hour != 12:
        hour += 12
    return time(hour=hour, minute=minute)


def _should_include_event(*, listing: dict, detail: dict) -> bool:
    title = detail["title"]
    title_blob = f" {_normalize_space(title).lower()} "
    categories = {category.casefold(): category for category in listing["categories"]}
    category_names = set(categories.values())
    text_blob = " ".join(
        filter(
            None,
            [
                listing["title"],
                detail["title"],
                detail.get("description"),
                detail.get("price_text"),
                detail.get("button_label"),
                " ".join(listing["categories"]),
            ],
        )
    ).lower()
    token_blob = f" {' '.join(text_blob.split())} "

    if category_names & EXCLUDED_CATEGORY_NAMES:
        return False
    if any(pattern in title_blob for pattern in STRONG_EXCLUDED_TITLE_PATTERNS):
        return False
    if " exhibition tour " in title_blob or " tour " in title_blob:
        return False
    if " ticketed event " in token_blob and "workshop" not in token_blob and "talk" not in token_blob:
        return False

    if category_names & STRONG_INCLUDE_CATEGORY_NAMES:
        return True
    if any(keyword in token_blob for keyword in INCLUDE_KEYWORDS):
        return True

    if "ICA Kids" in category_names and any(keyword in token_blob for keyword in SOFT_INCLUDE_KEYWORDS):
        return True
    if "ICA Teens" in category_names and any(keyword in token_blob for keyword in ("art-making", "art making", "creative", "create", "workshop")):
        return True
    if "Tours + Workshops" in category_names and any(
        keyword in token_blob
        for keyword in (
            "gallery talk",
            "talk",
            "conversation",
            "workshop",
            "workshops",
            "interactive",
            "drawing",
            "games",
            "educator",
        )
    ):
        return True

    return False


def _build_description(*, detail: dict, categories: list[str]) -> str | None:
    parts: list[str] = []
    description = detail.get("description")
    if description:
        parts.append(description)
    if categories:
        parts.append(f"Categories: {', '.join(categories)}")
    if detail.get("price_text"):
        parts.append(f"Price: {detail['price_text']}")
    return " | ".join(parts) if parts else None


def _infer_activity_type(*, title: str, categories: list[str], description: str | None) -> str:
    blob = " ".join(filter(None, [title, description, " ".join(categories)])).lower()
    if "Talks" in categories or any(
        marker in blob for marker in ("artist's voice", "artist’s voice", "gallery talk", "lecture", "conversation", "talk")
    ):
        return "lecture"
    return "workshop"


def _parse_age_range(description: str | None) -> tuple[int | None, int | None]:
    if not description:
        return None, None

    if match := AGE_RANGE_RE.search(description):
        return int(match.group(1)), int(match.group(2))
    if match := AGE_UNDER_RE.search(description):
        age_value = next((group for group in match.groups() if group), None)
        if age_value is not None:
            return None, int(age_value)
    if match := AGE_PLUS_RE.search(description):
        return int(match.group(1)), None
    return None, None


def _classify_price(*, detail: dict, categories: list[str], description: str | None) -> tuple[bool | None, str]:
    price_blob = " | ".join(
        part
        for part in [
            detail.get("price_text"),
            detail.get("button_label"),
            " ".join(categories),
            description,
        ]
        if part
    )
    normalized = f" {' '.join(price_blob.lower().split())} " if price_blob else ""
    if " with museum admission " in normalized:
        return None, "uncertain"
    return infer_price_classification(price_blob)


def _is_registration_required(*, detail: dict, text_blob: str) -> bool | None:
    price_text = (detail.get("price_text") or "").lower()
    button_label = (detail.get("button_label") or "").lower()
    if any(marker in price_text for marker in ("no pre-registration required", "drop-in", "first-come-first-served")):
        return False
    if any(marker in text_blob for marker in ("register", "registration required", "reserve ")) and "not required" not in text_blob:
        return True
    if button_label and any(marker in button_label for marker in ("ticket", "admission", "register", "reserve")):
        return True
    return False


def _extract_event_object(soup: BeautifulSoup) -> dict:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = (script.get_text() or "").strip()
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        for event_obj in _iter_event_objects(data):
            return event_obj
    return {}


def _iter_event_objects(value: object) -> list[dict]:
    if isinstance(value, dict):
        results: list[dict] = []
        value_type = value.get("@type")
        if value_type == "Event" or (isinstance(value_type, list) and "Event" in value_type):
            results.append(value)
        for item in value.values():
            results.extend(_iter_event_objects(item))
        return results
    if isinstance(value, list):
        results: list[dict] = []
        for item in value:
            results.extend(_iter_event_objects(item))
        return results
    return []


def _extract_offer_price_text(value: object) -> str:
    if isinstance(value, dict):
        return _normalize_space(str(value.get("price") or value.get("name") or ""))
    if isinstance(value, list):
        for item in value:
            extracted = _extract_offer_price_text(item)
            if extracted:
                return extracted
    return ""


def _extract_location_name(value: object) -> str | None:
    if isinstance(value, str):
        normalized = _normalize_space(value)
        return normalized or None
    if not isinstance(value, dict):
        return None

    parts = [
        _normalize_space(str(value.get("name") or "")),
        _normalize_space(str(value.get("address") or "")),
    ]
    parts = [part for part in parts if part]
    return ", ".join(parts) if parts else None


def _parse_iso_datetime_pair(start_value: object, end_value: object) -> tuple[datetime, datetime | None] | None:
    start_at = _parse_iso_datetime(start_value)
    if start_at is None:
        return None
    return start_at, _parse_iso_datetime(end_value)


def _parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(ZoneInfo(NY_TIMEZONE)).replace(tzinfo=None)
    return parsed


def _node_text(node: object) -> str:
    if node is None:
        return ""
    return node.get_text(" ", strip=True)


def _extract_date_display_texts(node: BeautifulSoup) -> list[str]:
    return [
        _normalize_space(date_node.get_text(" ", strip=True))
        for date_node in node.find_all(class_="event-date-display")
        if _normalize_space(date_node.get_text(" ", strip=True))
    ]


def _clean_description_text(value: str) -> str:
    normalized = _normalize_space(value)
    if not normalized:
        return ""

    cut_markers = (
        " Accessibility ",
        " Free Admission for Youth ",
        " Public Media Partner ",
    )
    padded = f" {normalized} "
    cut_positions = [padded.find(marker) for marker in cut_markers if padded.find(marker) != -1]
    if cut_positions:
        normalized = padded[: min(cut_positions)].strip()

    return normalized


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.replace("\xa0", " ").split())
