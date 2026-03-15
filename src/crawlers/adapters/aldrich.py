import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

ALDRICH_CALENDAR_URL = "https://thealdrich.org/calendar"

NY_TIMEZONE = "America/New_York"
ALDRICH_VENUE_NAME = "The Aldrich Contemporary Art Museum"
ALDRICH_CITY = "Ridgefield"
ALDRICH_STATE = "CT"
ALDRICH_DEFAULT_LOCATION = "Ridgefield, CT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": ALDRICH_CALENDAR_URL,
}

DATE_AT_TIME_RE = re.compile(
    r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s+at\s+"
    r"(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
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
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_UNDER_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:and|or)\s+under\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

STRONG_INCLUDE_TITLE_KEYWORDS = (
    "workshop",
    "hands-on",
    "lecture",
    "talk",
    "conversation",
    "class",
    "lab",
    "activity",
)
DETAIL_INCLUDE_KEYWORDS = (
    "hands-on",
    "art-making",
    "art making",
    "meaningful conversation",
    "conversation",
    "workshop",
    "lecture",
    "talk",
    "class",
    "lab",
)
HARD_EXCLUDED_TITLE_KEYWORDS = (
    "tour",
    "story time",
    "storytime",
    "camp",
    "gala",
    "reception",
    "opening",
    "third saturdays",
    "free admission",
    "travel with",
)
GENERIC_EXCLUDED_KEYWORDS = (
    "poet",
    "poetry",
    "poem",
    "film",
    "music",
    "performance",
    "dinner",
    "admission",
    "free admission",
    "member event",
    "member preview",
    "member tour",
    "fundraiser",
    "fundraising",
)


async def fetch_aldrich_page(
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
        raise RuntimeError("Unable to fetch The Aldrich calendar pages") from last_exception
    raise RuntimeError("Unable to fetch The Aldrich calendar pages after retries")


async def load_aldrich_calendar_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_aldrich_page(ALDRICH_CALENDAR_URL, client=client)
        listings = _parse_listing_cards(listing_html, list_url=ALDRICH_CALENDAR_URL)
        detail_urls = sorted({listing["source_url"] for listing in listings})
        detail_pages = await asyncio.gather(*(fetch_aldrich_page(url, client=client) for url in detail_urls))

    return {
        "listing_url": ALDRICH_CALENDAR_URL,
        "listings": listings,
        "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
    }


def parse_aldrich_calendar_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    listings_by_url = {listing["source_url"]: listing for listing in payload["listings"]}

    for source_url, html in payload["detail_pages"].items():
        listing = listings_by_url.get(source_url)
        if listing is None:
            continue

        detail = _parse_detail_page(html, source_url=source_url)
        if detail is None:
            continue
        if not _should_include_event(listing=listing, detail=detail):
            continue

        start_at, end_at = _resolve_datetimes(listing=listing, detail=detail)
        if start_at is None:
            continue

        text_blob = " ".join(
            filter(
                None,
                [
                    listing["title"],
                    listing.get("eyebrow"),
                    listing.get("description"),
                    detail["title"],
                    detail.get("eyebrow"),
                    detail.get("schedule_text"),
                    detail.get("price_text"),
                    detail.get("description"),
                    detail.get("button_label"),
                ],
            )
        ).lower()
        age_min, age_max = _parse_age_range(text_blob)
        price_blob = " ".join(
            filter(
                None,
                [
                    listing.get("eyebrow"),
                    detail.get("eyebrow"),
                    detail.get("price_text"),
                    detail.get("button_label"),
                ],
            )
        )
        is_free, free_status = infer_price_classification(price_blob)

        key = (source_url, detail["title"], start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=detail["title"],
                description=detail.get("description") or listing.get("description"),
                venue_name=ALDRICH_VENUE_NAME,
                location_text=ALDRICH_DEFAULT_LOCATION,
                city=ALDRICH_CITY,
                state=ALDRICH_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob or "no registration" in text_blob),
                registration_required=_is_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class AldrichCalendarAdapter(BaseSourceAdapter):
    source_name = "aldrich_calendar"

    async def fetch(self) -> list[str]:
        html = await fetch_aldrich_page(ALDRICH_CALENDAR_URL)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        listings = _parse_listing_cards(payload, list_url=ALDRICH_CALENDAR_URL)
        detail_urls = sorted({listing["source_url"] for listing in listings})
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
            detail_pages = await asyncio.gather(*(fetch_aldrich_page(url, client=client) for url in detail_urls))
        return parse_aldrich_calendar_payload(
            {
                "listing_url": ALDRICH_CALENDAR_URL,
                "listings": listings,
                "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
            }
        )


def _parse_listing_cards(html: str, *, list_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    listings: list[dict] = []
    seen: set[str] = set()

    for card in soup.select("div.tile.tile--c.tile--c-4"):
        link = card.select_one("a.tile__link[href]")
        title_node = card.select_one("h4.tile__title")
        if link is None or title_node is None:
            continue

        source_url = urljoin(list_url, (link.get("href") or "").strip())
        title = _normalize_space(title_node.get_text(" ", strip=True))
        if not source_url or not title or source_url in seen:
            continue

        eyebrow_node = card.select_one("div.tile__eyebrow")
        description_node = card.select_one("div.tile__description")
        button_node = card.select_one("a.tile__button")

        seen.add(source_url)
        listings.append(
            {
                "source_url": source_url,
                "title": title,
                "eyebrow": _normalize_space(eyebrow_node.get_text(" ", strip=True) if eyebrow_node else ""),
                "description": _normalize_space(
                    description_node.get_text(" ", strip=True) if description_node else ""
                )
                or None,
                "button_label": _normalize_space(button_node.get_text(" ", strip=True) if button_node else "")
                or None,
            }
        )

    return listings


def _parse_detail_page(html: str, *, source_url: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    content_root = soup.select_one("div.container div.row div.col-sm-12 div.longform")
    if content_root is None:
        return None

    title_node = content_root.select_one("h2.longform__feature")
    if title_node is None:
        return None

    eyebrow_node = content_root.select_one("div.longform__eyebrow")
    button_node = content_root.select_one("a.aldrich-button")
    schedule_text: str | None = None
    price_text: str | None = None
    description_parts: list[str] = []

    for bold in content_root.find_all("div", class_="longform__bold", recursive=False):
        text = _normalize_space(bold.get_text(" ", strip=True))
        if not text:
            continue
        classes = set(bold.get("class") or [])
        if "mt-0" in classes:
            if schedule_text is None:
                schedule_text = text
            elif price_text is None:
                price_text = text
            continue
        description_parts.append(text)

    for child in content_root.find_all(["p", "ul"], recursive=False):
        text = _normalize_space(child.get_text(" ", strip=True))
        if text:
            description_parts.append(text)

    return {
        "source_url": source_url,
        "title": _normalize_space(title_node.get_text(" ", strip=True)),
        "eyebrow": _normalize_space(eyebrow_node.get_text(" ", strip=True) if eyebrow_node else ""),
        "schedule_text": schedule_text,
        "price_text": price_text,
        "description": " | ".join(description_parts) if description_parts else None,
        "button_label": _normalize_space(button_node.get_text(" ", strip=True) if button_node else "") or None,
    }


def _should_include_event(*, listing: dict, detail: dict) -> bool:
    title = _normalize_space(detail["title"]).lower()
    listing_title = _normalize_space(listing["title"]).lower()
    detail_blob = _normalize_space(detail.get("description") or "").lower()
    listing_blob = _normalize_space(listing.get("description") or "").lower()
    category_blob = _normalize_space(
        " ".join(filter(None, [listing.get("eyebrow"), detail.get("eyebrow")]))
    ).lower()
    combined_blob = " ".join(
        filter(None, [title, listing_title, listing_blob, detail_blob, category_blob])
    )

    strong_title_include = any(keyword in title for keyword in STRONG_INCLUDE_TITLE_KEYWORDS)
    detail_include = any(keyword in combined_blob for keyword in DETAIL_INCLUDE_KEYWORDS)

    if any(keyword in title for keyword in HARD_EXCLUDED_TITLE_KEYWORDS) and not strong_title_include:
        return False
    if any(keyword in combined_blob for keyword in GENERIC_EXCLUDED_KEYWORDS) and not (
        strong_title_include or detail_include
    ):
        return False

    return strong_title_include or detail_include


def _resolve_datetimes(*, listing: dict, detail: dict) -> tuple[datetime | None, datetime | None]:
    base_date = _parse_date(detail.get("eyebrow")) or _parse_date(listing.get("eyebrow"))
    if base_date is None:
        return None, None

    start_at, end_at = _parse_schedule_text(detail.get("schedule_text"), base_date)
    if start_at is not None:
        return start_at, end_at

    eyebrow_datetime = _parse_datetime_from_eyebrow(detail.get("eyebrow")) or _parse_datetime_from_eyebrow(
        listing.get("eyebrow")
    )
    if eyebrow_datetime is not None:
        return eyebrow_datetime, None

    return base_date, None


def _parse_schedule_text(value: str | None, base_date: datetime) -> tuple[datetime | None, datetime | None]:
    if not value:
        return None, None

    time_range = TIME_RANGE_RE.search(value)
    if time_range is not None:
        start_meridiem = time_range.group("start_meridiem") or time_range.group("end_meridiem")
        start_at = base_date.replace(
            hour=_to_24_hour(int(time_range.group("start_hour")), start_meridiem),
            minute=int(time_range.group("start_minute") or 0),
        )
        end_at = base_date.replace(
            hour=_to_24_hour(int(time_range.group("end_hour")), time_range.group("end_meridiem")),
            minute=int(time_range.group("end_minute") or 0),
        )
        return start_at, end_at

    single = TIME_SINGLE_RE.search(value)
    if single is not None:
        return (
            base_date.replace(
                hour=_to_24_hour(int(single.group("hour")), single.group("meridiem")),
                minute=int(single.group("minute") or 0),
            ),
            None,
        )

    return None, None


def _parse_datetime_from_eyebrow(value: str | None) -> datetime | None:
    if not value:
        return None
    match = DATE_AT_TIME_RE.search(value)
    if match is None:
        return None
    try:
        base_date = datetime.strptime(match.group(1), "%B %d, %Y")
    except ValueError:
        return None

    parsed_time = _parse_time_component(match.group(2))
    if parsed_time is None:
        return None
    return base_date.replace(hour=parsed_time.hour, minute=parsed_time.minute)


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    match = DATE_AT_TIME_RE.search(value)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1), "%B %d, %Y")
    except ValueError:
        return None


def _parse_time_component(value: str) -> datetime | None:
    normalized = _normalize_meridiem(value)
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match is not None:
        return int(match.group(1)), int(match.group(2))

    match = AGE_UNDER_RE.search(text)
    if match is not None:
        return None, int(match.group(1))

    match = AGE_PLUS_RE.search(text)
    if match is not None:
        return int(match.group(1)), None

    return None, None


def _infer_activity_type(text_blob: str) -> str:
    if any(keyword in text_blob for keyword in ("lecture", "talk", "conversation", "discussion")):
        return "talk"
    return "workshop"


def _is_registration_required(text_blob: str) -> bool | None:
    if "no registration" in text_blob:
        return False
    return any(
        marker in text_blob
        for marker in (
            "pre-registration required",
            "pre registration required",
            "registration required",
            "general registration",
            "register",
            "purchase tickets",
            "purchase ticket",
        )
    )


def _to_24_hour(hour: int, meridiem: str | None) -> int:
    if meridiem is None:
        return hour
    normalized = _normalize_meridiem(meridiem).split()[-1]
    if normalized == "AM":
        return 0 if hour == 12 else hour
    return 12 if hour == 12 else hour + 12


def _normalize_meridiem(value: str) -> str:
    normalized = _normalize_space(value).lower().replace(".", "")
    normalized = normalized.replace("am", " AM").replace("pm", " PM")
    return " ".join(normalized.split()).upper()


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())
