import asyncio
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

EVERSON_EVENTS_URL = "https://everson.org/eversonevents/"

NY_TIMEZONE = "America/New_York"
EVERSON_VENUE_NAME = "Everson Museum of Art"
EVERSON_CITY = "Syracuse"
EVERSON_STATE = "NY"
EVERSON_DEFAULT_LOCATION = "Syracuse, NY"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": EVERSON_EVENTS_URL,
}

DATE_WITH_YEAR_FORMATS = (
    "%A, %B %d, %Y",
    "%B %d, %Y",
)
DATE_WITHOUT_YEAR_FORMATS = (
    "%A, %B %d",
    "%B %d",
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
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)(?:\b|(?=[^\w]))", re.IGNORECASE)

INCLUDE_KEYWORDS = (
    "activity",
    "activism",
    "art and activism",
    "button",
    "buttons",
    "ceramic",
    "class",
    "classes",
    "conversation",
    "conversations",
    "create your own",
    "creative",
    "drawing",
    "embroidery",
    "figure drawing",
    "interactive",
    "lecture",
    "make your own",
    "sign",
    "signs",
    "talk",
    "workshop",
)
STRONG_INCLUDE_KEYWORDS = (
    "class",
    "conversation",
    "drawing",
    "embroidery",
    "figure drawing",
    "interactive",
    "lecture",
    "workshop",
    "create your own",
    "make your own",
)
TITLE_EXCLUDE_KEYWORDS = (
    "board game",
    "ceramics social",
    "high tea",
    "meet-up",
    "meet up",
)
SOFT_EXCLUDE_KEYWORDS = (
    "board game",
    "cash bar",
    "fundraiser",
    "fundraising",
    "high tea",
    "member meet",
    "members meet",
    "performance",
    "poetry",
    "reception",
    "social event",
    "storytime",
    "tour",
    "yoga",
)


async def fetch_everson_page(
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
        raise RuntimeError("Unable to fetch Everson event pages") from last_exception
    raise RuntimeError("Unable to fetch Everson event pages after retries")


async def load_everson_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_everson_page(EVERSON_EVENTS_URL, client=client)
        listings = _parse_listing_items(listing_html, list_url=EVERSON_EVENTS_URL)
        detail_urls = sorted({listing["source_url"] for listing in listings})
        detail_pages = await asyncio.gather(*(fetch_everson_page(url, client=client) for url in detail_urls))

    return {
        "listing_url": EVERSON_EVENTS_URL,
        "listings": listings,
        "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
    }


def parse_everson_payload(payload: dict) -> list[ExtractedActivity]:
    listings_by_url = {listing["source_url"]: listing for listing in payload["listings"]}
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, html in payload["detail_pages"].items():
        listing = listings_by_url.get(source_url)
        detail = _parse_detail_page(html, source_url=source_url, listing=listing, current_date=current_date)
        if detail is None or detail["start_at"] is None:
            continue
        if detail["start_at"].date() < current_date:
            continue
        if not _should_include_event(listing=listing, detail=detail):
            continue

        key = (source_url, detail["title"], detail["start_at"])
        if key in seen:
            continue
        seen.add(key)

        description_parts = [detail.get("description")]
        if detail.get("price_text"):
            description_parts.append(f"Price: {detail['price_text']}")
        if detail.get("registration_url"):
            description_parts.append(f"Registration: {detail['registration_url']}")
        full_description = " | ".join(part for part in description_parts if part) or None

        text_blob = " ".join(
            filter(
                None,
                [
                    detail["title"],
                    detail.get("description"),
                    detail.get("price_text"),
                    listing.get("summary") if listing else None,
                ],
            )
        ).lower()
        age_min, age_max = _parse_age_range(full_description or text_blob)
        is_free, free_status = infer_price_classification(detail.get("price_text"))

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=detail["title"],
                description=full_description,
                venue_name=EVERSON_VENUE_NAME,
                location_text=detail.get("location_text") or EVERSON_DEFAULT_LOCATION,
                city=EVERSON_CITY,
                state=EVERSON_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=detail["registration_required"],
                start_at=detail["start_at"],
                end_at=detail["end_at"],
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class EversonEventsAdapter(BaseSourceAdapter):
    source_name = "everson_events"

    def __init__(self, url: str = EVERSON_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_everson_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        listings = _parse_listing_items(payload, list_url=self.url)
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
            detail_pages = await asyncio.gather(
                *(fetch_everson_page(listing["source_url"], client=client) for listing in listings)
            )
        return parse_everson_payload(
            {
                "listing_url": self.url,
                "listings": listings,
                "detail_pages": {
                    listing["source_url"]: html for listing, html in zip(listings, detail_pages, strict=True)
                },
            }
        )


def _parse_listing_items(html: str, *, list_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    listings: list[dict] = []
    seen_urls: set[str] = set()
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    for item in soup.select("li.clearfix"):
        source_url = _extract_connect_link(item, list_url=list_url)
        if not source_url or source_url in seen_urls:
            continue
        seen_urls.add(source_url)

        title = _extract_listing_title(item)
        if not title:
            continue

        raw_text = _normalize_space(item.get_text(" ", strip=True))
        date_text = _extract_listing_date_text(raw_text, title=title)
        summary = _extract_listing_summary(raw_text, title=title, date_text=date_text)
        start_date = _parse_date_text(date_text, current_date=today)

        listings.append(
            {
                "source_url": source_url,
                "title": title,
                "date_text": date_text,
                "summary": summary,
                "start_date": start_date,
            }
        )

    return listings


def _extract_connect_link(item, *, list_url: str) -> str | None:
    for anchor in item.find_all("a", href=True):
        href = urljoin(list_url, anchor.get("href", "").strip())
        if "/connect/" in href:
            return href
    return None


def _extract_listing_title(item) -> str | None:
    for anchor in item.find_all("a", href=True):
        text = _normalize_space(anchor.get_text(" ", strip=True))
        if not text or text.lower().startswith("read more"):
            continue
        return text
    return None


def _extract_listing_date_text(raw_text: str, *, title: str) -> str | None:
    remainder = raw_text
    if remainder.startswith(title):
        remainder = remainder[len(title) :].strip()
    if remainder.lower().startswith("ongoing"):
        return "Ongoing"

    patterns = (
        r"^(?:[A-Za-z]+,\s+)?[A-Za-z]+\s+\d{1,2},\s+\d{4}",
        r"^(?:[A-Za-z]+,\s+)?[A-Za-z]+\s+\d{1,2}",
    )
    for pattern in patterns:
        match = re.search(pattern, remainder)
        if match is not None:
            return match.group(0)
    return None


def _extract_listing_summary(raw_text: str, *, title: str, date_text: str | None) -> str | None:
    summary = raw_text
    if summary.startswith(title):
        summary = summary[len(title) :].strip()
    if date_text and summary.startswith(date_text):
        summary = summary[len(date_text) :].strip()
    summary = summary.removesuffix("Read More ›").strip()
    return summary or None


def _parse_detail_page(
    html: str,
    *,
    source_url: str,
    listing: dict | None,
    current_date: date,
) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    article = soup.find("article")
    if article is None:
        return None

    title_node = article.select_one("h1.entry_title")
    title = _normalize_space(title_node.get_text(" ", strip=True) if title_node else "")
    if not title:
        title = listing.get("title") if listing else ""
    if not title:
        return None

    primary_wrapper = article.select_one(".wpb_text_column .wpb_wrapper")
    if primary_wrapper is None:
        primary_wrapper = article

    metadata = _extract_metadata(primary_wrapper)
    reference_date = listing.get("start_date") if listing else None
    event_date = _parse_date_text(
        metadata.get("date_text") or (listing.get("date_text") if listing else None),
        current_date=current_date,
        reference_date=reference_date,
    )
    if event_date is None:
        return None

    start_at, end_at, location_text = _parse_time_and_location(
        event_date,
        metadata.get("time_location_text"),
    )
    if start_at is None:
        start_at = datetime.combine(event_date, time(0, 0))

    description = _build_description(primary_wrapper, listing_summary=listing.get("summary") if listing else None)
    registration_link = article.select_one('a.qbutton[href], a[href*="blackbaudhosting.com"]')
    registration_url = None
    registration_required = False
    if registration_link is not None:
        registration_url = urljoin(source_url, registration_link.get("href", "").strip())
        button_text = _normalize_space(registration_link.get_text(" ", strip=True)).lower()
        registration_required = "register" in button_text or "ticket" in button_text
    elif description:
        lowered = description.lower()
        registration_required = "register here" in lowered or "register today" in lowered

    return {
        "title": title,
        "description": description,
        "location_text": location_text,
        "price_text": metadata.get("price_text"),
        "registration_required": registration_required,
        "registration_url": registration_url,
        "start_at": start_at,
        "end_at": end_at,
    }


def _extract_metadata(wrapper) -> dict[str, str | None]:
    first_paragraph = wrapper.find("p")
    if first_paragraph is None:
        return {"date_text": None, "time_location_text": None, "price_text": None}

    strong_lines = [
        _normalize_space(node.get_text(" ", strip=True))
        for node in first_paragraph.find_all("strong")
        if _normalize_space(node.get_text(" ", strip=True))
    ]
    if strong_lines:
        return {
            "date_text": strong_lines[0] if len(strong_lines) >= 1 else None,
            "time_location_text": strong_lines[1] if len(strong_lines) >= 2 else None,
            "price_text": strong_lines[2] if len(strong_lines) >= 3 else None,
        }

    lines = [line.strip() for line in first_paragraph.get_text("\n").splitlines() if line.strip()]
    return {
        "date_text": lines[0] if len(lines) >= 1 else None,
        "time_location_text": lines[1] if len(lines) >= 2 else None,
        "price_text": lines[2] if len(lines) >= 3 else None,
    }


def _build_description(wrapper, *, listing_summary: str | None) -> str | None:
    lines: list[str] = []
    started = False

    for child in wrapper.children:
        name = getattr(child, "name", None)
        if name is None:
            continue

        if name == "p":
            text = _normalize_space(child.get_text(" ", strip=True))
            if not text:
                continue
            if not started:
                started = True
                continue
            if _should_stop_description(text):
                break
            if _should_skip_description_line(text):
                continue
            lines.append(text)
            continue

        if name in {"ul", "ol"}:
            for item in child.find_all("li", recursive=False):
                text = _normalize_space(item.get_text(" ", strip=True))
                if text and not _should_stop_description(text):
                    lines.append(text)

    if not lines and listing_summary:
        return listing_summary
    return " ".join(lines) if lines else None


def _should_skip_description_line(text: str) -> bool:
    normalized = text.lower()
    return (
        normalized.startswith("everson members:")
        or normalized.startswith("not a member?")
        or normalized in {"register here", "register today!", "register today"}
    )


def _should_stop_description(text: str) -> bool:
    normalized = text.lower()
    return (
        normalized.startswith("about ")
        or normalized.startswith("401 harrison")
    )


def _parse_time_and_location(
    event_date: date,
    line: str | None,
) -> tuple[datetime | None, datetime | None, str | None]:
    if not line:
        return None, None, None

    time_text = line
    location_text = None
    if "|" in line:
        time_text, location_text = [part.strip() or None for part in line.split("|", 1)]

    range_match = TIME_RANGE_RE.search(time_text)
    if range_match is not None:
        end_meridiem = range_match.group("end_meridiem")
        start_meridiem = range_match.group("start_meridiem") or end_meridiem
        start_clock = _build_clock(
            hour=int(range_match.group("start_hour")),
            minute=int(range_match.group("start_minute") or "0"),
            meridiem=start_meridiem,
        )
        end_clock = _build_clock(
            hour=int(range_match.group("end_hour")),
            minute=int(range_match.group("end_minute") or "0"),
            meridiem=end_meridiem,
        )
        return datetime.combine(event_date, start_clock), datetime.combine(event_date, end_clock), location_text

    single_match = TIME_SINGLE_RE.search(time_text)
    if single_match is not None:
        start_clock = _build_clock(
            hour=int(single_match.group("hour")),
            minute=int(single_match.group("minute") or "0"),
            meridiem=single_match.group("meridiem"),
        )
        return datetime.combine(event_date, start_clock), None, location_text

    return datetime.combine(event_date, time(0, 0)), None, location_text


def _build_clock(*, hour: int, minute: int, meridiem: str) -> time:
    normalized = meridiem.lower().replace(".", "")
    if normalized == "pm" and hour != 12:
        hour += 12
    if normalized == "am" and hour == 12:
        hour = 0
    return time(hour, minute)


def _parse_date_text(
    raw: str | None,
    *,
    current_date: date,
    reference_date: date | None = None,
) -> date | None:
    if not raw:
        return None

    normalized = _normalize_space(raw).replace("  ", " ")
    if normalized.lower() == "ongoing":
        return None

    for fmt in DATE_WITH_YEAR_FORMATS:
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue

    target_year = reference_date.year if reference_date is not None else None
    for fmt in DATE_WITHOUT_YEAR_FORMATS:
        try:
            parsed = datetime.strptime(normalized, fmt)
        except ValueError:
            continue
        year = target_year or _infer_event_year(month=parsed.month, day=parsed.day, current_date=current_date)
        return date(year, parsed.month, parsed.day)

    return None


def _infer_event_year(*, month: int, day: int, current_date: date) -> int:
    candidate = date(current_date.year, month, day)
    if candidate < current_date and (current_date - candidate).days > 30:
        return current_date.year + 1
    return current_date.year


def _should_include_event(*, listing: dict | None, detail: dict) -> bool:
    title = (detail.get("title") or (listing.get("title") if listing else "") or "").lower()
    combined = " ".join(
        filter(
            None,
            [
                detail.get("title"),
                detail.get("description"),
                detail.get("price_text"),
                listing.get("summary") if listing else None,
            ],
        )
    ).lower()

    if any(keyword in title for keyword in TITLE_EXCLUDE_KEYWORDS):
        return False

    has_include = any(keyword in combined for keyword in INCLUDE_KEYWORDS)
    has_strong_include = any(keyword in combined for keyword in STRONG_INCLUDE_KEYWORDS)
    has_soft_exclude = any(keyword in combined for keyword in SOFT_EXCLUDE_KEYWORDS)

    if not has_include:
        return False
    if has_soft_exclude and not has_strong_include:
        return False
    return True


def _infer_activity_type(text: str) -> str:
    if "conversation" in text or "lecture" in text or "talk" in text:
        return "lecture"
    if "class" in text or "workshop" in text or "drawing" in text or "embroidery" in text:
        return "workshop"
    return "activity"


def _parse_age_range(text: str | None) -> tuple[int | None, int | None]:
    if not text:
        return None, None

    range_match = AGE_RANGE_RE.search(text)
    if range_match is not None:
        return int(range_match.group(1)), int(range_match.group(2))

    plus_match = AGE_PLUS_RE.search(text)
    if plus_match is not None:
        return int(plus_match.group(1)), None

    return None, None


def _normalize_space(value: str) -> str:
    return " ".join(value.split())
