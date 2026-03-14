import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.types import ExtractedActivity

BRUCE_EVENTS_URL = "https://brucemuseum.org/events/"

NY_TIMEZONE = "America/New_York"
BRUCE_VENUE_NAME = "Bruce Museum"
BRUCE_CITY = "Greenwich"
BRUCE_STATE = "CT"
BRUCE_DEFAULT_LOCATION = "Greenwich, CT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BRUCE_EVENTS_URL,
}

TITLE_YOUTH_RE = re.compile(
    r"\b(bruce beginnings|junior docent|junior|museum movers|science solvers|art adventures|family|kids?|teen|birthday bash)\b",
    re.IGNORECASE,
)
DESCRIPTION_YOUTH_RE = re.compile(
    r"\b(children ages|children and their caregivers|teens and their friends|young people|families are invited|for teens|for children|for families)\b",
    re.IGNORECASE,
)
EXCLUDED_KEYWORDS = (
    "tour",
    "tours",
    "yoga",
    "bird walk",
    "cinema",
    "concert",
    "benefit",
    "trivia",
    "coffee",
    "exhibition highlights",
    "lunch and learn",
)
DATE_LINE_RE = re.compile(
    r"^[A-Za-z]+,\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}),\s+(.+)$",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>am|pm)",
    re.IGNORECASE,
)
AGE_YEARS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
PAGE_PARAM_RE = re.compile(r"[?&]page=(\d+)")


async def fetch_bruce_page(
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
        raise RuntimeError("Unable to fetch Bruce Museum events page") from last_exception
    raise RuntimeError("Unable to fetch Bruce Museum events page after retries")


def build_bruce_events_url(*, page_number: int) -> str:
    if page_number <= 1:
        return BRUCE_EVENTS_URL
    return f"{BRUCE_EVENTS_URL}?page={page_number}"


async def load_bruce_events_payload(*, page_limit: int | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        first_html = await fetch_bruce_page(BRUCE_EVENTS_URL, client=client)
        page_total = _parse_page_total(first_html)
        if page_limit is not None:
            page_total = min(page_total, max(page_limit, 1))

        page_payloads: list[tuple[str, str]] = [(BRUCE_EVENTS_URL, first_html)]
        additional_urls = [build_bruce_events_url(page_number=page_number) for page_number in range(2, page_total + 1)]
        if additional_urls:
            fetched = await asyncio.gather(*(fetch_bruce_page(url, client=client) for url in additional_urls))
            page_payloads.extend(zip(additional_urls, fetched, strict=True))

        listings: list[dict] = []
        for list_url, html in page_payloads:
            listings.extend(_parse_listing_cards(html, list_url=list_url))

        detail_urls = sorted({item["source_url"] for item in listings})
        detail_pages = await asyncio.gather(*(fetch_bruce_page(url, client=client) for url in detail_urls))

    return {
        "listings": listings,
        "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
    }


def parse_bruce_events_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for listing in payload["listings"]:
        html = payload["detail_pages"].get(listing["source_url"])
        if html is None:
            continue

        detail = _parse_detail_page(html, source_url=listing["source_url"])
        if detail is None:
            continue

        if not _should_include_event(title=detail["title"], description=detail["description"]):
            continue

        start_at, end_at = _parse_detail_datetime(detail["schedule_text"])
        if start_at is None:
            continue

        text_blob = " ".join([detail["title"], detail["description"] or "", detail["banner"] or ""]).lower()
        age_min, age_max = _parse_age_range(detail["description"])
        key = (listing["source_url"], detail["title"], start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=listing["source_url"],
                title=detail["title"],
                description=detail["description"],
                venue_name=BRUCE_VENUE_NAME,
                location_text=BRUCE_DEFAULT_LOCATION,
                city=BRUCE_CITY,
                state=BRUCE_STATE,
                activity_type=_infer_activity_type(detail["title"], detail["description"]),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob or "first come, first served" in text_blob),
                registration_required=(
                    ("registration" in text_blob or "register" in text_blob)
                    and "first come, first served" not in text_blob
                    and "no registration" not in text_blob
                ),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
            )
        )

    return rows


class BruceEventsAdapter(BaseSourceAdapter):
    source_name = "bruce_events"

    async def fetch(self) -> list[str]:
        html = await fetch_bruce_page(BRUCE_EVENTS_URL)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        parsed_payload = {
            "listings": _parse_listing_cards(payload, list_url=BRUCE_EVENTS_URL),
            "detail_pages": {},
        }
        return parse_bruce_events_payload(parsed_payload)


def _parse_page_total(html: str) -> int:
    matches = [int(match) for match in PAGE_PARAM_RE.findall(html)]
    if not matches:
        return 1
    return max(max(matches), 1)


def _parse_listing_cards(html: str, *, list_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict] = []
    for anchor in soup.select("a.card-overview[href]"):
        href = anchor.get("href", "").strip()
        title_node = anchor.select_one("h6.card-overview__title")
        title = _normalize_space(title_node.get_text(" ", strip=True) if title_node else "")
        if not href or not title:
            continue

        cards.append(
            {
                "title": title,
                "source_url": urljoin(list_url, href),
            }
        )

    return cards


def _parse_detail_page(html: str, *, source_url: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    title_node = soup.select_one("h2.event-header__title")
    schedule_node = soup.select_one("p.event-header__post-title")
    intro = soup.select_one("div.event-header__introduction")
    if title_node is None or schedule_node is None or intro is None:
        return None

    description_lines = [_normalize_space(node.get_text(" ", strip=True)) for node in intro.select("p")]
    description_lines = [line for line in description_lines if line]
    banner_node = soup.select_one("div.event-header__banner-title")
    banner = _normalize_space(banner_node.get_text(" ", strip=True) if banner_node else "")

    return {
        "source_url": source_url,
        "title": _normalize_space(title_node.get_text(" ", strip=True)),
        "schedule_text": _normalize_space(schedule_node.get_text(" ", strip=True)),
        "description": " | ".join(description_lines) if description_lines else None,
        "banner": banner or None,
    }


def _should_include_event(*, title: str, description: str | None) -> bool:
    normalized_title = title.lower()
    normalized_description = (description or "").lower()
    text_blob = " ".join([normalized_title, normalized_description])
    if any(keyword in text_blob for keyword in EXCLUDED_KEYWORDS):
        return False
    if TITLE_YOUTH_RE.search(normalized_title):
        return True
    return DESCRIPTION_YOUTH_RE.search(normalized_description) is not None


def _parse_detail_datetime(value: str) -> tuple[datetime | None, datetime | None]:
    match = DATE_LINE_RE.match(value)
    if match is None:
        return None, None

    date_part = match.group(1)
    time_part = match.group(2)
    try:
        date_value = datetime.strptime(date_part, "%B %d, %Y")
    except ValueError:
        return None, None

    time_range = TIME_RANGE_RE.search(time_part)
    if time_range is not None:
        start_meridiem = time_range.group("start_meridiem") or time_range.group("end_meridiem")
        start_at = date_value.replace(
            hour=_to_24_hour(
                int(time_range.group("start_hour")),
                start_meridiem,
            ),
            minute=int(time_range.group("start_minute") or 0),
        )
        end_at = date_value.replace(
            hour=_to_24_hour(
                int(time_range.group("end_hour")),
                time_range.group("end_meridiem"),
            ),
            minute=int(time_range.group("end_minute") or 0),
        )
        return start_at, end_at

    single = TIME_SINGLE_RE.search(time_part)
    if single is not None:
        start_at = date_value.replace(
            hour=_to_24_hour(int(single.group("hour")), single.group("meridiem")),
            minute=int(single.group("minute") or 0),
        )
        return start_at, None

    return date_value, None


def _to_24_hour(hour: int, meridiem: str | None) -> int:
    if meridiem is None:
        return hour
    normalized = meridiem.lower()
    if normalized == "am":
        return 0 if hour == 12 else hour
    return 12 if hour == 12 else hour + 12


def _parse_age_range(description: str | None) -> tuple[int | None, int | None]:
    if not description:
        return None, None

    if "month" in description.lower():
        return None, None

    match = AGE_YEARS_RE.search(description)
    if match is not None:
        first = int(match.group(1))
        second = int(match.group(2))
        return min(first, second), max(first, second)

    match = AGE_PLUS_RE.search(description)
    if match is not None:
        return int(match.group(1)), None

    return None, None


def _infer_activity_type(title: str, description: str | None) -> str:
    text_blob = " ".join([title, description or ""]).lower()
    if "science" in text_blob:
        return "activity"
    if "workshop" in text_blob:
        return "workshop"
    return "activity"


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()
