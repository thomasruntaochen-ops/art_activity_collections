import asyncio
import re
from datetime import date
from datetime import datetime
from decimal import Decimal
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

HUNTER_EVENTS_URL = "https://www.huntermuseum.org/events"

NY_TIMEZONE = "America/New_York"
MAX_FUTURE_DAYS = 365
HUNTER_VENUE_NAME = "Hunter Museum of American Art"
HUNTER_CITY = "Chattanooga"
HUNTER_STATE = "TN"
HUNTER_DEFAULT_LOCATION = "Hunter Museum of American Art, Chattanooga, TN"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

INCLUDE_PATTERNS = (
    " art after dark ",
    " art wise ",
    " art-making ",
    " art making ",
    " artist talk ",
    " artist talks ",
    " collage ",
    " collages ",
    " craft ",
    " crafts ",
    " create ",
    " drawing ",
    " make and move ",
    " printmaking ",
    " studio ",
    " symposium ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
HARD_EXCLUDE_PATTERNS = (
    " artful yoga ",
    " black professionals ",
    " exhibition opening ",
    " fashion show ",
    " performance ",
    " performances ",
    " poetry ",
    " rap ",
    " throwback thursday ",
    " vision + verse ",
    " vision+verse ",
    " yoga ",
)
AGE_RANGE_RE = re.compile(r"\b(?:ages?|teens?)\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\b(\d{1,2})\+\s*(?:only|years old|year old)?\b", re.IGNORECASE)
MULTI_SPACE_RE = re.compile(r"\s+")


async def fetch_hunter_page(
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
        raise RuntimeError("Unable to fetch Hunter Museum page") from last_exception
    raise RuntimeError("Unable to fetch Hunter Museum page after retries")


async def load_hunter_payload(*, page_limit: int | None = None) -> dict:
    events: list[dict] = []
    detail_cache: dict[str, str] = {}
    page_url = HUNTER_EVENTS_URL
    pages_seen = 0

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while page_url:
            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break
            html = await fetch_hunter_page(page_url, client=client)
            page_cards, next_url = _parse_listing_page(html)
            events.extend(page_cards)
            pages_seen += 1
            page_url = next_url

        for event in events:
            source_url = event.get("source_url")
            if not source_url or source_url in detail_cache:
                continue
            detail_cache[source_url] = await fetch_hunter_page(source_url, client=client)

    for event in events:
        event["detail_html"] = detail_cache.get(event.get("source_url"))

    return {"events": events}


def parse_hunter_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        row = _build_row(event_obj)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue
        if (row.start_at.date() - current_date).days > MAX_FUTURE_DAYS:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class HunterEventsAdapter(BaseSourceAdapter):
    source_name = "hunter_events"

    def __init__(self, url: str = HUNTER_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_hunter_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_hunter_payload/parse_hunter_payload from script runner.")


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("source_url"))
    if not title or not source_url:
        return None

    category = _normalize_space(event_obj.get("category"))
    detail_html = event_obj.get("detail_html")
    detail_soup = BeautifulSoup(detail_html, "html.parser") if isinstance(detail_html, str) else None
    description = _extract_description(detail_soup)
    info = _extract_info_map(detail_soup)
    signal_text = _normalize_signal_text(
        " ".join(
            part
            for part in [
                title,
                category,
                description,
                info.get("pricing"),
            ]
            if part
        )
    )
    token_blob = f" {signal_text or ''} "

    if any(pattern in token_blob for pattern in HARD_EXCLUDE_PATTERNS):
        return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    start_at, end_at = _parse_event_datetimes(event_obj.get("date_text"), info.get("time"))
    if start_at is None:
        return None

    price_text = info.get("pricing")
    amount = _extract_amount(price_text)
    is_free, free_status = (
        infer_price_classification_from_amount(amount, text=signal_text)
        if amount is not None
        else infer_price_classification(signal_text)
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=_join_non_empty(
            [
                description,
                f"Category: {category}" if category else None,
                f"Price: {price_text}" if price_text else None,
            ]
        ),
        venue_name=HUNTER_VENUE_NAME,
        location_text=info.get("location") or HUNTER_DEFAULT_LOCATION,
        city=HUNTER_CITY,
        state=HUNTER_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=_extract_age_min(token_blob),
        age_max=_extract_age_max(token_blob),
        drop_in=(" drop-in " in token_blob or " drop in " in token_blob),
        registration_required=_requires_registration(token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _parse_listing_page(html: str) -> tuple[list[dict], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict] = []
    for item in soup.select('[role="listitem"].w-dyn-item'):
        title_node = item.select_one(".event-card_title")
        if title_node is None:
            continue
        href = None
        for node in (
            item.select_one("a.archive_events_card[href]"),
            item.select_one("a.detailed-block_link[href]"),
            item.select_one("a.hide[href]"),
        ):
            if node is not None and node.get("href"):
                href = urljoin(HUNTER_EVENTS_URL, node.get("href"))
                break
        if not href:
            continue
        date_text = _normalize_space(_text_or_none(item.select_one(".event-card_timestamp"))) or _normalize_space(_text_or_none(item.select_one(".event-card_date")))
        cards.append(
            {
                "title": _normalize_space(title_node.get_text(" ", strip=True)),
                "source_url": href,
                "date_text": date_text,
                "category": _normalize_space(_text_or_none(item.select_one(".event-card_category")) or _text_or_none(item.select_one(".event-card_event-type"))),
            }
        )

    next_link = soup.select_one("a.w-pagination-next[href]")
    next_url = urljoin(HUNTER_EVENTS_URL, next_link.get("href")) if next_link and next_link.get("href") else None
    return cards, next_url


def _extract_description(detail_soup: BeautifulSoup | None) -> str | None:
    if detail_soup is None:
        return None
    node = detail_soup.select_one(".text-rich-text.is-event") or detail_soup.select_one(".text-rich-text")
    return _normalize_space(node.get_text(" ", strip=True)) if node is not None else None


def _extract_info_map(detail_soup: BeautifulSoup | None) -> dict[str, str]:
    result: dict[str, str] = {}
    if detail_soup is None:
        return result
    for item in detail_soup.select(".event_info-item"):
        label = _normalize_space(_text_or_none(item.select_one(".event_info-label")))
        value = _normalize_space(_text_or_none(item.select_one(".event_info-value")))
        if not label or not value:
            continue
        result[label.lower()] = value
    return result


def _parse_event_datetimes(date_text: str | None, time_text: str | None) -> tuple[datetime | None, datetime | None]:
    if not date_text:
        return None, None

    date_value = _normalize_hunter_date(date_text)
    if date_value is None:
        return None, None

    if not time_text:
        return datetime.combine(date_value, datetime.min.time()), None

    normalized_time = (
        time_text.replace("\u2013", "-")
        .replace("AM", " AM")
        .replace("PM", " PM")
        .replace("am", " am")
        .replace("pm", " pm")
    )
    parts = [part.strip() for part in normalized_time.split("-") if part.strip()]
    if len(parts) > 1:
        meridiem_match = re.search(r"\b(am|pm)\b", parts[-1], flags=re.IGNORECASE)
        if meridiem_match:
            meridiem = meridiem_match.group(1).upper()
            parts = [_append_meridiem_if_missing(part, meridiem) for part in parts]

    start_at = _combine_date_and_time(date_value, parts[0])
    end_at = _combine_date_and_time(date_value, parts[1]) if len(parts) > 1 else None
    return start_at, end_at


def _normalize_hunter_date(date_text: str) -> date | None:
    normalized = date_text.replace("&", "and").strip()
    if "," in normalized and re.search(r"\d{4}$", normalized):
        for fmt in ("%B %d, %Y", "%A, %B %d, %Y"):
            try:
                return datetime.strptime(normalized, fmt).date()
            except ValueError:
                continue

    if "," in normalized:
        normalized = f"{normalized}, {datetime.now(ZoneInfo(NY_TIMEZONE)).year}"
        for fmt in ("%A, %B %d, %Y", "%B %d, %Y"):
            try:
                return datetime.strptime(normalized, fmt).date()
            except ValueError:
                continue
    return None


def _combine_date_and_time(day: date, time_text: str) -> datetime | None:
    cleaned = _normalize_space(time_text)
    if not cleaned:
        return None
    cleaned = cleaned.replace("AM", " AM").replace("PM", " PM").replace("am", " AM").replace("pm", " PM")
    cleaned = _normalize_space(cleaned)
    if not cleaned:
        return None
    try:
        parsed_time = datetime.strptime(cleaned, "%I:%M %p").time()
    except ValueError:
        return None
    return datetime.combine(day, parsed_time)


def _append_meridiem_if_missing(text: str, meridiem: str) -> str:
    if re.search(r"\b(?:AM|PM)\b", text, flags=re.IGNORECASE):
        return text
    return f"{text} {meridiem}"


def _infer_activity_type(token_blob: str) -> str:
    if any(keyword in token_blob for keyword in (" symposium ", " talk ", " talks ", " discussion ", " speaker ")):
        return "lecture"
    return "workshop"


def _requires_registration(token_blob: str) -> bool:
    normalized = f" {token_blob.lower()} "
    return any(
        keyword in normalized
        for keyword in (" register ", " pre-registration ", " pre registration ", " permission form ", " full ", " sold out ", "[full]")
    )


def _extract_amount(text: str | None) -> Decimal | None:
    if not text:
        return None
    matches = re.findall(r"\$\s*(\d+(?:\.\d{1,2})?)", text.replace(",", ""))
    values: list[Decimal] = []
    for match in matches:
        try:
            values.append(Decimal(match))
        except Exception:
            continue
    if not values:
        return None
    return min(values)


def _extract_age_min(text: str) -> int | None:
    range_match = AGE_RANGE_RE.search(text)
    if range_match:
        return int(range_match.group(1))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1))
    return None


def _extract_age_max(text: str) -> int | None:
    range_match = AGE_RANGE_RE.search(text)
    if range_match:
        return int(range_match.group(2))
    return None


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    if not values:
        return None
    return " | ".join(values)


def _text_or_none(node: object) -> str | None:
    if node is None or not hasattr(node, "get_text"):
        return None
    return node.get_text(" ", strip=True)


def _normalize_space(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    normalized = MULTI_SPACE_RE.sub(" ", text).strip()
    return normalized or None


def _normalize_signal_text(text: str | None) -> str | None:
    normalized = _normalize_space(text)
    if not normalized:
        return None
    return re.sub(r"[.;,:()\[\]]+", " ", normalized).lower()
