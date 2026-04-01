import asyncio
import json
import re
from datetime import datetime
from decimal import Decimal

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

FRIST_EVENTS_URL = "https://fristartmuseum.org/events/"

NY_TIMEZONE = "America/New_York"
MAX_FUTURE_DAYS = 365
FRIST_VENUE_NAME = "Frist Art Museum"
FRIST_CITY = "Nashville"
FRIST_STATE = "TN"
FRIST_DEFAULT_LOCATION = "Frist Art Museum, Nashville, TN"

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
    " artlab ",
    " art-making ",
    " art making ",
    " class ",
    " classes ",
    " conversation ",
    " digital art ",
    " discussion ",
    " discussions ",
    " drawing ",
    " family ",
    " gallery talk ",
    " hands-on ",
    " homeschool ",
    " lecture ",
    " lectures ",
    " multisensory ",
    " perspective ",
    " steam tour ",
    " symposium ",
    " teen ",
    " teens ",
    " workshop ",
    " workshops ",
)
HARD_EXCLUDE_PATTERNS = (
    " architecture tour ",
    " member morning ",
    " music in the cafe ",
    " music in the café ",
)
REJECT_TAGS = {
    "member event",
    "music",
    "tours",
}
MULTI_SPACE_RE = re.compile(r"\s+")
FWP_JSON_RE = re.compile(r"window\.FWP_JSON\s*=\s*(\{.*?\});", flags=re.DOTALL)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)(?!\w)", re.IGNORECASE)


async def fetch_frist_page(
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
        raise RuntimeError("Unable to fetch Frist Art Museum page") from last_exception
    raise RuntimeError("Unable to fetch Frist Art Museum page after retries")


async def load_frist_payload(*, page_limit: int | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        first_html = await fetch_frist_page(FRIST_EVENTS_URL, client=client)
        first_page_cards = _parse_listing_cards(first_html)
        total_pages = _extract_total_pages(first_html)
        if page_limit is not None:
            total_pages = min(total_pages, max(page_limit, 1))

        events = list(first_page_cards)
        for page in range(2, total_pages + 1):
            page_html = await fetch_frist_page(_build_page_url(page), client=client)
            events.extend(_parse_listing_cards(page_html))

        detail_cache: dict[str, str] = {}
        for event in events:
            source_url = event.get("source_url")
            if not source_url or source_url in detail_cache:
                continue
            detail_cache[source_url] = await fetch_frist_page(source_url, client=client)

    for event in events:
        source_url = event.get("source_url")
        event["detail_html"] = detail_cache.get(source_url)

    return {"events": events}


def parse_frist_payload(payload: dict) -> list[ExtractedActivity]:
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


class FristEventsAdapter(BaseSourceAdapter):
    source_name = "frist_events"

    def __init__(self, url: str = FRIST_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_frist_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_frist_payload/parse_frist_payload from script runner.")


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("source_url"))
    if not title or not source_url:
        return None

    tags = [_normalize_space(value) for value in (event_obj.get("tags") or [])]
    tags = [value for value in tags if value]
    if any(tag.lower() in REJECT_TAGS for tag in tags):
        return None

    detail_html = event_obj.get("detail_html")
    detail_soup = BeautifulSoup(detail_html, "html.parser") if isinstance(detail_html, str) else None
    detail_title = _normalize_space(detail_soup.select_one("h1").get_text(" ", strip=True)) if detail_soup and detail_soup.select_one("h1") else None
    title = detail_title or title

    date_text = _normalize_space(_text_or_none(detail_soup.select_one(".event-date")) if detail_soup else None) or _normalize_space(event_obj.get("date_text"))
    time_text = _normalize_space(_text_or_none(detail_soup.select_one(".event-time")) if detail_soup else None) or _normalize_space(event_obj.get("time_text"))
    start_at, end_at = _parse_event_datetimes(date_text, time_text)
    if start_at is None:
        return None

    meta_location_text, price_text = _extract_location_and_price(detail_soup)
    content_text = _extract_detail_content_text(detail_soup)
    summary = _normalize_space(event_obj.get("summary"))
    register_text = _normalize_space(event_obj.get("register_text"))
    description = _join_non_empty(
        [
            summary,
            content_text,
            f"Tags: {', '.join(tags)}" if tags else None,
            f"Price: {price_text}" if price_text else None,
        ]
    )
    signal_text = _normalize_signal_text(" ".join(part for part in [price_text, description] if part))

    include_blob = " ".join([title, description or "", price_text or "", " ".join(tags)]).lower()
    token_blob = f" {' '.join(include_blob.split())} "
    if any(pattern in token_blob for pattern in HARD_EXCLUDE_PATTERNS):
        return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    amount = _extract_amount(price_text or description)
    is_free, free_status = (
        infer_price_classification_from_amount(amount, text=signal_text)
        if amount is not None
        else infer_price_classification(signal_text)
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=FRIST_VENUE_NAME,
        location_text=meta_location_text or FRIST_DEFAULT_LOCATION,
        city=FRIST_CITY,
        state=FRIST_STATE,
        activity_type=_infer_activity_type(token_blob, tags),
        age_min=_extract_age_min(token_blob),
        age_max=_extract_age_max(token_blob),
        drop_in=(" drop-in " in token_blob or " drop in " in token_blob),
        registration_required=_requires_registration(signal_text or token_blob, register_text),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _build_page_url(page: int) -> str:
    if page <= 1:
        return FRIST_EVENTS_URL
    return f"{FRIST_EVENTS_URL}?fwp_paged={page}"


def _parse_listing_cards(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict] = []
    for card in soup.select(".teaser-card.teaser-card--event"):
        link = card.select_one(".teaser-card__title a[href]")
        if link is None:
            continue
        tags = [_normalize_space(node.get_text(" ", strip=True)) for node in card.select(".teaser-card__tags a")]
        tags = [value for value in tags if value]
        cards.append(
            {
                "title": _normalize_space(link.get_text(" ", strip=True)),
                "source_url": _normalize_space(link.get("href")),
                "date_text": _normalize_space(_text_or_none(card.select_one(".event-date"))),
                "time_text": _normalize_space(_text_or_none(card.select_one(".event-time"))),
                "summary": _normalize_space(_text_or_none(card.select_one(".teaser-card__text"))),
                "tags": tags,
                "register_text": _normalize_space(_text_or_none(card.select_one(".teaser-card__register"))),
            }
        )
    return cards


def _extract_total_pages(html: str) -> int:
    match = FWP_JSON_RE.search(html)
    if not match:
        return 1
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return 1
    total_pages = payload.get("preload_data", {}).get("settings", {}).get("pager", {}).get("total_pages")
    if isinstance(total_pages, int) and total_pages >= 1:
        return total_pages
    return 1


def _extract_location_and_price(detail_soup: BeautifulSoup | None) -> tuple[str | None, str | None]:
    if detail_soup is None:
        return None, None
    meta_location = detail_soup.select_one(".events-card__location")
    if meta_location is None:
        return None, None

    lines = [_normalize_space(line) for line in meta_location.get_text("\n", strip=True).splitlines()]
    lines = [line for line in lines if line]
    if not lines:
        return None, None

    if len(lines) == 1 and _looks_like_price_text(lines[0]):
        return None, lines[0]
    location = lines[0]
    price = " ".join(lines[1:]) if len(lines) > 1 else None
    if location and location.startswith("$"):
        return None, " ".join(lines)
    return location, price


def _extract_detail_content_text(detail_soup: BeautifulSoup | None) -> str | None:
    if detail_soup is None:
        return None
    container = detail_soup.select_one("main .l-content .l-constrain--xtra-small")
    if container is None:
        return None
    return _normalize_space(container.get_text(" ", strip=True))


def _parse_event_datetimes(date_text: str | None, time_text: str | None) -> tuple[datetime | None, datetime | None]:
    if not date_text:
        return None, None
    try:
        day = datetime.strptime(date_text, "%A, %B %d, %Y").date()
    except ValueError:
        return None, None

    if not time_text:
        return datetime(day.year, day.month, day.day), None

    normalized_time = (
        time_text.replace("\u2013", "-")
        .replace("a.m.", "AM")
        .replace("p.m.", "PM")
        .replace("a.m", "AM")
        .replace("p.m", "PM")
    )
    parts = [part.strip() for part in normalized_time.split("-") if part.strip()]
    if not parts:
        return datetime(day.year, day.month, day.day), None
    if len(parts) > 1:
        meridiem_match = re.search(r"\b(AM|PM)\b", parts[-1], flags=re.IGNORECASE)
        if meridiem_match:
            meridiem = meridiem_match.group(1).upper()
            parts = [_append_meridiem_if_missing(part, meridiem) for part in parts]

    start_at = _combine_date_and_time(day, parts[0])
    end_at = _combine_date_and_time(day, parts[1]) if len(parts) > 1 else None
    return start_at, end_at


def _combine_date_and_time(day: datetime.date, time_text: str) -> datetime | None:
    try:
        parsed_time = datetime.strptime(time_text, "%I:%M %p").time()
    except ValueError:
        return None
    return datetime.combine(day, parsed_time)


def _append_meridiem_if_missing(time_text: str, meridiem: str) -> str:
    if re.search(r"\b(?:AM|PM)\b", time_text, flags=re.IGNORECASE):
        return time_text
    return f"{time_text} {meridiem}"


def _infer_activity_type(text_blob: str, tags: list[str]) -> str:
    tag_blob = " ".join(tags).lower()
    if any(keyword in text_blob for keyword in (" workshop ", " artlab ", " art-making ", " drawing ", " homeschool ", " family ")):
        return "workshop"
    if "workshops" in tag_blob:
        return "workshop"
    if any(keyword in text_blob for keyword in ("lecture", "gallery talk", "conversation", "symposium", "perspective", "discussion")):
        return "lecture"
    if "lectures + gallery talks" in tag_blob:
        return "lecture"
    return "workshop"


def _requires_registration(token_blob: str, register_text: str | None) -> bool:
    if register_text:
        return True
    normalized = f" {token_blob.lower()} "
    return any(
        keyword in normalized
        for keyword in (" register ", " registration required ", " tickets go on sale ", " sold out ", " wait list ")
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
    return re.sub(r"[;,:()]+", " ", normalized)


def _looks_like_price_text(text: str) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in ("free", "admission", "member", "registration", "$"))
