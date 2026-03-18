import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

NOGUCHI_EVENTS_URL = "https://www.noguchi.org/museum/calendar/"

NY_TIMEZONE = "America/New_York"
NOGUCHI_VENUE_NAME = "The Noguchi Museum"
NOGUCHI_CITY = "Long Island City"
NOGUCHI_STATE = "NY"
NOGUCHI_DEFAULT_LOCATION = "Long Island City, NY"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": NOGUCHI_EVENTS_URL,
}

EVENT_PATH_TOKEN = "/museum/calendar/event/"
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)

INCLUDE_PATTERNS = (
    " activity ",
    " art for tots ",
    " class ",
    " classes ",
    " conversation ",
    " discussion ",
    " drawing ",
    " family ",
    " families ",
    " hands-on ",
    " hands on ",
    " kids ",
    " lab ",
    " lecture ",
    " talk ",
    " talks ",
    " teen ",
    " teens ",
    " workshop ",
    " workshops ",
)
HARD_EXCLUDE_PATTERNS = (
    " admission ",
    " camp ",
    " camps ",
    " concert ",
    " dinner ",
    " exhibition ",
    " film ",
    " fundraising ",
    " fundraiser ",
    " meditation ",
    " mindfulness ",
    " music ",
    " open house ",
    " performance ",
    " poem ",
    " poetry ",
    " reading ",
    " reception ",
    " storytime ",
    " tai chi ",
    " tour ",
    " tours ",
    " writing ",
    " yoga ",
)


async def fetch_noguchi_page(
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
        raise RuntimeError("Unable to fetch Noguchi calendar page") from last_exception
    raise RuntimeError("Unable to fetch Noguchi calendar page after retries")


async def load_noguchi_payload(url: str = NOGUCHI_EVENTS_URL) -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        list_html = await fetch_noguchi_page(url, client=client)
        event_urls = _extract_event_urls(list_html, list_url=url)

        detail_pages: dict[str, str] = {}
        for event_url, detail_html in zip(
            event_urls,
            await asyncio.gather(*(fetch_noguchi_page(event_url, client=client) for event_url in event_urls)),
        ):
            detail_pages[event_url] = detail_html

    return {
        "list_html": list_html,
        "detail_pages": detail_pages,
    }


def parse_noguchi_payload(payload: dict, *, list_url: str = NOGUCHI_EVENTS_URL) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    detail_pages = payload.get("detail_pages") or {}
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in soup.select("div.grid-flexbox-layout.grid-events"):
        row = _build_row_from_card(card, list_url=list_url, detail_pages=detail_pages)
        if row is None:
            continue
        if row.start_at.date() < today:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class NoguchiEventsAdapter(BaseSourceAdapter):
    source_name = "noguchi_events"

    def __init__(self, url: str = NOGUCHI_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_noguchi_payload(self.url)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_noguchi_payload/parse_noguchi_payload from script runner.")


def _extract_event_urls(html: str, *, list_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    for title_tag in soup.select("div.grid-flexbox-layout.grid-events h3.subheadline a[href]"):
        event_url = urljoin(list_url, title_tag.get("href", "").strip())
        if EVENT_PATH_TOKEN not in event_url or event_url in seen:
            continue
        seen.add(event_url)
        urls.append(event_url)

    return urls


def _build_row_from_card(
    card,
    *,
    list_url: str,
    detail_pages: dict[str, str],
) -> ExtractedActivity | None:
    title_tag = card.select_one("h3.subheadline a[href]")
    add_to_calendar = card.select_one("div.add-to-calendar")
    if title_tag is None or add_to_calendar is None:
        return None

    source_url = urljoin(list_url, title_tag.get("href", "").strip())
    if EVENT_PATH_TOKEN not in source_url:
        return None

    title = _normalize_space(title_tag.get_text(" ", strip=True))
    start_at = _parse_datetime(_extract_card_field(add_to_calendar, "start"))
    if not title or start_at is None:
        return None
    end_at = _parse_datetime(_extract_card_field(add_to_calendar, "end"))

    category_names = [
        _normalize_space(tag.get_text(" ", strip=True))
        for tag in card.select("div.block-quarter.text-gray .eyebrow")
    ]
    category_names = [value for value in category_names if value and value != ","]
    card_description = _normalize_space(
        (card.select_one("div.block-half.body-text") or "").get_text(" ", strip=True)
        if card.select_one("div.block-half.body-text")
        else ""
    )
    location_text = _normalize_space(_extract_card_field(add_to_calendar, "location")) or NOGUCHI_DEFAULT_LOCATION

    detail_html = detail_pages.get(source_url)
    detail_description = _extract_detail_description(detail_html) if detail_html else ""
    price_text = _extract_detail_price_text(detail_html) if detail_html else ""
    register_url = _extract_register_url(card, detail_html, source_url=source_url)

    description_parts = [part for part in [detail_description or card_description, card_description] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = _join_unique(description_parts)

    text_blob = " ".join(
        [
            title,
            description or "",
            " ".join(category_names),
            price_text,
        ]
    ).lower()
    token_blob = _searchable_blob(text_blob)
    if any(pattern in token_blob for pattern in HARD_EXCLUDE_PATTERNS):
        return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    age_min, age_max = _parse_age_range(title=title, description=description)
    is_free, free_status = infer_price_classification(price_text or description)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=NOGUCHI_VENUE_NAME,
        location_text=location_text,
        city=NOGUCHI_CITY,
        state=NOGUCHI_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=(" drop-in " in token_blob or " drop in " in token_blob),
        registration_required=bool(register_url or " registration " in token_blob or " register " in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_card_field(container, class_name: str) -> str:
    tag = container.select_one(f".{class_name}")
    if tag is None:
        return ""
    return _normalize_space(tag.get_text(" ", strip=True))


def _extract_detail_description(detail_html: str) -> str:
    soup = BeautifulSoup(detail_html, "html.parser")
    body = soup.select_one("main.calendar.calendar_detail div.block-half.body-text.wysiwyg")
    if body is None:
        return ""
    paragraphs = [_normalize_space(p.get_text(" ", strip=True)) for p in body.select("p")]
    paragraphs = [text for text in paragraphs if text]
    if paragraphs:
        return " ".join(paragraphs)
    return _normalize_space(body.get_text(" ", strip=True))


def _extract_detail_price_text(detail_html: str) -> str:
    soup = BeautifulSoup(detail_html, "html.parser")
    main = soup.select_one("main.calendar.calendar_detail")
    if main is None:
        return ""
    for node in main.select("div.subheadline-s"):
        text = _normalize_space(node.get_text(" ", strip=True))
        lower = text.lower()
        if "$" in text or "free" in lower or "member" in lower or "non-member" in lower:
            return text
    return ""


def _extract_register_url(card, detail_html: str | None, *, source_url: str) -> str | None:
    button = card.select_one("a.button[href]")
    href = urljoin(source_url, button.get("href", "").strip()) if button is not None else ""
    if href:
        return href

    if not detail_html:
        return None
    soup = BeautifulSoup(detail_html, "html.parser")
    detail_button = soup.select_one("a.button[href]")
    if detail_button is None:
        return None
    href = urljoin(source_url, detail_button.get("href", "").strip())
    return href or None


def _infer_activity_type(token_blob: str) -> str:
    if (
        " lecture " in token_blob
        or " talk " in token_blob
        or " talks " in token_blob
        or " conversation " in token_blob
        or " discussion " in token_blob
    ):
        return "lecture"
    return "workshop"


def _parse_age_range(*, title: str, description: str | None) -> tuple[int | None, int | None]:
    text = " ".join(part for part in [title, description or ""] if part)
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    for fmt in ("%m/%d/%Y %I:%M %p", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(value, fmt)
            if fmt == "%m/%d/%Y":
                return datetime.combine(parsed.date(), datetime.min.time())
            return parsed
        except ValueError:
            continue
    return None


def _join_unique(parts: list[str]) -> str | None:
    seen: set[str] = set()
    ordered: list[str] = []
    for part in parts:
        text = _normalize_space(part)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    if not ordered:
        return None
    return " | ".join(ordered)


def _searchable_blob(text: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", text.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())
