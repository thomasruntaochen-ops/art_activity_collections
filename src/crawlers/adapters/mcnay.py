import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

MCNAY_EVENTS_URL = "https://www.mcnayart.org/events/"
MCNAY_TIMEZONE = "America/Chicago"
MCNAY_VENUE_NAME = "McNay Art Museum"
MCNAY_CITY = "San Antonio"
MCNAY_STATE = "TX"
MCNAY_LOCATION = "6000 N New Braunfels Ave, San Antonio, TX"

MCNAY_STRONG_INCLUDE_KEYWORDS = (
    "art activit",
    "creative activit",
    "art activity",
    "storytime",
    "children’s storybook",
    "children's storybook",
    "school-aged",
)
MCNAY_INCLUDE_KEYWORDS = (
    "workshop",
    "class",
    "activity",
    "activities",
    "studio",
    "family",
    "children",
    "child",
    "teen",
)
MCNAY_EXCLUDE_KEYWORDS = (
    "members only",
    "member preview",
    "poetry",
    "meditation",
    "mindfulness",
    "lecture",
    "concert",
    "film",
    "performance",
)
DATE_RE = re.compile(
    r"(?P<weekday>[A-Za-z]+),\s+(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+(?P<year>\d{4})"
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?|am|pm))\s*[–-]\s*(?P<end>\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<value>\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
WHITESPACE_RE = re.compile(r"\s+")
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": MCNAY_EVENTS_URL,
}


async def fetch_mcnay_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[mcnay-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[mcnay-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[mcnay-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[mcnay-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[mcnay-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[mcnay-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[mcnay-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch McNay events page") from last_exception
    raise RuntimeError("Unable to fetch McNay events page after retries")


async def load_mcnay_payload(base_url: str = MCNAY_EVENTS_URL) -> dict[str, object]:
    listing_html = await fetch_mcnay_page(base_url)
    cards = _iter_mcnay_cards(listing_html, list_url=base_url)
    detail_urls: list[str] = []
    seen: set[str] = set()
    for card in cards:
        source_url = card["source_url"]
        if not isinstance(source_url, str) or source_url in seen:
            continue
        seen.add(source_url)
        detail_urls.append(source_url)

    detail_pages_raw = await asyncio.gather(*(fetch_mcnay_page(url) for url in detail_urls))
    detail_pages = dict(zip(detail_urls, detail_pages_raw))
    return {
        "listing_html": listing_html,
        "detail_pages": detail_pages,
    }


class McnayEventsAdapter(BaseSourceAdapter):
    source_name = "mcnay_events"

    def __init__(self, url: str = MCNAY_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_mcnay_payload(self.url)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_mcnay_payload(data)


def parse_mcnay_payload(payload: dict[str, object]) -> list[ExtractedActivity]:
    listing_html = payload.get("listing_html")
    detail_pages = payload.get("detail_pages") or {}
    if not isinstance(listing_html, str) or not isinstance(detail_pages, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in _iter_mcnay_cards(listing_html, list_url=MCNAY_EVENTS_URL):
        source_url = card["source_url"]
        detail_html = detail_pages.get(source_url)
        detail = _parse_mcnay_detail_page(detail_html if isinstance(detail_html, str) else None)

        title = str(detail["title"] or card["title"]).strip()
        description = _combine_description(
            card_description=card["description"],
            detail_description=detail["description"],
        )
        text_blob = " ".join(
            [
                title,
                description or "",
                card["category_class"],
                detail["price_text"] or "",
            ]
        )
        if not _should_keep_mcnay_event(title=title, text_blob=text_blob):
            continue

        start_at = detail["start_at"] or _parse_card_start_at(card["date_text"], card["time_text"])
        if start_at is None:
            continue
        end_at = detail["end_at"] or _parse_card_end_at(card["date_text"], card["time_text"])
        is_free, free_status = infer_price_classification(
            " | ".join(part for part in [detail["price_text"], description] if part)
        )

        item = ExtractedActivity(
            source_url=source_url,
            title=title,
            description=description,
            venue_name=MCNAY_VENUE_NAME,
            location_text=MCNAY_LOCATION,
            city=MCNAY_CITY,
            state=MCNAY_STATE,
            activity_type=_infer_mcnay_activity_type(title=title, description=description),
            age_min=None,
            age_max=None,
            drop_in=("drop-in" in text_blob.lower() or "drop in" in text_blob.lower()),
            registration_required=(
                detail["registration_required"]
                or "registration is required" in text_blob.lower()
                or "register" in text_blob.lower()
            ),
            start_at=start_at,
            end_at=end_at,
            timezone=MCNAY_TIMEZONE,
            is_free=is_free,
            free_verification_status=free_status,
        )
        key = (item.source_url, item.title, item.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(item)

    return rows


def _iter_mcnay_cards(html: str, *, list_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict[str, str]] = []
    for node in soup.select(".secondary-events-single"):
        title_node = node.select_one("a.title")
        if title_node is None:
            continue
        title = _normalize_space(title_node.get_text(" ", strip=True))
        href = (title_node.get("href") or "").strip()
        if not title or not href:
            continue

        date_node = node.select_one(".event-date")
        time_node = node.select_one(".event-time")
        description_node = node.select_one(".secondary-events-description-copy")
        cards.append(
            {
                "source_url": urljoin(list_url, href),
                "title": title,
                "date_text": _normalize_space(date_node.get_text(" ", strip=True) if date_node else ""),
                "time_text": _normalize_space(time_node.get_text(" ", strip=True) if time_node else ""),
                "description": _normalize_text_block(
                    description_node.get_text("\n", strip=True) if description_node else ""
                ),
                "category_class": " ".join(sorted(node.get("class") or [])),
            }
        )

    return cards


def _parse_mcnay_detail_page(html: str | None) -> dict[str, object]:
    if not html:
        return {
            "title": None,
            "description": None,
            "price_text": None,
            "start_at": None,
            "end_at": None,
            "registration_required": False,
        }

    soup = BeautifulSoup(html, "html.parser")
    title_node = soup.select_one(".single-event-title-section .heading-blue")
    info_paragraphs = [
        _normalize_space(node.get_text(" ", strip=True))
        for node in soup.select(".single-event-title-info p")
        if _normalize_space(node.get_text(" ", strip=True))
    ]

    date_text = info_paragraphs[0] if len(info_paragraphs) >= 1 else ""
    time_text = info_paragraphs[1] if len(info_paragraphs) >= 2 else ""
    price_text = None
    for value in info_paragraphs[2:]:
        if "free" in value.lower() or "$" in value or "admission" in value.lower():
            price_text = value
            break

    description_parts = [
        _normalize_text_block(node.get_text("\n", strip=True))
        for node in soup.select(".single-event-paragraphs p")
        if _normalize_text_block(node.get_text("\n", strip=True))
    ]
    description = " | ".join(description_parts) if description_parts else None

    start_at = _parse_card_start_at(date_text, time_text)
    end_at = _parse_card_end_at(date_text, time_text)
    registration_required = bool(
        soup.select_one("a.blue-button[href*='blackbaud']")
        or (description and "registration is required" in description.lower())
    )
    return {
        "title": _normalize_space(title_node.get_text(" ", strip=True) if title_node else ""),
        "description": description,
        "price_text": price_text,
        "start_at": start_at,
        "end_at": end_at,
        "registration_required": registration_required,
    }


def _parse_card_start_at(date_text: str, time_text: str) -> datetime | None:
    base_date = _parse_date(date_text)
    if base_date is None:
        return None

    match = TIME_RANGE_RE.search(time_text)
    if match:
        return _combine_date_and_time(base_date, match.group("start"))

    match = TIME_SINGLE_RE.search(time_text)
    if match:
        return _combine_date_and_time(base_date, match.group("value"))

    return datetime(base_date.year, base_date.month, base_date.day)


def _parse_card_end_at(date_text: str, time_text: str) -> datetime | None:
    base_date = _parse_date(date_text)
    if base_date is None:
        return None

    match = TIME_RANGE_RE.search(time_text)
    if match:
        return _combine_date_and_time(base_date, match.group("end"))

    return None


def _parse_date(value: str) -> datetime | None:
    match = DATE_RE.search(value)
    if not match:
        return None
    try:
        return datetime.strptime(
            f"{match.group('month')} {match.group('day')} {match.group('year')}",
            "%B %d %Y",
        )
    except ValueError:
        return None


def _combine_date_and_time(base_date: datetime, value: str) -> datetime | None:
    normalized = value.replace(".", "").upper().strip()
    for fmt in ("%I:%M %p",):
        try:
            parsed_time = datetime.strptime(normalized, fmt)
            return datetime(
                base_date.year,
                base_date.month,
                base_date.day,
                parsed_time.hour,
                parsed_time.minute,
            )
        except ValueError:
            continue
    return None


def _should_keep_mcnay_event(*, title: str, text_blob: str) -> bool:
    normalized = text_blob.lower()
    strong_include = any(keyword in normalized for keyword in MCNAY_STRONG_INCLUDE_KEYWORDS)
    has_include = strong_include or any(keyword in normalized for keyword in MCNAY_INCLUDE_KEYWORDS)
    has_exclude = any(keyword in normalized for keyword in MCNAY_EXCLUDE_KEYWORDS)

    if title.lower().startswith("members only"):
        return False
    if "preview" in normalized and not strong_include:
        return False
    if has_exclude and not strong_include:
        return False
    return has_include


def _infer_mcnay_activity_type(*, title: str, description: str | None) -> str:
    text = " ".join([title, description or ""]).lower()
    if "workshop" in text:
        return "workshop"
    if "class" in text:
        return "class"
    return "activity"


def _combine_description(*, card_description: str, detail_description: str | None) -> str | None:
    parts: list[str] = []
    if card_description:
        parts.append(card_description)
    if detail_description:
        for chunk in detail_description.split(" | "):
            if chunk and chunk not in parts:
                parts.append(chunk)
    return " | ".join(parts) if parts else None


def _normalize_space(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def _normalize_text_block(value: str) -> str:
    lines = [_normalize_space(line) for line in value.splitlines()]
    return " ".join(line for line in lines if line).strip()
