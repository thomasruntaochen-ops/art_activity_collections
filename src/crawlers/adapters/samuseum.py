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

SAMA_EVENTS_URL = "https://www.samuseum.org/events/"
SAMA_TIMEZONE = "America/Chicago"
SAMA_VENUE_NAME = "San Antonio Museum of Art"
SAMA_CITY = "San Antonio"
SAMA_STATE = "TX"
SAMA_LOCATION = "San Antonio, TX"

SAMA_INCLUDED_KEYWORDS = (
    "artmaking",
    "art-making",
    "art cart",
    "drawing",
    "hands-on",
    "hands on",
    "mask",
    "storytime",
    "storytelling",
    "activity",
    "activities",
    "family day",
    "second sunday",
)
SAMA_EXCLUDED_KEYWORDS = (
    "lecture",
    "poetry",
    "film",
    "concert",
    "performance",
    "music",
    "tour",
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SAMA_EVENTS_URL,
}
WHITESPACE_RE = re.compile(r"\s+")


async def fetch_sama_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[sama-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[sama-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[sama-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[sama-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[sama-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[sama-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[sama-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch SAMA events page") from last_exception
    raise RuntimeError("Unable to fetch SAMA events page after retries")


async def load_sama_payload(
    base_url: str = SAMA_EVENTS_URL,
    *,
    months: int = 2,
) -> dict[str, object]:
    list_pages: dict[str, str] = {}
    next_url = base_url

    for _ in range(max(months, 1)):
        if next_url in list_pages:
            break
        html = await fetch_sama_page(next_url)
        list_pages[next_url] = html
        next_url = _extract_next_month_url(html, list_url=next_url)
        if not next_url:
            break

    detail_urls: list[str] = []
    seen_urls: set[str] = set()
    for page_url, html in list_pages.items():
        for card in _iter_sama_cards(html, list_url=page_url):
            if not _should_keep_sama_event(
                title=card["title"],
                tags=card["tags"],
                description=card["time_text"],
            ):
                continue
            if card["source_url"] in seen_urls:
                continue
            seen_urls.add(card["source_url"])
            detail_urls.append(card["source_url"])

    detail_pages_raw = await asyncio.gather(*(fetch_sama_page(url) for url in detail_urls))
    detail_pages = dict(zip(detail_urls, detail_pages_raw))
    return {
        "list_pages": list_pages,
        "detail_pages": detail_pages,
    }


class SamuseumEventsAdapter(BaseSourceAdapter):
    source_name = "sama_events"

    def __init__(self, url: str = SAMA_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_sama_payload(self.url)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_sama_payload(data)


def parse_sama_payload(payload: dict[str, object]) -> list[ExtractedActivity]:
    list_pages = payload.get("list_pages") or {}
    detail_pages = payload.get("detail_pages") or {}
    if not isinstance(list_pages, dict) or not isinstance(detail_pages, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for page_url, html in list_pages.items():
        if not isinstance(page_url, str) or not isinstance(html, str):
            continue

        for card in _iter_sama_cards(html, list_url=page_url):
            detail_html = detail_pages.get(card["source_url"])
            detail = _parse_sama_detail_page(detail_html if isinstance(detail_html, str) else None)
            combined_description = _combine_description(
                detail_description=detail["description"],
                tags=card["tags"],
                ticket_price=detail["ticket_price"],
            )

            if not _should_keep_sama_event(
                title=card["title"],
                tags=card["tags"],
                description=combined_description,
            ):
                continue

            if detail["start_at"] is not None:
                start_at = detail["start_at"]
                end_at = detail["end_at"]
            else:
                start_at, end_at = _parse_card_datetimes(
                    date_label=card["date_label"],
                    time_text=card["time_text"],
                    default_year=card["default_year"],
                )
            if start_at is None:
                continue

            description = combined_description
            text_blob = " ".join(
                [
                    card["title"],
                    " ".join(card["tags"]),
                    description or "",
                ]
            ).lower()
            is_free, free_status = infer_price_classification(
                " | ".join(part for part in [detail["ticket_price"], description, card["title"]] if part)
            )

            item = ExtractedActivity(
                source_url=card["source_url"],
                title=card["title"],
                description=description,
                venue_name=SAMA_VENUE_NAME,
                location_text=SAMA_LOCATION,
                city=SAMA_CITY,
                state=SAMA_STATE,
                activity_type=_infer_sama_activity_type(card["title"], description),
                age_min=None,
                age_max=None,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=("register" in text_blob and "not required" not in text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=SAMA_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
            key = (item.source_url, item.title, item.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(item)

    return rows


def _iter_sama_cards(html: str, *, list_url: str) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    calendar = soup.select_one("[data-calendar-date]")
    default_year = datetime.now().year
    if calendar is not None:
        calendar_date = (calendar.get("data-calendar-date") or "").strip()
        if len(calendar_date) >= 4 and calendar_date[:4].isdigit():
            default_year = int(calendar_date[:4])

    cards: list[dict[str, object]] = []
    for row in soup.select(".calendar__row"):
        date_label_node = row.select_one(".calendar__day .u-tt-uppercase")
        date_label = _normalize_space(date_label_node.get_text(" ", strip=True) if date_label_node else "")
        if not date_label:
            continue

        for grid_item in row.select(".grid-item"):
            title_node = grid_item.select_one(".grid-item__heading")
            if title_node is None:
                continue
            title = _normalize_space(title_node.get_text(" ", strip=True))
            href = (title_node.get("href") or "").strip()
            if not title or not href:
                continue

            time_node = grid_item.select_one(".grid-item__eyebrow")
            time_text = _normalize_space(time_node.get_text(" ", strip=True) if time_node else "")
            tags = [
                _normalize_space(tag.get_text(" ", strip=True))
                for tag in grid_item.select(".grid-item__tag")
                if _normalize_space(tag.get_text(" ", strip=True))
            ]
            cards.append(
                {
                    "source_url": urljoin(list_url, href),
                    "title": title,
                    "tags": tags,
                    "time_text": time_text,
                    "date_label": date_label,
                    "default_year": default_year,
                }
            )

    return cards


def _extract_next_month_url(html: str, *, list_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    node = soup.find("a", attrs={"aria-label": re.compile(r"next month", re.IGNORECASE)})
    href = (node.get("href") or "").strip() if node is not None else ""
    return urljoin(list_url, href) if href else None


def _parse_sama_detail_page(html: str | None) -> dict[str, object]:
    if not html:
        return {
            "start_at": None,
            "end_at": None,
            "description": None,
            "ticket_price": None,
        }

    soup = BeautifulSoup(html, "html.parser")
    add_event = soup.select_one(".addeventatc")
    start_at = _parse_datetime_text(add_event.select_one(".start").get_text(" ", strip=True)) if add_event and add_event.select_one(".start") else None
    end_at = _parse_datetime_text(add_event.select_one(".end").get_text(" ", strip=True)) if add_event and add_event.select_one(".end") else None

    description_node = soup.select_one(".event-detail__desc .desc__body")
    description = _normalize_text_block(description_node.get_text("\n", strip=True) if description_node else "")

    ticket_price = None
    for paragraph in soup.select("p"):
        text = _normalize_space(paragraph.get_text(" ", strip=True))
        if text.lower().startswith("ticket price:"):
            ticket_price = text
            break

    return {
        "start_at": start_at,
        "end_at": end_at,
        "description": description or None,
        "ticket_price": ticket_price,
    }


def _parse_card_datetimes(
    *,
    date_label: str,
    time_text: str,
    default_year: int,
) -> tuple[datetime | None, datetime | None]:
    if not date_label:
        return None, None
    try:
        base_date = datetime.strptime(f"{default_year} {date_label}", "%Y %b %d")
    except ValueError:
        return None, None

    if not time_text:
        return datetime(base_date.year, base_date.month, base_date.day), None

    parts = [part.strip() for part in time_text.split("-", maxsplit=1)]
    start_at = _parse_time_on_date(base_date, parts[0])
    end_at = _parse_time_on_date(base_date, parts[1]) if len(parts) > 1 else None
    return start_at, end_at


def _parse_time_on_date(base_date: datetime, value: str) -> datetime | None:
    normalized = _normalize_space(value).upper().replace(".", "")
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            parsed = datetime.strptime(normalized, fmt)
            return datetime(
                base_date.year,
                base_date.month,
                base_date.day,
                parsed.hour,
                parsed.minute,
            )
        except ValueError:
            continue
    return None


def _parse_datetime_text(value: str | None) -> datetime | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None
    for fmt in ("%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _should_keep_sama_event(*, title: str, tags: list[str], description: str | None) -> bool:
    tags_lower = {tag.lower() for tag in tags}
    text = " ".join([title, " ".join(tags), description or ""]).lower()
    included = any(keyword in text for keyword in SAMA_INCLUDED_KEYWORDS)
    family_or_teen = "families" in tags_lower or "teens" in tags_lower or "family" in text or "teen" in text
    if not family_or_teen:
        return False
    if "tour" in title.lower():
        return False
    if any(keyword in text for keyword in SAMA_EXCLUDED_KEYWORDS) and not included:
        return False
    return included


def _infer_sama_activity_type(title: str, description: str | None) -> str:
    text = " ".join([title, description or ""]).lower()
    if "family day" in text:
        return "activity"
    if "workshop" in text or "artmaking" in text or "art-making" in text:
        return "workshop"
    if "story" in text:
        return "storytime"
    if "draw" in text or "mask" in text or "activity" in text:
        return "activity"
    return "activity"


def _combine_description(
    *,
    detail_description: str | None,
    tags: list[str],
    ticket_price: str | None,
) -> str | None:
    parts: list[str] = []
    if detail_description:
        parts.append(detail_description)
    if tags:
        parts.append("Tags: " + ", ".join(tags))
    if ticket_price:
        parts.append(ticket_price)
    return " | ".join(parts) if parts else None


def _normalize_space(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def _normalize_text_block(value: str) -> str:
    lines = [_normalize_space(line) for line in value.splitlines()]
    return " ".join(line for line in lines if line)
