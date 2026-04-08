import asyncio
import json
import re
from datetime import datetime
from html import unescape
from urllib.parse import parse_qs
from urllib.parse import unquote
from urllib.parse import urljoin
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

BEMIS_EVENTS_URL = "https://www.bemiscenter.org/events"
BEMIS_TIMEZONE = "America/Chicago"
BEMIS_VENUE_NAME = "Bemis Center for Contemporary Arts"
BEMIS_CITY = "Omaha"
BEMIS_STATE = "NE"
BEMIS_DEFAULT_LOCATION = "Bemis Center for Contemporary Arts, Omaha, NE"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BEMIS_EVENTS_URL,
}

INCLUDE_MARKERS = (
    " talk ",
    " talks ",
    " lecture ",
    " lectures ",
    " class ",
    " classes ",
    " workshop ",
    " workshops ",
    " conversation ",
)
EXCLUDE_MARKERS = (
    " tour ",
    " tours ",
    " music ",
    " performance ",
    " low end ",
    " film ",
    " fundraising ",
    " opening ",
    " open studios ",
    " exhibition ",
)
DATE_WITH_TIME_RE = re.compile(
    r"(?P<weekday>[A-Za-z]+),\s+"
    r"(?P<month>[A-Za-z]+)\s+"
    r"(?P<day>\d{1,2}),\s+"
    r"(?P<year>\d{4})\s+"
    r"(?P<times>.+)$"
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?"
    r"\s*(?P<start_meridiem>a\.?m\.?|p\.?m\.?|am|pm)?"
    r"\s*(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?"
    r"\s*(?P<end_meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?|am|pm)\b",
    re.IGNORECASE,
)


async def fetch_bemis_page(
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
        raise RuntimeError("Unable to fetch Bemis events page") from last_exception
    raise RuntimeError("Unable to fetch Bemis events page after retries")


async def load_bemis_payload() -> dict:
    listing_html = await fetch_bemis_page(BEMIS_EVENTS_URL)
    detail_urls = _extract_event_urls(listing_html)
    detail_pages: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for url in detail_urls:
            try:
                detail_pages[url] = await fetch_bemis_page(url, client=client)
            except Exception as exc:
                print(f"[bemis] warning: detail fetch failed for {url}: {exc}")

    return {
        "listing_html": listing_html,
        "detail_pages": detail_pages,
    }


def parse_bemis_payload(payload: dict) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("listing_html") or "", "html.parser")
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in _iter_event_cards(soup):
        category = _normalize_space(
            card.select_one("div.text-smallCaps.uppercase.mb-4").get_text(" ", strip=True)
            if card.select_one("div.text-smallCaps.uppercase.mb-4")
            else ""
        )
        title_link = card.select_one("div.uppercase.font-mohol.text-heading4.mb-16 a[href]")
        if title_link is None:
            continue

        title = _normalize_space(title_link.get_text(" ", strip=True))
        source_url = _absolute_url(title_link.get("href", ""))
        if not title or not source_url:
            continue

        detail_html = detail_pages.get(source_url, "")
        description = _extract_detail_description(detail_html)
        blob = _search_blob(" ".join(filter(None, [category, title, description])))
        if not _should_include_event(category=category, blob=blob):
            continue

        start_at, end_at = _extract_datetimes(card)
        if start_at is None:
            start_at, end_at = _extract_datetimes_from_detail(detail_html)
        if start_at is None:
            continue

        location_text = _extract_location_text(detail_html) or _extract_location_text_from_card(card) or BEMIS_DEFAULT_LOCATION
        action_text = _extract_action_text(card, detail_html)
        price_text = _extract_price_text(detail_html, action_text)
        age_min, age_max = _parse_age_range(" ".join(filter(None, [title, description])))

        row = ExtractedActivity(
            source_url=source_url,
            title=title,
            description=_join_non_empty([description, f"Category: {category}" if category else None]),
            venue_name=BEMIS_VENUE_NAME,
            location_text=location_text,
            city=BEMIS_CITY,
            state=BEMIS_STATE,
            activity_type=_infer_activity_type(category=category, blob=blob),
            age_min=age_min,
            age_max=age_max,
            drop_in=("drop in" in blob or "drop-in" in blob),
            registration_required=any(marker in blob for marker in (" register ", " registration ", " rsvp ", " tickets ")),
            start_at=start_at,
            end_at=end_at,
            timezone=BEMIS_TIMEZONE,
            **price_classification_kwargs(price_text),
        )
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class BemisEventsAdapter(BaseSourceAdapter):
    source_name = "bemis_events"

    async def fetch(self) -> list[str]:
        payload = await load_bemis_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_bemis_payload(json.loads(payload))


def _iter_event_cards(soup: BeautifulSoup) -> list[BeautifulSoup]:
    cards: list[BeautifulSoup] = []
    for div in soup.find_all("div"):
        classes = div.get("class") or []
        if "mb-24" not in classes:
            continue
        if div.select_one("div.uppercase.font-mohol.text-heading4.mb-16 a[href]") is None:
            continue
        cards.append(div)
    return cards


def _extract_event_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for card in _iter_event_cards(soup):
        link = card.select_one("div.uppercase.font-mohol.text-heading4.mb-16 a[href]")
        if link is None:
            continue
        url = _absolute_url(link.get("href", ""))
        if not url or url.endswith("/events/past") or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _extract_datetimes(card: BeautifulSoup) -> tuple[datetime | None, datetime | None]:
    for link in card.select("a[href*='calendar.google.com/calendar/render']"):
        href = link.get("href", "").strip()
        if not href:
            continue
        parsed = urlparse(href)
        dates_value = parse_qs(parsed.query).get("dates", [None])[0]
        if not dates_value or "/" not in dates_value:
            continue
        start_text, end_text = dates_value.split("/", 1)
        try:
            start_at = datetime.strptime(start_text, "%Y%m%dT%H%M%S")
            end_at = datetime.strptime(end_text, "%Y%m%dT%H%M%S")
            return start_at, end_at
        except ValueError:
            continue

    date_text = _normalize_space(card.select_one("div.text-heading6small").get_text(" ", strip=True) if card.select_one("div.text-heading6small") else "")
    return _parse_human_datetime_range(date_text)


def _extract_datetimes_from_detail(detail_html: str) -> tuple[datetime | None, datetime | None]:
    if not detail_html:
        return None, None
    soup = BeautifulSoup(detail_html, "html.parser")
    sidebar_values = [
        _normalize_space(node.get_text(" ", strip=True))
        for node in soup.select("div.text-heading6")
    ]
    if len(sidebar_values) >= 2:
        date_text = sidebar_values[0]
        time_text = sidebar_values[1]
        return _parse_detail_datetime_range(date_text=date_text, time_text=time_text)
    return None, None


def _extract_detail_description(detail_html: str) -> str | None:
    if not detail_html:
        return None
    soup = BeautifulSoup(detail_html, "html.parser")
    node = soup.select_one("div.text-heading5.mt-20.g--link-purple")
    if node is None:
        return None
    return _normalize_space(node.get_text(" ", strip=True))


def _extract_location_text(detail_html: str) -> str | None:
    if not detail_html:
        return None
    soup = BeautifulSoup(detail_html, "html.parser")
    labels = soup.select("div.text-smallCaps.text-black.text-opacity-50.uppercase")
    values = soup.select("div.text-heading6")
    for index, label in enumerate(labels):
        if _normalize_space(label.get_text(" ", strip=True)).lower() != "location":
            continue
        if index >= len(values):
            continue
        value = _normalize_space(values[index].get_text(" ", strip=True))
        return value or None
    return None


def _extract_location_text_from_card(card: BeautifulSoup) -> str | None:
    for link in card.select("a[href*='calendar.google.com/calendar/render']"):
        href = link.get("href", "").strip()
        if not href:
            continue
        parsed = urlparse(href)
        location_value = parse_qs(parsed.query).get("location", [None])[0]
        if not location_value:
            continue
        text = _normalize_space(BeautifulSoup(unquote(location_value), "html.parser").get_text(" ", strip=True))
        if text:
            return text
    return None


def _extract_action_text(card: BeautifulSoup, detail_html: str) -> str:
    parts = [
        _normalize_space(node.get_text(" ", strip=True))
        for node in card.select("a")
    ]
    if detail_html:
        soup = BeautifulSoup(detail_html, "html.parser")
        parts.extend(_normalize_space(node.get_text(" ", strip=True)) for node in soup.select("a"))
    return _normalize_space(" ".join(part for part in parts if part))


def _extract_price_text(detail_html: str, action_text: str) -> str:
    detail_text = _normalize_space(BeautifulSoup(detail_html or "", "html.parser").get_text(" ", strip=True))
    return _normalize_space(" ".join(part for part in [detail_text, action_text] if part))


def _parse_human_datetime_range(value: str) -> tuple[datetime | None, datetime | None]:
    match = DATE_WITH_TIME_RE.search(value)
    if not match:
        return None, None
    base_date = f"{match.group('month')} {match.group('day')}, {match.group('year')}"
    return _build_range_from_date_and_time(base_date, match.group("times"))


def _parse_detail_datetime_range(*, date_text: str, time_text: str) -> tuple[datetime | None, datetime | None]:
    normalized_date = _normalize_space(date_text)
    normalized_time = _normalize_space(time_text).replace(" CT", "").replace(" CST", "").replace(" CDT", "")
    try:
        base_date = datetime.strptime(normalized_date, "%a, %b %d, %Y").strftime("%B %d, %Y")
    except ValueError:
        return None, None
    return _build_range_from_date_and_time(base_date, normalized_time)


def _build_range_from_date_and_time(base_date: str, time_text: str) -> tuple[datetime | None, datetime | None]:
    match = TIME_RANGE_RE.search(time_text)
    if match:
        start_meridiem = _normalize_meridiem(match.group("start_meridiem") or match.group("end_meridiem"))
        end_meridiem = _normalize_meridiem(match.group("end_meridiem"))
        start_text = _format_clock(match.group("start_hour"), match.group("start_minute"), start_meridiem)
        end_text = _format_clock(match.group("end_hour"), match.group("end_minute"), end_meridiem)
        return _parse_datetime(base_date, start_text), _parse_datetime(base_date, end_text)

    single = TIME_SINGLE_RE.search(time_text)
    if not single:
        return None, None
    start_text = _format_clock(single.group("hour"), single.group("minute"), _normalize_meridiem(single.group("meridiem")))
    return _parse_datetime(base_date, start_text), None


def _parse_datetime(date_text: str, time_text: str) -> datetime | None:
    try:
        return datetime.strptime(f"{date_text} {time_text}", "%B %d, %Y %I:%M %p")
    except ValueError:
        return None


def _format_clock(hour: str | None, minute: str | None, meridiem: str | None) -> str:
    return f"{int(hour or '0')}:{int(minute or '0'):02d} {meridiem or 'AM'}"


def _normalize_meridiem(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.replace(".", "").strip().lower()
    if cleaned == "am":
        return "AM"
    if cleaned == "pm":
        return "PM"
    return value.upper()


def _should_include_event(*, category: str, blob: str) -> bool:
    category_blob = _search_blob(category)
    if any(marker in category_blob for marker in (" tour ", " tours ", " music ", " performance ", " low end ")):
        return False
    if any(marker in category_blob for marker in (" talk ", " talks ", " class ", " classes ", " workshop ", " workshops ")):
        return True
    if any(marker in blob for marker in EXCLUDE_MARKERS) and not any(marker in blob for marker in INCLUDE_MARKERS):
        return False
    return any(marker in blob for marker in INCLUDE_MARKERS)


def _infer_activity_type(*, category: str, blob: str) -> str:
    category_blob = _search_blob(category)
    if any(marker in category_blob for marker in (" talk ", " talks ", " lecture ", " lectures ", " conversation ")):
        return "lecture"
    if any(marker in blob for marker in (" lecture ", " lectures ", " talk ", " talks ", " conversation ")):
        return "lecture"
    return "workshop"


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = re.search(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", text, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus = re.search(r"\bages?\s*(\d{1,2})\+\b", text, re.IGNORECASE)
    if plus:
        return int(plus.group(1)), None
    return None, None


def _absolute_url(value: str) -> str:
    return urljoin(BEMIS_EVENTS_URL, value.strip())


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    return " | ".join(values) if values else None


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(unescape(value).split())


def _search_blob(value: str) -> str:
    normalized = _normalize_space(value).lower()
    return f" {normalized} "
