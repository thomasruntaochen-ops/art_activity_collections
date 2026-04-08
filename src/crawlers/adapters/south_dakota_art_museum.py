import asyncio
import json
import re
from datetime import date
from datetime import datetime
from datetime import time as dt_time
from html import unescape
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

SOUTH_DAKOTA_ART_MUSEUM_LIST_URL = (
    "https://www.sdstate.edu/event-calendar"
    "?field_booking_start_date%5Bdate_range%5D=all"
    "&field_booking_end_date_value%5Bdate%5D="
    "&field_booking_start_date_value%5Bdate%5D="
    "&combine="
    "&departments=262866"
)
SOUTH_DAKOTA_ART_MUSEUM_TIMEZONE = "America/Chicago"
SOUTH_DAKOTA_ART_MUSEUM_VENUE_NAME = "South Dakota Art Museum"
SOUTH_DAKOTA_ART_MUSEUM_CITY = "Brookings"
SOUTH_DAKOTA_ART_MUSEUM_STATE = "SD"
SOUTH_DAKOTA_ART_MUSEUM_DEFAULT_LOCATION = "South Dakota Art Museum, Brookings, SD"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.sdstate.edu/event-calendar",
}

INCLUDE_MARKERS = (
    " art day ",
    " workshop ",
    " workshops ",
    " training ",
    " portraiture ",
    " portrait ",
    " collage ",
    " collaged ",
    " construction paper ",
    " paint night ",
    " mural ",
    " create ",
    " creative ",
)
EXCLUDE_MARKERS = (
    " film ",
    " music ",
    " performance ",
    " fundraising ",
    " fundraiser ",
    " storytime ",
    " poetry ",
    " reading ",
    " yoga ",
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)\s*(?:-|–|—|to)\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b", re.IGNORECASE)


async def fetch_html(
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
        raise RuntimeError("Unable to fetch South Dakota Art Museum HTML") from last_exception
    raise RuntimeError("Unable to fetch South Dakota Art Museum HTML after retries")


async def load_south_dakota_art_museum_payload() -> dict:
    list_html = await fetch_html(SOUTH_DAKOTA_ART_MUSEUM_LIST_URL)
    occurrences = _extract_occurrences_from_list_html(list_html)
    detail_html_by_url: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for occurrence in occurrences:
            source_url = occurrence["source_url"]
            if source_url in detail_html_by_url:
                continue
            detail_html_by_url[source_url] = await fetch_html(source_url, client=client)

    return {
        "list_html": list_html,
        "occurrences": occurrences,
        "detail_html_by_url": detail_html_by_url,
    }


def parse_south_dakota_art_museum_payload(
    payload: dict,
    *,
    start_date: str | None = None,
) -> list[ExtractedActivity]:
    cutoff = _resolve_cutoff_date(start_date)
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    occurrences = payload.get("occurrences") or _extract_occurrences_from_list_html(payload.get("list_html") or "")
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for occurrence in occurrences:
        row = _build_row(
            occurrence=occurrence,
            detail_html=detail_html_by_url.get(occurrence["source_url"]),
        )
        if row is None or row.start_at.date() < cutoff:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class SouthDakotaArtMuseumAdapter(BaseSourceAdapter):
    source_name = "south_dakota_art_museum_events"

    async def fetch(self) -> list[str]:
        payload = await load_south_dakota_art_museum_payload()
        return [json.dumps(payload, ensure_ascii=False)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_south_dakota_art_museum_payload(json.loads(payload))


def _extract_occurrences_from_list_html(html: str) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    occurrences: list[dict[str, object]] = []

    for card in soup.select("div.event-calendar-event.sds-ec__date-item--wrapper"):
        title_link = card.select_one(".sds-ec__date-item--title a[href]")
        if title_link is None:
            continue
        title = _normalize_space(title_link.get_text(" ", strip=True))
        source_url = urljoin(SOUTH_DAKOTA_ART_MUSEUM_LIST_URL, title_link.get("href") or "")
        if not title or not source_url:
            continue

        tags = [
            _normalize_space(tag.get_text(" ", strip=True))
            for tag in card.select(".sds-ec__date-item--tags a")
            if _normalize_space(tag.get_text(" ", strip=True))
        ]
        occurrences.append(
            {
                "title": title,
                "source_url": source_url,
                "month_text": _normalize_space(_text_or_none(card.select_one(".sds-ec__date-item--month"))),
                "day_text": _normalize_space(_text_or_none(card.select_one(".sds-ec__date-item--day"))),
                "time_text": _normalize_space(_text_or_none(card.select_one(".sds-ec__date-item--time"))),
                "summary": _normalize_space(_text_or_none(card.select_one(".sds-ec__date-item--body"))),
                "tags": tags,
            }
        )

    return occurrences


def _build_row(
    *,
    occurrence: dict[str, object],
    detail_html: str | None,
) -> ExtractedActivity | None:
    title = _normalize_space(occurrence.get("title"))
    source_url = _normalize_space(occurrence.get("source_url"))
    if not title or not source_url:
        return None

    detail = _parse_detail_html(detail_html or "", fallback_summary=_normalize_space(occurrence.get("summary")))
    description = detail["description"] or _normalize_space(occurrence.get("summary"))
    tags = [tag for tag in occurrence.get("tags") or [] if isinstance(tag, str)]
    blob = _search_blob(" ".join(filter(None, [title, description, " ".join(tags)])))
    if not _is_qualifying(blob):
        return None

    start_at, end_at = _resolve_datetimes(
        detail_date_text=detail["date_text"],
        detail_time_text=detail["time_text"],
        occurrence=occurrence,
    )
    if start_at is None:
        return None

    location_text = detail["location_text"] or SOUTH_DAKOTA_ART_MUSEUM_DEFAULT_LOCATION
    pricing_text = _normalize_price_text(" | ".join(part for part in [description, " ".join(tags)] if part))
    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=_join_non_empty(
            [
                description,
                f"Categories: {', '.join(tags)}" if tags else None,
            ]
        ),
        venue_name=SOUTH_DAKOTA_ART_MUSEUM_VENUE_NAME,
        location_text=location_text,
        city=SOUTH_DAKOTA_ART_MUSEUM_CITY,
        state=SOUTH_DAKOTA_ART_MUSEUM_STATE,
        activity_type="workshop",
        age_min=None,
        age_max=None,
        drop_in=(" drop in " in blob or " come and go " in blob),
        registration_required=(" register " in blob or " registration " in blob or " rsvp " in blob),
        start_at=start_at,
        end_at=end_at,
        timezone=SOUTH_DAKOTA_ART_MUSEUM_TIMEZONE,
        **price_classification_kwargs(pricing_text, default_is_free=None),
    )


def _parse_detail_html(html: str, *, fallback_summary: str | None) -> dict[str, str | None]:
    if not html:
        return {
            "date_text": None,
            "time_text": None,
            "location_text": None,
            "description": fallback_summary,
        }

    soup = BeautifulSoup(html, "html.parser")
    date_text = _normalize_space(_text_or_none(soup.select_one(".sds-ev__details--date")))
    time_text = _normalize_space(_text_or_none(soup.select_one(".sds-ev__details--time")))
    location_text = _normalize_space(_text_or_none(soup.select_one(".sds-ev__details--location")))

    description_parts: list[str] = []
    description_root = soup.select_one(".sds-ev__description")
    if description_root is not None:
        for child in description_root.find_all(["p", "div", "li"], recursive=True):
            text = _normalize_space(child.get_text(" ", strip=True))
            if text and text not in description_parts:
                description_parts.append(text)

    if not description_parts:
        meta_description = soup.find("meta", attrs={"property": "og:description"})
        if meta_description is None:
            meta_description = soup.find("meta", attrs={"name": "description"})
        if meta_description is not None:
            description_parts.append(_normalize_space(meta_description.get("content")))

    description = _join_non_empty(description_parts or [fallback_summary])
    return {
        "date_text": date_text or None,
        "time_text": time_text or None,
        "location_text": location_text or None,
        "description": description,
    }


def _resolve_datetimes(
    *,
    detail_date_text: str | None,
    detail_time_text: str | None,
    occurrence: dict[str, object],
) -> tuple[datetime | None, datetime | None]:
    start_at = _parse_detail_start(detail_date_text, detail_time_text)
    end_at = _parse_detail_end(detail_date_text, detail_time_text)
    if start_at is not None:
        return start_at, end_at

    month_text = _normalize_space(occurrence.get("month_text"))
    day_text = _normalize_space(occurrence.get("day_text"))
    time_text = _normalize_space(occurrence.get("time_text"))
    if not month_text or not day_text:
        return None, None

    current_year = datetime.now(ZoneInfo(SOUTH_DAKOTA_ART_MUSEUM_TIMEZONE)).year
    try:
        parsed_date = datetime.strptime(f"{month_text} {day_text} {current_year}", "%b %d %Y")
    except ValueError:
        return None, None

    start_time, end_time = _parse_time_range(time_text)
    if start_time is None:
        start_time = datetime.strptime("12:00 AM", "%I:%M %p").time()
    start_at = datetime.combine(parsed_date.date(), start_time)
    end_at_value = datetime.combine(parsed_date.date(), end_time) if end_time is not None else None
    return start_at, end_at_value


def _parse_detail_start(date_text: str | None, time_text: str | None) -> datetime | None:
    parsed_date = _parse_event_date(date_text)
    if parsed_date is None:
        return None
    start_time, _ = _parse_time_range(time_text)
    if start_time is None:
        start_time = datetime.strptime("12:00 AM", "%I:%M %p").time()
    return datetime.combine(parsed_date, start_time)


def _parse_detail_end(date_text: str | None, time_text: str | None) -> datetime | None:
    parsed_date = _parse_event_date(date_text)
    if parsed_date is None:
        return None
    _, end_time = _parse_time_range(time_text)
    if end_time is None:
        return None
    return datetime.combine(parsed_date, end_time)


def _parse_event_date(value: str | None) -> date | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, "%A, %B %d, %Y").date()
    except ValueError:
        return None


def _parse_time_range(value: str | None) -> tuple[dt_time | None, dt_time | None]:
    normalized = _normalize_time_text(value)
    if not normalized:
        return None, None

    match = TIME_RANGE_RE.search(normalized)
    if match:
        end_text = match.group("end")
        start_text = _apply_meridiem(match.group("start"), fallback=_extract_meridiem(end_text))
        return _parse_time_text(start_text), _parse_time_text(end_text)

    single = TIME_SINGLE_RE.search(normalized)
    if single:
        return _parse_time_text(single.group("time")), None
    return None, None


def _parse_time_text(value: str | None) -> dt_time | None:
    normalized = _normalize_time_text(value)
    if not normalized:
        return None
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            return datetime.strptime(normalized, fmt).time()
        except ValueError:
            continue
    return None


def _apply_meridiem(value: str, *, fallback: str | None) -> str:
    normalized = _normalize_time_text(value)
    if fallback and not re.search(r"\b(?:AM|PM)\b", normalized):
        normalized = f"{normalized} {fallback}"
    return normalized


def _extract_meridiem(value: str | None) -> str | None:
    normalized = _normalize_time_text(value)
    match = re.search(r"\b(AM|PM)\b", normalized)
    return match.group(1) if match else None


def _normalize_time_text(value: object) -> str:
    text = _normalize_space(value)
    if not text:
        return ""
    text = text.replace("a.m.", "AM").replace("p.m.", "PM")
    text = text.replace("a.m", "AM").replace("p.m", "PM")
    text = re.sub(
        r"(?i)\b(\d{1,2}(?::\d{2})?)\s*([ap])\.?m?\.?\b",
        lambda match: f"{match.group(1)} {'AM' if match.group(2).lower() == 'a' else 'PM'}",
        text,
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_price_text(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9$]+", " ", value).strip()


def _is_qualifying(blob: str) -> bool:
    if any(marker in blob for marker in EXCLUDE_MARKERS) and not any(marker in blob for marker in INCLUDE_MARKERS):
        return False
    return any(marker in blob for marker in INCLUDE_MARKERS)


def _resolve_cutoff_date(start_date: str | None) -> date:
    if start_date:
        return datetime.strptime(start_date, "%Y-%m-%d").date()
    return datetime.now(ZoneInfo(SOUTH_DAKOTA_ART_MUSEUM_TIMEZONE)).date()


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    return " | ".join(values) if values else None


def _text_or_none(node: object) -> str | None:
    if node is None or not hasattr(node, "get_text"):
        return None
    return node.get_text(" ", strip=True)


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(unescape(value).split())


def _search_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9$]+", " ", _normalize_space(value).lower()).strip()
    return f" {normalized} "
