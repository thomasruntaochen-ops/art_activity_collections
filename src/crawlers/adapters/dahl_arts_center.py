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

DAHL_ARTS_CENTER_EVENTS_URL = "https://www.rapidcityartscouncil.org/events.html"
DAHL_ARTS_CENTER_TIMEZONE = "America/Denver"
DAHL_ARTS_CENTER_VENUE_NAME = "Dahl Arts Center"
DAHL_ARTS_CENTER_CITY = "Rapid City"
DAHL_ARTS_CENTER_STATE = "SD"
DAHL_ARTS_CENTER_DEFAULT_LOCATION = "Dahl Arts Center, Rapid City, SD"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": DAHL_ARTS_CENTER_EVENTS_URL,
}

INCLUDE_MARKERS = (
    " class ",
    " classes ",
    " art class ",
    " drawing group ",
    " workshop ",
    " workshops ",
    " art adventure ",
    " self-portrait ",
    " self portrait ",
    " portraiture ",
    " drawing ",
    " collaged ",
    " studio environment ",
    " creative problem-solving ",
)
EXCLUDE_MARKERS = (
    " exhibition ",
    " exhibit ",
    " showcase ",
    " opening reception ",
    " reception ",
    " film ",
    " festival ",
    " performance ",
    " performing ",
    " open mic ",
    " happy hour ",
    " call for art ",
    " submission due ",
    " screening ",
    " jazz ",
)
MONTH_DAY_LIST_RE = re.compile(
    r"(?P<month>[A-Za-z]+)\s+(?P<days>\d{1,2}(?:\s*(?:,|-)\s*\d{1,2})*)",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)\s*(?:-|–|—|to)\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b", re.IGNORECASE)


async def fetch_dahl_events_html(
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(DAHL_ARTS_CENTER_EVENTS_URL)
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

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Dahl Arts Center events HTML") from last_exception
    raise RuntimeError("Unable to fetch Dahl Arts Center events HTML after retries")


async def load_dahl_arts_center_payload() -> dict:
    return {"html": await fetch_dahl_events_html()}


def parse_dahl_arts_center_payload(
    payload: dict,
    *,
    start_date: str | None = None,
) -> list[ExtractedActivity]:
    html = payload.get("html") or ""
    soup = BeautifulSoup(html, "html.parser")
    cutoff = _resolve_cutoff_date(start_date)
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for cell in soup.select("td.wsite-multicol-col"):
        header = cell.select_one("h2.wsite-content-title")
        if header is None:
            continue

        title_lines = _header_lines(header)
        title, meta_lines = _split_title_and_meta(title_lines)
        if not title or not meta_lines:
            continue

        description = _join_non_empty(
            [
                _normalize_space(paragraph.get_text(" ", strip=True))
                for paragraph in cell.select("div.paragraph")
                if _normalize_space(paragraph.get_text(" ", strip=True))
            ]
        )
        cta_links = [
            {
                "href": urljoin(DAHL_ARTS_CENTER_EVENTS_URL, href),
                "text": text,
            }
            for anchor in cell.select("a[href]")
            for href, text in [(_normalize_space(anchor.get("href")), _normalize_space(anchor.get_text(" ", strip=True)))]
            if href and text and href != "javascript:;"
        ]

        blob = _search_blob(" ".join(filter(None, [title, description, " ".join(link["text"] for link in cta_links)])))
        if not _is_qualifying(blob):
            continue

        date_text, time_text, location_text = _extract_meta(meta_lines)
        if not date_text:
            continue

        datetimes = _expand_occurrence_datetimes(date_text=date_text, time_text=time_text, cutoff=cutoff)
        if not datetimes:
            continue

        description_with_location = _join_non_empty(
            [
                description,
                f"Location: {location_text}" if location_text else None,
            ]
        )
        pricing_text = _normalize_price_text(" | ".join(
            part for part in [description_with_location, " ".join(link["text"] for link in cta_links)] if part
        ))
        source_url = cta_links[0]["href"] if cta_links else DAHL_ARTS_CENTER_EVENTS_URL

        for start_at, end_at in datetimes:
            key = (source_url, title, start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=description_with_location,
                    venue_name=DAHL_ARTS_CENTER_VENUE_NAME,
                    location_text=location_text or DAHL_ARTS_CENTER_DEFAULT_LOCATION,
                    city=DAHL_ARTS_CENTER_CITY,
                    state=DAHL_ARTS_CENTER_STATE,
                    activity_type="workshop",
                    age_min=None,
                    age_max=None,
                    drop_in=(" drop in " in blob or " come and go " in blob),
                    registration_required=bool(cta_links)
                    or " reserve " in blob
                    or " register " in blob
                    or " registration " in blob,
                    start_at=start_at,
                    end_at=end_at,
                    timezone=DAHL_ARTS_CENTER_TIMEZONE,
                    **price_classification_kwargs(pricing_text, default_is_free=None),
                )
            )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class DahlArtsCenterAdapter(BaseSourceAdapter):
    source_name = "dahl_arts_center_events"

    async def fetch(self) -> list[str]:
        payload = await load_dahl_arts_center_payload()
        return [json.dumps(payload, ensure_ascii=False)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_dahl_arts_center_payload(json.loads(payload))


def _header_lines(header) -> list[str]:
    lines = [_normalize_space(line) for line in header.get_text("\n", strip=True).splitlines()]
    return [line for line in lines if line]


def _split_title_and_meta(lines: list[str]) -> tuple[str, list[str]]:
    if not lines:
        return "", []
    meta_index = next((idx for idx, line in enumerate(lines) if _looks_like_meta_line(line)), len(lines))
    title = " ".join(lines[:meta_index]).strip()
    return title, lines[meta_index:]


def _looks_like_meta_line(value: str) -> bool:
    lowered = value.lower()
    if "submission due" in lowered:
        return True
    if "|" in value:
        return True
    return any(month in lowered for month in _month_names()) or lowered.startswith("sundays")


def _extract_meta(lines: list[str]) -> tuple[str | None, str | None, str | None]:
    date_text: str | None = None
    time_text: str | None = None
    location_parts: list[str] = []

    for line in lines:
        if "|" in line and date_text is None:
            left, right = [part.strip() for part in line.split("|", 1)]
            date_text = left or None
            time_text = right or None
            continue
        if date_text is None and _looks_like_meta_line(line):
            date_text = line
            continue
        if time_text is None and (_normalize_time_text(line) or re.search(r"\d", line)):
            parsed_time = _parse_time_range(line, prefer_pm=True)
            if parsed_time[0] is not None:
                time_text = line
                continue
        location_parts.append(line)

    location_text = _join_non_empty(location_parts)
    return date_text, time_text, location_text


def _expand_occurrence_datetimes(
    *,
    date_text: str,
    time_text: str | None,
    cutoff: date,
) -> list[tuple[datetime, datetime | None]]:
    normalized_date = _normalize_space(date_text)
    if not normalized_date or normalized_date.lower().startswith("sundays"):
        return []

    match = MONTH_DAY_LIST_RE.search(normalized_date)
    if match is None:
        return []

    month = match.group("month")
    days = _expand_day_list(match.group("days"))
    if not days:
        return []

    start_time, end_time = _parse_time_range(time_text, prefer_pm=True)
    results: list[tuple[datetime, datetime | None]] = []
    today = datetime.now(ZoneInfo(DAHL_ARTS_CENTER_TIMEZONE)).date()

    for day_value in days:
        event_date = _infer_event_date(month=month, day_value=day_value, today=today)
        if event_date is None or event_date < cutoff:
            continue
        start_at = datetime.combine(
            event_date,
            start_time if start_time is not None else datetime.strptime("12:00 AM", "%I:%M %p").time(),
        )
        end_at_value = datetime.combine(event_date, end_time) if end_time is not None else None
        results.append((start_at, end_at_value))

    return results


def _expand_day_list(value: str) -> list[int]:
    normalized = re.sub(r"\s+", "", value)
    if not normalized:
        return []
    if "-" in normalized and "," not in normalized:
        start_text, end_text = normalized.split("-", 1)
        if start_text.isdigit() and end_text.isdigit():
            start_value = int(start_text)
            end_value = int(end_text)
            if start_value <= end_value:
                return list(range(start_value, end_value + 1))
    days: list[int] = []
    for part in normalized.split(","):
        if part.isdigit():
            days.append(int(part))
    return days


def _infer_event_date(*, month: str, day_value: int, today: date) -> date | None:
    month_number = datetime.strptime(month[:3].title(), "%b").month
    year = today.year
    try:
        candidate = date(year, month_number, day_value)
    except ValueError:
        return None

    if candidate < today and (today - candidate).days > 180:
        try:
            return date(year + 1, month_number, day_value)
        except ValueError:
            return None
    return candidate


def _parse_time_range(value: str | None, *, prefer_pm: bool) -> tuple[dt_time | None, dt_time | None]:
    normalized = _normalize_time_text(value)
    if not normalized:
        return None, None

    match = TIME_RANGE_RE.search(normalized)
    if match:
        end_text = match.group("end")
        end_meridiem = _extract_meridiem(end_text)
        start_text = _apply_meridiem(match.group("start"), fallback=end_meridiem, prefer_pm=prefer_pm)
        end_text = _apply_meridiem(end_text, fallback=end_meridiem, prefer_pm=prefer_pm)
        return _parse_time_text(start_text), _parse_time_text(end_text)

    single = TIME_SINGLE_RE.search(normalized)
    if single:
        text = _apply_meridiem(single.group("time"), fallback=None, prefer_pm=prefer_pm)
        return _parse_time_text(text), None
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


def _apply_meridiem(value: str, *, fallback: str | None, prefer_pm: bool) -> str:
    normalized = _normalize_time_text(value)
    if re.search(r"\b(?:AM|PM)\b", normalized):
        return normalized
    if fallback:
        return f"{normalized} {fallback}"
    if prefer_pm:
        return f"{normalized} PM"
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


def _is_qualifying(blob: str) -> bool:
    if not any(marker in blob for marker in INCLUDE_MARKERS):
        return False
    if any(marker in blob for marker in EXCLUDE_MARKERS):
        return False
    return True


def _resolve_cutoff_date(start_date: str | None) -> date:
    if start_date:
        return datetime.strptime(start_date, "%Y-%m-%d").date()
    return datetime.now(ZoneInfo(DAHL_ARTS_CENTER_TIMEZONE)).date()


def _month_names() -> tuple[str, ...]:
    return (
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    )


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    return " | ".join(values) if values else None


def _normalize_price_text(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9$]+", " ", value).strip()


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(unescape(value).split())


def _search_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9$]+", " ", _normalize_space(value).lower()).strip()
    return f" {normalized} "
