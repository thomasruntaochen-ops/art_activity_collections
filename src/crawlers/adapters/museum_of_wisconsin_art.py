from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import DEFAULT_HEADERS
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

MOWA_EVENTS_URL = "https://wisconsinart.org/events/"
MOWA_EVENTS_API_URL = "https://wisconsinart.org/wp-json/wp/v2/mec-events"
MOWA_TIMEZONE = "America/Chicago"
MOWA_VENUE_NAME = "Museum of Wisconsin Art"
MOWA_CITY = "West Bend"
MOWA_STATE = "WI"
MOWA_LOCATION = "Museum of Wisconsin Art, West Bend, WI"

REJECT_PATTERNS = (
    " exhibition opening ",
    " member opportunity ",
    " achievement awards ",
    " award ",
    " gala ",
    " reception ",
)
INCLUDE_PATTERNS = (
    " art+wellness ",
    " artist talk ",
    " class ",
    " classes ",
    " meetup ",
    " mini makers ",
    " second saturday ",
    " studio ",
    " talk ",
    " teen ",
    " workshop ",
    " workshops ",
    " youth ",
)
TITLE_FALLBACK_PATTERNS = (
    " lamp ",
    " makers ",
    " possibilities ",
    " assembl",
    " collage ",
    " texture ",
    " nature ",
)
DATE_TEXT_RE = re.compile(r"\bDate\s+(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE)


async def load_museum_of_wisconsin_art_payload(
    *,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        events = await _load_rest_events(client=client, page_limit=page_limit, per_page=per_page)
        details: dict[str, str] = {}
        for event in events:
            detail_url = normalize_space(event.get("link"))
            if not detail_url or not _is_candidate(event):
                continue
            details[detail_url] = await fetch_html(detail_url, referer=MOWA_EVENTS_URL, client=client)
    return {"events": events, "details": details}


class MuseumOfWisconsinArtAdapter(BaseSourceAdapter):
    source_name = "museum_of_wisconsin_art_events"

    async def fetch(self) -> list[str]:
        payload = await load_museum_of_wisconsin_art_payload(page_limit=1)
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_museum_of_wisconsin_art_payload(json.loads(payload))


def parse_museum_of_wisconsin_art_payload(payload: dict) -> list[ExtractedActivity]:
    details = payload.get("details") or {}
    today = datetime.now(ZoneInfo(MOWA_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event in payload.get("events") or []:
        detail_url = normalize_space(event.get("link"))
        detail_html = details.get(detail_url)
        if not detail_html:
            continue
        row = _build_row(event, detail_html)
        if row is None or row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


async def _load_rest_events(
    *,
    client: httpx.AsyncClient,
    page_limit: int | None,
    per_page: int,
) -> list[dict]:
    events: list[dict] = []
    total_pages = 1
    page = 1
    while page <= total_pages:
        if page_limit is not None and page > max(page_limit, 1):
            break
        response = await client.get(
            MOWA_EVENTS_API_URL,
            params={"per_page": per_page, "page": page},
        )
        response.raise_for_status()
        total_pages = int(response.headers.get("x-wp-totalpages", "1"))
        events.extend(response.json())
        page += 1
    return events


def _build_row(event: dict, detail_html: str) -> ExtractedActivity | None:
    title = normalize_space(BeautifulSoup((event.get("title") or {}).get("rendered") or "", "html.parser").get_text(" ", strip=True))
    source_url = normalize_space(event.get("link"))
    description = _extract_description(detail_html) or _rest_description(event)
    if not title or not source_url:
        return None

    if not _should_include(title=title, description=description):
        return None

    start_at, end_at = _extract_datetimes(detail_html)
    if start_at is None:
        return None

    cost_text = _extract_selector_text(detail_html, ".mec-event-cost")
    full_description = join_non_empty(
        [
            description,
            f"Cost: {cost_text}" if cost_text else None,
        ]
    )
    blob = _blob(" ".join(part for part in [title, full_description or ""] if part))
    age_min, age_max = parse_age_range(" ".join(part for part in [title, full_description or ""] if part))

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=MOWA_VENUE_NAME,
        location_text=MOWA_LOCATION,
        city=MOWA_CITY,
        state=MOWA_STATE,
        activity_type=infer_activity_type(title, full_description),
        age_min=age_min,
        age_max=age_max,
        drop_in=(" drop-in " in blob or " open studio " in blob),
        registration_required=(
            (" registration " in blob)
            or (" register " in blob)
            or (" reserve " in blob)
            or (" tickets " in blob)
        ) and " drop-in " not in blob,
        start_at=start_at,
        end_at=end_at,
        timezone=MOWA_TIMEZONE,
        **price_classification_kwargs(full_description),
    )


def _is_candidate(event: dict) -> bool:
    title = normalize_space(BeautifulSoup((event.get("title") or {}).get("rendered") or "", "html.parser").get_text(" ", strip=True))
    description = _rest_description(event)
    return _should_include(title=title, description=description)


def _should_include(*, title: str, description: str | None) -> bool:
    blob = _blob(" ".join(part for part in [title, description or ""] if part))
    if any(pattern in blob for pattern in REJECT_PATTERNS):
        return False
    if any(pattern in blob for pattern in INCLUDE_PATTERNS):
        return True
    return any(pattern in blob for pattern in TITLE_FALLBACK_PATTERNS)


def _extract_description(detail_html: str) -> str | None:
    return _extract_selector_text(detail_html, ".mec-single-event-description")


def _extract_datetimes(detail_html: str) -> tuple[datetime | None, datetime | None]:
    soup = BeautifulSoup(detail_html, "html.parser")
    date_text = _extract_selector_text(detail_html, ".mec-single-event-date")
    time_text = _extract_selector_text(detail_html, ".mec-single-event-time")
    parsed_date = _parse_mec_date(date_text)

    event_obj = _extract_event_object(soup)
    if event_obj is not None:
        start_date = event_obj.get("startDate")
        end_date = event_obj.get("endDate")
        start_at = parse_iso_datetime(start_date, timezone_name=MOWA_TIMEZONE) if start_date else None
        end_at = parse_iso_datetime(end_date, timezone_name=MOWA_TIMEZONE) if end_date else None
        if start_at is not None and time_text:
            parsed_start, parsed_end = parse_time_range(base_date=start_at.date(), time_text=time_text)
            return parsed_start or start_at, parsed_end or end_at
        if start_at is not None:
            return start_at, end_at

    if parsed_date is None:
        return None, None
    return parse_time_range(base_date=parsed_date, time_text=time_text)


def _extract_selector_text(detail_html: str, selector: str) -> str | None:
    soup = BeautifulSoup(detail_html, "html.parser")
    node = soup.select_one(selector)
    if node is None:
        return None
    return normalize_space(node.get_text(" ", strip=True))


def _parse_mec_date(text: str | None):
    normalized = normalize_space(text)
    if not normalized:
        return None
    match = DATE_TEXT_RE.search(normalized.replace("Expired!", ""))
    if match is None:
        return None
    date_text = match.group("date")
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%B %d %Y"):
        try:
            return datetime.strptime(date_text, fmt).date()
        except ValueError:
            continue
    return None


def _rest_description(event: dict) -> str | None:
    parts = [
        _html_to_text((event.get("excerpt") or {}).get("rendered")),
        _html_to_text((event.get("content") or {}).get("rendered")),
    ]
    yoast = event.get("yoast_head_json") or {}
    parts.append(normalize_space(yoast.get("description")))
    return join_non_empty(parts)


def _extract_event_object(soup: BeautifulSoup) -> dict | None:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = (script.get_text() or "").strip()
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        found = _find_event_object(data)
        if found is not None:
            return found
    return None


def _find_event_object(value: object) -> dict | None:
    if isinstance(value, dict):
        value_type = value.get("@type")
        if value_type == "Event" or (isinstance(value_type, list) and "Event" in value_type):
            return value
        for nested in value.values():
            found = _find_event_object(nested)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _find_event_object(nested)
            if found is not None:
                return found
    return None


def _html_to_text(value: str | None) -> str | None:
    if not value:
        return None
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    normalized = normalize_space(text)
    return normalized or None


def _blob(value: str) -> str:
    return f" {normalize_space(value).lower()} "
