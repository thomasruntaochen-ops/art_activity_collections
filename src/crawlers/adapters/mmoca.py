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
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

MMOCA_EVENTS_URL = "https://www.mmoca.org/events/"
MMOCA_TIMEZONE = "America/Chicago"
MMOCA_VENUE_NAME = "Madison Museum of Contemporary Art"
MMOCA_CITY = "Madison"
MMOCA_STATE = "WI"
MMOCA_LOCATION = "Madison Museum of Contemporary Art, Madison, WI"

SCHEDULE_RE = re.compile(
    r"(?P<date>[A-Z][a-z]+\s+\d{1,2},\s+\d{4})\s+(?P<time>\d{1,2}:\d{2}\s*[ap]\.?m\.?"
    r"(?:\s*[–-]\s*\d{1,2}(?::\d{2})?\s*[ap]\.?m\.?)?)",
    re.IGNORECASE,
)
BULLET_SCHEDULE_RE = re.compile(
    r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"(?P<month>[A-Z][a-z]+)\s+(?P<day>\d{1,2})\s*[•|]\s*(?P<time>[^•]{1,40})"
    r"(?:\s*[•|]\s*(?P<location>[^•]{1,80}))?"
    r"(?:\s*[•|]\s*(?P<cost>(?:Free Admission|\$[^•]{1,80})))?",
    re.IGNORECASE,
)
INCLUDE_PATTERNS = (
    " class ",
    " classes ",
    " discussion ",
    " draw ",
    " drawing ",
    " lecture ",
    " lectures ",
    " slow looking ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
REJECT_PATTERNS = (
    " art fair ",
    " fashion show ",
    " gallery night ",
    " member tour ",
    " meditation ",
    " tai chi ",
    " tour ",
    " tours ",
    " yoga ",
)


async def load_mmoca_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_html(MMOCA_EVENTS_URL, client=client)
        listing_items = _extract_listing_items(listing_html)
        detail_pages: dict[str, str] = {}
        for detail_url in listing_items:
            detail_pages[detail_url] = await fetch_html(detail_url, referer=MMOCA_EVENTS_URL, client=client)

    return {"listing_items": listing_items, "detail_pages": detail_pages}


class MmocaAdapter(BaseSourceAdapter):
    source_name = "mmoca_events"

    async def fetch(self) -> list[str]:
        payload = await load_mmoca_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_mmoca_payload(json.loads(payload))


def parse_mmoca_payload(payload: dict) -> list[ExtractedActivity]:
    listing_items = payload.get("listing_items") or {}
    detail_pages = payload.get("detail_pages") or {}
    today = datetime.now(ZoneInfo(MMOCA_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for detail_url, detail_html in detail_pages.items():
        row = _build_row(detail_url=detail_url, detail_html=detail_html, listing_item=listing_items.get(detail_url))
        if row is None or row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _extract_listing_items(listing_html: str) -> dict[str, dict]:
    soup = BeautifulSoup(listing_html, "html.parser")
    items: dict[str, dict] = {}

    for container in soup.select("div[data-events]"):
        for anchor in container.select("a[href]"):
            title = normalize_space(anchor.get_text(" ", strip=True))
            if not title or title.lower() == "see event":
                continue
            detail_url = absolute_url(MMOCA_EVENTS_URL, anchor.get("href"))
            if "/events/" not in detail_url:
                continue
            if not _should_include(title=title, description=None):
                continue
            if detail_url not in items:
                items[detail_url] = {"title": title}

    return items


def _build_row(*, detail_url: str, detail_html: str, listing_item: dict | None) -> ExtractedActivity | None:
    soup = BeautifulSoup(detail_html, "html.parser")
    title = normalize_space(
        soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else (listing_item or {}).get("title")
    )
    main = soup.select_one("main")
    main_text = normalize_space(main.get_text(" ", strip=True) if main else soup.get_text(" ", strip=True))
    if not title or not _should_include(title=title, description=main_text):
        return None

    start_at, end_at = _extract_datetimes(main_text)
    if start_at is None:
        return None

    bullet_match = BULLET_SCHEDULE_RE.search(main_text)
    cost_text = normalize_space(bullet_match.group("cost")) if bullet_match else None
    location_text = normalize_space(bullet_match.group("location")) if bullet_match else MMOCA_LOCATION
    description = _extract_description(main_text, title)
    full_description = join_non_empty(
        [
            description,
            f"Cost: {cost_text}" if cost_text else None,
        ]
    )
    blob = _blob(" ".join(part for part in [title, full_description or ""] if part))

    return ExtractedActivity(
        source_url=detail_url,
        title=title,
        description=full_description,
        venue_name=MMOCA_VENUE_NAME,
        location_text=location_text or MMOCA_LOCATION,
        city=MMOCA_CITY,
        state=MMOCA_STATE,
        activity_type=infer_activity_type(title, full_description),
        age_min=None,
        age_max=None,
        drop_in=" drop-in " in blob,
        registration_required=(
            (" advance registration " in blob)
            or (" register " in blob)
            or (" registration " in blob)
            or (" reserve " in blob)
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=MMOCA_TIMEZONE,
        **price_classification_kwargs(full_description),
    )


def _extract_datetimes(text: str) -> tuple[datetime | None, datetime | None]:
    schedule_match = SCHEDULE_RE.search(text)
    if schedule_match is not None:
        base_date = datetime.strptime(schedule_match.group("date"), "%B %d, %Y").date()
        return parse_time_range(base_date=base_date, time_text=schedule_match.group("time"))

    bullet_match = BULLET_SCHEDULE_RE.search(text)
    if bullet_match is None:
        return None, None
    year = datetime.now(ZoneInfo(MMOCA_TIMEZONE)).year
    date_text = f"{bullet_match.group('month')} {bullet_match.group('day')}, {year}"
    base_date = datetime.strptime(date_text, "%B %d, %Y").date()
    return parse_time_range(base_date=base_date, time_text=bullet_match.group("time"))


def _extract_description(text: str, title: str) -> str | None:
    normalized = normalize_space(text)
    start_index = normalized.find(title)
    if start_index >= 0:
        normalized = normalized[start_index + len(title) :]
    for marker in ("← All Events", "+ Google Calendar", "Google Calendar"):
        marker_index = normalized.find(marker)
        if marker_index >= 0:
            normalized = normalized[marker_index + len(marker) :]
    normalized = normalized.strip()
    return normalized or None


def _should_include(*, title: str, description: str | None) -> bool:
    blob = _blob(" ".join(part for part in [title, description or ""] if part))
    if any(pattern in blob for pattern in REJECT_PATTERNS):
        return False
    return any(pattern in blob for pattern in INCLUDE_PATTERNS)


def _blob(value: str) -> str:
    return f" {normalize_space(value).lower()} "
