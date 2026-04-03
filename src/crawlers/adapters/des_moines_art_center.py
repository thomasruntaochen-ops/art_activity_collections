from __future__ import annotations

import json
import re
from datetime import datetime
from html import unescape
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import AGE_PLUS_RE
from src.crawlers.adapters.oh_common import AGE_RANGE_RE
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

DMAC_CALENDAR_URL = "https://desmoinesartcenter.org/calendar/"
DMAC_VENUE_NAME = "Des Moines Art Center"
DMAC_CITY = "Des Moines"
DMAC_STATE = "IA"
DMAC_TIMEZONE = "America/Chicago"
DMAC_LOCATION = "Des Moines Art Center, Des Moines, IA"

EVENTS_JSON_RE = re.compile(r'(\[\{"@context":"http://schema\.org".*?\])', re.DOTALL)
INCLUDE_MARKERS = (
    "activity",
    "art for brunch",
    "art spectrums",
    "art workshop",
    "conversation",
    "creative aging",
    "craft",
    "discussion",
    "make art",
    "social saturday",
    "youth art workshop",
    "workshop",
)
EXCLUDE_MARKERS = (
    "admission",
    "animation showcase",
    "dinner",
    "dinner with friends",
    "exhibition opening",
    "exhibition tour",
    "film screening",
    "guided tour",
    "live performance",
    "music",
    "performance",
    "storytime",
    "tour",
)


async def load_des_moines_art_center_payload(*, max_pages: int = 4) -> dict:
    pages: list[str] = []
    url = DMAC_CALENDAR_URL
    fetched = 0

    while url and fetched < max_pages:
        html = await fetch_html(url, referer=DMAC_CALENDAR_URL)
        pages.append(html)
        fetched += 1

        soup = BeautifulSoup(html, "html.parser")
        next_link = soup.select_one('link[rel="next"][href]')
        url = next_link.get("href", "").strip() or None

    return {"pages": pages}


class DesMoinesArtCenterAdapter(BaseSourceAdapter):
    source_name = "des_moines_art_center_events"

    async def fetch(self) -> list[str]:
        payload = await load_des_moines_art_center_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_des_moines_art_center_payload(json.loads(payload))


def parse_des_moines_art_center_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for html in payload.get("pages", []):
        for event in _extract_events_from_html(html):
            row = _build_row(event)
            if row is None:
                continue
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _extract_events_from_html(html: str) -> list[dict]:
    match = EVENTS_JSON_RE.search(html)
    if match is None:
        return []
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []
    return [item for item in payload if isinstance(item, dict) and item.get("@type") == "Event"]


def _build_row(event: dict) -> ExtractedActivity | None:
    title = normalize_space(unescape(event.get("name") or ""))
    if not title:
        return None

    description = normalize_space(
        BeautifulSoup(unescape(event.get("description") or ""), "html.parser").get_text(" ", strip=True)
    )
    if not _should_keep(title, description):
        return None

    source_url = normalize_space(event.get("url")) or DMAC_CALENDAR_URL
    try:
        start_at = datetime.fromisoformat(event["startDate"])
    except (KeyError, TypeError, ValueError):
        return None
    end_at_raw = event.get("endDate")
    end_at = None
    if end_at_raw:
        try:
            end_at = datetime.fromisoformat(end_at_raw)
        except ValueError:
            end_at = None

    location_text = DMAC_LOCATION
    location = event.get("location")
    if isinstance(location, dict):
        address = location.get("address") or {}
        street = normalize_space(address.get("streetAddress"))
        locality = normalize_space(address.get("addressLocality"))
        region = normalize_space(address.get("addressRegion"))
        if street and locality and region:
            location_text = f"{street}, {locality}, {region}"

    age_min, age_max = _parse_age_range(title, description)
    full_description = description or None
    blob = f" {title.lower()} {description.lower()} "

    return ExtractedActivity(
        source_url=urljoin(DMAC_CALENDAR_URL, source_url),
        title=title,
        description=full_description,
        venue_name=DMAC_VENUE_NAME,
        location_text=location_text,
        city=DMAC_CITY,
        state=DMAC_STATE,
        activity_type=_infer_activity_type(title, description),
        age_min=age_min,
        age_max=age_max,
        drop_in="drop in" in blob or "drop-in" in blob,
        registration_required=" register " in blob or " ticket " in blob or " tickets " in blob,
        start_at=start_at,
        end_at=end_at,
        timezone=DMAC_TIMEZONE,
        **price_classification_kwargs(" ".join(part for part in [title, description] if part)),
    )


def _should_keep(title: str, description: str) -> bool:
    blob = f" {title.lower()} {description.lower()} "
    title_blob = f" {title.lower()} "
    has_include = any(marker in blob for marker in INCLUDE_MARKERS)
    has_exclude = any(marker in blob for marker in EXCLUDE_MARKERS)
    strong_include = any(
        marker in title_blob
        for marker in (
            "art workshop",
            "art spectrums",
            "creative aging",
            "social saturday",
            "youth art workshop",
        )
    )
    if has_exclude and not strong_include:
        return False
    return has_include


def _infer_activity_type(title: str, description: str) -> str:
    blob = f" {title.lower()} {description.lower()} "
    if "talk" in blob or "conversation" in blob or "aging" in blob or "brunch" in blob:
        return "talk"
    if "class" in blob:
        return "class"
    return "workshop"


def _parse_age_range(*parts: str | None) -> tuple[int | None, int | None]:
    text = " ".join(normalize_space(part) for part in parts if normalize_space(part))
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = AGE_PLUS_RE.search(text)
    if match:
        return int(match.group(1)), None
    return None, None
