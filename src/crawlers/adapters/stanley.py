from __future__ import annotations

import json
import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import AGE_PLUS_RE
from src.crawlers.adapters.oh_common import AGE_RANGE_RE
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

STANLEY_CALENDAR_URL = "https://stanleymuseum.uiowa.edu/programming-events/calendar"
STANLEY_VENUE_NAME = "Stanley Museum of Art"
STANLEY_CITY = "Iowa City"
STANLEY_STATE = "IA"
STANLEY_TIMEZONE = "America/Chicago"
STANLEY_LOCATION = "Stanley Museum of Art, Iowa City, IA"

SUBTITLE_RE = re.compile(
    r"^[A-Za-z]+,\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\s+(?P<time>.+)$"
)
INCLUDE_MARKERS = (
    "activity",
    "art-making",
    "brush pen calligraphy",
    "crafternoons",
    "craft",
    "embroidery workshop",
    "night at the museum",
    "school's out at the stanley",
    "student showcase",
    "workshop",
)
EXCLUDE_MARKERS = (
    "film",
    "music",
    "performance",
    "screening",
    "tour",
    "write at the stanley",
    "write night",
    "writing workshop",
    "writing",
)


async def load_stanley_payload(*, max_pages: int = 4) -> dict:
    pages: list[str] = []
    seen_urls: set[str] = set()
    next_url = STANLEY_CALENDAR_URL

    while next_url and len(pages) < max_pages and next_url not in seen_urls:
        seen_urls.add(next_url)
        html = await fetch_html(next_url, referer=STANLEY_CALENDAR_URL)
        pages.append(html)
        soup = BeautifulSoup(html, "html.parser")
        next_link = soup.select_one('.pager__item--next a[href]')
        next_url = urljoin(STANLEY_CALENDAR_URL, next_link.get("href", "").strip()) if next_link else None
    return {"pages": pages}


class StanleyAdapter(BaseSourceAdapter):
    source_name = "stanley_events"

    async def fetch(self) -> list[str]:
        payload = await load_stanley_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_stanley_payload(json.loads(payload))


def parse_stanley_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for html in payload.get("pages", []):
        soup = BeautifulSoup(html, "html.parser")
        for card in soup.select(".card"):
            row = _build_row(card)
            if row is None:
                continue
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _build_row(card: BeautifulSoup) -> ExtractedActivity | None:
    title = normalize_space(card.select_one("h2 .headline__heading").get_text(" ", strip=True) if card.select_one("h2 .headline__heading") else "")
    link = card.select_one("h2 a[href]")
    subtitle = normalize_space(card.select_one(".card__subtitle").get_text(" ", strip=True) if card.select_one(".card__subtitle") else "")
    meta = normalize_space(card.select_one(".card__meta").get_text(" ", strip=True) if card.select_one(".card__meta") else "")
    description = normalize_space(card.select_one(".card__description").get_text(" ", strip=True) if card.select_one(".card__description") else "")
    if not title or link is None or not subtitle:
        return None
    if not _should_keep(title, description, meta):
        return None

    match = SUBTITLE_RE.match(subtitle)
    if match is None:
        return None
    base_date = parse_date_text(match.group("date"))
    if base_date is None:
        return None
    start_at, end_at = parse_time_range(base_date=base_date, time_text=match.group("time"))
    if start_at is None:
        return None

    source_url = urljoin(STANLEY_CALENDAR_URL, link.get("href", "").strip())
    age_min, age_max = _parse_age_range(title, description)
    blob = f" {title.lower()} {description.lower()} {meta.lower()} "
    location_text = meta if meta and meta != "MERGE" else STANLEY_LOCATION

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description or None,
        venue_name=STANLEY_VENUE_NAME,
        location_text=location_text,
        city=STANLEY_CITY,
        state=STANLEY_STATE,
        activity_type=_infer_activity_type(title, description),
        age_min=age_min,
        age_max=age_max,
        drop_in=(
            "drop by" in blob
            or "drop-in" in blob
            or "drop in" in blob
            or "swing by" in blob
            or "stay for five minutes" in blob
        ),
        registration_required="reserve your spot" in blob or "register" in blob or "ticket" in blob,
        start_at=start_at,
        end_at=end_at,
        timezone=STANLEY_TIMEZONE,
        **price_classification_kwargs(description),
    )


def _should_keep(title: str, description: str, meta: str) -> bool:
    blob = f" {title.lower()} {description.lower()} {meta.lower()} "
    venue_related = "stanley museum of art" in meta.lower() or "stanley" in blob
    has_include = any(marker in blob for marker in INCLUDE_MARKERS)
    has_exclude = any(marker in blob for marker in EXCLUDE_MARKERS)
    strong_include = any(
        marker in blob
        for marker in (
            "crafternoons",
            "embroidery workshop",
            "night at the museum",
            "school's out at the stanley",
        )
    )
    if not venue_related:
        return False
    if has_exclude and not strong_include:
        return False
    return has_include


def _infer_activity_type(title: str, description: str) -> str:
    blob = f" {title.lower()} {description.lower()} "
    if "showcase" in blob:
        return "activity"
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
