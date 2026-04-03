from __future__ import annotations

from datetime import datetime
from html import unescape
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import DEFAULT_HEADERS
from src.crawlers.adapters.oh_common import AGE_PLUS_RE
from src.crawlers.adapters.oh_common import AGE_RANGE_RE
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

BLANDEN_CLASSES_EVENTS_URL = "https://www.blanden.org/classes-events"
BLANDEN_VENUE_NAME = "Blanden Memorial Art Museum"
BLANDEN_CITY = "Fort Dodge"
BLANDEN_STATE = "IA"
BLANDEN_TIMEZONE = "America/Chicago"
BLANDEN_LOCATION = "Blanden Memorial Art Museum, Fort Dodge, IA"

INCLUDE_MARKERS = (
    "art after school",
    "class",
    "classes",
    "create like",
    "create like a famous artist",
    "free art saturday",
    "kids create",
    "museum discoveries",
    "open studio",
)
EXCLUDE_MARKERS = (
    "camp",
    "fundraiser",
    "gala",
    "holiday open house",
    "hospice",
    "tour",
)


async def load_blanden_payload() -> dict:
    html = await fetch_html(BLANDEN_CLASSES_EVENTS_URL, referer=BLANDEN_CLASSES_EVENTS_URL)
    return {"html": html}


class BlandenAdapter(BaseSourceAdapter):
    source_name = "blanden_events"

    async def fetch(self) -> list[str]:
        payload = await load_blanden_payload()
        return [payload["html"]]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_blanden_html(payload)


def parse_blanden_html(html: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for title_node in soup.select('[data-hook="ev-list-item-title"]'):
        title = normalize_space(title_node.get_text(" ", strip=True))
        link_node = title_node.find_next('a', attrs={"data-hook": "ev-rsvp-button", "href": True})
        date_node = title_node.find_next(attrs={"data-hook": "date"})
        location_node = title_node.find_next(attrs={"data-hook": "location"})
        if not title or link_node is None or date_node is None:
            continue
        if not _should_keep(title):
            continue

        start_at, end_at = _parse_datetime_text(date_node.get_text(" ", strip=True))
        if start_at is None:
            continue

        source_url = urljoin(BLANDEN_CLASSES_EVENTS_URL, link_node.get("href", "").strip())
        location_text = normalize_space(location_node.get_text(" ", strip=True)) or BLANDEN_LOCATION
        age_min, age_max = _parse_age_range(title)
        description = normalize_space(
            " | ".join(
                part
                for part in [
                    location_text if location_text != BLANDEN_LOCATION else None,
                    date_node.get_text(" ", strip=True),
                ]
                if normalize_space(part)
            )
        ) or None

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=BLANDEN_VENUE_NAME,
                location_text=location_text,
                city=BLANDEN_CITY,
                state=BLANDEN_STATE,
                activity_type=_infer_activity_type(title),
                age_min=age_min,
                age_max=age_max,
                drop_in="free art saturday" in title.lower() or "open studio" in title.lower(),
                registration_required=False,
                start_at=start_at,
                end_at=end_at,
                timezone=BLANDEN_TIMEZONE,
                **price_classification_kwargs(unescape(title)),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _parse_datetime_text(value: str) -> tuple[datetime | None, datetime | None]:
    text = normalize_space(value).replace("–", "-").replace("—", "-")
    parts = [part.strip() for part in text.split(",")]
    if len(parts) < 3:
        return None, None

    try:
        base_date = datetime.strptime(", ".join(parts[:2]), "%b %d, %Y").date()
    except ValueError:
        return None, None

    time_text = parts[2]
    if "-" not in time_text:
        try:
            return datetime.strptime(
                f"{base_date.isoformat()} {time_text}",
                "%Y-%m-%d %I:%M %p",
            ), None
        except ValueError:
            return None, None

    start_text, end_text = [part.strip() for part in time_text.split("-", 1)]
    try:
        start_at = datetime.strptime(
            f"{base_date.isoformat()} {start_text}",
            "%Y-%m-%d %I:%M %p",
        )
        end_at = datetime.strptime(
            f"{base_date.isoformat()} {end_text}",
            "%Y-%m-%d %I:%M %p",
        )
    except ValueError:
        return None, None
    return start_at, end_at


def _parse_age_range(*parts: str | None) -> tuple[int | None, int | None]:
    text = " ".join(normalize_space(part) for part in parts if normalize_space(part))
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = AGE_PLUS_RE.search(text)
    if match:
        return int(match.group(1)), None
    return None, None


def _should_keep(title: str) -> bool:
    lowered = f" {title.lower()} "
    has_include = any(marker in lowered for marker in INCLUDE_MARKERS)
    has_exclude = any(marker in lowered for marker in EXCLUDE_MARKERS)
    return has_include and not has_exclude


def _infer_activity_type(title: str) -> str:
    lowered = title.lower()
    if "open studio" in lowered:
        return "activity"
    if "class" in lowered:
        return "class"
    return "workshop"
