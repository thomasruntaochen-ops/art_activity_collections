from __future__ import annotations

import json
from datetime import datetime

import httpx

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import DEFAULT_HEADERS
from src.crawlers.adapters.oh_common import AGE_PLUS_RE
from src.crawlers.adapters.oh_common import AGE_RANGE_RE
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

IASTATE_CALENDAR_API_URL = (
    "https://www.museums.iastate.edu/api/events?taxonomy-and-or=or&cat-merge=17,26,22,21,25,19,46,24,23,20"
)
IASTATE_VENUE_NAME = "University Museums - Iowa State University"
IASTATE_CITY = "Ames"
IASTATE_STATE = "IA"
IASTATE_TIMEZONE = "America/Chicago"
IASTATE_LOCATION = "University Museums, Ames, IA"

EXCLUDED_TAGS = {
    "art walks",
    "closings",
    "films & media",
    "tours",
}
INCLUDED_TAGS = {
    "family programs",
    "lectures & talks",
    "workshops",
}
INCLUDE_MARKERS = (
    "activity",
    "craft",
    "discussion",
    "explore!",
    "gallery talk",
    "lecture",
    "talk",
    "workshop",
)
EXCLUDE_MARKERS = (
    "film",
    "screening",
    "tour",
    "walk",
)


async def load_university_museums_iastate_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        response = await client.get(IASTATE_CALENDAR_API_URL)
        response.raise_for_status()
        return response.json()


class UniversityMuseumsIastateAdapter(BaseSourceAdapter):
    source_name = "university_museums_iastate_events"

    async def fetch(self) -> list[str]:
        payload = await load_university_museums_iastate_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_university_museums_iastate_payload(json.loads(payload))


def parse_university_museums_iastate_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in payload.get("events", []):
        row = _build_row(item)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _build_row(item: dict) -> ExtractedActivity | None:
    title = normalize_space(item.get("title"))
    description = normalize_space(item.get("description"))
    source_url = normalize_space(item.get("url"))
    if not title or not source_url:
        return None

    tag_names = [normalize_space(tag.get("name")) for tag in item.get("eventTags", []) if isinstance(tag, dict)]
    if not _should_keep(title, description, tag_names):
        return None

    try:
        start_at = datetime.fromisoformat(item["start"])
    except (KeyError, TypeError, ValueError):
        return None

    end_at = None
    end_raw = item.get("end")
    if end_raw:
        try:
            end_at = datetime.fromisoformat(end_raw)
        except ValueError:
            end_at = None
    if end_at is not None and (end_at - start_at).total_seconds() > 12 * 3600:
        end_at = None

    location_text = normalize_space(item.get("location")) or IASTATE_LOCATION
    age_min, age_max = _parse_age_range(title, description)
    full_description = normalize_space(
        " | ".join(
            part
            for part in [
                description,
                f"Tags: {', '.join(tag_names)}" if tag_names else None,
            ]
            if normalize_space(part)
        )
    ) or None
    blob = f" {title.lower()} {description.lower()} "

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=IASTATE_VENUE_NAME,
        location_text=location_text,
        city=IASTATE_CITY,
        state=IASTATE_STATE,
        activity_type=_infer_activity_type(title, description, tag_names),
        age_min=age_min,
        age_max=age_max,
        drop_in="drop by" in blob or "drop-in" in blob or "come-and-go" in blob,
        registration_required="register" in blob or "ticket" in blob or "rsvp" in blob,
        start_at=start_at,
        end_at=end_at,
        timezone=IASTATE_TIMEZONE,
        **price_classification_kwargs(" ".join(part for part in [description, " ".join(tag_names)] if part)),
    )


def _should_keep(title: str, description: str, tag_names: list[str]) -> bool:
    tag_set = {tag.lower() for tag in tag_names}
    if tag_set & EXCLUDED_TAGS and not (tag_set & INCLUDED_TAGS):
        return False
    blob = f" {title.lower()} {description.lower()} {' '.join(tag.lower() for tag in tag_names)} "
    has_include = bool(tag_set & INCLUDED_TAGS) or any(marker in blob for marker in INCLUDE_MARKERS)
    has_exclude = bool(tag_set & EXCLUDED_TAGS) or any(marker in blob for marker in EXCLUDE_MARKERS)
    return has_include and not (has_exclude and not has_include)


def _infer_activity_type(title: str, description: str, tag_names: list[str]) -> str:
    blob = f" {title.lower()} {description.lower()} {' '.join(tag.lower() for tag in tag_names)} "
    if "lecture" in blob or "talk" in blob:
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
