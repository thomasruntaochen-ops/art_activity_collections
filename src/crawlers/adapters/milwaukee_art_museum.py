from __future__ import annotations

import asyncio
import html
import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

MAM_EVENTS_URL = "https://mam.org/events/"
MAM_EVENTS_API_URL = "https://mam.org/events/wp-json/tribe/events/v1/events"
MAM_TIMEZONE = "America/Chicago"
MAM_VENUE_NAME = "Milwaukee Art Museum"
MAM_CITY = "Milwaukee"
MAM_STATE = "WI"
MAM_LOCATION = "Milwaukee Art Museum, Milwaukee, WI"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": MAM_EVENTS_URL,
}

STRONG_INCLUDE_PATTERNS = (
    " art making ",
    " art-making ",
    " class ",
    " classes ",
    " draw ",
    " drop in art making ",
    " drop-in art making ",
    " family ",
    " families ",
    " gallery talk ",
    " lecture ",
    " lectures ",
    " play date ",
    " playdate ",
    " story time ",
    " storytime ",
    " talk ",
    " talks ",
    " teen ",
    " teens ",
    " workshop ",
    " workshops ",
    " youth ",
    " zine ",
)
TITLE_REJECT_PATTERNS = (
    " art in bloom ",
    " book sale ",
    " member mingle ",
    " member swap ",
    " museum closed ",
)
CATEGORY_REJECT_NAMES = {
    "announcements",
    "for members",
}
KIDS_AUDIENCE_PATTERNS = (
    " drop in art making ",
    " family ",
    " families ",
    " first stage ",
    " kohl s art studio ",
    " play date ",
    " playdate ",
    " story time ",
    " storytime ",
    " youth ",
    " youth + family ",
)
ADULT_AUDIENCE_PATTERNS = (
    " artist talk ",
    " expert series ",
    " gallery talk ",
    " haberman local luminaries ",
    " lecture ",
    " lectures ",
    " lectures + talks ",
    " local luminaries ",
    " slow art saturday ",
    " talk ",
    " talks ",
)
WEAK_INCLUDE_PATTERNS = (
    " art ",
    " artist ",
    " child ",
    " children ",
    " family ",
    " kid ",
    " kids ",
    " teen ",
    " teens ",
    " youth ",
)
REJECT_PATTERNS = (
    " camp ",
    " camps ",
    " celebration ",
    " gala ",
    " member preview ",
    " meditation ",
    " music ",
    " performance ",
    " performances ",
    " tour ",
    " tours ",
    " yoga ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
AGE_AND_UNDER_RE = re.compile(r"\bages?\s*(\d{1,2})\s*and under\b", re.IGNORECASE)


async def fetch_mam_events_page(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> dict:
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
                return response.json()

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Milwaukee Art Museum events endpoint") from last_exception
    raise RuntimeError("Unable to fetch Milwaukee Art Museum events endpoint after retries")


async def load_milwaukee_art_museum_payload(
    *,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, list[dict]]:
    events: list[dict] = []
    next_url = f"{MAM_EVENTS_API_URL}?per_page={per_page}"
    pages_seen = 0

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while next_url:
            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break
            payload = await fetch_mam_events_page(next_url, client=client)
            pages_seen += 1
            events.extend(payload.get("events") or [])
            next_url = payload.get("next_rest_url")

    return {"events": events}


class MilwaukeeArtMuseumAdapter(BaseSourceAdapter):
    source_name = "milwaukee_art_museum_events"

    async def fetch(self) -> list[str]:
        payload = await load_milwaukee_art_museum_payload(page_limit=1)
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_milwaukee_art_museum_payload(json.loads(payload))


def parse_milwaukee_art_museum_payload(payload: dict) -> list[ExtractedActivity]:
    today = datetime.now(ZoneInfo(MAM_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        row = _build_row(event_obj)
        if row is None or row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = normalize_space(html.unescape(event_obj.get("title") or ""))
    source_url = normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("end_date"))
    description = join_non_empty(
        [
            _html_to_text(event_obj.get("excerpt")),
            _html_to_text(event_obj.get("description")),
        ]
    )
    categories = ", ".join(
        normalize_space(category.get("name"))
        for category in event_obj.get("categories") or []
        if normalize_space(category.get("name"))
    )

    if not _should_include_event(title=title, description=description, categories=categories):
        return None

    blob = _searchable_blob(" ".join(part for part in [title, description or "", categories] if part))
    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))
    drop_in = " drop in " in blob
    venue = event_obj.get("venue_details") or {}
    location_text = (
        join_non_empty(
            [
                normalize_space(venue.get("venue")),
                normalize_space(venue.get("city")),
                normalize_space(venue.get("state")),
            ]
        )
        or MAM_LOCATION
    )
    pricing_text = join_non_empty(
        [
            description,
            categories,
            _cost_text(event_obj.get("cost")),
            _structured_text(event_obj.get("cost_details")),
        ]
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=join_non_empty([description, f"Categories: {categories}" if categories else None]),
        venue_name=MAM_VENUE_NAME,
        location_text=location_text,
        city=MAM_CITY,
        state=MAM_STATE,
        activity_type=infer_activity_type(title, description, categories),
        age_min=age_min,
        age_max=age_max,
        drop_in=drop_in,
        registration_required=(
            (" registration " in blob)
            or (" register " in blob)
            or (" reserve " in blob)
            or (" ticket " in blob)
            or (" tickets " in blob)
        ) and not drop_in,
        start_at=start_at,
        end_at=end_at,
        timezone=MAM_TIMEZONE,
        audience_segment=_infer_mam_audience(
            title=title,
            description=description,
            category=categories,
            source_url=source_url,
            age_min=age_min,
            age_max=age_max,
        ),
        **price_classification_kwargs(pricing_text),
    )


def _infer_mam_audience(
    *,
    title: str,
    description: str | None,
    category: str | None,
    source_url: str,
    age_min: int | None,
    age_max: int | None,
) -> str:
    age_segment = infer_audience_segment(age_min=age_min, age_max=age_max)
    if age_segment != "unknown":
        return age_segment

    title_blob = _searchable_blob(title)
    category_blob = _searchable_blob(category or "")
    title_category_blob = _searchable_blob(" ".join(part for part in [title, category or ""] if part))

    if any(pattern in title_category_blob for pattern in KIDS_AUDIENCE_PATTERNS):
        return "kids"
    if any(pattern in title_blob for pattern in ADULT_AUDIENCE_PATTERNS) or any(
        pattern in category_blob for pattern in ADULT_AUDIENCE_PATTERNS
    ):
        return "adults"

    return infer_audience_segment(
        title=title,
        description=description,
        category=category,
        source_url=source_url,
    )


def _should_include_event(*, title: str, description: str | None, categories: str | None) -> bool:
    title_blob = _searchable_blob(title)
    blob = _searchable_blob(" ".join(part for part in [title, description or "", categories or ""] if part))
    if any(pattern in title_blob for pattern in TITLE_REJECT_PATTERNS):
        return False
    if _has_reject_category(categories):
        return False
    if any(pattern in blob for pattern in REJECT_PATTERNS):
        return False
    if any(pattern in blob for pattern in STRONG_INCLUDE_PATTERNS):
        return True
    if " youth + family " in blob or " youth family " in blob:
        return True
    return any(pattern in blob for pattern in WEAK_INCLUDE_PATTERNS) and " art " in blob


def _has_reject_category(categories: str | None) -> bool:
    if not categories:
        return False
    names = {normalize_space(part).lower() for part in categories.split(",")}
    return bool(names.intersection(CATEGORY_REJECT_NAMES))


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    under_match = AGE_AND_UNDER_RE.search(text)
    if under_match:
        return None, int(under_match.group(1))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _parse_datetime(value: str | None) -> datetime | None:
    normalized = normalize_space(value)
    if not normalized:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _html_to_text(value: str | None) -> str | None:
    if not value:
        return None
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    normalized = normalize_space(html.unescape(text))
    return normalized or None


def _cost_text(value: dict | None) -> str | None:
    if not isinstance(value, dict):
        return None
    values = value.get("values")
    if isinstance(values, list) and values:
        return ", ".join(str(entry) for entry in values if normalize_space(str(entry)))
    return None


def _structured_text(value: object) -> str | None:
    if isinstance(value, str):
        return normalize_space(value) or None
    if isinstance(value, list):
        parts = [_structured_text(entry) for entry in value]
        return join_non_empty([part for part in parts if part])
    if isinstance(value, dict):
        parts = [_structured_text(entry) for entry in value.values()]
        return join_non_empty([part for part in parts if part])
    if value is None:
        return None
    return normalize_space(str(value)) or None


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9+]+", " ", value.lower())
    return f" {normalize_space(normalized)} "
