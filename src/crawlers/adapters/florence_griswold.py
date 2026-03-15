import asyncio
import json
import re
from datetime import datetime
from datetime import timedelta
from html import unescape
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

FLORENCE_GRISWOLD_EVENTS_PAGE_URL = "https://flogris.org/calendar/"
FLORENCE_GRISWOLD_EVENTS_API_URL = "https://flogris.org/wp-json/tribe/events/v1/events"

NY_TIMEZONE = "America/New_York"
FLORENCE_GRISWOLD_VENUE_NAME = "Florence Griswold Museum"
FLORENCE_GRISWOLD_CITY = "Old Lyme"
FLORENCE_GRISWOLD_STATE = "CT"
FLORENCE_GRISWOLD_DEFAULT_LOCATION = "96 Lyme Street, Old Lyme, CT 06371"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": FLORENCE_GRISWOLD_EVENTS_PAGE_URL,
}

INCLUDED_KEYWORDS = (
    "lecture",
    "virtual lecture",
    "talk",
    "conversation",
    "discussion",
    "workshop",
    "class",
    "classes",
    "course",
    "studies",
    "art bar",
    "painting",
    "printmaking",
    "ceramics",
    "mosaic",
    "photography",
)
TITLE_EXCLUDED_KEYWORDS = (
    "camp",
    "film",
    "sale",
    "luncheon",
    "day trip",
    "concert",
    "festival",
    "gardens day",
    "blooms with a view",
    "miss florence's tea",
    "tour",
    "gala",
    "reception",
)
TEXT_EXCLUDED_KEYWORDS = (
    "camp",
    "film screening",
    "live music",
    "poetry reading",
    "garden tour",
    "scavenger hunt",
    "fundraiser",
)

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
TRAILING_NUMERIC_SUFFIX_RE = re.compile(r"\s+#\d+\s*$")


async def fetch_florence_griswold_events_json(
    url: str,
    *,
    params: dict[str, object] | None = None,
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
                response = await client.get(url, params=params)
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
        raise RuntimeError("Unable to fetch Florence Griswold Museum events JSON") from last_exception
    raise RuntimeError("Unable to fetch Florence Griswold Museum events JSON after retries")


def _default_start_date() -> str:
    return datetime.now(ZoneInfo(NY_TIMEZONE)).date().isoformat()


async def load_florence_griswold_events_payload(
    *,
    start_date: str | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
    window_days: int = 365,
) -> dict:
    resolved_start_date = start_date or _default_start_date()
    start_value = datetime.strptime(resolved_start_date, "%Y-%m-%d")
    end_value = start_value + timedelta(days=window_days)
    params = {
        "start_date": resolved_start_date,
        "end_date": end_value.strftime("%Y-%m-%d"),
        "per_page": per_page,
    }

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        pages: list[dict] = [
            await fetch_florence_griswold_events_json(
                FLORENCE_GRISWOLD_EVENTS_API_URL,
                params=params,
                client=client,
            )
        ]
        next_url = pages[0].get("next_rest_url")

        while next_url and (page_limit is None or len(pages) < max(page_limit, 1)):
            pages.append(await fetch_florence_griswold_events_json(str(next_url), client=client))
            next_url = pages[-1].get("next_rest_url")

    return {"pages": pages, "start_date": resolved_start_date}


def parse_florence_griswold_events_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for page in payload["pages"]:
        for event_obj in page.get("events", []):
            row = _build_row_from_event(event_obj)
            if row is None:
                continue
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class FlorenceGriswoldEventsAdapter(BaseSourceAdapter):
    source_name = "florence_griswold_events"

    async def fetch(self) -> list[str]:
        payload = await load_florence_griswold_events_payload(page_limit=1)
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_florence_griswold_events_payload(json.loads(payload))


def _build_row_from_event(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(unescape(str(event_obj.get("title") or "")))
    description = _clean_event_text(str(event_obj.get("description") or ""))
    excerpt = _clean_event_text(str(event_obj.get("excerpt") or ""))
    category_names = _category_names(event_obj.get("categories") or [])

    combined_parts: list[str] = []
    for part in (description, excerpt):
        if part and part not in combined_parts:
            combined_parts.append(part)
    if category_names:
        combined_parts.append(f"Categories: {', '.join(category_names)}")
    combined_description = " | ".join(combined_parts)

    if not _should_include_event(title=title, description=combined_description, category_names=category_names):
        return None

    source_url = urljoin(FLORENCE_GRISWOLD_EVENTS_PAGE_URL, str(event_obj.get("url") or "").strip())
    if not source_url:
        return None

    start_at = _parse_datetime(event_obj.get("start_date"))
    if start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("end_date"))

    venue = event_obj.get("venue") or {}
    location_text = _build_location_text(venue)
    text_blob = _normalize_space(" ".join([title, combined_description])).lower()
    age_min, age_max = _parse_age_range(text_blob)
    is_free, free_status = infer_price_classification(text_blob)

    return ExtractedActivity(
        source_url=source_url,
        title=TRAILING_NUMERIC_SUFFIX_RE.sub("", title),
        description=combined_description or None,
        venue_name=FLORENCE_GRISWOLD_VENUE_NAME,
        location_text=location_text,
        city=_normalize_space(str(venue.get("city") or FLORENCE_GRISWOLD_CITY)),
        state=_normalize_space(str(venue.get("stateprovince") or venue.get("province") or FLORENCE_GRISWOLD_STATE)),
        activity_type=_infer_activity_type(title=title, description=combined_description),
        age_min=age_min,
        age_max=age_max,
        drop_in=_is_drop_in(text_blob),
        registration_required=_has_registration_signal(text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _should_include_event(*, title: str, description: str, category_names: list[str]) -> bool:
    normalized_title = title.lower()
    normalized_description = description.lower()
    category_blob = " ".join(category_names).lower()
    text_blob = _normalize_space(f"{normalized_title} {normalized_description} {category_blob}")
    if not text_blob:
        return False

    if any(keyword in normalized_title for keyword in TITLE_EXCLUDED_KEYWORDS):
        return False
    if any(keyword in text_blob for keyword in TEXT_EXCLUDED_KEYWORDS):
        return False

    return any(keyword in text_blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(*, title: str, description: str) -> str:
    text_blob = _normalize_space(f"{title} {description}").lower()
    if any(keyword in text_blob for keyword in ("lecture", "talk", "conversation", "discussion")):
        return "talk"
    if any(keyword in text_blob for keyword in ("class", "course", "studies")):
        return "class"
    return "workshop"


def _has_registration_signal(text_blob: str) -> bool:
    return any(
        keyword in text_blob
        for keyword in (
            "register",
            "registration",
            "purchase tickets",
            "tickets here",
            "ticket here",
            "book now",
            "limited enrollment",
            "zoom",
        )
    )


def _is_drop_in(text_blob: str) -> bool:
    return any(
        keyword in text_blob
        for keyword in (
            "drop-in",
            "drop in",
            "no advance reservations required",
            "no registration required",
        )
    )


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))

    match = AGE_PLUS_RE.search(text)
    if match:
        return int(match.group(1)), None

    return None, None


def _build_location_text(venue: dict) -> str:
    if not isinstance(venue, dict) or not venue:
        return FLORENCE_GRISWOLD_DEFAULT_LOCATION

    state = _normalize_space(str(venue.get("stateprovince") or venue.get("province") or FLORENCE_GRISWOLD_STATE))
    zip_code = _normalize_space(str(venue.get("zip") or ""))
    state_zip = " ".join(part for part in (state, zip_code) if part)
    parts = [
        _normalize_space(str(venue.get("address") or "")),
        _normalize_space(str(venue.get("city") or "")),
        state_zip,
    ]
    return ", ".join(part for part in parts if part) or FLORENCE_GRISWOLD_DEFAULT_LOCATION


def _category_names(categories: list[object]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for category in categories:
        if not isinstance(category, dict):
            continue
        name = _normalize_space(str(category.get("name") or ""))
        if not name:
            continue
        lowered = name.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        names.append(name)
    return names


def _clean_event_text(html: str) -> str:
    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")
    lines: list[str] = []
    seen: set[str] = set()

    for raw_line in soup.get_text("\n").splitlines():
        line = _normalize_space(unescape(raw_line))
        if not line:
            continue

        lowered = line.lower()
        if lowered.startswith("the museum’s educational programming is supported by"):
            break
        if lowered.startswith("the museum's educational programming is supported by"):
            break
        if lowered.startswith("educational & travel program refund policy"):
            break

        if lowered in seen:
            continue
        seen.add(lowered)
        lines.append(line)

    return " ".join(lines)


def _normalize_space(value: str) -> str:
    return " ".join(value.split())
