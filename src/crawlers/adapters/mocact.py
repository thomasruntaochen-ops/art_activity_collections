import asyncio
import json
from datetime import datetime
from datetime import timedelta
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

MOCACT_EVENTS_PAGE_URL = "https://mocact.org/events/#calendar"
MOCACT_EVENTS_API_URL = "https://mocact.org/wp-json/tribe/events/v1/events"

NY_TIMEZONE = "America/New_York"
MOCACT_VENUE_NAME = "MoCA CT"
MOCACT_CITY = "Westport"
MOCACT_STATE = "CT"
MOCACT_DEFAULT_LOCATION = "Westport, CT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": MOCACT_EVENTS_PAGE_URL,
}

INCLUDED_KEYWORDS = (
    "talk",
    "lecture",
    "conversation",
    "discussion",
    "workshop",
    "class",
    "art class",
    "activity",
    "lab",
    "gallery talk",
)
TITLE_EXCLUDED_KEYWORDS = (
    "tour",
    "camp",
    "storytime",
    "reception",
    "film",
    "screening",
    "concert",
    "session",
    "quartet",
    "solo piano",
    "writers workshop",
    "writing",
    "members only",
)
ADULT_ONLY_KEYWORDS = (
    "adult workshop",
    "for adults",
    "adults only",
)
AGE_RANGE_MARKERS = (
    "ages ",
    "age ",
)


async def fetch_mocact_events_json(
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
        raise RuntimeError("Unable to fetch MoCA CT events JSON") from last_exception
    raise RuntimeError("Unable to fetch MoCA CT events JSON after retries")


def _default_start_date() -> str:
    return datetime.now(ZoneInfo(NY_TIMEZONE)).date().isoformat()


async def load_mocact_events_payload(
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
        pages: list[dict] = [await fetch_mocact_events_json(MOCACT_EVENTS_API_URL, params=params, client=client)]
        next_url = pages[0].get("next_rest_url")

        while next_url and (page_limit is None or len(pages) < max(page_limit, 1)):
            pages.append(await fetch_mocact_events_json(str(next_url), client=client))
            next_url = pages[-1].get("next_rest_url")

    return {"pages": pages}


def parse_mocact_events_payload(payload: dict) -> list[ExtractedActivity]:
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


class MoCACTEventsAdapter(BaseSourceAdapter):
    source_name = "mocact_events"

    async def fetch(self) -> list[str]:
        payload = await load_mocact_events_payload(page_limit=1)
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_mocact_events_payload(json.loads(payload))


def _build_row_from_event(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(str(event_obj.get("title") or ""))
    description = _html_to_text(str(event_obj.get("description") or ""))
    excerpt = _html_to_text(str(event_obj.get("excerpt") or ""))
    combined_description = " | ".join(part for part in [description, excerpt] if part)

    if not _should_include_event(title=title, description=combined_description):
        return None

    source_url = urljoin(MOCACT_EVENTS_PAGE_URL, str(event_obj.get("url") or "").strip())
    if not source_url:
        return None

    start_at = _parse_datetime(event_obj.get("start_date"))
    if start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("end_date"))

    venue = event_obj.get("venue") or {}
    location_text = _build_location_text(venue)
    text_blob = " ".join(part for part in [title, combined_description] if part).lower()
    age_min, age_max = _parse_age_range(combined_description)
    is_free, free_status = infer_price_classification(text_blob)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=combined_description or None,
        venue_name=MOCACT_VENUE_NAME,
        location_text=location_text,
        city=_normalize_space(str(venue.get("city") or MOCACT_CITY)),
        state=_normalize_space(str(venue.get("stateprovince") or venue.get("state") or MOCACT_STATE)),
        activity_type=_infer_activity_type(title=title, description=combined_description),
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=_has_registration_signal(event_obj=event_obj, text_blob=text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _should_include_event(*, title: str, description: str) -> bool:
    normalized_title = title.lower()
    normalized_description = description.lower()
    text_blob = _normalize_space(f"{normalized_title} {normalized_description}")
    if not text_blob:
        return False

    if any(keyword in normalized_title for keyword in TITLE_EXCLUDED_KEYWORDS):
        return False
    if any(keyword in text_blob for keyword in ADULT_ONLY_KEYWORDS):
        return False

    return any(keyword in text_blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(*, title: str, description: str) -> str:
    text_blob = _normalize_space(f"{title} {description}").lower()
    if any(keyword in text_blob for keyword in ("talk", "lecture", "conversation", "discussion", "gallery talk")):
        return "talk"
    return "workshop"


def _has_registration_signal(*, event_obj: dict, text_blob: str) -> bool:
    if any(keyword in text_blob for keyword in ("register here", "register", "book now", "checkout")):
        return True

    description_html = str(event_obj.get("description") or "")
    return "checkout.mocact.org" in description_html.lower()


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


def _parse_age_range(description: str) -> tuple[int | None, int | None]:
    lowered = description.lower()
    for marker in AGE_RANGE_MARKERS:
        start = lowered.find(marker)
        if start == -1:
            continue
        fragment = lowered[start : start + 24]
        digits = [part for part in fragment.replace("+", " + ").replace("-", " - ").split() if part.isdigit()]
        if "+" in fragment and digits:
            return int(digits[0]), None
        if len(digits) >= 2:
            return int(digits[0]), int(digits[1])
    return None, None


def _build_location_text(venue: dict) -> str:
    state = _normalize_space(str(venue.get("stateprovince") or venue.get("state") or MOCACT_STATE))
    parts = [
        _normalize_space(str(venue.get("address") or "")),
        _normalize_space(str(venue.get("city") or "")),
        state,
    ]
    cleaned = [part for part in parts if part]
    return ", ".join(cleaned) if cleaned else MOCACT_DEFAULT_LOCATION


def _html_to_text(value: str) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    lines = [_normalize_space(line) for line in soup.get_text("\n").splitlines()]
    return " | ".join(line for line in lines if line)


def _normalize_space(value: str) -> str:
    return " ".join(value.split())
