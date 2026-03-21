import asyncio
import html
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

WCMFA_EVENTS_URL = "https://wcmfa.org/events/"
WCMFA_EVENTS_API_URL = "https://wcmfa.org/wp-json/tribe/events/v1/events"
WCMFA_TIMEZONE = "America/New_York"
WCMFA_VENUE_NAME = "Washington County Museum of Fine Arts"
WCMFA_CITY = "Hagerstown"
WCMFA_STATE = "MD"
WCMFA_DEFAULT_LOCATION = "Hagerstown, MD"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": WCMFA_EVENTS_URL,
}

STRONG_EXCLUDE_PATTERNS = (
    " camp ",
    " fundraiser ",
    " fundraising ",
    " last day ",
    " preview party ",
    " yoga ",
)
SOFT_INCLUDE_PATTERNS = (
    " author talk ",
    " class ",
    " classes ",
    " lecture ",
    " lectures ",
    " talk ",
    " workshop ",
    " workshops ",
)
ART_RELEVANCE_PATTERNS = (
    " artist ",
    " art ",
    " bloom ",
    " gallery ",
    " museum ",
    " painting ",
    " sketch ",
)
ENROLLMENT_RE = re.compile(r"\benrollment opens?\b", re.IGNORECASE)


async def fetch_wcmfa_events_page(
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
        raise RuntimeError("Unable to fetch WCMFA events endpoint") from last_exception
    raise RuntimeError("Unable to fetch WCMFA events endpoint after retries")


async def load_wcmfa_events_payload(
    *,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, list[dict]]:
    events: list[dict] = []
    next_url = f"{WCMFA_EVENTS_API_URL}?per_page={per_page}"
    pages_seen = 0

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while next_url:
            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break
            payload = await fetch_wcmfa_events_page(next_url, client=client)
            pages_seen += 1
            events.extend(payload.get("events") or [])
            next_url = payload.get("next_rest_url")

    return {"events": events}


class WcmfaEventsAdapter(BaseSourceAdapter):
    source_name = "wcmfa_events"

    async def fetch(self) -> list[str]:
        payload = await load_wcmfa_events_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_wcmfa_events_payload/parse_wcmfa_events_payload from script runner.")


def parse_wcmfa_events_payload(payload: dict) -> list[ExtractedActivity]:
    current_day = datetime.now().date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        row = _build_row(event_obj)
        if row is None or row.start_at.date() < current_day:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(html.unescape(event_obj.get("title") or ""))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("end_date"))
    description_text = _html_to_text(event_obj.get("description"))
    excerpt_text = _html_to_text(event_obj.get("excerpt"))
    description_parts = [part for part in [excerpt_text, description_text] if part]
    description = " | ".join(description_parts) if description_parts else None

    if not _should_include_event(title=title, description=description):
        return None

    blob = _searchable_blob(" ".join([title, description or ""]))

    return ExtractedActivity(
        source_url=urljoin(WCMFA_EVENTS_URL, source_url),
        title=title,
        description=description,
        venue_name=WCMFA_VENUE_NAME,
        location_text=WCMFA_DEFAULT_LOCATION,
        city=WCMFA_CITY,
        state=WCMFA_STATE,
        activity_type=_infer_activity_type(blob),
        age_min=None,
        age_max=None,
        drop_in=(" drop-in " in blob or " drop in " in blob),
        registration_required=(
            (" registration required " in blob)
            or (" application required " in blob)
            or (" register " in blob)
        ) and " no registration required " not in blob,
        start_at=start_at,
        end_at=end_at,
        timezone=WCMFA_TIMEZONE,
        **price_classification_kwargs(
            description,
            default_is_free=True if " free " in blob else None,
        ),
    )


def _should_include_event(*, title: str, description: str | None) -> bool:
    blob = _searchable_blob(" ".join([title, description or ""]))
    if ENROLLMENT_RE.search(title) or ENROLLMENT_RE.search(description or ""):
        return False
    if any(pattern in blob for pattern in STRONG_EXCLUDE_PATTERNS):
        return False
    has_include = any(pattern in blob for pattern in SOFT_INCLUDE_PATTERNS)
    if not has_include:
        return False
    if " celebration " in blob and " author talk " not in blob and " lecture " not in blob and " talk " not in blob:
        return False
    if " reception " in blob and " author talk " not in blob and " lecture " not in blob and " talk " not in blob:
        return False
    if " exhibition " in blob and " author talk " not in blob and " lecture " not in blob and " talk " not in blob:
        return False
    return any(pattern in blob for pattern in ART_RELEVANCE_PATTERNS)


def _infer_activity_type(blob: str) -> str:
    if " lecture " in blob or " talk " in blob:
        return "lecture"
    return "workshop"


def _parse_datetime(value: str | None) -> datetime | None:
    normalized = _normalize_space(value)
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
    normalized = _normalize_space(html.unescape(text))
    return normalized or None


def _normalize_space(value: str | None) -> str:
    return " ".join((value or "").split())


def _searchable_blob(value: str) -> str:
    return f" {_normalize_space(value).lower()} "
