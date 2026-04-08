import asyncio
import json
import re
from datetime import datetime
from html import unescape
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

JOSLYN_CALENDAR_URL = "https://joslyn.org/visit/calendar"
JOSLYN_EVENT_URL_PREFIX = "https://joslyn.org/events/"
JOSLYN_TIMEZONE = "America/Chicago"
JOSLYN_VENUE_NAME = "Joslyn Art Museum"
JOSLYN_CITY = "Omaha"
JOSLYN_STATE = "NE"
JOSLYN_DEFAULT_LOCATION = "Joslyn Art Museum, Omaha, NE"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": JOSLYN_CALENDAR_URL,
}

INCLUDE_MARKERS = (
    " activity ",
    " art-making ",
    " class ",
    " classes ",
    " workshop ",
    " workshops ",
    " studio ",
    " makers ",
    " drawing ",
    " clay ",
    " ceramics ",
    " comics ",
    " print ",
    " paint ",
    " lettering ",
    " collage ",
    " teaching artist ",
)
EXCLUDE_MARKERS = (
    " tour ",
    " tours ",
    " exhibition ",
    " film ",
    " performance ",
    " music ",
    " storytime ",
    " writing ",
    " reading ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)


async def fetch_joslyn_page(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
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
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Joslyn page") from last_exception
    raise RuntimeError("Unable to fetch Joslyn page after retries")


async def load_joslyn_payload() -> dict:
    listing_html = await fetch_joslyn_page(JOSLYN_CALENDAR_URL)
    events = _extract_listing_events(listing_html)
    events_to_fetch = _filter_listing_events_for_details(events)
    detail_pages: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        semaphore = asyncio.Semaphore(12)

        async def fetch_detail(event: dict) -> None:
            url = _event_url_from_slug(event.get("slug", ""))
            if not url:
                return
            async with semaphore:
                try:
                    detail_pages[url] = await fetch_joslyn_page(url, client=client)
                except Exception as exc:
                    print(f"[joslyn] warning: detail fetch failed for {url}: {exc}")

        await asyncio.gather(*(fetch_detail(event) for event in events_to_fetch))

    return {
        "events": events,
        "detail_pages": detail_pages,
    }


def parse_joslyn_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(JOSLYN_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    detail_pages = payload.get("detail_pages") or {}

    for event in payload.get("events") or []:
        slug = _normalize_space(event.get("slug"))
        source_url = _event_url_from_slug(slug)
        if not slug or not source_url:
            continue

        detail = _parse_detail_page(detail_pages.get(source_url, ""))
        title = detail.get("title") or _normalize_space(event.get("title"))
        description = detail.get("description")
        blob = _search_blob(" ".join(filter(None, [title, description])))
        if not _should_include_event(blob):
            continue

        start_at = _parse_datetime(detail.get("start_at") or event.get("start_at"))
        if start_at is None or start_at.date() < current_date:
            continue
        end_at = _parse_datetime(detail.get("end_at") or event.get("end_at"))
        age_min, age_max = _parse_age_range(" ".join(filter(None, [title, description])))

        row = ExtractedActivity(
            source_url=source_url,
            title=title,
            description=description,
            venue_name=JOSLYN_VENUE_NAME,
            location_text=detail.get("location_text") or JOSLYN_DEFAULT_LOCATION,
            city=JOSLYN_CITY,
            state=JOSLYN_STATE,
            activity_type=_infer_activity_type(blob),
            age_min=age_min,
            age_max=age_max,
            drop_in=("drop in" in blob or "drop-in" in blob),
            registration_required=(" register " in blob or " registration " in blob),
            start_at=start_at,
            end_at=end_at,
            timezone=JOSLYN_TIMEZONE,
            **price_classification_kwargs(description),
        )
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class JoslynEventsAdapter(BaseSourceAdapter):
    source_name = "joslyn_events"

    async def fetch(self) -> list[str]:
        payload = await load_joslyn_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_joslyn_payload(json.loads(payload))


def _extract_listing_events(listing_html: str) -> list[dict]:
    next_data = _extract_next_data(listing_html)
    if next_data is None:
        return []

    collected: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    stack = [next_data]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if current.get("__typename") == "Event":
                event_fields = current.get("eventFields") or {}
                key = (
                    _normalize_space(current.get("slug")),
                    _normalize_space(current.get("title")),
                    _normalize_space(event_fields.get("startDate")),
                )
                if key not in seen:
                    seen.add(key)
                    collected.append(
                        {
                            "slug": current.get("slug"),
                            "title": current.get("title"),
                            "short_description": event_fields.get("shortDescription"),
                            "tags": [_normalize_space(tag.get("name")) for tag in (current.get("tags") or []) if isinstance(tag, dict)],
                            "start_at": event_fields.get("startDate"),
                            "end_at": event_fields.get("endDate"),
                        }
                    )
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return collected


def _parse_detail_page(html: str) -> dict[str, str | None]:
    next_data = _extract_next_data(html)
    if next_data is None:
        return {"title": None, "description": None, "start_at": None, "end_at": None, "location_text": None}

    event_by = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("data", {})
        .get("eventBy", {})
    )
    event_fields = event_by.get("eventFields") or {}
    description_html = event_fields.get("description")
    description = _normalize_space(BeautifulSoup(description_html or "", "html.parser").get_text(" ", strip=True)) or None
    return {
        "title": _normalize_space(event_by.get("title")) or None,
        "description": description,
        "start_at": _normalize_space(event_fields.get("startDate")) or None,
        "end_at": _normalize_space(event_fields.get("endDate")) or None,
        "location_text": JOSLYN_DEFAULT_LOCATION,
    }


def _extract_next_data(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    node = soup.find("script", id="__NEXT_DATA__")
    if node is None:
        return None
    try:
        return json.loads(node.get_text())
    except Exception:
        return None


def _event_url_from_slug(slug: str) -> str:
    normalized = _normalize_space(slug)
    if not normalized:
        return ""
    return urljoin(JOSLYN_EVENT_URL_PREFIX, f"{normalized}/")


def _filter_listing_events_for_details(events: list[dict]) -> list[dict]:
    current_date = datetime.now(ZoneInfo(JOSLYN_TIMEZONE)).date()
    filtered: list[dict] = []
    seen: set[str] = set()

    for event in events:
        start_at = _parse_datetime(event.get("start_at"))
        if start_at is None or start_at.date() < current_date:
            continue
        slug = _normalize_space(event.get("slug"))
        if not slug or slug in seen:
            continue

        blob = _search_blob(
            " ".join(
                filter(
                    None,
                    [
                        event.get("title"),
                        event.get("short_description"),
                        " ".join(event.get("tags") or []),
                    ],
                )
            )
        )
        if not _should_include_event(blob):
            continue

        seen.add(slug)
        filtered.append(event)

    return filtered


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = _normalize_space(value)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _should_include_event(blob: str) -> bool:
    if _has_exclusion(blob):
        return False
    return any(marker in blob for marker in INCLUDE_MARKERS)


def _infer_activity_type(blob: str) -> str:
    if any(marker in blob for marker in (" lecture ", " talk ", " conversation ", " discussion ")):
        return "lecture"
    return "workshop"


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus = AGE_PLUS_RE.search(text)
    if plus:
        return int(plus.group(1)), None
    return None, None


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(unescape(value).split())


def _search_blob(value: str) -> str:
    normalized = _normalize_space(value).lower()
    return f" {normalized} "


def _has_exclusion(blob: str) -> bool:
    if re.search(r"\bcamp(?:s|ers)?\b", blob, re.IGNORECASE):
        return True
    return any(marker in blob for marker in EXCLUDE_MARKERS)
