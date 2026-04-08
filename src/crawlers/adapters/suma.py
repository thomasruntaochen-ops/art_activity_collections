import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

SUMA_CALENDAR_URL = "https://events.suu.edu/ma/calendar"

SUMA_TIMEZONE = "America/Denver"
SUMA_VENUE_NAME = "Southern Utah Museum of Art"
SUMA_CITY = "Cedar City"
SUMA_STATE = "UT"
SUMA_DEFAULT_LOCATION = "Southern Utah Museum of Art, Cedar City, UT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SUMA_CALENDAR_URL,
}

EVENT_URL_RE = re.compile(r"^https://events\.suu\.edu/event/[^/?#]+/?$", re.IGNORECASE)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|\u2013|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

EXPLICIT_INCLUDE_PATTERNS = (
    " first friday ",
    " create playdate ",
    " educator ",
    " family ",
    " gallery talk ",
    " open studio ",
    " talk ",
    " wonder wednesday ",
    " workshop ",
)
EXPLICIT_EXCLUDE_PATTERNS = (
    " exhibition ",
    " members ",
    " storytime ",
    " opening reception ",
    " pickup ",
    " pick-up ",
    " reception ",
    " submit an event ",
    " tour ",
)


@dataclass(frozen=True, slots=True)
class SumaEventStub:
    source_url: str
    title: str


async def fetch_suma_page(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers=DEFAULT_HEADERS,
            verify=False,
        )

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
        raise RuntimeError("Unable to fetch SUMA events page") from last_exception
    raise RuntimeError("Unable to fetch SUMA events page after retries")


async def load_suma_payload() -> dict:
    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers=DEFAULT_HEADERS,
        verify=False,
    ) as client:
        list_html = await fetch_suma_page(SUMA_CALENDAR_URL, client=client)
        stubs = _extract_stubs(list_html)
        detail_urls = [stub.source_url for stub in stubs]
        detail_htmls = await asyncio.gather(*(fetch_suma_page(url, client=client) for url in detail_urls))
        detail_pages = {url: html for url, html in zip(detail_urls, detail_htmls, strict=True)}

    return {
        "list_html": list_html,
        "stubs": [
            {
                "source_url": stub.source_url,
                "title": stub.title,
            }
            for stub in stubs
        ],
        "detail_pages": detail_pages,
    }


def parse_suma_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(SUMA_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    detail_pages: dict[str, str] = payload.get("detail_pages") or {}

    for raw_stub in payload.get("stubs") or []:
        stub = SumaEventStub(
            source_url=raw_stub.get("source_url", ""),
            title=raw_stub.get("title", ""),
        )
        detail_html = detail_pages.get(stub.source_url)
        if not detail_html:
            continue
        row = _build_row(stub=stub, html=detail_html)
        if row is None or row.start_at.date() < current_date:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class SumaEventsAdapter(BaseSourceAdapter):
    source_name = "suma_events"

    async def fetch(self) -> list[str]:
        payload = await load_suma_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_suma_payload(json.loads(payload))


def _extract_stubs(html: str) -> list[SumaEventStub]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    stubs: list[SumaEventStub] = []

    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if "/confirm?" in href:
            continue
        if not EVENT_URL_RE.match(href):
            continue
        title = _normalize_space(anchor.get_text(" ", strip=True))
        if not title or href in seen:
            continue
        seen.add(href)
        stubs.append(SumaEventStub(source_url=href, title=title))

    return stubs


def _build_row(*, stub: SumaEventStub, html: str) -> ExtractedActivity | None:
    event_obj = _extract_event_object(html)
    if event_obj is None:
        return None

    title = _normalize_space(str(event_obj.get("name") or stub.title or ""))
    description = _normalize_space(str(event_obj.get("description") or ""))
    blob = _searchable_blob(" ".join([stub.title, title, description]))
    if not _should_include_event(blob):
        return None

    start_at = _parse_iso_datetime(event_obj.get("startDate"))
    if start_at is None:
        return None
    end_at = _parse_iso_datetime(event_obj.get("endDate"))

    location = event_obj.get("location")
    location_text = _extract_location_text(location) or SUMA_DEFAULT_LOCATION
    age_min, age_max = _parse_age_range(description)

    is_free, free_status = infer_price_classification(
        description,
        default_is_free=True if " free " in blob else None,
    )

    return ExtractedActivity(
        source_url=urljoin(SUMA_CALENDAR_URL, str(event_obj.get("url") or stub.source_url)),
        title=title,
        description=description or None,
        venue_name=SUMA_VENUE_NAME,
        location_text=location_text,
        city=SUMA_CITY,
        state=SUMA_STATE,
        activity_type=_infer_activity_type(blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=(" drop in " in blob or " drop-in " in blob),
        registration_required=(" register " in blob or " rsvp " in blob or " ticket " in blob),
        start_at=start_at,
        end_at=end_at,
        timezone=SUMA_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_event_object(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = (script.get_text() or "").strip()
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type") == "Event":
                    return item
        elif isinstance(data, dict) and data.get("@type") == "Event":
            return data
    return None


def _should_include_event(blob: str) -> bool:
    if any(pattern in blob for pattern in EXPLICIT_EXCLUDE_PATTERNS):
        return False
    return any(pattern in blob for pattern in EXPLICIT_INCLUDE_PATTERNS)


def _extract_location_text(location: object) -> str | None:
    if not isinstance(location, dict):
        return None
    parts = [
        _normalize_space(location.get("name")),
        _normalize_space(location.get("address")),
    ]
    parts = [part for part in parts if part]
    if not parts:
        return None
    return ", ".join(parts)


def _infer_activity_type(blob: str) -> str:
    if " talk " in blob or " educator " in blob:
        return "lecture"
    return "workshop"


def _parse_age_range(value: str | None) -> tuple[int | None, int | None]:
    text = value or ""
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = AGE_PLUS_RE.search(text)
    if match:
        return int(match.group(1)), None
    return None, None


def _parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _searchable_blob(value: str) -> str:
    return f" {_normalize_space(value).lower()} "
