from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import DEFAULT_HEADERS
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

CHAZEN_EVENTS_URL = "https://chazen.wisc.edu/events/"
CHAZEN_EVENTS_API_URL = "https://chazen.wisc.edu/wp-json/wp/v2/chazen_event"
CHAZEN_TIMEZONE = "America/Chicago"
CHAZEN_VENUE_NAME = "Chazen Museum of Art"
CHAZEN_CITY = "Madison"
CHAZEN_STATE = "WI"
CHAZEN_LOCATION = "Chazen Museum of Art, Madison, WI"

REJECT_PATTERNS = (
    " festival ",
    " film ",
    " music ",
    " performance ",
    " performances ",
    " recital ",
    " screening ",
    " tour ",
    " tours ",
)
INCLUDE_PATTERNS = (
    " art department event ",
    " artist ",
    " colloquium ",
    " hands-on ",
    " lecture ",
    " lectures ",
    " student night ",
    " studio ",
    " talk ",
    " talks ",
    " visiting artist ",
    " workshop ",
    " workshops ",
)


async def load_chazen_payload(
    *,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        events = await _load_rest_events(client=client, page_limit=page_limit, per_page=per_page)
        detail_pages: dict[str, str] = {}
        for event in events:
            detail_url = normalize_space(event.get("link"))
            if not detail_url or not _is_candidate(event):
                continue
            detail_pages[detail_url] = await fetch_html(detail_url, referer=CHAZEN_EVENTS_URL, client=client)
    return {"events": events, "details": detail_pages}


class ChazenAdapter(BaseSourceAdapter):
    source_name = "chazen_events"

    async def fetch(self) -> list[str]:
        payload = await load_chazen_payload(page_limit=1)
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_chazen_payload(json.loads(payload))


def parse_chazen_payload(payload: dict) -> list[ExtractedActivity]:
    details = payload.get("details") or {}
    today = datetime.now(ZoneInfo(CHAZEN_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event in payload.get("events") or []:
        detail_url = normalize_space(event.get("link"))
        detail_html = details.get(detail_url)
        if not detail_html:
            continue
        row = _build_row(event, detail_html)
        if row is None or row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


async def _load_rest_events(
    *,
    client: httpx.AsyncClient,
    page_limit: int | None,
    per_page: int,
) -> list[dict]:
    events: list[dict] = []
    total_pages = 1
    page = 1
    while page <= total_pages:
        if page_limit is not None and page > max(page_limit, 1):
            break
        response = await client.get(
            CHAZEN_EVENTS_API_URL,
            params={"per_page": per_page, "page": page},
        )
        response.raise_for_status()
        total_pages = int(response.headers.get("x-wp-totalpages", "1"))
        events.extend(response.json())
        page += 1
    return events


def _build_row(event: dict, detail_html: str) -> ExtractedActivity | None:
    soup = BeautifulSoup(detail_html, "html.parser")
    rest_title = normalize_space(
        BeautifulSoup((event.get("title") or {}).get("rendered") or "", "html.parser").get_text(" ", strip=True)
    )
    heading_title = normalize_space(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")
    title = rest_title or heading_title
    if rest_title and heading_title and rest_title.lower().endswith("colloquium") and heading_title.lower().startswith(rest_title.lower()):
        title = heading_title
    source_url = normalize_space(event.get("link"))
    if not title or not source_url:
        return None

    fields = _extract_fields(soup)
    description = join_non_empty(
        [
            fields.get("description"),
            f"Tags: {fields['tags']}" if fields.get("tags") else None,
        ]
    )
    if not _should_include(title=title, description=description):
        return None

    base_date = parse_date_text(fields.get("date"))
    if base_date is None:
        return None
    start_at, end_at = parse_time_range(base_date=base_date, time_text=fields.get("time"))
    if start_at is None:
        return None

    location_text = fields.get("location") or CHAZEN_LOCATION
    full_description = join_non_empty(
        [
            description,
            "Tickets available" if _has_ticket_link(soup) else None,
        ]
    )
    blob = _blob(" ".join(part for part in [title, full_description or ""] if part))

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=CHAZEN_VENUE_NAME,
        location_text=location_text,
        city=CHAZEN_CITY,
        state=CHAZEN_STATE,
        activity_type=infer_activity_type(title, full_description),
        age_min=None,
        age_max=None,
        drop_in=" drop-in " in blob,
        registration_required=(
            (" register " in blob)
            or (" tickets available " in blob)
            or (" reservation " in blob)
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=CHAZEN_TIMEZONE,
        **price_classification_kwargs(full_description),
    )


def _is_candidate(event: dict) -> bool:
    title = normalize_space(BeautifulSoup((event.get("title") or {}).get("rendered") or "", "html.parser").get_text(" ", strip=True))
    description = join_non_empty(
        [
            _html_to_text((event.get("excerpt") or {}).get("rendered")),
            " ".join(str(value) for value in (event.get("chazen_event_type") or [])),
        ]
    )
    return _should_include(title=title, description=description)


def _extract_fields(soup: BeautifulSoup) -> dict[str, str]:
    values: dict[str, str] = {}
    for dt in soup.select("dt"):
        dd = dt.find_next_sibling("dd")
        if dd is None:
            continue
        key = normalize_space(dt.get_text(" ", strip=True)).lower()
        values[key] = normalize_space(dd.get_text(" ", strip=True))
    return values


def _has_ticket_link(soup: BeautifulSoup) -> bool:
    for anchor in soup.select("a[href]"):
        text = normalize_space(anchor.get_text(" ", strip=True)).lower()
        href = normalize_space(anchor.get("href")).lower()
        if "ticket" in text or "ticket" in href:
            return True
    return False


def _should_include(*, title: str, description: str | None) -> bool:
    blob = _blob(" ".join(part for part in [title, description or ""] if part))
    if any(pattern in blob for pattern in REJECT_PATTERNS):
        return False
    return any(pattern in blob for pattern in INCLUDE_PATTERNS)


def _html_to_text(value: str | None) -> str | None:
    if not value:
        return None
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    normalized = normalize_space(text)
    return normalized or None


def _blob(value: str) -> str:
    return f" {normalize_space(value).lower()} "
