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

SHELDON_EVENTS_URL = "https://sheldonartmuseum.org/events/"
SHELDON_TIMEZONE = "America/Chicago"
SHELDON_VENUE_NAME = "Sheldon Museum of Art"
SHELDON_CITY = "Lincoln"
SHELDON_STATE = "NE"
SHELDON_DEFAULT_LOCATION = "Sheldon Museum of Art, Lincoln, NE"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SHELDON_EVENTS_URL,
}

INCLUDE_MARKERS = (
    " lecture ",
    " visiting artist ",
    " workshop ",
    " teach-in ",
    " conversation ",
    " family day ",
    " artmaking ",
    " hand-lettering ",
    " activity ",
)
EXCLUDE_MARKERS = (
    " tour ",
    " tours ",
    " yoga ",
    " study hall ",
    " first friday ",
    " writing ",
)
DATE_RE = re.compile(
    r"(?P<weekday>[A-Za-z]+),\s+"
    r"(?P<month>[A-Za-z]+)\s+"
    r"(?P<day>\d{1,2}),\s+"
    r"(?P<year>\d{4})\s*-\s*"
    r"(?P<start>\d{1,2}:\d{2}[AP]M)"
    r"(?:\s+to\s+(?P<end>\d{2}:\d{2}[AP]M))?",
    re.IGNORECASE,
)


async def fetch_sheldon_page(
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
        raise RuntimeError("Unable to fetch Sheldon page") from last_exception
    raise RuntimeError("Unable to fetch Sheldon page after retries")


async def load_sheldon_payload() -> dict:
    listing_html = await fetch_sheldon_page(SHELDON_EVENTS_URL)
    detail_urls = _extract_detail_urls(listing_html)
    detail_pages: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for url in detail_urls:
            try:
                detail_pages[url] = await fetch_sheldon_page(url, client=client)
            except Exception as exc:
                print(f"[sheldon] warning: detail fetch failed for {url}: {exc}")

    return {
        "listing_html": listing_html,
        "detail_pages": detail_pages,
    }


def parse_sheldon_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(SHELDON_TIMEZONE)).date()
    soup = BeautifulSoup(payload.get("listing_html") or "", "html.parser")
    detail_pages = payload.get("detail_pages") or {}
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in soup.select("div.event-listing.views-row"):
        title_link = card.select_one("h4.event-heading a[href]")
        if title_link is None:
            continue
        source_url = urljoin(SHELDON_EVENTS_URL, title_link.get("href", "").strip())
        title = _normalize_space(title_link.get_text(" ", strip=True))
        detail = _parse_detail_page(detail_pages.get(source_url, ""))
        description = detail.get("description") or _normalize_space(
            card.select_one("p.event-abbr-teaser").get_text(" ", strip=True) if card.select_one("p.event-abbr-teaser") else ""
        ) or None
        blob = _search_blob(" ".join(filter(None, [title, description])))
        if not _should_include_event(blob):
            continue

        start_at, end_at = _parse_datetime_range(detail.get("date_text") or _normalize_space(
            card.select_one("span.event-date").get_text(" ", strip=True) if card.select_one("span.event-date") else ""
        ))
        if start_at is None or start_at.date() < current_date:
            continue

        row = ExtractedActivity(
            source_url=source_url,
            title=detail.get("title") or title,
            description=description,
            venue_name=SHELDON_VENUE_NAME,
            location_text=detail.get("location_text") or SHELDON_DEFAULT_LOCATION,
            city=SHELDON_CITY,
            state=SHELDON_STATE,
            activity_type=_infer_activity_type(blob),
            age_min=None,
            age_max=None,
            drop_in=("drop in" in blob or "drop-in" in blob),
            registration_required=(" register " in blob or " registration " in blob),
            start_at=start_at,
            end_at=end_at,
            timezone=SHELDON_TIMEZONE,
            **price_classification_kwargs(description),
        )
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class SheldonEventsAdapter(BaseSourceAdapter):
    source_name = "sheldon_events"

    async def fetch(self) -> list[str]:
        payload = await load_sheldon_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_sheldon_payload(json.loads(payload))


def _extract_detail_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for link in soup.select("h4.event-heading a[href]"):
        url = urljoin(SHELDON_EVENTS_URL, link.get("href", "").strip())
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _parse_detail_page(html: str) -> dict[str, str | None]:
    if not html:
        return {"title": None, "description": None, "date_text": None, "location_text": None}
    soup = BeautifulSoup(html, "html.parser")
    article = soup.select_one("article.event-listing")
    if article is None:
        return {"title": None, "description": None, "date_text": None, "location_text": None}
    return {
        "title": _normalize_space(article.select_one("h4.event-heading").get_text(" ", strip=True) if article.select_one("h4.event-heading") else "") or None,
        "description": _normalize_space(article.select_one("article.event-listing div").get_text(" ", strip=True) if article.select_one("article.event-listing div") else "") or None,
        "date_text": _normalize_space(article.select_one("span.event-date").get_text(" ", strip=True) if article.select_one("span.event-date") else "") or None,
        "location_text": _normalize_space(article.select_one("span.event-location").get_text(" ", strip=True) if article.select_one("span.event-location") else "") or None,
    }


def _parse_datetime_range(text: str | None) -> tuple[datetime | None, datetime | None]:
    normalized = _normalize_space(text)
    if not normalized:
        return None, None
    match = DATE_RE.search(normalized)
    if not match:
        return None, None
    base_date = f"{match.group('month')} {match.group('day')}, {match.group('year')}"
    try:
        start_at = datetime.strptime(f"{base_date} {match.group('start').upper()}", "%B %d, %Y %I:%M%p")
    except ValueError:
        return None, None
    end_value = match.group("end")
    end_at = None
    if end_value:
        try:
            end_at = datetime.strptime(f"{base_date} {end_value.upper()}", "%B %d, %Y %I:%M%p")
        except ValueError:
            end_at = None
    return start_at, end_at


def _should_include_event(blob: str) -> bool:
    if any(marker in blob for marker in EXCLUDE_MARKERS):
        return False
    return any(marker in blob for marker in INCLUDE_MARKERS)


def _infer_activity_type(blob: str) -> str:
    if any(marker in blob for marker in (" lecture ", " visiting artist ", " teach-in ", " conversation ")):
        return "lecture"
    return "workshop"


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(unescape(value).split())


def _search_blob(value: str) -> str:
    normalized = _normalize_space(value).lower()
    return f" {normalized} "
