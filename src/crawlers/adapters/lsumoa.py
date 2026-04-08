import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

LSUMOA_PROGRAMS_URL = "https://www.lsumoa.org/museum-programs"
CT_TIMEZONE = "America/Chicago"
LSUMOA_VENUE_NAME = "LSU Museum of Art"
LSUMOA_CITY = "Baton Rouge"
LSUMOA_STATE = "LA"
LSUMOA_LOCATION = "LSU Museum of Art, Baton Rouge, LA"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " art break ",
    " art making ",
    " art-making ",
    " conversation ",
    " creative connections ",
    " curator talk ",
    " family day ",
    " free first sunday ",
    " lecture ",
    " lectures ",
    " make art ",
    " workshop ",
    " workshops ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " members only ",
    " member only ",
    " reception ",
    " student led tour ",
    " tour ",
    " tours ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
)
REGISTER_PATTERNS = (
    " pre-register ",
    " preregister ",
    " register ",
    " registration ",
)
NEGATIVE_REGISTER_PATTERNS = (
    " no need to register ",
    " no pre registration required ",
    " no pre-registration required ",
    " no preregistration required ",
    " no registration required ",
    " registration is not required ",
)


async def fetch_lsumoa_page(
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
        raise RuntimeError(f"Unable to fetch LSUMOA page: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch LSUMOA page after retries: {url}")


async def load_lsumoa_payload(
    *,
    list_url: str = LSUMOA_PROGRAMS_URL,
) -> dict[str, object]:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        list_html = await fetch_lsumoa_page(list_url, client=client)
        detail_urls = _extract_detail_urls(list_html, list_url=list_url)
        detail_pages: dict[str, str] = {}
        for detail_url in detail_urls:
            detail_pages[detail_url] = await fetch_lsumoa_page(detail_url, client=client)
    return {"list_html": list_html, "detail_pages": detail_pages}


def parse_lsumoa_payload(payload: dict) -> list[ExtractedActivity]:
    today = datetime.now(ZoneInfo(CT_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for source_url, html in (payload.get("detail_pages") or {}).items():
        row = _build_row(source_url=source_url, html=html)
        if row is None:
            continue
        if row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class LsumoaProgramsAdapter(BaseSourceAdapter):
    source_name = "lsumoa_programs"

    async def fetch(self) -> list[str]:
        payload = await load_lsumoa_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_lsumoa_payload/parse_lsumoa_payload from script runner.")


def _extract_detail_urls(list_html: str, *, list_url: str) -> list[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    detail_urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select(".summary-item a[href]"):
        href = _normalize_space(anchor.get("href"))
        if not href or not href.startswith("/calendar-1"):
            continue
        absolute = urljoin(list_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        detail_urls.append(absolute)
    return detail_urls


def _build_row(*, source_url: str, html: str) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    article = soup.select_one("article.eventitem")
    title = _normalize_space(
        article.select_one("h1.eventitem-title").get_text(" ", strip=True)
        if article and article.select_one("h1.eventitem-title")
        else ""
    )
    if not title:
        return None

    date_text = _normalize_space(
        article.select_one("time.event-date").get_text(" ", strip=True)
        if article and article.select_one("time.event-date")
        else ""
    )
    base_date = parse_date_text(date_text)
    if base_date is None:
        return None

    start_text = _normalize_space(
        article.select_one(".event-time-12hr-start").get_text(" ", strip=True)
        if article and article.select_one(".event-time-12hr-start")
        else ""
    )
    end_text = _normalize_space(
        article.select_one(".event-time-12hr-end").get_text(" ", strip=True)
        if article and article.select_one(".event-time-12hr-end")
        else ""
    )
    time_text = f"{start_text} - {end_text}" if start_text and end_text else start_text
    start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
    if start_at is None:
        return None

    location_lines = [
        _normalize_space(node.get_text(" ", strip=True))
        for node in soup.select(".eventitem-meta-address-line")
        if _normalize_space(node.get_text(" ", strip=True))
    ]
    location_text = ", ".join(location_lines) if location_lines else LSUMOA_LOCATION

    body_parts = [
        _normalize_space(node.get_text(" ", strip=True))
        for node in soup.select(".eventitem-column-content .sqs-html-content p, .eventitem-column-content .sqs-html-content li")
        if _normalize_space(node.get_text(" ", strip=True))
    ]
    description = _join_non_empty(body_parts[:8])
    token_blob = _searchable_blob(" ".join(part for part in [title, description or "", location_text] if part))
    title_blob = _searchable_blob(title)
    if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
        return None

    age_min, age_max = parse_age_range(" ".join(part for part in [title, description or ""] if part))
    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=LSUMOA_VENUE_NAME,
        location_text=location_text,
        city=LSUMOA_CITY,
        state=LSUMOA_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=_requires_registration(token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=CT_TIMEZONE,
        **price_classification_kwargs(description),
    )


def _should_keep_event(*, title_blob: str, token_blob: str) -> bool:
    if not any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS):
        return False
    if any(pattern in title_blob for pattern in CONTEXTUAL_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS):
        return False
    return True


def _infer_activity_type(token_blob: str) -> str:
    if any(pattern in token_blob for pattern in (" conversation ", " curator talk ", " lecture ", " talk ")):
        return "lecture"
    return "workshop"


def _requires_registration(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in NEGATIVE_REGISTER_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in DROP_IN_PATTERNS):
        return False
    return any(pattern in token_blob for pattern in REGISTER_PATTERNS)


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _join_non_empty(parts: list[str]) -> str | None:
    cleaned = [part for part in parts if part]
    if not cleaned:
        return None
    return " | ".join(cleaned)


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())
