import asyncio
import json
import re
from datetime import datetime
from datetime import time
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

MOCACT_EVENTS_PAGE_URL = "https://mocact.org/events/#calendar"
# MoCA CT dropped The Events Calendar (tribe) around 2026-08-30 -- its REST route
# 404s and the namespace is gone from /wp-json/. The site now runs Modern Events
# Calendar (MEC), whose monthly view is the only public listing with dates on it:
# /mec/v1/events and /wp/v2/mec-events return the posts but not their schedules.
MOCACT_CALENDAR_URL = "https://mocact.org/calendar/"

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
AGE_RANGE_MARKERS = (
    "ages ",
    "age ",
)
TIME_RE = re.compile(r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*(?P<meridiem>[ap])\.?m\.?", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"\d{1,2}:\d{2}\s*[ap]\.?m\.?\s*(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2}):(?P<end_minute>\d{2})\s*(?P<end_meridiem>[ap])\.?m\.?",
    re.IGNORECASE,
)


async def fetch_mocact_html(
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
        raise RuntimeError(f"Unable to fetch MoCA CT page: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch MoCA CT page after retries: {url}")


async def load_mocact_events_payload(*, detail_limit: int | None = None) -> dict:
    """Calendar page plus a detail page per dated event.

    The MEC monthly view carries the schedule (one `data-mec-cell` per day); the
    detail pages carry the blurb the include/price/audience rules read.
    """
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        calendar_html = await fetch_mocact_html(MOCACT_CALENDAR_URL, client=client)

        detail_urls: list[str] = []
        for occurrence in _extract_calendar_occurrences(calendar_html):
            if occurrence["source_url"] not in detail_urls:
                detail_urls.append(occurrence["source_url"])
        if detail_limit is not None:
            detail_urls = detail_urls[: max(detail_limit, 0)]

        detail_pages: dict[str, str] = {}
        for detail_url in detail_urls:
            try:
                detail_pages[detail_url] = await fetch_mocact_html(detail_url, client=client)
            except Exception as exc:  # a single dead detail page must not sink the run
                print(f"[mocact-fetch] detail failed url={detail_url}: {exc}")

    return {"calendar_html": calendar_html, "detail_pages": detail_pages}


def _extract_calendar_occurrences(calendar_html: str) -> list[dict]:
    """Dated events in the MEC monthly view, one entry per day cell.

    Ongoing exhibitions are repeated into every cell without a time; only real
    scheduled activities carry `.mec-event-time`, so requiring one keeps the
    three standing exhibitions out of the 35 day cells they appear in.
    """
    soup = BeautifulSoup(calendar_html, "html.parser")
    occurrences: list[dict] = []

    for section in soup.select(".mec-calendar-events-sec[data-mec-cell]"):
        cell = _normalize_space(str(section.get("data-mec-cell") or ""))
        try:
            cell_date = datetime.strptime(cell, "%Y%m%d").date()
        except ValueError:
            continue

        for article in section.select("article.mec-event-article"):
            time_el = article.select_one(".mec-event-time")
            if time_el is None:
                continue
            link = article.select_one("h4.mec-event-title a[href]")
            if link is None:
                continue
            title = _normalize_space(link.get_text(" ", strip=True))
            if not title:
                continue
            occurrences.append(
                {
                    "date": cell_date.isoformat(),
                    "time_text": _normalize_space(time_el.get_text(" ", strip=True)),
                    "title": title,
                    "source_url": urljoin(MOCACT_CALENDAR_URL, str(link.get("href") or "").strip()),
                }
            )

    return occurrences


def parse_mocact_events_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    detail_pages = payload.get("detail_pages") or {}

    for occurrence in _extract_calendar_occurrences(payload.get("calendar_html") or ""):
        row = _build_row_from_occurrence(
            occurrence,
            detail_html=detail_pages.get(occurrence["source_url"]) or "",
        )
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
        payload = await load_mocact_events_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_mocact_events_payload(json.loads(payload))


def _build_row_from_occurrence(occurrence: dict, *, detail_html: str) -> ExtractedActivity | None:
    title = _normalize_space(str(occurrence.get("title") or ""))
    source_url = _normalize_space(str(occurrence.get("source_url") or ""))
    if not title or not source_url:
        return None

    description = _extract_detail_description(detail_html)
    if not _should_include_event(title=title, description=description):
        return None

    start_at = _parse_occurrence_datetime(
        date_text=str(occurrence.get("date") or ""),
        time_text=str(occurrence.get("time_text") or ""),
    )
    if start_at is None:
        return None
    end_at = _extract_detail_end_time(detail_html, start_at=start_at)

    text_blob = " ".join(part for part in [title, description] if part).lower()
    age_min, age_max = _parse_age_range(description)
    is_free, free_status = infer_price_classification(text_blob)
    activity_type = _infer_activity_type(title=title, description=description)
    audience_segment = _infer_mocact_audience(
        title=title,
        description=description,
        source_url=source_url,
        age_min=age_min,
        age_max=age_max,
        activity_type=activity_type,
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description or None,
        venue_name=MOCACT_VENUE_NAME,
        location_text=MOCACT_DEFAULT_LOCATION,
        city=MOCACT_CITY,
        state=MOCACT_STATE,
        activity_type=activity_type,
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=_has_registration_signal(detail_html=detail_html, text_blob=text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
        audience_segment=audience_segment,
    )


def _extract_detail_description(detail_html: str) -> str:
    """The Divi detail pages carry no event markup, but the meta blurb is clean."""
    if not detail_html:
        return ""
    soup = BeautifulSoup(detail_html, "html.parser")
    for selector in ("meta[property='og:description']", "meta[name='description']"):
        tag = soup.select_one(selector)
        if tag is not None:
            text = _normalize_space(str(tag.get("content") or ""))
            if text:
                return text
    return ""


def _parse_occurrence_datetime(*, date_text: str, time_text: str) -> datetime | None:
    try:
        day = datetime.strptime(date_text.strip(), "%Y-%m-%d").date()
    except ValueError:
        return None

    match = TIME_RE.search(time_text or "")
    if match is None:
        return datetime.combine(day, time(0, 0))

    hour = int(match.group("hour")) % 12
    if match.group("meridiem").lower().startswith("p"):
        hour += 12
    return datetime.combine(day, time(hour, int(match.group("minute") or 0)))


def _extract_detail_end_time(detail_html: str, *, start_at: datetime) -> datetime | None:
    """MEC only prints the start time in the calendar; the blurb often has a range."""
    if not detail_html:
        return None

    match = TIME_RANGE_RE.search(_extract_detail_description(detail_html))
    if match is None:
        return None

    hour = int(match.group("end_hour")) % 12
    if match.group("end_meridiem").lower().startswith("p"):
        hour += 12
    end_at = datetime.combine(start_at.date(), time(hour, int(match.group("end_minute") or 0)))
    return end_at if end_at > start_at else None


def _should_include_event(*, title: str, description: str) -> bool:
    normalized_title = title.lower()
    normalized_description = description.lower()
    text_blob = _normalize_space(f"{normalized_title} {normalized_description}")
    if not text_blob:
        return False

    if any(keyword in normalized_title for keyword in TITLE_EXCLUDED_KEYWORDS):
        return False

    return any(keyword in text_blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(*, title: str, description: str) -> str:
    text_blob = _normalize_space(f"{title} {description}").lower()
    if any(keyword in text_blob for keyword in ("talk", "lecture", "conversation", "discussion", "gallery talk")):
        return "talk"
    return "workshop"


def _infer_mocact_audience(
    *,
    title: str,
    description: str,
    source_url: str,
    age_min: int | None,
    age_max: int | None,
    activity_type: str,
) -> str:
    audience = infer_audience_segment(
        title=title,
        description=description,
        source_url=source_url,
        age_min=age_min,
        age_max=age_max,
    )
    if audience != "unknown":
        return audience
    if activity_type in {"talk", "workshop"}:
        return "adults"
    return audience


def _has_registration_signal(*, detail_html: str, text_blob: str) -> bool:
    if any(keyword in text_blob for keyword in ("register here", "register", "book now", "checkout")):
        return True

    return "checkout.mocact.org" in (detail_html or "").lower()


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


def _normalize_space(value: str) -> str:
    return " ".join(value.split())
