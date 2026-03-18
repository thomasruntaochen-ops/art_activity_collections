import asyncio
import json
import re
from datetime import datetime
from decimal import Decimal
from html import unescape
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

CCMOA_EVENTS_URL = "https://www.ccmoa.org/upcoming-events"
CCMOA_EVENT_URL_PREFIX = "https://www.ccmoa.org/events/"
NY_TIMEZONE = "America/New_York"
CCMOA_VENUE_NAME = "Cape Cod Museum of Art"
CCMOA_CITY = "Dennis"
CCMOA_STATE = "MA"
CCMOA_DEFAULT_LOCATION = "Cape Cod Museum of Art, 60 Hope Ln, Dennis, MA 02638, USA"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CCMOA_EVENTS_URL,
}

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*"
    r"(?P<start_meridiem>a\.?m\.?|p\.?m\.?|am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*"
    r"(?P<end_meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)

INCLUDE_MARKERS = (
    "talk",
    "lecture",
    "workshop",
    "class",
    "activity",
    "family activity",
    "artist talk",
    "art talk",
    "paint night",
    "weaving",
    "hands-on",
    "hands on",
    "discussion",
    "discuss",
    "q/a",
    "q&a",
    "historian",
    "audience q&a",
)
TITLE_EXCLUDE_MARKERS = (
    "postponed",
    "new date tba",
    "annual meeting",
    "opening reception",
    "reception",
    "film screening",
)
BODY_EXCLUDE_MARKERS = (
    "screening",
    "music",
    "band",
    "concert",
    "sold out",
)


async def fetch_ccmoa_page(
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
        raise RuntimeError("Unable to fetch Cape Cod Museum of Art page") from last_exception
    raise RuntimeError("Unable to fetch Cape Cod Museum of Art page after retries")


async def load_ccmoa_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_ccmoa_page(CCMOA_EVENTS_URL, client=client)
        detail_urls = _extract_detail_urls(listing_html)
        detail_pages_raw = await asyncio.gather(*(fetch_ccmoa_page(url, client=client) for url in detail_urls))

    return {
        "listing_url": CCMOA_EVENTS_URL,
        "listing_html": listing_html,
        "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages_raw, strict=True)},
    }


def parse_ccmoa_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, html in payload.get("detail_pages", {}).items():
        row = _build_row_from_detail_page(source_url=source_url, html=html)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class CcmoaEventsAdapter(BaseSourceAdapter):
    source_name = "ccmoa_events"

    async def fetch(self) -> list[str]:
        html = await fetch_ccmoa_page(CCMOA_EVENTS_URL)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        detail_urls = _extract_detail_urls(payload)
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
            detail_pages_raw = await asyncio.gather(*(fetch_ccmoa_page(url, client=client) for url in detail_urls))
        return parse_ccmoa_payload(
            {
                "listing_url": CCMOA_EVENTS_URL,
                "listing_html": payload,
                "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages_raw, strict=True)},
            }
        )


def _extract_detail_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    for anchor in soup.select("a[data-hook='title'][href]"):
        href = _normalize_space(anchor.get("href") or "")
        if not href.startswith(CCMOA_EVENT_URL_PREFIX):
            continue
        if href in seen:
            continue
        seen.add(href)
        urls.append(href)

    if urls:
        return urls

    for anchor in soup.find_all("a", href=True):
        href = _normalize_space(anchor.get("href") or "")
        if not href.startswith(CCMOA_EVENT_URL_PREFIX):
            continue
        if href in seen:
            continue
        seen.add(href)
        urls.append(href)

    return urls


def _build_row_from_detail_page(*, source_url: str, html: str) -> ExtractedActivity | None:
    event_obj = _extract_event_object(html)
    if event_obj is None:
        return None

    title = _normalize_space(unescape(str(event_obj.get("name") or event_obj.get("title") or "")))
    if not title:
        return None

    detail = _parse_detail_page(html)
    teaser = _normalize_space(unescape(str(event_obj.get("description") or ""))) or detail.get("summary") or ""
    about_text = detail.get("about_text") or ""
    inclusion_blob = " ".join(part for part in [title, teaser, about_text] if part)
    if not _should_include_event(title=title, text_blob=inclusion_blob):
        return None

    start_at = _parse_datetime(event_obj.get("startDate") or event_obj.get("start_date"))
    if start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("endDate") or event_obj.get("end_date"))
    if end_at is None:
        end_at = _parse_end_at(text=" ".join(filter(None, [teaser, about_text])), start_at=start_at)

    description = _join_non_empty([teaser or None, about_text or None])
    text_blob = " ".join(part for part in [title, description or "", detail.get("location_text") or ""]).lower()
    age_min, age_max = _parse_age_range(description or "")
    offer_amount = _extract_offer_amount(event_obj.get("offers"))

    return ExtractedActivity(
        source_url=urljoin(CCMOA_EVENTS_URL, str(event_obj.get("url") or source_url).strip()),
        title=title,
        description=description or None,
        venue_name=CCMOA_VENUE_NAME,
        location_text=detail.get("location_text") or _build_location_text(event_obj.get("location")),
        city=CCMOA_CITY,
        state=CCMOA_STATE,
        activity_type=_infer_activity_type(title=title, text_blob=inclusion_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob or "pop-in" in text_blob or "pop in" in text_blob),
        registration_required=_has_registration_offer(event_obj.get("offers"), text_blob=text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **price_classification_kwargs_from_amount(offer_amount, text=text_blob),
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
        for event_obj in _iter_event_objects(data):
            if isinstance(event_obj, dict):
                return event_obj
    return None


def _iter_event_objects(value: object) -> list[dict]:
    if isinstance(value, dict):
        result: list[dict] = []
        if value.get("@type") == "Event":
            result.append(value)
        for item in value.values():
            result.extend(_iter_event_objects(item))
        return result
    if isinstance(value, list):
        result: list[dict] = []
        for item in value:
            result.extend(_iter_event_objects(item))
        return result
    return []


def _parse_detail_page(html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    summary = _extract_first_text(soup.select("p[data-hook='event-description']"))
    location_text = _extract_first_text(soup.select("p[data-hook='event-full-location']"))

    about_paragraphs: list[str] = []
    for paragraph in soup.select("section p"):
        text = _normalize_space(unescape(paragraph.get_text(" ", strip=True)))
        if not text:
            continue

        data_hook = _normalize_space(paragraph.get("data-hook") or "")
        if data_hook in {"event-description", "event-full-date", "event-full-location"}:
            continue
        if summary and text == summary:
            continue

        lowered = text.lower()
        if lowered.startswith("we have scholarships!"):
            break
        if "image credit:" in lowered:
            continue
        if "museum school scholarship fund" in lowered:
            break
        if "click 'show more'" in lowered:
            continue
        if lowered in {"presented in conjunction with", "total"}:
            continue
        if re.fullmatch(r"\$?\d+(?:\.\d{2})?", text):
            continue
        about_paragraphs.append(text)

    about_text = _join_non_empty(_dedupe_preserving_order(about_paragraphs[:4]))
    return {
        "summary": summary or None,
        "about_text": about_text or None,
        "location_text": location_text or None,
    }


def _should_include_event(*, title: str, text_blob: str) -> bool:
    token_title = _tokenize_text(title)
    token_blob = _tokenize_text(text_blob)

    if _contains_marker(token_title, TITLE_EXCLUDE_MARKERS):
        return False
    if _contains_marker(token_blob, BODY_EXCLUDE_MARKERS):
        strong_include = _contains_marker(token_blob, ("workshop", "paint night", "family activity", "weaving"))
        if not strong_include:
            return False

    return _contains_marker(token_blob, INCLUDE_MARKERS)


def _infer_activity_type(*, title: str, text_blob: str) -> str:
    token_blob = _tokenize_text(f"{title} {text_blob}")
    if _contains_marker(token_blob, ("workshop", "paint night", "weaving")):
        return "workshop"
    if _contains_marker(token_blob, ("talk", "lecture", "discussion", "discuss", "q a", "historian")):
        return "lecture"
    return "activity"


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    text = _normalize_space(str(value))
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _parse_end_at(*, text: str, start_at: datetime) -> datetime | None:
    for match in TIME_RANGE_RE.finditer(text):
        end_meridiem = _normalize_meridiem(match.group("end_meridiem"))
        if end_meridiem is None:
            continue

        start_meridiem_candidates: list[str]
        explicit_start_meridiem = _normalize_meridiem(match.group("start_meridiem"))
        if explicit_start_meridiem is not None:
            start_meridiem_candidates = [explicit_start_meridiem]
        else:
            start_meridiem_candidates = ["am", "pm"]

        for start_meridiem in start_meridiem_candidates:
            start_candidate = _build_time(
                hour=int(match.group("start_hour")),
                minute=int(match.group("start_minute") or 0),
                meridiem=start_meridiem,
            )
            if (
                start_candidate.hour != start_at.hour
                or start_candidate.minute != start_at.minute
            ):
                continue

            end_candidate = _build_time(
                hour=int(match.group("end_hour")),
                minute=int(match.group("end_minute") or 0),
                meridiem=end_meridiem,
            )
            return start_at.replace(
                hour=end_candidate.hour,
                minute=end_candidate.minute,
                second=0,
                microsecond=0,
            )

    return None


def _build_time(*, hour: int, minute: int, meridiem: str) -> datetime:
    normalized_hour = hour % 12
    if meridiem == "pm":
        normalized_hour += 12
    return datetime(2000, 1, 1, normalized_hour, minute)


def _normalize_meridiem(value: str | None) -> str | None:
    if not value:
        return None
    lowered = value.lower().replace(".", "")
    if lowered in {"am", "pm"}:
        return lowered
    return None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match is not None:
        return int(match.group(1)), int(match.group(2))

    match = AGE_PLUS_RE.search(text)
    if match is not None:
        return int(match.group(1)), None

    return None, None


def _extract_offer_amount(offers: object) -> Decimal | None:
    for offer in _iter_offer_dicts(offers):
        price = offer.get("price")
        if price in (None, ""):
            continue
        try:
            return Decimal(str(price))
        except Exception:
            continue
    return None


def _has_registration_offer(offers: object, *, text_blob: str) -> bool | None:
    for offer in _iter_offer_dicts(offers):
        url = _normalize_space(str(offer.get("url") or ""))
        if url:
            return True
    if "register" in text_blob or "registration" in text_blob or "rsvp" in text_blob or "ticket" in text_blob:
        return True
    return None


def _iter_offer_dicts(value: object) -> list[dict]:
    if isinstance(value, dict):
        nested = value.get("offers")
        if isinstance(nested, list):
            return [value, *[item for item in nested if isinstance(item, dict)]]
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _build_location_text(location: object) -> str:
    if isinstance(location, dict):
        name = _normalize_space(str(location.get("name") or ""))
        address = _normalize_space(str(location.get("address") or ""))
        if name and address:
            if name in address:
                return address
            return f"{name}, {address}"
        if name:
            return name
        if address:
            return address
    return CCMOA_DEFAULT_LOCATION


def _extract_first_text(nodes: list) -> str:
    for node in nodes:
        text = _normalize_space(unescape(node.get_text(" ", strip=True)))
        if text:
            return text
    return ""


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _join_non_empty(values: list[str | None]) -> str:
    return " | ".join(value for value in values if value)


def _normalize_space(value: str) -> str:
    return " ".join(value.split()).strip()


def _tokenize_text(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _contains_marker(tokenized_value: str, markers: tuple[str, ...]) -> bool:
    return any(f" {marker} " in tokenized_value for marker in markers)
