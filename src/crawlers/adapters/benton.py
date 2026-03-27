import asyncio
import json
import re
from datetime import datetime
from urllib.parse import quote, urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

BENTON_CALENDAR_URL = "https://benton.uconn.edu/calendar/"
BENTON_EVENT_SEARCH_URL = (
    "https://events.uconn.edu/live/json/v2/events/search/{query}/"
    "response_fields/location,summary,description,registration,group_title,status,event_types,tags,image"
)

NY_TIMEZONE = "America/New_York"
BENTON_VENUE_NAME = "William Benton Museum of Art"
BENTON_CITY = "Storrs"
BENTON_STATE = "CT"
BENTON_DEFAULT_LOCATION = "Storrs, CT"
BENTON_GROUP_TITLE = "The Benton"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BENTON_CALENDAR_URL,
}

EVENT_ID_RE = re.compile(r"/event/(\d+)-")
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

INCLUDED_KEYWORDS = (
    "workshop",
    "art encounter",
    "art encounters",
    "lecture",
    "talk",
    "discussion",
    "craft",
    "hands-on",
    "class",
)
EXCLUDED_KEYWORDS = (
    "open house",
    "walk",
    "tour",
    "storytime",
    "story time",
    "admission",
    "music",
    "concert",
    "film",
    "gala",
    "member preview",
    "reception",
    "opening reception",
    "performance",
)
FREE_MARKERS = (
    " free ",
    "free and open",
    "free.",
    "free!",
    "free\n",
    "no cost",
)
PAID_MARKERS = (
    "$",
    "fee:",
    "fee ",
    "per child",
    "museum member",
    "members $",
    "non-member",
    "non member",
    "ticket",
    "tickets",
    "admission:",
)


async def fetch_benton_url(
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
        raise RuntimeError("Unable to fetch William Benton Museum of Art pages") from last_exception
    raise RuntimeError("Unable to fetch William Benton Museum of Art pages after retries")


async def fetch_benton_event_detail_json(
    title: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> dict | None:
    search_url = BENTON_EVENT_SEARCH_URL.format(query=quote(title, safe=""))
    text = await fetch_benton_url(
        search_url,
        client=client,
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


async def load_benton_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_benton_url(BENTON_CALENDAR_URL, client=client)
        entries = _extract_listing_entries(listing_html)
        payload_entries = await asyncio.gather(
            *(_load_benton_entry_payload(entry, client=client) for entry in entries)
        )

    return {
        "listing_url": BENTON_CALENDAR_URL,
        "listing_html": listing_html,
        "entries": payload_entries,
    }


def parse_benton_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for entry in payload["entries"]:
        detail = _build_event_detail(entry)
        if detail is None:
            continue
        if not _should_include_event(detail):
            continue

        start_at = detail["start_at"]
        if start_at is None:
            continue

        key = (detail["source_url"], detail["title"], start_at)
        if key in seen:
            continue
        seen.add(key)

        age_min, age_max = _parse_age_range(detail["text_blob"])
        text_blob = detail["text_blob"].lower()
        is_free, free_status = infer_price_classification(text_blob)
        rows.append(
            ExtractedActivity(
                source_url=detail["source_url"],
                title=detail["title"],
                description=detail["description"],
                venue_name=BENTON_VENUE_NAME,
                location_text=detail["location_text"] or BENTON_DEFAULT_LOCATION,
                city=BENTON_CITY,
                state=BENTON_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob or "no registration" in text_blob),
                registration_required=bool(detail["registration_required"]),
                start_at=start_at,
                end_at=detail["end_at"],
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class BentonCalendarAdapter(BaseSourceAdapter):
    source_name = "benton_calendar"

    async def fetch(self) -> list[str]:
        html = await fetch_benton_url(BENTON_CALENDAR_URL)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        entries = _extract_listing_entries(payload)
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
            payload_entries = await asyncio.gather(
                *(_load_benton_entry_payload(entry, client=client) for entry in entries)
            )
        return parse_benton_payload(
            {
                "listing_url": BENTON_CALENDAR_URL,
                "listing_html": payload,
                "entries": payload_entries,
            }
        )


async def _load_benton_entry_payload(entry: dict, *, client: httpx.AsyncClient) -> dict:
    detail_html, detail_json = await asyncio.gather(
        fetch_benton_url(entry["source_url"], client=client),
        fetch_benton_event_detail_json(entry["title"], client=client),
    )
    return {
        **entry,
        "detail_html": detail_html,
        "detail_json_payload": detail_json,
    }


def _extract_listing_entries(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    entries: list[dict] = []
    seen: set[str] = set()

    for anchor in soup.select("ul.uc-events-list a.uc-event-link[href]"):
        source_url = urljoin(BENTON_CALENDAR_URL, (anchor.get("href") or "").strip())
        title = _normalize_space(anchor.get_text(" ", strip=True))
        if not source_url or not title or source_url in seen:
            continue
        seen.add(source_url)
        entries.append(
            {
                "title": title,
                "source_url": source_url,
                "event_id": _extract_event_id(source_url),
            }
        )

    return entries


def _build_event_detail(entry: dict) -> dict | None:
    title = _normalize_space(str(entry.get("title") or ""))
    source_url = str(entry.get("source_url") or "").strip()
    if not title or not source_url:
        return None

    meta_description = _extract_meta_description(str(entry.get("detail_html") or ""))
    detail_obj = _select_matching_detail_obj(
        entry.get("detail_json_payload"),
        expected_event_id=entry.get("event_id"),
        expected_title=title,
    )

    description_parts = [
        _html_to_text(detail_obj.get("summary")) if detail_obj else "",
        _html_to_text(detail_obj.get("description")) if detail_obj else "",
        meta_description,
    ]
    description = _normalize_space("\n\n".join(part for part in description_parts if part))
    if not description:
        description = meta_description or None

    location_text = _normalize_space(
        str((detail_obj or {}).get("location_title") or (detail_obj or {}).get("location") or "")
    )
    if not location_text:
        location_text = BENTON_DEFAULT_LOCATION

    start_at = _parse_datetime((detail_obj or {}).get("date_iso")) or _extract_meta_datetime(
        str(entry.get("detail_html") or ""),
        property_name="og:start_time",
    )
    end_at = _parse_datetime((detail_obj or {}).get("date2_iso"))
    if end_at is None:
        end_at = _extract_meta_datetime(
            str(entry.get("detail_html") or ""),
            property_name="og:end_time",
        )

    registration_required = bool((detail_obj or {}).get("has_registration"))
    if not registration_required:
        text_blob = " ".join(filter(None, [title, description or "", meta_description])).lower()
        registration_required = "register" in text_blob or "registration" in text_blob

    text_blob = " ".join(
        filter(
            None,
            [
                title,
                description or "",
                meta_description,
                str((detail_obj or {}).get("cost") or ""),
                " ".join(str(tag) for tag in ((detail_obj or {}).get("tags") or [])),
                " ".join(str(tag) for tag in ((detail_obj or {}).get("event_types") or [])),
            ],
        )
    )

    return {
        "title": title,
        "source_url": source_url,
        "description": description,
        "location_text": location_text,
        "start_at": start_at,
        "end_at": end_at,
        "registration_required": registration_required,
        "text_blob": text_blob,
    }


def _select_matching_detail_obj(
    payload: dict | None,
    *,
    expected_event_id: str | None,
    expected_title: str,
) -> dict | None:
    if not isinstance(payload, dict):
        return None

    data = payload.get("data")
    if not isinstance(data, list):
        return None

    normalized_title = _normalize_for_match(expected_title)
    for item in data:
        if not isinstance(item, dict):
            continue
        if str(item.get("group_title") or "") != BENTON_GROUP_TITLE:
            continue
        if expected_event_id and str(item.get("id") or "") == expected_event_id:
            return item

    for item in data:
        if not isinstance(item, dict):
            continue
        if str(item.get("group_title") or "") != BENTON_GROUP_TITLE:
            continue
        if _normalize_for_match(str(item.get("title") or "")) == normalized_title:
            return item

    return None


def _extract_event_id(url: str) -> str | None:
    match = EVENT_ID_RE.search(url)
    return match.group(1) if match else None


def _extract_meta_description(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("meta", attrs={"name": "description"})
    return _normalize_space(str(tag.get("content") or "")) if tag else ""


def _extract_meta_datetime(html: str, *, property_name: str) -> datetime | None:
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("meta", attrs={"property": property_name})
    return _parse_datetime(tag.get("content")) if tag else None


def _should_include_event(detail: dict) -> bool:
    text = " ".join(filter(None, [detail["title"], detail["description"] or "", detail["location_text"]])).lower()
    if any(keyword in text for keyword in EXCLUDED_KEYWORDS):
        return False
    return any(keyword in text for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(text: str) -> str:
    if "workshop" in text or "craft" in text or "hands-on" in text or "art encounter" in text:
        return "workshop"
    if "lecture" in text or "talk" in text or "discussion" in text:
        return "talk"
    if "class" in text:
        return "class"
    return "workshop"


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    range_match = AGE_RANGE_RE.search(text)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        return (min(start, end), max(start, end))

    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return (int(plus_match.group(1)), None)

    return (None, None)


def _parse_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return parse_iso_datetime(str(value), timezone_name=NY_TIMEZONE)
    except ValueError:
        return None


def _html_to_text(value: object) -> str:
    if value in (None, ""):
        return ""
    return _normalize_space(BeautifulSoup(str(value), "html.parser").get_text(" ", strip=True))


def _normalize_for_match(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()
