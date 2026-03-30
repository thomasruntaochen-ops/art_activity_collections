import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

CROW_EVENTS_URL = "https://crowmuseum.org/events/"
CROW_TIMEZONE = "America/Chicago"
CROW_VENUE_NAME = "Crow Museum of Asian Art of The University of Texas at Dallas"
CROW_CITY = "Dallas"
CROW_STATE = "TX"
CROW_LOCATION = "2010 Flora St, Dallas, TX"

CROW_INCLUDE_KEYWORDS = (
    "workshop",
    "class",
    "lecture",
    "talk",
    "conversation",
    "lab",
    "activity",
    "studio",
)
CROW_EXCLUDE_KEYWORDS = (
    "yoga",
    "mindful",
    "mindfulness",
    "meditation",
    "film",
    "performance",
    "concert",
    "music",
    "dance",
    "reception",
    "tour",
    "storytime",
    "poetry",
)
TIME_RANGE_RE = re.compile(
    r"(?P<month>\d{1,2})/(?P<day>\d{1,2})\s+"
    r"(?P<start>\d{1,2}(?:\s*:\s*\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))"
    r"(?:\s+to\s+"
    r"(?P<end>\d{1,2}(?:\s*:\s*\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)))?",
    re.IGNORECASE,
)
WHITESPACE_RE = re.compile(r"\s+")
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CROW_EVENTS_URL,
}


async def fetch_crow_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[crow-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[crow-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[crow-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[crow-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[crow-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[crow-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[crow-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Crow Museum events page") from last_exception
    raise RuntimeError("Unable to fetch Crow Museum events page after retries")


async def load_crow_payload(base_url: str = CROW_EVENTS_URL) -> dict[str, object]:
    listing_html = await fetch_crow_page(base_url)
    return {"listing_html": listing_html}


class CrowEventsAdapter(BaseSourceAdapter):
    source_name = "crow_events"

    def __init__(self, url: str = CROW_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_crow_payload(self.url)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_crow_payload(data)


def parse_crow_payload(
    payload: dict[str, object],
    *,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    listing_html = payload.get("listing_html")
    if not isinstance(listing_html, str):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    reference_now = now or datetime.now()

    for card in _iter_crow_cards(listing_html, list_url=CROW_EVENTS_URL):
        combined_text = " ".join(
            [
                card["title"],
                card["description"] or "",
            ]
        ).lower()
        if not _should_keep_crow_event(text_blob=combined_text):
            continue

        start_at, end_at = _parse_card_datetimes(card["date_text"], now=reference_now)
        if start_at is None:
            continue

        is_free, free_status = infer_price_classification(card["description"])
        registration_required = any(
            marker in combined_text
            for marker in ("register", "registration", "reserve your spot", "reserve a spot", "rsvp")
        )
        item = ExtractedActivity(
            source_url=card["source_url"],
            title=card["title"],
            description=card["description"],
            venue_name=CROW_VENUE_NAME,
            location_text=CROW_LOCATION,
            city=CROW_CITY,
            state=CROW_STATE,
            activity_type=_infer_crow_activity_type(card["title"], card["description"]),
            age_min=None,
            age_max=None,
            drop_in=("drop-in" in combined_text or "drop in" in combined_text),
            registration_required=registration_required,
            start_at=start_at,
            end_at=end_at,
            timezone=CROW_TIMEZONE,
            is_free=is_free,
            free_verification_status=free_status,
        )
        key = (item.source_url, item.title, item.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(item)

    return rows


def _iter_crow_cards(html: str, *, list_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict[str, str]] = []
    for node in soup.select(".comet-calendar-listing"):
        title_node = node.select_one(".comet-calendar-info h2 a")
        date_node = node.select_one(".comet-calendar-date")
        description_node = node.select_one(".comet-calendar-info p")
        if title_node is None or date_node is None:
            continue

        title = _normalize_space(title_node.get_text(" ", strip=True))
        href = (title_node.get("href") or "").strip()
        if not title or not href:
            continue

        cards.append(
            {
                "source_url": urljoin(list_url, href),
                "title": title,
                "date_text": _normalize_space(date_node.get_text(" ", strip=True)),
                "description": _normalize_space(description_node.get_text(" ", strip=True))
                if description_node is not None
                else "",
            }
        )
    return cards


def _should_keep_crow_event(*, text_blob: str) -> bool:
    if any(keyword in text_blob for keyword in CROW_EXCLUDE_KEYWORDS):
        return False
    return any(keyword in text_blob for keyword in CROW_INCLUDE_KEYWORDS)


def _infer_crow_activity_type(title: str, description: str | None) -> str:
    title_text = title.lower()
    text = f"{title} {description or ''}".lower()
    if "workshop" in title_text or "studio" in title_text or "lab" in title_text:
        return "workshop"
    if "lecture" in title_text or "talk" in title_text or "conversation" in title_text:
        return "talk"
    if "workshop" in text or "studio" in text or "lab" in text:
        return "workshop"
    if "class" in text:
        return "class"
    return "activity"


def _parse_card_datetimes(date_text: str, *, now: datetime) -> tuple[datetime | None, datetime | None]:
    match = TIME_RANGE_RE.search(date_text)
    if match is None:
        return None, None

    month = int(match.group("month"))
    day = int(match.group("day"))
    year = now.year
    if month < now.month - 6:
        year += 1

    start_at = _combine_date_and_time(
        year=year,
        month=month,
        day=day,
        time_text=match.group("start"),
    )
    end_at = _combine_date_and_time(
        year=year,
        month=month,
        day=day,
        time_text=match.group("end"),
    )
    return start_at, end_at


def _combine_date_and_time(
    *,
    year: int,
    month: int,
    day: int,
    time_text: str | None,
) -> datetime | None:
    if not time_text:
        return None

    normalized = (
        time_text.lower()
        .replace(" ", "")
        .replace("a.m.", "am")
        .replace("p.m.", "pm")
        .replace("a.m", "am")
        .replace("p.m", "pm")
        .strip()
    )
    for fmt in ("%m/%d/%Y %I:%M%p", "%m/%d/%Y %I%p"):
        try:
            return datetime.strptime(f"{month:02d}/{day:02d}/{year} {normalized}", fmt)
        except ValueError:
            continue
    return None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return WHITESPACE_RE.sub(" ", value).strip()
