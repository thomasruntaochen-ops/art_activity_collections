import asyncio
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

LACMA_EVENTS_URL = "https://www.lacma.org/event"

LA_TIMEZONE = "America/Los_Angeles"
LACMA_VENUE_NAME = "Los Angeles County Museum of Art"
LACMA_CITY = "Los Angeles"
LACMA_STATE = "CA"
LACMA_DEFAULT_LOCATION = "Los Angeles, CA"

EXCLUDED_PRIMARY_KEYWORDS = (
    "tour",
    "tours",
    "open house",
    "performance",
    "screening",
    "film",
    "music",
    "concert",
    "camp",
    "free night",
    "fundraising",
    "gala",
    "reading",
    "writing",
    "tv",
)
SOFT_EXCLUDED_KEYWORDS = (
    "ticket",
    "tickets",
    "registration",
    "admission",
    "exhibition",
)
INCLUDED_KEYWORDS = (
    "talk",
    "lecture",
    "class",
    "activity",
    "workshop",
    "drop-in",
    "drop in",
    "lab",
    "conversation",
    "panel",
    "maker",
    "artmaking",
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.lacma.org/",
}


async def fetch_lacma_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[lacma-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[lacma-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[lacma-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[lacma-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[lacma-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[lacma-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[lacma-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch LACMA events page") from last_exception
    raise RuntimeError("Unable to fetch LACMA events page after retries")


class LacmaEventsAdapter(BaseSourceAdapter):
    source_name = "lacma_events"

    def __init__(self, url: str = LACMA_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_lacma_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_lacma_events_html(payload, list_url=self.url)


def parse_lacma_events_html(
    html: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in soup.select("div.card-event"):
        row = _build_row_from_card(card=card, list_url=list_url, now=now)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    return rows


def _build_row_from_card(
    *,
    card,
    list_url: str,
    now: datetime | None = None,
) -> ExtractedActivity | None:
    link = card.select_one(".card-event__name a[href]") or card.select_one("a[href]")
    if link is None:
        return None

    title = _normalize_space(link.get_text(" ", strip=True))
    if not title or is_irrelevant_item_text(title):
        return None

    source_url = urljoin(list_url, link.get("href", "").strip())
    category = _normalize_space(_text_or_none(card.select_one(".card-event__type")))
    description = _normalize_space(_text_or_none(card.select_one(".card-event__content")))
    date_node = card.select_one(".card-event__date")
    start_at = _parse_start_at(date_node, now=now)
    if start_at is None:
        return None

    if not _should_keep_event(title=title, category=category, description=description):
        return None

    location_parts = [
        _normalize_space(span.get_text(" ", strip=True))
        for span in card.select(".card-event__location span")
    ]
    location_parts = [value for value in location_parts if value]
    location_text = ", ".join(location_parts) if location_parts else LACMA_DEFAULT_LOCATION
    if location_text.strip().lower() == "lacma":
        location_text = LACMA_DEFAULT_LOCATION

    text_blob = " ".join([title, category or "", description or "", location_text]).lower()
    full_description = _join_non_empty(
        [
            description,
            f"Category: {category}" if category else None,
            f"Location: {location_text}" if location_parts else None,
        ]
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=LACMA_VENUE_NAME,
        location_text=location_text,
        city=LACMA_CITY,
        state=LACMA_STATE,
        activity_type=_detect_activity_type(title=title, category=category, description=description),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=("registration" in text_blob and "not required" not in text_blob),
        start_at=start_at,
        end_at=None,
        timezone=LA_TIMEZONE,
        free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
    )


def _should_keep_event(*, title: str, category: str | None, description: str | None) -> bool:
    title_blob = " ".join(part for part in [title, category] if part).lower()
    text_blob = " ".join(part for part in [title, category or "", description or ""]).lower()
    included = any(keyword in text_blob for keyword in INCLUDED_KEYWORDS)
    if any(keyword in title_blob for keyword in EXCLUDED_PRIMARY_KEYWORDS):
        return False
    if any(keyword in text_blob for keyword in SOFT_EXCLUDED_KEYWORDS) and not included:
        return False
    return included


def _detect_activity_type(*, title: str, category: str | None, description: str | None) -> str:
    text_blob = " ".join(part for part in [title, category or "", description or ""]).lower()
    if any(keyword in text_blob for keyword in ("talk", "lecture", "conversation", "panel")):
        return "talk"
    return "workshop"


def _parse_start_at(node, *, now: datetime | None = None) -> datetime | None:
    if node is None:
        return None

    spans = [_normalize_space(span.get_text(" ", strip=True)) for span in node.select("span")]
    if len(spans) < 2:
        return None

    date_text = spans[0]
    time_text = spans[1].removesuffix(" PT").strip()
    current = now or datetime.now()
    base_year = current.year

    for year in (base_year - 1, base_year, base_year + 1):
        try:
            candidate_date = datetime.strptime(f"{date_text} {year}", "%a %b %d %Y")
            time_value = _parse_time_component(time_text)
            if time_value is None:
                continue
            candidate = candidate_date.replace(hour=time_value.hour, minute=time_value.minute)
        except ValueError:
            continue

        if abs((candidate.date() - current.date()).days) <= 180:
            return candidate

    return None


def _parse_time_component(value: str) -> datetime | None:
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _text_or_none(node) -> str | None:
    if node is None:
        return None
    value = _normalize_space(node.get_text(" ", strip=True))
    if value in {"...", ". . ."}:
        return None
    return value


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    return " | ".join(values) if values else None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())
