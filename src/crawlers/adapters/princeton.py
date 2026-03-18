import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

PRINCETON_EVENTS_URL = "https://artmuseum.princeton.edu/exhibitions-events/events"

NY_TIMEZONE = "America/New_York"
PRINCETON_VENUE_NAME = "Princeton University Art Museum"
PRINCETON_CITY = "Princeton"
PRINCETON_STATE = "NJ"
PRINCETON_DEFAULT_LOCATION = "Princeton, NJ"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": PRINCETON_EVENTS_URL,
}

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

HARD_EXCLUDED_KEYWORDS = (
    "ticket",
    "tour",
    "registration",
    "camp",
    "yoga",
    "sports",
    "free night",
    "fundraising",
    "admission",
    "exhibition",
    "film",
    "tv",
    "reading",
    "writing",
    "open house",
    "performance",
    "music",
    "storytime",
    "dinner",
    "reception",
    "jazz",
    "orchestra",
    "band",
    "tai chi",
    "poetry",
)
STRONG_INCLUDED_KEYWORDS = (
    "talk",
    "class",
    "lecture",
    "activity",
    "workshop",
    "lab",
    "conversation",
    "art making",
)
SOFT_INCLUDED_KEYWORDS = (
    "family",
    "teen",
    "teens",
    "youth",
    "kids",
    "children",
    "child",
)


async def fetch_princeton_page(
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
        raise RuntimeError("Unable to fetch Princeton events page") from last_exception
    raise RuntimeError("Unable to fetch Princeton events page after retries")


async def load_princeton_events_payload() -> dict:
    html = await fetch_princeton_page(PRINCETON_EVENTS_URL)
    return {
        "listing_url": PRINCETON_EVENTS_URL,
        "html": html,
    }


def parse_princeton_events_payload(payload: dict) -> list[ExtractedActivity]:
    list_url = str(payload.get("listing_url") or PRINCETON_EVENTS_URL)
    html = str(payload.get("html") or "")
    return parse_princeton_events_html(html, list_url=list_url)


class PrincetonEventsAdapter(BaseSourceAdapter):
    source_name = "princeton_events"

    def __init__(self, url: str = PRINCETON_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_princeton_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_princeton_events_html(payload, list_url=self.url)


def parse_princeton_events_html(html: str, *, list_url: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    today = datetime.now().date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in soup.select("a.c-card.node.node--type-event"):
        source_url = urljoin(list_url, card.get("href", "").strip())
        title = _normalize_text(card.select_one("h3.c-card__headline").get_text(" ", strip=True) if card.select_one("h3.c-card__headline") else None)
        if not source_url or not title:
            continue

        eyebrow = _normalize_text(card.select_one("p.c-card__eyebrow").get_text(" ", strip=True) if card.select_one("p.c-card__eyebrow") else None)
        attribution = _normalize_text(card.select_one("div.c-card__attribution-date").get_text(" ", strip=True) if card.select_one("div.c-card__attribution-date") else None)
        times = card.select("time.datetime")
        start_at, end_at = _parse_card_datetimes(times, fallback_text=attribution)
        if start_at is None:
            continue
        if start_at.date() < today:
            continue

        text_blob = " ".join(part for part in [title, eyebrow or "", attribution or ""] if part).lower()
        if _has_any_keywords(text_blob, HARD_EXCLUDED_KEYWORDS):
            continue

        has_strong_include = _has_any_keywords(text_blob, STRONG_INCLUDED_KEYWORDS)
        has_soft_include = _has_any_keywords(text_blob, SOFT_INCLUDED_KEYWORDS)
        if not has_strong_include and not has_soft_include:
            continue

        description = " | ".join(part for part in [f"Category: {eyebrow}" if eyebrow else None, attribution] if part)
        age_min, age_max = _parse_age_range(" ".join(part for part in [title, description] if part))
        is_free, free_status = infer_price_classification(description)

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=PRINCETON_VENUE_NAME,
                location_text=PRINCETON_DEFAULT_LOCATION,
                city=PRINCETON_CITY,
                state=PRINCETON_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=(
                    "registration" in text_blob or "register" in text_blob or "ticket" in text_blob
                ) and "no registration" not in text_blob,
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_card_datetimes(times, *, fallback_text: str | None) -> tuple[datetime | None, datetime | None]:
    if times:
        start_at = _parse_iso_datetime(_normalize_text(times[0].get("datetime")))
        end_at = _parse_iso_datetime(_normalize_text(times[1].get("datetime"))) if len(times) > 1 else None
        if start_at is not None:
            return start_at, end_at

    text = fallback_text or ""
    match = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", text)
    if not match:
        return None, None
    try:
        base = datetime.strptime(match.group(1), "%B %d, %Y").replace(hour=0, minute=0, second=0, microsecond=0)
    except ValueError:
        return None, None
    return base, None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            return None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _infer_activity_type(text: str) -> str:
    if _has_any_keywords(text, ("talk", "lecture", "conversation")):
        return "talk"
    if _has_any_keywords(text, ("workshop", "class", "lab", "art making")):
        return "workshop"
    return "activity"


def _has_any_keywords(text: str, keywords: tuple[str, ...]) -> bool:
    return any(_contains_keyword(text, keyword) for keyword in keywords)


def _contains_keyword(text: str, keyword: str) -> bool:
    return bool(re.search(rf"\b{re.escape(keyword)}\b", text, re.IGNORECASE))


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.replace("\xa0", " ").replace("\u202f", " ").split())
    return normalized or None
