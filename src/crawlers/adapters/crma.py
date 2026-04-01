import asyncio
import json
import re
from datetime import date, datetime, timedelta
from urllib.parse import urlencode

import httpx
from bs4 import BeautifulSoup

from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

CRMA_CALENDAR_URL = "https://www.crma.org/full-events-calendar"
CRMA_FC_EVENTS_URL = "https://www.crma.org/full-events-calendar/get_events/163"
CRMA_VENUE_NAME = "Cedar Rapids Museum of Art"
CRMA_CITY = "Cedar Rapids"
CRMA_STATE = "IA"
CRMA_TIMEZONE = "America/Chicago"
CRMA_DEFAULT_LOCATION = "Cedar Rapids, IA"

MONTHS_AHEAD = 6
DETAIL_CONCURRENCY = 5

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.crma.org/",
}

AGE_RANGE_RE = re.compile(
    r"\bages?\s*(\d{1,2})\s*(?:-|\u2013|to)\s*(\d{1,2})\b", re.IGNORECASE
)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)

# Phrases whose presence as a primary topic disqualifies the event.
# Each entry is matched as a substring against lowercased combined text.
_EXCLUDE_KEYWORDS = (
    "tour",
    "book club",
    "storytime",
    "story time",
    "pajama",
    "meditation",
    "mindfulness",
    "yoga",
    " camp",
    "film screening",
    "reception",
    "dinner",
    "jazz",
    "poetry",
    "poem",
    "orchestra",
    "tai chi",
    "open house",
    "fundrais",
    "free night",
    "free admission",
    "free third thursday",
    "exhibition opening",
    "concert",
    "guitar",
    "music performance",
    "musical artist",
)

# Patterns that confirm the event is a qualifying art activity.
# Use \b word boundaries for short tokens that are substrings of common words
# (e.g. "class" ⊂ "classic", "lab" ⊂ "elaborate", "demo" ⊂ "democracy").
_INCLUDE_RE = re.compile(
    r"\b(?:class(?:es)?|workshop|lecture|lab\b|conversation|activity|activities|"
    r"doodlebug|art\s+bites|art\s+journal|salon|demonstration|demo\b|family\s+fun|"
    r"jam\s+session|art\s+jam)\b",
    re.IGNORECASE,
)


def _has_include(text: str) -> bool:
    return bool(_INCLUDE_RE.search(text))


def _has_exclude(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in _EXCLUDE_KEYWORDS)


def _should_keep(title: str, description: str | None) -> bool:
    if is_irrelevant_item_text(title):
        return False
    combined = f"{title} {description or ''}".lower()
    has_include = _has_include(combined)
    has_exclude = _has_exclude(combined)
    # Exclusion wins when present and no inclusion signal rescues the event.
    if has_exclude and not has_include:
        return False
    return True


def _infer_activity_type(title: str) -> str:
    lower = title.lower()
    if "art bites" in lower or "talk" in lower or "lecture" in lower or "salon" in lower:
        return "talk"
    if "doodlebug" in lower or "class" in lower or "family fun" in lower:
        return "class"
    return "workshop"


def _infer_is_free(title: str, description: str | None) -> bool | None:
    title_norm = title.rstrip("\u00a0").strip().lower()
    # Title explicitly marks the event as free.
    if title_norm.endswith(", free") or ", free " in title_norm or title_norm.endswith(" free"):
        return True
    # Description mentions a price.
    if "$" in (description or ""):
        return False
    return None


def _parse_age_range(title: str, description: str | None) -> tuple[int | None, int | None]:
    blob = f"{title} {description or ''}"
    match = AGE_RANGE_RE.search(blob)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = AGE_PLUS_RE.search(blob)
    if match:
        return int(match.group(1)), None
    return None, None


def _parse_fc_datetime(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(dt_str.strip(), fmt)
        except ValueError:
            continue
    return None


async def _fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    max_attempts: int = 3,
    base_backoff: float = 1.5,
    label: str = "",
) -> str | None:
    for attempt in range(1, max_attempts + 1):
        try:
            r = await client.get(url)
            if r.status_code < 400:
                return r.text
            if r.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff * (2 ** (attempt - 1)))
                continue
            print(f"[crma-fetch] {label} HTTP {r.status_code} for {url}")
            return None
        except httpx.HTTPError as exc:
            if attempt < max_attempts:
                await asyncio.sleep(base_backoff * (2 ** (attempt - 1)))
            else:
                print(f"[crma-fetch] {label} error: {exc} for {url}")
                return None
    return None


async def fetch_crma_events(
    *,
    months_ahead: int = MONTHS_AHEAD,
    detail_concurrency: int = DETAIL_CONCURRENCY,
) -> list[dict]:
    """Fetch events from the FullCalendar JSON API and enrich with detail page descriptions."""
    today = date.today()
    # Advance by months_ahead calendar months.
    month = today.month - 1 + months_ahead
    end_year = today.year + month // 12
    end_month = month % 12 + 1
    end_date = date(end_year, end_month, 1)

    params = {"start": today.isoformat(), "end": end_date.isoformat()}
    list_url = f"{CRMA_FC_EVENTS_URL}?{urlencode(params)}"
    print(f"[crma] Fetching events: {list_url}")

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        events_text = await _fetch_with_retry(client, list_url, label="events-list")
        if not events_text:
            print("[crma] Failed to fetch events list")
            return []

        try:
            events: list[dict] = json.loads(events_text)
        except json.JSONDecodeError as exc:
            print(f"[crma] JSON parse error: {exc}")
            return []

        print(f"[crma] Got {len(events)} raw events from FullCalendar endpoint")

        sem = asyncio.Semaphore(detail_concurrency)

        async def _enrich(event: dict) -> dict:
            url = event.get("url") or ""
            if not url:
                return {**event, "description": None}
            async with sem:
                html = await _fetch_with_retry(
                    client, url, label=f"detail({event.get('id', '')})"
                )
            if not html:
                return {**event, "description": None}
            soup = BeautifulSoup(html, "html.parser")
            desc_div = soup.select_one(".ccm-block-calendar-event-description")
            description = desc_div.get_text(" ", strip=True) if desc_div else None
            return {**event, "description": description or None}

        enriched = await asyncio.gather(*[_enrich(ev) for ev in events])
        return list(enriched)


def parse_crma_events(events: list[dict]) -> list[ExtractedActivity]:
    """Filter and convert enriched CRMA events to ExtractedActivity objects."""
    results: list[ExtractedActivity] = []
    seen: set[tuple[str, datetime]] = set()

    for event in events:
        title = (event.get("title") or "").strip().rstrip("\u00a0").strip()
        description = event.get("description")

        if not _should_keep(title, description):
            print(f"[crma] skip (filtered): {title!r}")
            continue

        start_at = _parse_fc_datetime(event.get("start"))
        if start_at is None:
            print(f"[crma] skip (no date): {title!r}")
            continue

        end_at = _parse_fc_datetime(event.get("end"))
        source_url = event.get("url") or CRMA_CALENDAR_URL

        key = (source_url, start_at)
        if key in seen:
            continue
        seen.add(key)

        age_min, age_max = _parse_age_range(title, description)
        is_free = _infer_is_free(title, description)
        activity_type = _infer_activity_type(title)
        text_blob = f"{title} {description or ''}".lower()

        results.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=CRMA_VENUE_NAME,
                location_text=CRMA_DEFAULT_LOCATION,
                city=CRMA_CITY,
                state=CRMA_STATE,
                activity_type=activity_type,
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=(
                    "registration" in text_blob and "not required" not in text_blob
                ),
                start_at=start_at,
                end_at=end_at,
                timezone=CRMA_TIMEZONE,
                is_free=is_free,
                free_verification_status="inferred",
            )
        )

    print(f"[crma] {len(results)} events kept after filtering")
    return results
