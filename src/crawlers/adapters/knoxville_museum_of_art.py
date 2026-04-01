import asyncio
import re
from datetime import datetime
from decimal import Decimal

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

KNOXVILLE_EVENTS_URL = "https://knoxart.org/events/"
KNOXVILLE_EVENTS_API_URL = "https://knoxart.org/wp-json/tribe/events/v1/events"

NY_TIMEZONE = "America/New_York"
MAX_FUTURE_DAYS = 365
KNOXVILLE_VENUE_NAME = "Knoxville Museum of Art"
KNOXVILLE_CITY = "Knoxville"
KNOXVILLE_STATE = "TN"
KNOXVILLE_DEFAULT_LOCATION = "Knoxville Museum of Art, Knoxville, TN"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": KNOXVILLE_EVENTS_URL,
}

INCLUDE_PATTERNS = (
    " activity ",
    " art activity ",
    " art class ",
    " art classes ",
    " class ",
    " classes ",
    " discussion ",
    " drawing ",
    " family ",
    " families ",
    " figure drawing ",
    " hands-on ",
    " lecture ",
    " lectures ",
    " paint ",
    " painting ",
    " panel ",
    " plein air ",
    " puppet ",
    " studio ",
    " talk ",
    " watercolor ",
    " workshop ",
    " workshops ",
)
HARD_EXCLUDE_PATTERNS = (
    " alcohol ",
    " brew ",
    " camp ",
    " camps ",
    " cash bar ",
    " cinema ",
    " concert ",
    " festival ",
    " film ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " music ",
    " open house ",
    " party ",
    " poetry ",
    " reading ",
    " reception ",
    " screening ",
    " soundscapes ",
    " tour ",
    " tours ",
    " wine ",
    " writers guild ",
)

MULTI_SPACE_RE = re.compile(r"\s+")


async def fetch_knoxville_events_page(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> dict:
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
                return response.json()

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Knoxville Museum of Art events API") from last_exception
    raise RuntimeError("Unable to fetch Knoxville Museum of Art events API after retries")


async def load_knoxville_payload(*, page_limit: int | None = None, per_page: int = 50) -> dict:
    events: list[dict] = []
    next_url = f"{KNOXVILLE_EVENTS_API_URL}?per_page={per_page}"
    pages_seen = 0

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while next_url:
            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break
            payload = await fetch_knoxville_events_page(next_url, client=client)
            pages_seen += 1
            events.extend(payload.get("events") or [])
            next_url = payload.get("next_rest_url")

    return {"events": events}


def parse_knoxville_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        row = _build_row(event_obj)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue
        if (row.start_at.date() - current_date).days > MAX_FUTURE_DAYS:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class KnoxvilleMuseumOfArtAdapter(BaseSourceAdapter):
    source_name = "knoxville_museum_of_art_events"

    def __init__(self, url: str = KNOXVILLE_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_knoxville_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_knoxville_payload/parse_knoxville_payload from script runner.")


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("end_date"))
    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    tag_names = [_normalize_space(item.get("name")) for item in (event_obj.get("tags") or [])]
    tag_names = [value for value in tag_names if value]
    price_text = _normalize_space(event_obj.get("cost") or "")

    description_parts = [
        _html_to_text(event_obj.get("excerpt")),
        _html_to_text(event_obj.get("description")),
    ]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if tag_names:
        description_parts.append(f"Tags: {', '.join(tag_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = _join_non_empty(description_parts)

    include_blob = " ".join([title, description or "", " ".join(category_names), " ".join(tag_names)]).lower()
    token_blob = f" {' '.join(include_blob.split())} "
    reject_matches = [pattern for pattern in HARD_EXCLUDE_PATTERNS if pattern in token_blob]
    if _should_reject(token_blob, reject_matches):
        return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    amount = _extract_amount(event_obj.get("cost") or event_obj.get("cost_details"))
    is_free, free_status = infer_price_classification_from_amount(amount, text=price_text or description)
    location_text = _extract_location_name(event_obj.get("venue")) or KNOXVILLE_DEFAULT_LOCATION

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=KNOXVILLE_VENUE_NAME,
        location_text=location_text,
        city=KNOXVILLE_CITY,
        state=KNOXVILLE_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=_extract_age_min(description),
        age_max=_extract_age_max(description),
        drop_in=("drop-in" in token_blob or "drop in" in token_blob),
        registration_required=("register" in token_blob or "ticket" in token_blob or "tuition" in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_amount(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, dict):
        values = value.get("values")
        if isinstance(values, list):
            for item in values:
                try:
                    return Decimal(str(item))
                except Exception:
                    continue
        return None

    text = _normalize_space(value)
    if not text:
        return None
    matches = re.findall(r"\$?\s*(\d+(?:\.\d{1,2})?)", text.replace(",", ""))
    values: list[Decimal] = []
    for match in matches:
        try:
            values.append(Decimal(match))
        except Exception:
            continue
    if not values:
        return None
    return min(values)


def _extract_location_name(venue_obj: object) -> str | None:
    if not isinstance(venue_obj, dict):
        return None
    name = _normalize_space(venue_obj.get("venue"))
    city = _normalize_space(venue_obj.get("city"))
    state = _normalize_space(venue_obj.get("state") or venue_obj.get("stateprovince"))
    parts = [part for part in [name, city, state] if part]
    if not parts:
        return None
    return ", ".join(parts)


def _extract_age_min(description: str | None) -> int | None:
    if not description:
        return None
    match = re.search(r"\bages?\s+(\d+)\s*(?:-|to)\s*(\d+)\b", description, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    match = re.search(r"\bages?\s+(\d+)\+\b", description, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    match = re.search(r"\bfor teens?\s+(\d+)\s*-\s*(\d+)\b", description, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def _extract_age_max(description: str | None) -> int | None:
    if not description:
        return None
    match = re.search(r"\bages?\s+(\d+)\s*(?:-|to)\s*(\d+)\b", description, flags=re.IGNORECASE)
    if match:
        return int(match.group(2))
    match = re.search(r"\bfor teens?\s+(\d+)\s*-\s*(\d+)\b", description, flags=re.IGNORECASE)
    if match:
        return int(match.group(2))
    return None


def _infer_activity_type(text_blob: str) -> str:
    if any(keyword in text_blob for keyword in ("lecture", "talk", "discussion", "panel")):
        return "lecture"
    return "workshop"


def _should_reject(token_blob: str, reject_matches: list[str]) -> bool:
    if not reject_matches:
        return False

    if " reception " in token_blob and any(
        keyword in token_blob
        for keyword in (" lecture ", " lectures ", " talk ", " discussion ", " panel ", " workshop ")
    ):
        reject_matches = [pattern for pattern in reject_matches if pattern != " reception "]

    return bool(reject_matches)


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return parse_iso_datetime(text, timezone_name=NY_TIMEZONE)
    except ValueError:
        return None


def _html_to_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text(" ", strip=True)
    return _normalize_space(text)


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    if not values:
        return None
    return " | ".join(values)


def _normalize_space(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    normalized = MULTI_SPACE_RE.sub(" ", text).strip()
    return normalized or None
