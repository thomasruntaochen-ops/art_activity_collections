import asyncio
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

NY_TIMEZONE = "America/New_York"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

INCLUDE_PATTERNS = (
    " activity ",
    " art ",
    " class ",
    " classes ",
    " conversation ",
    " discussion ",
    " family ",
    " families ",
    " kids ",
    " lab ",
    " lecture ",
    " student ",
    " students ",
    " talk ",
    " workshop ",
    " workshops ",
)
HARD_EXCLUDE_PATTERNS = (
    " admission ",
    " camp ",
    " camps ",
    " closed ",
    " concert ",
    " dance ",
    " exhibition ",
    " film ",
    " fundraiser ",
    " fundraising ",
    " gallery closed ",
    " galleries closed ",
    " mindfulness ",
    " music ",
    " open house ",
    " orchestra ",
    " performance ",
    " poetry ",
    " reading ",
    " reception ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)
STRICT_EXCLUDE_PATTERNS = (
    " admission ",
    " camp ",
    " camps ",
    " dinner ",
    " exhibition ",
    " film ",
    " fundraiser ",
    " fundraising ",
    " meditation ",
    " mindfulness ",
    " music ",
    " orchestra ",
    " performance ",
    " reception ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)


@dataclass(frozen=True, slots=True)
class VaTribeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    api_url: str


VA_TRIBE_VENUES: tuple[VaTribeVenueConfig, ...] = (
    VaTribeVenueConfig(
        slug="chrysler",
        source_name="chrysler_events",
        venue_name="Chrysler Museum of Art",
        city="Norfolk",
        state="VA",
        list_url="https://chrysler.org/events/",
        api_url="https://chrysler.org/wp-json/tribe/events/v1/events",
    ),
    VaTribeVenueConfig(
        slug="maier",
        source_name="maier_events",
        venue_name="Maier Museum of Art at Randolph College",
        city="Lynchburg",
        state="VA",
        list_url="https://maiermuseum.org/events/",
        api_url="https://maiermuseum.org/wp-json/tribe/events/v1/events",
    ),
    VaTribeVenueConfig(
        slug="taubman",
        source_name="taubman_events",
        venue_name="Taubman Museum of Art",
        city="Roanoke",
        state="VA",
        list_url="https://www.taubmanmuseum.org/events/",
        api_url="https://www.taubmanmuseum.org/wp-json/tribe/events/v1/events",
    ),
    VaTribeVenueConfig(
        slug="branch",
        source_name="branch_events",
        venue_name="The Branch Museum of Design",
        city="Richmond",
        state="VA",
        list_url="https://branchmuseum.org/makers-studio/",
        api_url="https://branchmuseum.org/wp-json/tribe/events/v1/events",
    ),
    VaTribeVenueConfig(
        slug="virginia_moca",
        source_name="virginia_moca_events",
        venue_name="Virginia Museum of Contemporary Art",
        city="Virginia Beach",
        state="VA",
        list_url="https://virginiamoca.org/event-calendar/",
        api_url="https://virginiamoca.org/wp-json/tribe/events/v1/events",
    ),
    VaTribeVenueConfig(
        slug="william_king",
        source_name="william_king_events",
        venue_name="William King Museum of Art",
        city="Abingdon",
        state="VA",
        list_url="https://williamkingmuseum.org/events/",
        api_url="https://williamkingmuseum.org/wp-json/tribe/events/v1/events",
    ),
)

VA_TRIBE_VENUES_BY_SLUG = {venue.slug: venue for venue in VA_TRIBE_VENUES}


async def fetch_tribe_events_page(
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
        raise RuntimeError("Unable to fetch VA tribe events endpoint") from last_exception
    raise RuntimeError("Unable to fetch VA tribe events endpoint after retries")


async def load_va_tribe_bundle_payload(
    *,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, list[dict]]:
    events_by_slug: dict[str, list[dict]] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in VA_TRIBE_VENUES:
            events: list[dict] = []
            next_url = f"{venue.api_url}?per_page={per_page}"
            pages_seen = 0
            while next_url:
                if page_limit is not None and pages_seen >= max(page_limit, 1):
                    break
                payload = await fetch_tribe_events_page(next_url, client=client)
                pages_seen += 1
                events.extend(payload.get("events") or [])
                next_url = payload.get("next_rest_url")
            events_by_slug[venue.slug] = events

    return events_by_slug


def parse_va_tribe_events(
    events: list[dict],
    *,
    venue: VaTribeVenueConfig,
) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in events:
        row = _build_row(event_obj, venue=venue)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class VaTribeBundleAdapter(BaseSourceAdapter):
    source_name = "va_tribe_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_va_tribe_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_va_tribe_bundle_payload/parse_va_tribe_events from script runner.")


def _build_row(event_obj: dict, *, venue: VaTribeVenueConfig) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("end_date"))

    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    list_description = _html_to_text(event_obj.get("description"))
    excerpt = _html_to_text(event_obj.get("excerpt"))
    price_text = _normalize_space(event_obj.get("cost") or "")

    description_parts = [part for part in [excerpt, list_description] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = " | ".join(description_parts) if description_parts else None

    include_blob = " ".join([title, description or "", " ".join(category_names)]).lower()
    token_blob = _searchable_blob(include_blob)
    if any(pattern in token_blob for pattern in STRICT_EXCLUDE_PATTERNS):
        return None
    if any(pattern in token_blob for pattern in HARD_EXCLUDE_PATTERNS):
        if (
            " workshop " not in token_blob
            and " class " not in token_blob
            and " classes " not in token_blob
            and " lecture " not in token_blob
            and " talk " not in token_blob
            and " conversation " not in token_blob
        ):
            return None
        if " camp " in token_blob or " camps " in token_blob:
            return None
        if " galleries closed " in token_blob or " gallery closed " in token_blob:
            return None
        if " tour " in token_blob or " tours " in token_blob:
            return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    amount = _extract_amount(event_obj.get("cost_details"))
    is_free, free_status = infer_price_classification_from_amount(amount, text=price_text)
    location_text = _extract_location_name(event_obj.get("venue")) or f"{venue.city}, {venue.state}"
    activity_type = _infer_activity_type(token_blob)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=activity_type,
        age_min=None,
        age_max=None,
        drop_in=(" drop-in " in token_blob or " drop in " in token_blob),
        registration_required=(" register " in token_blob or " ticket " in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _extract_amount(cost_details: object) -> Decimal | None:
    if not isinstance(cost_details, dict):
        return None
    values = cost_details.get("values")
    if isinstance(values, list):
        for value in values:
            try:
                return Decimal(str(value))
            except Exception:
                continue
    return None


def _extract_location_name(venue_obj: object) -> str | None:
    if not isinstance(venue_obj, dict):
        return None
    name = _normalize_space(venue_obj.get("venue"))
    city = _normalize_space(venue_obj.get("city"))
    state = _normalize_space(venue_obj.get("state"))
    if not state:
        state = _normalize_space(venue_obj.get("province"))
    parts = [part for part in [name, city, state] if part]
    if not parts:
        return None
    return ", ".join(parts)


def _infer_activity_type(token_blob: str) -> str:
    if (
        " talk " in token_blob
        or " lecture " in token_blob
        or " conversation " in token_blob
        or " discussion " in token_blob
    ):
        return "lecture"
    return "workshop"


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y-%m-%d":
                return datetime.combine(parsed.date(), datetime.min.time())
            return parsed
        except ValueError:
            continue

    try:
        iso_text = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(iso_text)
        return parsed.replace(tzinfo=None)
    except ValueError:
        return None


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def get_va_tribe_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in VA_TRIBE_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)
