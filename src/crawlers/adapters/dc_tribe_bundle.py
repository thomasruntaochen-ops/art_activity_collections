import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
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

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

INCLUDE_PATTERNS = (
    " art making ",
    " family ",
    " kids ",
    " studio ",
    " teen ",
    " teens ",
    " workshop ",
    " workshops ",
    " youth ",
)
EXCLUDE_PATTERNS = (
    " cancelled ",
    " camp ",
    " camps ",
    " concert ",
    " film ",
    " performance ",
    " storytime ",
    " tour ",
    " tours ",
)


@dataclass(frozen=True, slots=True)
class DcTribeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    api_urls: tuple[str, ...]


DC_TRIBE_VENUES: tuple[DcTribeVenueConfig, ...] = (
    DcTribeVenueConfig(
        slug="hirshhorn",
        source_name="hirshhorn_events",
        venue_name="Hirshhorn Museum and Sculpture Garden",
        city="Washington",
        state="DC",
        list_url="https://hirshhorn.si.edu/explore/teen-studio/",
        api_urls=(
            "https://hirshhorn.si.edu/wp-json/tribe/events/v1/events?per_page=50&categories=teens",
            "https://hirshhorn.si.edu/wp-json/tribe/events/v1/events?per_page=50&categories=hirshhorn-kids",
        ),
    ),
)

DC_TRIBE_VENUES_BY_SLUG = {venue.slug: venue for venue in DC_TRIBE_VENUES}


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
                try:
                    payload = response.json()
                except json.JSONDecodeError as exc:
                    last_exception = exc
                    if attempt < max_attempts:
                        await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                        continue
                    raise RuntimeError(
                        "DC tribe endpoint returned a non-JSON success response: "
                        f"url={response.url} status={response.status_code}"
                    ) from exc

                if not isinstance(payload, dict):
                    raise RuntimeError(
                        "DC tribe endpoint returned an unexpected JSON payload: "
                        f"url={response.url} status={response.status_code}"
                    )
                return payload

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError(f"Unable to fetch DC tribe events endpoint: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch DC tribe events endpoint after retries: {url}")


async def load_dc_tribe_bundle_payload(
    *,
    venues: list[DcTribeVenueConfig] | tuple[DcTribeVenueConfig, ...] | None = None,
) -> dict[str, dict[str, list[dict]] | dict[str, str]]:
    events_by_slug: dict[str, list[dict]] = {}
    errors_by_slug: dict[str, str] = {}
    venues_to_load = list(venues) if venues is not None else list(DC_TRIBE_VENUES)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in venues_to_load:
            merged_events: list[dict] = []
            try:
                for api_url in venue.api_urls:
                    payload = await fetch_tribe_events_page(api_url, client=client)
                    merged_events.extend(payload.get("events") or [])
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[dc-tribe-fetch] venue={venue.slug} failed: {exc}")
                continue
            events_by_slug[venue.slug] = merged_events

    return {"events_by_slug": events_by_slug, "errors_by_slug": errors_by_slug}


def parse_dc_tribe_events(
    events: list[dict],
    *,
    venue: DcTribeVenueConfig,
) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in events:
        row = _build_row(event_obj, venue=venue)
        if row is None or row.start_at.date() < current_date:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class DcTribeBundleAdapter(BaseSourceAdapter):
    source_name = "dc_tribe_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_dc_tribe_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_dc_tribe_bundle_payload/parse_dc_tribe_events from script runner.")


def _build_row(event_obj: dict, *, venue: DcTribeVenueConfig) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("end_date"))
    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    description = _html_to_text(event_obj.get("description"))
    excerpt = _html_to_text(event_obj.get("excerpt"))
    location_name = _extract_location_name(event_obj.get("venue"))
    cost_text = _normalize_space(event_obj.get("cost"))

    description_parts = [part for part in [excerpt, description] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if cost_text:
        description_parts.append(f"Cost: {cost_text}")
    if location_name:
        description_parts.append(f"Location: {location_name}")
    full_description = " | ".join(description_parts) if description_parts else None

    token_blob = _searchable_blob(" ".join([title, full_description or "", " ".join(category_names)]))
    if any(pattern in token_blob for pattern in EXCLUDE_PATTERNS):
        return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    age_min, age_max = _parse_age_range(title=title, description=full_description)
    is_free, free_verification_status = infer_price_classification(
        " ".join(part for part in [cost_text, full_description] if part),
        default_is_free=True,
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=venue.venue_name,
        location_text=location_name or f"{venue.city}, {venue.state}",
        city=venue.city,
        state=venue.state,
        activity_type="workshop",
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in token_blob or " drop in " in token_blob or " walk up " in token_blob),
        registration_required=("registration required" in token_blob and "no registration" not in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_verification_status,
    )


def _extract_location_name(value: object) -> str:
    if isinstance(value, dict):
        for key in ("venue", "address"):
            text = _normalize_space(value.get(key))
            if text:
                return text
    return ""


def _parse_age_range(*, title: str, description: str | None) -> tuple[int | None, int | None]:
    blob = f"{title} {description or ''}"
    range_match = AGE_RANGE_RE.search(blob)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))
    plus_match = AGE_PLUS_RE.search(blob)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(ZoneInfo(NY_TIMEZONE)).replace(tzinfo=None) if parsed.tzinfo else parsed
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


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def get_dc_tribe_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in DC_TRIBE_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)
