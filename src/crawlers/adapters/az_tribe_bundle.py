from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from urllib.parse import urlencode
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

AZ_TIMEZONE = "America/Phoenix"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " art making ",
    " artmaking ",
    " class ",
    " classes ",
    " create artwork ",
    " creative ",
    " discussion ",
    " hands on ",
    " hands-on ",
    " lecture ",
    " lectures ",
    " mini talks ",
    " symposium ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
WEAK_INCLUDE_PATTERNS = (
    " art ",
    " artist ",
    " family ",
    " kid ",
    " kids ",
    " teen ",
    " teens ",
    " youth ",
)
ALWAYS_REJECT_PATTERNS = (
    " admission ",
    " camp ",
    " camps ",
    " concert ",
    " exhibit ",
    " exhibition ",
    " exhibitions ",
    " fair ",
    " film ",
    " fundraiser ",
    " gala ",
    " market ",
    " marketplace ",
    " member preview ",
    " members preview ",
    " movie ",
    " opening celebration ",
    " performance ",
    " performances ",
    " reading ",
    " reception ",
    " storytime ",
    " storytelling ",
    " tour ",
    " tours ",
    " trivia ",
    " yoga ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " first friday ",
    " first thursday ",
    " free admission ",
    " pay what you wish admission ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
)
REGISTRATION_PATTERNS = (
    " preregistration ",
    " register ",
    " registration ",
    " reservation ",
    " reserve ",
    " rsvp ",
    " ticket required ",
    " tickets required ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class AzTribeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    api_url: str
    default_location: str


AZ_TRIBE_VENUES: tuple[AzTribeVenueConfig, ...] = (
    AzTribeVenueConfig(
        slug="asu_art_museum",
        source_name="az_asu_art_museum_events",
        venue_name="ASU Art Museum",
        city="Tempe",
        state="AZ",
        list_url="https://asuartmuseum.org/exhibitions-and-events/events/",
        api_url="https://asuartmuseum.org/wp-json/tribe/events/v1/events",
        default_location="ASU Art Museum, Tempe, AZ",
    ),
    AzTribeVenueConfig(
        slug="center_for_creative_photography",
        source_name="az_center_for_creative_photography_events",
        venue_name="Center for Creative Photography",
        city="Tucson",
        state="AZ",
        list_url="https://ccp.arizona.edu/events/",
        api_url="https://ccp.arizona.edu/wp-json/tribe/events/v1/events",
        default_location="Center for Creative Photography, Tucson, AZ",
    ),
    AzTribeVenueConfig(
        slug="tucson_museum_of_art",
        source_name="az_tucson_museum_of_art_events",
        venue_name="Tucson Museum of Art and Historic Block",
        city="Tucson",
        state="AZ",
        list_url="https://www.tucsonmuseumofart.org/events/month/",
        api_url="https://www.tucsonmuseumofart.org/wp-json/tribe/events/v1/events",
        default_location="Tucson Museum of Art and Historic Block, Tucson, AZ",
    ),
)

AZ_TRIBE_VENUES_BY_SLUG = {venue.slug: venue for venue in AZ_TRIBE_VENUES}


class AzTribeBundleAdapter(BaseSourceAdapter):
    source_name = "az_tribe_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_az_tribe_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_az_tribe_bundle_payload/parse_az_tribe_events from the script runner.")


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
                    raise RuntimeError(
                        "AZ tribe endpoint returned non-JSON content: "
                        f"url={response.url} status={response.status_code}"
                    ) from exc
                if isinstance(payload, dict):
                    return payload
                raise RuntimeError(
                    "AZ tribe endpoint returned unexpected payload type: "
                    f"{type(payload).__name__}"
                )

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch AZ tribe endpoint") from last_exception
    raise RuntimeError("Unable to fetch AZ tribe endpoint after retries")


async def load_az_tribe_bundle_payload(
    *,
    venues: list[AzTribeVenueConfig] | tuple[AzTribeVenueConfig, ...] | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, dict]:
    selected = list(venues) if venues is not None else list(AZ_TRIBE_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}
    today = datetime.now(ZoneInfo(AZ_TIMEZONE)).date().isoformat()

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            events: list[dict] = []
            next_url = _with_query(venue.api_url, per_page=per_page, page=1, start_date=today)
            pages_seen = 0

            try:
                while next_url:
                    if page_limit is not None and pages_seen >= max(page_limit, 1):
                        break
                    page_payload = await fetch_tribe_events_page(next_url, client=client)
                    pages_seen += 1
                    events.extend(page_payload.get("events") or [])
                    next_url = page_payload.get("next_rest_url")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[az-tribe-fetch] venue={venue.slug} failed: {exc}")
                continue

            payload_by_slug[venue.slug] = {"events": events}

    return {"payload_by_slug": payload_by_slug, "errors_by_slug": errors_by_slug}


def parse_az_tribe_events(payload: dict, *, venue: AzTribeVenueConfig) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(AZ_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        row = _build_tribe_row(event_obj, venue=venue)
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


def get_az_tribe_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in AZ_TRIBE_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)


def _build_tribe_row(event_obj: dict, *, venue: AzTribeVenueConfig) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("end_date"))
    description_text = _html_to_text(event_obj.get("description"))
    excerpt_text = _html_to_text(event_obj.get("excerpt"))
    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    price_text = _normalize_space(event_obj.get("cost") or "")
    location_text = _extract_location_name(event_obj.get("venue")) or venue.default_location

    description_parts = [part for part in [excerpt_text, description_text] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = " | ".join(description_parts) if description_parts else None

    title_blob = _searchable_blob(" ".join([title, " ".join(category_names)]))
    token_blob = _searchable_blob(" ".join([title, description or "", " ".join(category_names)]))
    if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
        return None

    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))
    amount = _extract_amount(event_obj.get("cost_details"))

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(token_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=_registration_required(token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=AZ_TIMEZONE,
        **price_classification_kwargs_from_amount(amount, text=price_text or description),
    )


def _should_keep_event(*, title_blob: str, token_blob: str) -> bool:
    if any(pattern in title_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if any(pattern in title_blob for pattern in CONTEXTUAL_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS) and not any(
        pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS
    ):
        return False

    has_strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    if not has_strong_include:
        if not any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS):
            return False
        if " artmaking " not in token_blob and " hands on " not in token_blob and " hands-on " not in token_blob:
            return False

    return True


def _infer_activity_type(token_blob: str) -> str:
    if any(
        pattern in token_blob
        for pattern in (
            " conversation ",
            " discussion ",
            " lecture ",
            " lectures ",
            " symposium ",
            " talk ",
            " talks ",
        )
    ):
        return "lecture"
    return "workshop"


def _registration_required(token_blob: str) -> bool | None:
    if any(pattern in token_blob for pattern in REGISTRATION_PATTERNS):
        return True
    if " registration closed " in token_blob:
        return True
    return None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


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
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None)
    except ValueError:
        return None


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
    state = _normalize_space(venue_obj.get("state") or venue_obj.get("province"))
    parts = [part for part in [name, city, state] if part]
    return ", ".join(parts) if parts else None


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    without_shortcodes = re.sub(r"\[[^\]]+\]", " ", value)
    return _normalize_space(BeautifulSoup(without_shortcodes, "html.parser").get_text(" ", strip=True))


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _with_query(url: str, **params: object) -> str:
    query = urlencode({key: value for key, value in params.items() if value is not None})
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{query}" if query else url
