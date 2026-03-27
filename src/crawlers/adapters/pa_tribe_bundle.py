import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

NY_TIMEZONE = "America/New_York"
MAX_FUTURE_DAYS = 180

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
    " art club ",
    " artmaking ",
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " discussion ",
    " discussions ",
    " drawing ",
    " figure drawing ",
    " gallery talk ",
    " lecture ",
    " lectures ",
    " open studio ",
    " studio ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
WEAK_INCLUDE_PATTERNS = STRONG_INCLUDE_PATTERNS + (
    " education ",
    " family ",
    " families ",
    " kids ",
    " kid ",
    " teen ",
    " teens ",
    " youth ",
)
ALWAYS_REJECT_PATTERNS = (
    " admission ",
    " camp ",
    " camps ",
    " concert ",
    " exhibition ",
    " exhibitions ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " membership ",
    " members day ",
    " movie ",
    " music ",
    " open house ",
    " party ",
    " performance ",
    " poetry ",
    " reading ",
    " reception ",
    " sold out ",
    " storytime ",
    " story time ",
    " tour ",
    " tours ",
    " yoga ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " art in bloom ",
    " community day ",
    " family day ",
    " free day ",
    " social ",
    " third in the burg ",
)
DROP_IN_PATTERNS = (" drop in ", " drop-in ", " open studio ", " open figure drawing ")
REGISTRATION_PATTERNS = (" register ", " registration ", " tickets ", " ticket ")
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class PaTribeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    api_url: str


PA_TRIBE_VENUES: tuple[PaTribeVenueConfig, ...] = (
    PaTribeVenueConfig(
        slug="susquehanna",
        source_name="susquehanna_events",
        venue_name="Susquehanna Art Museum",
        city="Harrisburg",
        state="PA",
        list_url="https://susquehannaartmuseum.org/upcoming-events/",
        api_url="https://susquehannaartmuseum.org/wp-json/tribe/events/v1/events",
    ),
    PaTribeVenueConfig(
        slug="warhol",
        source_name="warhol_events",
        venue_name="The Andy Warhol Museum",
        city="Pittsburgh",
        state="PA",
        list_url="https://www.warhol.org/calendar/",
        api_url="https://www.warhol.org/wp-json/tribe/events/v1/events",
    ),
    PaTribeVenueConfig(
        slug="westmoreland",
        source_name="westmoreland_events",
        venue_name="The Westmoreland Museum of American Art",
        city="Greensburg",
        state="PA",
        list_url="https://thewestmoreland.org/events/",
        api_url="https://thewestmoreland.org/wp-json/tribe/events/v1/events",
    ),
)

PA_TRIBE_VENUES_BY_SLUG = {venue.slug: venue for venue in PA_TRIBE_VENUES}


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
                    snippet = _debug_snippet(response.text)
                    raise RuntimeError(
                        "PA tribe endpoint returned a non-JSON success response: "
                        f"url={response.url} status={response.status_code} "
                        f"content_type={response.headers.get('content-type', '')!r} "
                        f"snippet={snippet!r}"
                    ) from exc

                if not isinstance(payload, dict):
                    snippet = _debug_snippet(response.text)
                    raise RuntimeError(
                        "PA tribe endpoint returned an unexpected JSON payload: "
                        f"url={response.url} status={response.status_code} "
                        f"payload_type={type(payload).__name__} snippet={snippet!r}"
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
        raise RuntimeError("Unable to fetch PA tribe events endpoint") from last_exception
    raise RuntimeError("Unable to fetch PA tribe events endpoint after retries")


async def load_pa_tribe_bundle_payload(
    *,
    venues: list[PaTribeVenueConfig] | tuple[PaTribeVenueConfig, ...] | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, dict[str, list[dict]] | dict[str, str]]:
    events_by_slug: dict[str, list[dict]] = {}
    errors_by_slug: dict[str, str] = {}
    venues_to_load = list(venues) if venues is not None else list(PA_TRIBE_VENUES)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in venues_to_load:
            events: list[dict] = []
            next_url = f"{venue.api_url}?per_page={per_page}"
            pages_seen = 0
            try:
                while next_url:
                    if page_limit is not None and pages_seen >= max(page_limit, 1):
                        break
                    payload = await fetch_tribe_events_page(next_url, client=client)
                    pages_seen += 1
                    events.extend(payload.get("events") or [])
                    next_url = payload.get("next_rest_url")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[pa-tribe-fetch] venue={venue.slug} failed: {exc}")
                continue
            events_by_slug[venue.slug] = events

    return {"events_by_slug": events_by_slug, "errors_by_slug": errors_by_slug}


def parse_pa_tribe_events(
    events: list[dict],
    *,
    venue: PaTribeVenueConfig,
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
        if (row.start_at.date() - current_date).days > MAX_FUTURE_DAYS:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class PaTribeBundleAdapter(BaseSourceAdapter):
    source_name = "pa_tribe_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_pa_tribe_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_pa_tribe_bundle_payload/parse_pa_tribe_events from script runner.")


def get_pa_tribe_source_prefixes() -> tuple[str, ...]:
    return tuple(venue.list_url for venue in PA_TRIBE_VENUES)


def _build_row(event_obj: dict, *, venue: PaTribeVenueConfig) -> ExtractedActivity | None:
    title = _normalize_space(_html_to_text(event_obj.get("title")))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("end_date"))

    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    list_description = _html_to_text(event_obj.get("description"))
    excerpt = _html_to_text(event_obj.get("excerpt"))
    price_text = _normalize_space(_html_to_text(event_obj.get("cost") or ""))
    location_text = _extract_location_name(event_obj.get("venue")) or f"{venue.city}, {venue.state}"

    description_parts = [part for part in [excerpt, list_description] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = " | ".join(description_parts) if description_parts else None

    title_blob = _searchable_blob(" ".join([title, " ".join(category_names)]))
    token_blob = _searchable_blob(" ".join([title, description or "", " ".join(category_names)]))
    if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
        return None

    amount = _extract_amount(event_obj.get("cost_details"))
    is_free, free_status = infer_price_classification_from_amount(amount, text=price_text or description)
    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))

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
        registration_required=any(pattern in token_blob for pattern in REGISTRATION_PATTERNS),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _should_keep_event(*, title_blob: str, token_blob: str) -> bool:
    if " cancelled " in token_blob or " canceled " in token_blob or " postponed " in token_blob:
        return False
    if " art in bloom " in title_blob and " workshop " not in title_blob:
        return False
    if " third in the burg " in title_blob:
        return False

    strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    weak_include = strong_include or any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS)
    if not weak_include:
        return False

    if any(pattern in title_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS) and not strong_include:
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_include:
        return False

    return True


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
    state = _normalize_space(venue_obj.get("state") or venue_obj.get("province"))
    parts = [part for part in [name, city, state] if part]
    return ", ".join(parts) if parts else None


def _infer_activity_type(token_blob: str) -> str:
    if any(
        pattern in token_blob
        for pattern in (" class ", " classes ", " workshop ", " workshops ", " studio ", " drawing ", " artmaking ")
    ):
        return "workshop"
    if any(pattern in token_blob for pattern in (" lecture ", " lectures ", " talk ", " talks ", " panel ", " conversation ", " conversations ", " discussion ", " discussions ")):
        return "lecture"
    return "workshop"


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
            return parsed.replace(tzinfo=ZoneInfo(NY_TIMEZONE))
        except ValueError:
            continue
    return None


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _html_to_text(value: object) -> str:
    if not value:
        return ""
    if not isinstance(value, str):
        return _normalize_space(value)
    cleaned = re.sub(r"\[[^\[\]]+\]", " ", value)
    soup = BeautifulSoup(cleaned, "html.parser")
    return _normalize_space(soup.get_text(" ", strip=True))


def _debug_snippet(text: str, *, limit: int = 240) -> str:
    return " ".join(text.split())[:limit]
