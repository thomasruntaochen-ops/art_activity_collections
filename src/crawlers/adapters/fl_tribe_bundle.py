import asyncio
import json
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

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " animation ",
    " art making ",
    " ceramics ",
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " creative ",
    " discussion ",
    " discussions ",
    " drawing ",
    " family studio ",
    " lecture ",
    " lectures ",
    " maker ",
    " makers ",
    " mini makers ",
    " open studio ",
    " paint ",
    " painting ",
    " panel ",
    " panels ",
    " portrait ",
    " sketch ",
    " studio ",
    " student artist ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
WEAK_INCLUDE_PATTERNS = (
    " art ",
    " families ",
    " family ",
    " kid ",
    " kids ",
    " teen ",
    " teens ",
    " youth ",
)
ALWAYS_REJECT_PATTERNS = (
    " admission ",
    " book club ",
    " camp ",
    " camps ",
    " concert ",
    " dinner ",
    " film ",
    " films ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " jazz ",
    " meditation ",
    " mindful ",
    " mindfulness ",
    " music ",
    " orchestra ",
    " performance ",
    " performances ",
    " poetry ",
    " reception ",
    " story time ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " exhibition ",
    " exhibitions ",
    " member event ",
    " member events ",
    " picnic ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
    " open studio ",
)
REGISTRATION_PATTERNS = (
    " register ",
    " registration ",
    " reserve ",
    " reserved tickets ",
    " tickets required ",
    " ticket required ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class FlTribeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    api_url: str


FL_TRIBE_VENUES: tuple[FlTribeVenueConfig, ...] = (
    FlTribeVenueConfig(
        slug="orlando",
        source_name="orlando_museum_events",
        venue_name="Orlando Museum of Art",
        city="Orlando",
        state="FL",
        list_url="https://omart.org/events/",
        api_url="https://omart.org/wp-json/tribe/events/v1/events",
    ),
    FlTribeVenueConfig(
        slug="harn",
        source_name="harn_events",
        venue_name="Samuel P. Harn Museum of Art",
        city="Gainesville",
        state="FL",
        list_url="https://harn.ufl.edu/calendar/",
        api_url="https://harn.ufl.edu/wp-json/tribe/events/v1/events",
    ),
    FlTribeVenueConfig(
        slug="tampa",
        source_name="tampa_museum_events",
        venue_name="Tampa Museum of Art",
        city="Tampa",
        state="FL",
        list_url="https://tampamuseum.org/events/",
        api_url="https://tampamuseum.org/wp-json/tribe/events/v1/events",
    ),
    FlTribeVenueConfig(
        slug="bass",
        source_name="bass_events",
        venue_name="The Bass",
        city="Miami Beach",
        state="FL",
        list_url="https://thebass.org/events/",
        api_url="https://thebass.org/wp-json/tribe/events/v1/events",
    ),
    FlTribeVenueConfig(
        slug="dali",
        source_name="dali_events",
        venue_name="The Dali Museum",
        city="St. Petersburg",
        state="FL",
        list_url="https://thedali.org/events/category/featured/",
        api_url="https://thedali.org/wp-json/tribe/events/v1/events?categories=featured",
    ),
)

FL_TRIBE_VENUES_BY_SLUG = {venue.slug: venue for venue in FL_TRIBE_VENUES}


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
                        "FL tribe endpoint returned non-JSON success response: "
                        f"url={response.url} status={response.status_code} "
                        f"content_type={response.headers.get('content-type', '')!r} "
                        f"snippet={snippet!r}"
                    ) from exc

                if not isinstance(payload, dict):
                    snippet = _debug_snippet(response.text)
                    raise RuntimeError(
                        "FL tribe endpoint returned unexpected JSON payload: "
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
        raise RuntimeError("Unable to fetch FL tribe events endpoint") from last_exception
    raise RuntimeError("Unable to fetch FL tribe events endpoint after retries")


async def load_fl_tribe_bundle_payload(
    *,
    venues: list[FlTribeVenueConfig] | tuple[FlTribeVenueConfig, ...] | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, dict[str, list[dict]] | dict[str, str]]:
    events_by_slug: dict[str, list[dict]] = {}
    errors_by_slug: dict[str, str] = {}
    venues_to_load = list(venues) if venues is not None else list(FL_TRIBE_VENUES)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in venues_to_load:
            events: list[dict] = []
            next_url = _with_per_page(venue.api_url, per_page)
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
                print(f"[fl-tribe-fetch] venue={venue.slug} failed: {exc}")
                continue
            events_by_slug[venue.slug] = events

    return {"events_by_slug": events_by_slug, "errors_by_slug": errors_by_slug}


def parse_fl_tribe_events(
    events: list[dict],
    *,
    venue: FlTribeVenueConfig,
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


class FlTribeBundleAdapter(BaseSourceAdapter):
    source_name = "fl_tribe_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_fl_tribe_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_fl_tribe_bundle_payload/parse_fl_tribe_events from script runner.")


def _build_row(event_obj: dict, *, venue: FlTribeVenueConfig) -> ExtractedActivity | None:
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
    is_free, free_status = infer_price_classification_from_amount(
        amount,
        text=price_text or description,
    )
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
    if " cancelled " in token_blob or " canceled " in token_blob or " sold out " in token_blob:
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
    if any(pattern in token_blob for pattern in (" lecture ", " lectures ", " talk ", " talks ", " panel ", " conversation ", " conversations ", " discussion ")):
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


def get_fl_tribe_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in FL_TRIBE_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)


def _with_per_page(url: str, per_page: int) -> str:
    delimiter = "&" if "?" in url else "?"
    return f"{url}{delimiter}per_page={per_page}"


def _debug_snippet(value: str | None, *, limit: int = 200) -> str:
    if not value:
        return ""
    return " ".join(value.split())[:limit]
