import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

CT_TIMEZONE = "America/Chicago"

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
    " art activity ",
    " art making ",
    " art making for ",
    " art talk ",
    " artmaking ",
    " artsplay ",
    " art studi ",
    " beading ",
    " class ",
    " classes ",
    " collage ",
    " conversation ",
    " craft ",
    " discussions ",
    " drawing ",
    " gallery talk ",
    " guided class ",
    " journal ",
    " journaling ",
    " lab ",
    " lecture ",
    " lectures ",
    " little learners ",
    " makers ",
    " nature journaling ",
    " painting ",
    " roundtable ",
    " sketch ",
    " sketching ",
    " stitching ",
    " studio ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)
WEAK_INCLUDE_PATTERNS = (
    " adult ",
    " baby ",
    " child ",
    " children ",
    " family ",
    " families ",
    " kid ",
    " kids ",
    " teen ",
    " teens ",
    " youth ",
)
NON_OVERRIDE_REJECT_PATTERNS = (
    " book club ",
    " camp ",
    " camps ",
    " member event ",
    " member events ",
    " operating hours ",
    " tai chi ",
    " yoga ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " concert ",
    " concerts ",
    " exhibition ",
    " exhibitions ",
    " festival ",
    " film ",
    " films ",
    " gala ",
    " music ",
    " performance ",
    " performances ",
    " reception ",
    " screening ",
    " tour ",
    " tours ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
    " no pre registration required ",
    " no preregistration required ",
    " no registration required ",
)
REGISTER_PATTERNS = (
    " preregister ",
    " pre-register ",
    " pre register ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
    " sign up ",
)
NEGATIVE_REGISTER_PATTERNS = (
    " no pre registration required ",
    " no pre-registration required ",
    " no preregistration required ",
    " no registration required ",
    " registration is not required ",
)
GENERIC_SERIES_TITLE_PATTERNS = (
    " gallery talks every wednesday ",
)
PERFORMANCE_FIRST_TITLE_PATTERNS = (
    " musical encounters ",
    " ogden after hours ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class LaTribeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    api_url: str


LA_TRIBE_VENUES: tuple[LaTribeVenueConfig, ...] = (
    LaTribeVenueConfig(
        slug="noma",
        source_name="noma_events",
        venue_name="New Orleans Museum of Art",
        city="New Orleans",
        state="LA",
        list_url="https://noma.org/events/",
        api_url="https://noma.org/wp-json/tribe/events/v1/events",
    ),
    LaTribeVenueConfig(
        slug="ogden",
        source_name="ogden_events",
        venue_name="Ogden Museum of Southern Art",
        city="New Orleans",
        state="LA",
        list_url="https://ogdenmuseum.org/events/",
        api_url="https://ogdenmuseum.org/wp-json/tribe/events/v1/events",
    ),
    LaTribeVenueConfig(
        slug="hilliard",
        source_name="hilliard_events",
        venue_name="Hilliard Art Museum",
        city="Lafayette",
        state="LA",
        list_url="https://hilliardartmuseum.org/events/",
        api_url="https://hilliardartmuseum.org/wp-json/tribe/events/v1/events",
    ),
    LaTribeVenueConfig(
        slug="rwnorton",
        source_name="rw_norton_events",
        venue_name="R.W. Norton Art Gallery",
        city="Shreveport",
        state="LA",
        list_url="https://rwnaf.org/events/",
        api_url="https://rwnaf.org/wp-json/tribe/events/v1/events",
    ),
)

LA_TRIBE_VENUES_BY_SLUG = {venue.slug: venue for venue in LA_TRIBE_VENUES}


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
                    snippet = _debug_snippet(response.text)
                    raise RuntimeError(
                        "Louisiana tribe endpoint returned non-JSON success response: "
                        f"url={response.url} status={response.status_code} snippet={snippet!r}"
                    ) from exc
                if not isinstance(payload, dict):
                    raise RuntimeError(
                        "Louisiana tribe endpoint returned unexpected JSON payload: "
                        f"url={response.url} payload_type={type(payload).__name__}"
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
        raise RuntimeError("Unable to fetch Louisiana tribe endpoint") from last_exception
    raise RuntimeError("Unable to fetch Louisiana tribe endpoint after retries")


async def load_la_tribe_bundle_payload(
    *,
    venues: list[LaTribeVenueConfig] | tuple[LaTribeVenueConfig, ...] | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, dict[str, list[dict]] | dict[str, str]]:
    events_by_slug: dict[str, list[dict]] = {}
    errors_by_slug: dict[str, str] = {}
    venues_to_load = list(venues) if venues is not None else list(LA_TRIBE_VENUES)

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
                print(f"[la-tribe-fetch] venue={venue.slug} failed: {exc}")
                continue
            events_by_slug[venue.slug] = events

    return {"events_by_slug": events_by_slug, "errors_by_slug": errors_by_slug}


def parse_la_tribe_events(
    events: list[dict],
    *,
    venue: LaTribeVenueConfig,
) -> list[ExtractedActivity]:
    today = datetime.now(ZoneInfo(CT_TIMEZONE)).date()
    horizon = today + timedelta(days=365)
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in events:
        row = _build_row(event_obj, venue=venue)
        if row is None:
            continue
        if row.start_at.date() < today:
            continue
        if row.start_at.date() > horizon:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class LaTribeBundleAdapter(BaseSourceAdapter):
    source_name = "la_tribe_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_la_tribe_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_la_tribe_bundle_payload/parse_la_tribe_events from script runner.")


def _build_row(event_obj: dict, *, venue: LaTribeVenueConfig) -> ExtractedActivity | None:
    title = _clean_html(event_obj.get("title"))
    if not title:
        return None

    description = _clean_html(event_obj.get("description"))
    excerpt = _clean_html(event_obj.get("excerpt"))
    category_text = " | ".join(
        _clean_html(category.get("name"))
        for category in (event_obj.get("categories") or [])
        if _clean_html(category.get("name"))
    )
    cost_text = _normalize_space(event_obj.get("cost"))
    token_blob = _searchable_blob(" ".join(part for part in [title, description, excerpt, category_text, cost_text] if part))
    title_blob = _searchable_blob(title)
    if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
        return None

    start_at = _parse_datetime(event_obj.get("start_date") or event_obj.get("startDate"))
    if start_at is None:
        return None
    end_at = _parse_datetime(event_obj.get("end_date") or event_obj.get("endDate"))

    venue_obj = event_obj.get("venue") or {}
    location_text = _extract_location_text(venue_obj, venue)
    description_text = _join_non_empty(
        [
            description or excerpt,
            category_text and f"Category: {category_text}",
            cost_text and f"Cost: {cost_text}",
        ]
    )
    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description, excerpt] if part))

    return ExtractedActivity(
        source_url=_normalize_space(event_obj.get("url")) or venue.list_url,
        title=title,
        description=description_text,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(token_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=_requires_registration(token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=CT_TIMEZONE,
        **price_classification_kwargs_from_amount(_extract_offer_amount(event_obj.get("cost_details")), text=cost_text),
    )


def _should_keep_event(*, title_blob: str, token_blob: str) -> bool:
    if any(pattern in title_blob for pattern in GENERIC_SERIES_TITLE_PATTERNS):
        return False
    if any(pattern in title_blob for pattern in PERFORMANCE_FIRST_TITLE_PATTERNS):
        return False
    if " tour " in title_blob or " tours " in title_blob:
        return False
    if any(pattern in title_blob for pattern in NON_OVERRIDE_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in NON_OVERRIDE_REJECT_PATTERNS):
        return False

    strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    weak_include = any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS)
    art_context = any(pattern in token_blob for pattern in (" art ", " museum ", " gallery ", " creative "))
    if not strong_include and not (weak_include and art_context):
        return False

    if any(pattern in title_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_include:
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_include:
        return False

    return True


def _infer_activity_type(token_blob: str) -> str:
    if any(
        pattern in token_blob
        for pattern in (
            " conversation ",
            " discussion ",
            " gallery talk ",
            " gallery talks ",
            " lecture ",
            " roundtable ",
            " talk ",
            " talks ",
        )
    ):
        return "lecture"
    return "workshop"


def _requires_registration(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in NEGATIVE_REGISTER_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in DROP_IN_PATTERNS):
        return False
    return any(pattern in token_blob for pattern in REGISTER_PATTERNS)


def _extract_location_text(venue_obj: object, venue: LaTribeVenueConfig) -> str:
    if not isinstance(venue_obj, dict):
        return f"{venue.venue_name}, {venue.city}, {venue.state}"
    parts = [
        _normalize_space(venue_obj.get("venue")),
        _normalize_space(venue_obj.get("address")),
        _normalize_space(venue_obj.get("city")),
        _normalize_space(venue_obj.get("state") or venue_obj.get("stateprovince") or venue_obj.get("province")),
    ]
    cleaned: list[str] = []
    for part in parts:
        if not part:
            continue
        if part in cleaned:
            continue
        cleaned.append(part)
    if cleaned:
        return ", ".join(cleaned)
    return f"{venue.venue_name}, {venue.city}, {venue.state}"


def _extract_offer_amount(cost_details: object) -> Decimal | None:
    if not isinstance(cost_details, dict):
        return None
    values = cost_details.get("values")
    if not isinstance(values, list):
        return None
    for value in values:
        cleaned = re.sub(r"[^0-9.]+", "", str(value))
        if not cleaned:
            continue
        try:
            return Decimal(cleaned)
        except Exception:
            continue
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

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is not None:
                return parsed.astimezone(ZoneInfo(CT_TIMEZONE)).replace(tzinfo=None)
            return parsed
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(ZoneInfo(CT_TIMEZONE)).replace(tzinfo=None)
    return parsed


def _with_per_page(url: str, per_page: int) -> str:
    if "per_page=" in url:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}per_page={per_page}"


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _clean_html(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _join_non_empty(parts: list[str | None]) -> str | None:
    cleaned = [_normalize_space(part) for part in parts if _normalize_space(part)]
    if not cleaned:
        return None
    return " | ".join(cleaned)


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _debug_snippet(text: str, limit: int = 200) -> str:
    return _normalize_space(text)[:limit]


def get_la_tribe_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in LA_TRIBE_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)
