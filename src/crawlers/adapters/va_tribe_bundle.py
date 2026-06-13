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
from src.crawlers.pipeline.audience import infer_audience_segment
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
    " artmaking ",
    " class ",
    " classes ",
    " conversation ",
    " discussion ",
    " family ",
    " families ",
    " kids ",
    " lab ",
    " lecture ",
    " open studio ",
    " painting ",
    " presentation ",
    " student ",
    " students ",
    " talk ",
    " workshop ",
    " workshops ",
)
CHRYSLER_TITLE_EXCLUDE_PATTERNS = (
    " mixtape first thursday ",
    " summer intern ",
    " teacher institute ",
    " teacher two day workshop ",
)
CHRYSLER_CATEGORY_EXCLUDE_NAMES = {
    "teacher professional development",
    "zinnia",
}
KIDS_AUDIENCE_PATTERNS = (
    " art babies ",
    " camp art stars ",
    " kids families ",
    " kids family ",
    " pre k ",
    " pre k art play ",
    " wonder wednesday ",
)
ALL_AGES_AUDIENCE_PATTERNS = (
    " all ages ",
    " for all ages ",
    " second saturday open studio ",
)
ADULT_AUDIENCE_PATTERNS = (
    " class ",
    " classes ",
    " glass studio ",
    " glassblowing ",
    " lecture ",
    " masterclass ",
    " open studio ",
    " teacher ",
    " workshop ",
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
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to|&#8211;)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|\band up\b)", re.IGNORECASE)
AGE_AND_UNDER_RE = re.compile(r"\bages?\s*(\d{1,2})\s*and under\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class VaTribeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    api_url: str
    # Some venues have locked their Tribe REST API behind authentication
    # (HTTP 403 rest_forbidden). When an iCal feed URL is provided, the loader
    # uses it instead of the REST API.
    ical_url: str | None = None


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
        # REST API returns 403 rest_forbidden; use the public iCal feed instead.
        ical_url="https://virginiamoca.org/event-calendar/?ical=1",
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
                try:
                    payload = response.json()
                except json.JSONDecodeError as exc:
                    last_exception = exc
                    if attempt < max_attempts:
                        await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                        continue
                    snippet = _debug_snippet(response.text)
                    raise RuntimeError(
                        "VA tribe endpoint returned a non-JSON success response: "
                        f"url={response.url} status={response.status_code} "
                        f"content_type={response.headers.get('content-type', '')!r} "
                        f"snippet={snippet!r}"
                    ) from exc

                if not isinstance(payload, dict):
                    snippet = _debug_snippet(response.text)
                    raise RuntimeError(
                        "VA tribe endpoint returned an unexpected JSON payload: "
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
        raise RuntimeError("Unable to fetch VA tribe events endpoint") from last_exception
    raise RuntimeError("Unable to fetch VA tribe events endpoint after retries")


async def load_va_tribe_bundle_payload(
    *,
    venues: list[VaTribeVenueConfig] | tuple[VaTribeVenueConfig, ...] | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, dict[str, list[dict]] | dict[str, str]]:
    events_by_slug: dict[str, list[dict]] = {}
    errors_by_slug: dict[str, str] = {}
    venues_to_load = list(venues) if venues is not None else list(VA_TRIBE_VENUES)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in venues_to_load:
            if venue.ical_url:
                try:
                    events_by_slug[venue.slug] = await _load_ical_events(venue.ical_url, client=client)
                except Exception as exc:
                    errors_by_slug[venue.slug] = str(exc)
                    print(f"[va-tribe-fetch] venue={venue.slug} (ical) failed: {exc}")
                continue

            events = []
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
                print(f"[va-tribe-fetch] venue={venue.slug} failed: {exc}")
                continue
            events_by_slug[venue.slug] = events

    return {"events_by_slug": events_by_slug, "errors_by_slug": errors_by_slug}


async def _load_ical_events(
    url: str,
    *,
    client: httpx.AsyncClient,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> list[dict]:
    """Fetch an iCal feed and convert its VEVENTs into Tribe-API-shaped dicts."""
    last_exception: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = await client.get(url, headers={**DEFAULT_HEADERS, "Accept": "text/calendar,*/*;q=0.8"})
        except httpx.HTTPError as exc:
            last_exception = exc
            if attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue
            break

        if response.status_code < 400:
            return _ical_text_to_event_dicts(response.text)
        if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
            continue
        response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch VA tribe iCal feed") from last_exception
    raise RuntimeError("Unable to fetch VA tribe iCal feed after retries")


def _ical_text_to_event_dicts(ics_text: str) -> list[dict]:
    dicts: list[dict] = []
    for event in _parse_ics_events(ics_text):
        title = _ics_text_value(event.get("SUMMARY"))
        url = _ics_text_value(event.get("URL"))
        start_date = _ics_datetime_to_api_str(event.get("DTSTART"))
        if not title or not url or not start_date:
            continue
        category_names = [name.strip() for name in _ics_text_value(event.get("CATEGORIES")).split(",") if name.strip()]
        location = _ics_text_value(event.get("LOCATION"))
        dicts.append(
            {
                "title": title,
                "url": url,
                "start_date": start_date,
                "end_date": _ics_datetime_to_api_str(event.get("DTEND")),
                "description": _ics_text_value(event.get("DESCRIPTION")),
                "excerpt": "",
                "categories": [{"name": name} for name in category_names],
                "cost": "",
                "cost_details": None,
                "venue": {"venue": location} if location else None,
            }
        )
    return dicts


def _parse_ics_events(ics_text: str) -> list[dict[str, dict[str, object]]]:
    events: list[dict[str, dict[str, object]]] = []
    current_event: dict[str, dict[str, object]] | None = None
    for line in _unfold_ics_lines(ics_text):
        if line == "BEGIN:VEVENT":
            current_event = {}
            continue
        if line == "END:VEVENT":
            if current_event:
                events.append(current_event)
            current_event = None
            continue
        if current_event is None or ":" not in line:
            continue
        raw_key, raw_value = line.split(":", 1)
        key_parts = raw_key.split(";")
        key = key_parts[0].upper()
        params: dict[str, str] = {}
        for item in key_parts[1:]:
            if "=" in item:
                param_key, param_value = item.split("=", 1)
                params[param_key.upper()] = param_value
        current_event[key] = {"value": raw_value, "params": params}
    return events


def _unfold_ics_lines(ics_text: str) -> list[str]:
    unfolded: list[str] = []
    for raw_line in ics_text.splitlines():
        line = raw_line.rstrip("\r")
        if line.startswith((" ", "\t")) and unfolded:
            unfolded[-1] += line[1:]
            continue
        unfolded.append(line)
    return unfolded


def _ics_text_value(field: dict[str, object] | None) -> str:
    if not field:
        return ""
    value = str(field.get("value") or "")
    return (
        value.replace("\\n", " ")
        .replace("\\N", " ")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
    )


def _ics_datetime_to_api_str(field: dict[str, object] | None) -> str | None:
    """Return the iCal datetime as a naive local 'YYYY-MM-DD HH:MM:SS' string.

    Tribe exports these times labeled TZID=UTC but the values are the venue's
    local wall-clock times, which is exactly what the pipeline stores.
    """
    if not field:
        return None
    value = str(field.get("value") or "").strip().rstrip("Z")
    if not value:
        return None
    if len(value) == 8:
        try:
            parsed = datetime.strptime(value, "%Y%m%d")
        except ValueError:
            return None
        return parsed.strftime("%Y-%m-%d")
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


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
    title = _html_to_text(event_obj.get("title"))
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
    if venue.slug == "chrysler" and _should_reject_chrysler_event(
        title=title,
        category_names=category_names,
    ):
        return None
    if venue.slug == "virginia_moca" and any(
        pattern in _searchable_blob(title) for pattern in VIRGINIA_MOCA_TITLE_EXCLUDE_PATTERNS
    ):
        return None
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

    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))
    amount = _extract_amount(event_obj.get("cost_details"))
    price_context = _pricing_text(" ".join(part for part in [price_text, description or ""] if part))
    is_free, free_status = _infer_va_tribe_price(
        venue=venue,
        amount=amount,
        price_context=price_context,
    )
    location_text = _extract_location_name(event_obj.get("venue")) or f"{venue.city}, {venue.state}"
    activity_type = _infer_activity_type(token_blob)
    audience_segment = _infer_va_tribe_audience(
        venue=venue,
        title=title,
        description=description,
        category_names=category_names,
        source_url=source_url,
        age_min=age_min,
        age_max=age_max,
        token_blob=token_blob,
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=activity_type,
        age_min=age_min,
        age_max=age_max,
        drop_in=(" drop-in " in token_blob or " drop in " in token_blob),
        registration_required=_infer_registration_required(token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
        audience_segment=audience_segment,
    )


def _audience_from_moca_categories(category_names: list[str]) -> str | None:
    """Map Virginia MOCA's Tribe audience categories to an audience segment.

    Tribe tags carry the audience directly (e.g. "Adults", "Teens",
    "Kids & Families", "Ages 2-5"). Taxonomy buckets such as "Programs",
    "Classes", "Members", "Teachers", "Types", and "Exhibitions" are ignored.
    """
    audience: set[str] = set()
    for raw in category_names:
        name = raw.strip().lower()
        if name == "adults":
            audience.add("adults")
        elif name == "teens":
            audience.add("teens")
        elif (
            name.startswith("ages ")
            or "caregiver" in name
            or "kids" in name
            or "families" in name
            or "family" in name
            or "youth" in name
            or "children" in name
        ):
            audience.add("kids")

    if not audience:
        return None
    has_kids = "kids" in audience
    has_teen = "teens" in audience
    has_adult = "adults" in audience
    if has_kids and (has_teen or has_adult):
        return "all_ages"
    if has_teen and has_adult:
        return "teens_adults"
    if has_kids:
        return "kids"
    if has_teen:
        return "teens"
    return "adults"


# Virginia MOCA runs a "Summer Teachers Institute" professional-development
# program that should not surface as a public art activity.
VIRGINIA_MOCA_TITLE_EXCLUDE_PATTERNS = (
    " teachers institute ",
    " teacher institute ",
    " summer institute ",
)


def _should_reject_chrysler_event(*, title: str, category_names: list[str]) -> bool:
    title_blob = _searchable_blob(title)
    if any(pattern in title_blob for pattern in CHRYSLER_TITLE_EXCLUDE_PATTERNS):
        return True
    categories = {_normalize_space(name).lower() for name in category_names}
    return bool(categories.intersection(CHRYSLER_CATEGORY_EXCLUDE_NAMES))


def _infer_va_tribe_audience(
    *,
    venue: VaTribeVenueConfig,
    title: str,
    description: str | None,
    category_names: list[str],
    source_url: str,
    age_min: int | None,
    age_max: int | None,
    token_blob: str,
) -> str:
    category = " ".join(category_names)

    if venue.slug == "virginia_moca":
        segment = _audience_from_moca_categories(category_names)
        if segment is not None:
            return segment
        generic = infer_audience_segment(
            title=title,
            description=description,
            category=category,
            source_url=source_url,
            age_min=age_min,
            age_max=age_max,
        )
        # Virginia MOCA's uncategorized programs (after-hours events, socials)
        # are adult-audience by default.
        return generic if generic != "unknown" else "adults"

    if venue.slug == "chrysler":
        title_category_blob = _searchable_blob(" ".join(part for part in [title, category] if part))
        if any(pattern in title_category_blob or pattern in token_blob for pattern in ALL_AGES_AUDIENCE_PATTERNS):
            return "all_ages"
        if any(pattern in title_category_blob or pattern in token_blob for pattern in KIDS_AUDIENCE_PATTERNS):
            return "kids"
        age_segment = infer_audience_segment(age_min=age_min, age_max=age_max)
        if age_segment in {"teens", "teens_adults"}:
            return age_segment
        if any(pattern in title_category_blob for pattern in ADULT_AUDIENCE_PATTERNS):
            return "adults"

    if venue.slug == "william_king":
        if any(marker in token_blob for marker in (" teen adult ", " teen adults ", " teen through adult ")):
            return "teens_adults"
        if " teens " in token_blob and any(marker in token_blob for marker in (" adult class ", " adults ", " adult ")):
            return "teens_adults"
        generic = infer_audience_segment(
            title=title,
            description=description,
            category=category,
            source_url=source_url,
            age_min=age_min,
            age_max=age_max,
        )
        return generic if generic != "unknown" else "adults"

    return infer_audience_segment(
        title=title,
        description=description,
        category=category,
        source_url=source_url,
        age_min=age_min,
        age_max=age_max,
    )


def _infer_va_tribe_price(
    *,
    venue: VaTribeVenueConfig,
    amount: Decimal | None,
    price_context: str,
) -> tuple[bool | None, str]:
    default_is_free = True if venue.slug == "william_king" else None
    return infer_price_classification_from_amount(
        amount,
        text=price_context,
        default_is_free=default_is_free,
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


def _infer_registration_required(token_blob: str) -> bool:
    if (
        " registration not required " in token_blob
        or " no registration required " in token_blob
        or " registration recommended " in token_blob
    ):
        return False
    return (
        " register " in token_blob
        or " registration " in token_blob
        or " ticket " in token_blob
        or " tickets " in token_blob
    )


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    under_match = AGE_AND_UNDER_RE.search(text)
    if under_match:
        return None, int(under_match.group(1))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _pricing_text(value: str) -> str:
    return re.sub(r"[,.;:]", " ", value)


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


def get_va_tribe_source_prefixes(
    venues: tuple[VaTribeVenueConfig, ...] | list[VaTribeVenueConfig] | None = None,
) -> tuple[str, ...]:
    prefixes: list[str] = []
    venues_to_use = list(venues) if venues is not None else list(VA_TRIBE_VENUES)
    for venue in venues_to_use:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)


def _debug_snippet(value: str | None, *, limit: int = 200) -> str:
    if not value:
        return ""
    return " ".join(value.split())[:limit]
