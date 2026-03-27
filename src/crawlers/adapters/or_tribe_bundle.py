from __future__ import annotations

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
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

OR_TIMEZONE = "America/Los_Angeles"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

OVERRIDE_INCLUDE_PATTERNS = (
    " activity ",
    " art and conversation ",
    " artist talk ",
    " beginning drawing ",
    " botanical illustrations ",
    " class ",
    " classes ",
    " collage ",
    " conversation ",
    " creative aging ",
    " discussion ",
    " drawing ",
    " drawing club ",
    " family ",
    " families ",
    " fused glass ",
    " illustrations ",
    " kids ",
    " lab ",
    " lecture ",
    " look with me ",
    " paint out ",
    " painting ",
    " plein air ",
    " pottery ",
    " talk ",
    " watercolor ",
    " workshop ",
    " workshops ",
    " wonderlab ",
    " youth ",
    " zentangle ",
)
NO_OVERRIDE_EXCLUDES = (
    " concert ",
    " fundraiser ",
    " fundraising ",
    " music ",
    " orchestra ",
    " poetry ",
    " reading ",
    " reception ",
    " screening ",
    " screenings ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)
SOFT_EXCLUDES = (
    " admission ",
    " camp ",
    " camps ",
    " closed ",
    " film ",
    " films ",
    " exhibition ",
    " exhibitions ",
    " gallery closed ",
    " galleries closed ",
    " open house ",
    " performance ",
)


@dataclass(frozen=True, slots=True)
class OrTribeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    api_url: str


OR_TRIBE_VENUES: tuple[OrTribeVenueConfig, ...] = (
    OrTribeVenueConfig(
        slug="coos",
        source_name="or_coos_art_museum_events",
        venue_name="Coos Art Museum",
        city="Coos Bay",
        state="OR",
        list_url="https://coosartmuseum.org/education/classes/",
        api_url="https://coosartmuseum.org/wp-json/tribe/events/v1/events",
    ),
    OrTribeVenueConfig(
        slug="portland",
        source_name="or_portland_art_museum_events",
        venue_name="Portland Art Museum",
        city="Portland",
        state="OR",
        list_url="https://portlandartmuseum.org/calendar/",
        api_url="https://portlandartmuseum.org/wp-json/tribe/events/v1/events",
    ),
)

OR_TRIBE_VENUES_BY_SLUG = {venue.slug: venue for venue in OR_TRIBE_VENUES}


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
                        "OR tribe endpoint returned non-JSON content: "
                        f"url={response.url} status={response.status_code} snippet={snippet!r}"
                    ) from exc

                if not isinstance(payload, dict):
                    snippet = _debug_snippet(response.text)
                    raise RuntimeError(
                        "OR tribe endpoint returned unexpected JSON payload: "
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
        raise RuntimeError("Unable to fetch Oregon Tribe events endpoint") from last_exception
    raise RuntimeError("Unable to fetch Oregon Tribe events endpoint after retries")


async def load_or_tribe_bundle_payload(
    *,
    venues: list[OrTribeVenueConfig] | tuple[OrTribeVenueConfig, ...] | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
    start_date: str | None = None,
) -> dict[str, dict[str, list[dict]] | dict[str, str]]:
    today = start_date or datetime.now(ZoneInfo(OR_TIMEZONE)).date().isoformat()
    events_by_slug: dict[str, list[dict]] = {}
    errors_by_slug: dict[str, str] = {}
    venues_to_load = list(venues) if venues is not None else list(OR_TRIBE_VENUES)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in venues_to_load:
            next_url = f"{venue.api_url}?per_page={per_page}&page=1&start_date={today}"
            pages_seen = 0
            events: list[dict] = []
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
                print(f"[or-tribe-fetch] venue={venue.slug} failed: {exc}")
                continue
            events_by_slug[venue.slug] = events

    return {"events_by_slug": events_by_slug, "errors_by_slug": errors_by_slug}


def parse_or_tribe_events(
    events: list[dict],
    *,
    venue: OrTribeVenueConfig,
) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(OR_TIMEZONE)).date()
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


class OrTribeBundleAdapter(BaseSourceAdapter):
    source_name = "or_tribe_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_or_tribe_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_or_tribe_bundle_payload/parse_or_tribe_events from script runner.")


def _build_row(event_obj: dict, *, venue: OrTribeVenueConfig) -> ExtractedActivity | None:
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
    price_text = _normalize_price_text(_normalize_space(event_obj.get("cost") or ""))
    location_text = _extract_location_name(event_obj.get("venue")) or f"{venue.city}, {venue.state}"

    description_parts = [part for part in [excerpt, list_description] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = " | ".join(description_parts) if description_parts else None

    token_blob = _searchable_blob(" ".join([title, description or "", " ".join(category_names)]))
    if not _should_include_event(token_blob=token_blob, title=title, venue=venue):
        return None

    amount = _extract_amount(event_obj.get("cost_details"))
    if " virtual " in token_blob:
        location_text = "Online"

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(token_blob),
        age_min=None,
        age_max=None,
        drop_in=(" drop in " in token_blob or " drop-in " in token_blob),
        registration_required=(
            " register " in token_blob
            or " registration " in token_blob
            or " reserve " in token_blob
            or (" ticket " in token_blob and " no ticket " not in token_blob)
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=OR_TIMEZONE,
        **price_classification_kwargs_from_amount(amount, text=price_text or description),
    )


def _should_include_event(*, token_blob: str, title: str, venue: OrTribeVenueConfig) -> bool:
    if any(pattern in token_blob for pattern in NO_OVERRIDE_EXCLUDES):
        return False

    has_override_include = any(pattern in token_blob for pattern in OVERRIDE_INCLUDE_PATTERNS)
    if any(pattern in token_blob for pattern in SOFT_EXCLUDES) and not has_override_include:
        return False

    if venue.slug == "portland" and " screenings experiences " in token_blob and not has_override_include:
        return False
    if venue.slug == "portland" and " tomorrow theater " in token_blob and not has_override_include:
        return False

    if not has_override_include:
        return False

    normalized_title = title.lower()
    if "camp" in normalized_title or "exhibition" in normalized_title:
        return False
    if "opening celebration" in normalized_title or "opening reception" in normalized_title:
        return False
    if "free day" in normalized_title or "free first thursday" in normalized_title:
        return False
    if " camp overview " in token_blob:
        return False
    if "storytime" in normalized_title:
        return False
    if "tour" in normalized_title and "artist talk" not in normalized_title:
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
    if not parts:
        return None
    return ", ".join(parts)


def _infer_activity_type(token_blob: str) -> str:
    if any(marker in token_blob for marker in (" workshop ", " class ", " classes ", " drawing club ", " wonderlab ")):
        return "workshop"
    if any(marker in token_blob for marker in (" lecture ", " talk ", " discussion ", " conversation ", " symposium ")):
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
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
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


def _normalize_price_text(value: str) -> str:
    return value.replace("$Free", "Free")


def get_or_tribe_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in OR_TRIBE_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)


def _debug_snippet(value: str | None, *, limit: int = 200) -> str:
    if not value:
        return ""
    return " ".join(value.split())[:limit]
