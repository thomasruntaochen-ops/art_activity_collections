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
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

WA_TIMEZONE = "America/Los_Angeles"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

ALWAYS_EXCLUDE_PATTERNS = (
    " admission ",
    " admissions ",
    " camp ",
    " camps ",
    " concert ",
    " concerts ",
    " dinner ",
    " exhibition ",
    " exhibitions ",
    " film ",
    " films ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " market ",
    " meditation ",
    " mindfulness ",
    " music ",
    " orchestra ",
    " party ",
    " performance ",
    " performances ",
    " poetry ",
    " reading ",
    " reception ",
    " storytime ",
    " story time ",
    " tour ",
    " tours ",
    " yoga ",
)
INCLUDE_PATTERNS = (
    " activity ",
    " artist's corner ",
    " artists corner ",
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " creative aging ",
    " design yourself ",
    " discussion ",
    " discussions ",
    " family ",
    " families ",
    " lecture ",
    " lectures ",
    " studio ",
    " talk ",
    " talks ",
    " think & tinker ",
    " think and tinker ",
    " tinker ",
    " workshop ",
    " workshops ",
)
STRONG_WORKSHOP_PATTERNS = (
    " activity ",
    " class ",
    " classes ",
    " creative aging ",
    " design yourself ",
    " studio ",
    " think & tinker ",
    " think and tinker ",
    " workshop ",
    " workshops ",
)
STRONG_TALK_PATTERNS = (
    " artist's corner ",
    " artists corner ",
    " conversation ",
    " conversations ",
    " discussion ",
    " discussions ",
    " lecture ",
    " lectures ",
    " talk ",
    " talks ",
)


@dataclass(frozen=True, slots=True)
class WaTribeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    api_url: str


WA_TRIBE_VENUES: tuple[WaTribeVenueConfig, ...] = (
    WaTribeVenueConfig(
        slug="tacoma",
        source_name="wa_tacoma_events",
        venue_name="Tacoma Art Museum",
        city="Tacoma",
        state="WA",
        list_url="https://www.tacomaartmuseum.org/events/",
        api_url="https://www.tacomaartmuseum.org/wp-json/tribe/events/v1/events",
    ),
    WaTribeVenueConfig(
        slug="whatcom",
        source_name="wa_whatcom_events",
        venue_name="Whatcom Museum",
        city="Bellingham",
        state="WA",
        list_url="https://www.whatcommuseum.org/events/",
        api_url="https://www.whatcommuseum.org/wp-json/tribe/events/v1/events",
    ),
)

WA_TRIBE_VENUES_BY_SLUG = {venue.slug: venue for venue in WA_TRIBE_VENUES}


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
                        "WA tribe endpoint returned a non-JSON success response: "
                        f"url={response.url} status={response.status_code}"
                    ) from exc

                if not isinstance(payload, dict):
                    raise RuntimeError(
                        "WA tribe endpoint returned an unexpected JSON payload: "
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
        raise RuntimeError("Unable to fetch WA tribe events endpoint") from last_exception
    raise RuntimeError("Unable to fetch WA tribe events endpoint after retries")


async def load_wa_tribe_bundle_payload(
    *,
    venues: list[WaTribeVenueConfig] | tuple[WaTribeVenueConfig, ...] | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, dict[str, list[dict]] | dict[str, str]]:
    events_by_slug: dict[str, list[dict]] = {}
    errors_by_slug: dict[str, str] = {}
    venues_to_load = list(venues) if venues is not None else list(WA_TRIBE_VENUES)

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
                print(f"[wa-tribe-fetch] venue={venue.slug} failed: {exc}")
                continue
            events_by_slug[venue.slug] = events

    return {"events_by_slug": events_by_slug, "errors_by_slug": errors_by_slug}


def parse_wa_tribe_events(
    events: list[dict],
    *,
    venue: WaTribeVenueConfig,
) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(WA_TIMEZONE)).date()
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


class WaTribeBundleAdapter(BaseSourceAdapter):
    source_name = "wa_tribe_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_wa_tribe_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_wa_tribe_bundle_payload/parse_wa_tribe_events from script runner.")


def _build_row(event_obj: dict, *, venue: WaTribeVenueConfig) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _normalize_end_at(_parse_datetime(event_obj.get("end_date")), start_at=start_at)
    category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    tag_names = [_normalize_space(item.get("name")) for item in (event_obj.get("tags") or [])]
    tag_names = [value for value in tag_names if value]
    excerpt = _html_to_text(event_obj.get("excerpt"))
    description_text = _html_to_text(event_obj.get("description"))
    price_text = _normalize_space(event_obj.get("cost"))

    description_parts = [part for part in [excerpt, description_text] if part]
    if category_names:
        description_parts.append(f"Categories: {', '.join(category_names)}")
    if tag_names:
        description_parts.append(f"Tags: {', '.join(tag_names)}")
    if price_text:
        description_parts.append(f"Price: {price_text}")
    description = " | ".join(description_parts) if description_parts else None

    token_blob = _searchable_blob(
        " ".join(
            [
                title,
                description or "",
                " ".join(category_names),
                " ".join(tag_names),
            ]
        )
    )
    if not _should_include_event(token_blob):
        return None

    amount = _extract_amount(event_obj.get("cost_details"))
    is_free, free_status = _price_classification(price_text=price_text, amount=amount, description=description)
    location_text = _extract_location_name(event_obj.get("venue")) or f"{venue.city}, {venue.state}"

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
        drop_in=("drop-in" in token_blob or "drop in" in token_blob),
        registration_required=("register" in token_blob or "ticket" in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=WA_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _should_include_event(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in ALWAYS_EXCLUDE_PATTERNS):
        return False
    return any(pattern in token_blob for pattern in INCLUDE_PATTERNS)


def _price_classification(
    *,
    price_text: str | None,
    amount: Decimal | None,
    description: str | None,
) -> tuple[bool | None, str]:
    if price_text and "included with admission" in price_text.lower():
        return None, "uncertain"
    return infer_price_classification_from_amount(amount, text=" | ".join(part for part in [price_text, description] if part))


def _extract_amount(cost_details: object) -> Decimal | None:
    if isinstance(cost_details, dict):
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
    parts = [
        _normalize_space(venue_obj.get("venue")),
        _normalize_space(venue_obj.get("city")),
        _normalize_space(venue_obj.get("state")),
    ]
    parts = [part for part in parts if part]
    if not parts:
        return None
    return ", ".join(parts)


def _infer_activity_type(token_blob: str) -> str:
    if any(pattern in token_blob for pattern in STRONG_WORKSHOP_PATTERNS):
        return "workshop"
    if any(pattern in token_blob for pattern in STRONG_TALK_PATTERNS):
        return "lecture"
    return "workshop"


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return parse_iso_datetime(text, timezone_name=WA_TIMEZONE)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y-%m-%d":
                return datetime.combine(parsed.date(), datetime.min.time())
            return parsed
        except ValueError:
            continue
    return None


def _normalize_end_at(end_at: datetime | None, *, start_at: datetime) -> datetime | None:
    if end_at is None:
        return None
    if end_at < start_at:
        return None
    if end_at.date() != start_at.date():
        return None
    if (end_at - start_at).total_seconds() > 12 * 60 * 60:
        return None
    return end_at


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _searchable_blob(value: str) -> str:
    return f" {' '.join(value.lower().split())} "


def get_wa_tribe_source_prefixes() -> list[str]:
    prefixes: list[str] = []
    for venue in WA_TRIBE_VENUES:
        parsed = urlparse(venue.list_url)
        if parsed.scheme and parsed.netloc:
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}")
    return sorted(set(prefixes))
