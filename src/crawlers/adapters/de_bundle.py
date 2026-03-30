from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from urllib.parse import urlencode
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

DE_TIMEZONE = "America/New_York"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

JSON_HEADERS = {
    **DEFAULT_HEADERS,
    "Accept": "application/json,text/plain,*/*",
}

PLAYWRIGHT_LAUNCH_ARGS = (
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
)

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " art activities ",
    " art making ",
    " artmaking ",
    " class ",
    " classes ",
    " create ",
    " creation station ",
    " curator talk ",
    " discussion ",
    " drawing ",
    " family 2nd sunday ",
    " figure drawing ",
    " gallery talk ",
    " guided group discussion ",
    " hands on ",
    " hands-on ",
    " intro to figure drawing ",
    " slow art ",
    " studio explorers ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)

WEAK_INCLUDE_PATTERNS = (
    " art ",
    " children ",
    " child ",
    " draw ",
    " drawing ",
    " family ",
    " kids ",
    " preschool ",
)

ALWAYS_REJECT_PATTERNS = (
    " admission ",
    " art loop ",
    " bar ",
    " celebration ",
    " cabaret ",
    " concert ",
    " concerts ",
    " cocktail ",
    " dinner ",
    " discotheque ",
    " drag ",
    " exhibition ",
    " exhibitions ",
    " film ",
    " films ",
    " fundraiser ",
    " gala ",
    " live music ",
    " literary ",
    " opening ",
    " openings ",
    " performance ",
    " performances ",
    " readers ",
    " reception ",
    " story time ",
    " storytime ",
    " vendor ",
    " vendors ",
)

CONTEXTUAL_REJECT_PATTERNS = (
    " first friday ",
    " food vendor ",
    " powwow ",
)

DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
    " first-come, first-served ",
)

REGISTRATION_PATTERNS = (
    " book now ",
    " pre-register ",
    " preregister ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
    " ticket ",
    " tickets ",
)

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
YEARS_AND_OLDER_RE = re.compile(r"\b(\d{1,2})\s*(?:years?\s*and\s*older|and older)\b", re.IGNORECASE)
MONTH_DAY_RE = re.compile(r"([A-Za-z]{3,9})\s+(\d{1,2})")
TIME_RANGE_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)\s*(?:-|–|—|to)\s*(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
DECONTEMP_SINGLE_DATE_TIME_RE = re.compile(
    r"([A-Za-z]{3,9})\s+(\d{1,2})\s*\|\s*(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)\s*(?:-|–|—|to)\s*(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class DeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    discovery_mode: str
    api_url: str | None = None


DE_VENUES: tuple[DeVenueConfig, ...] = (
    DeVenueConfig(
        slug="delaware_art_museum",
        source_name="de_delaware_art_museum_events",
        venue_name="Delaware Art Museum",
        city="Wilmington",
        state="DE",
        list_url="https://delart.org/whats-on/",
        default_location="Delaware Art Museum, Wilmington, DE",
        discovery_mode="tribe_api",
        api_url="https://delart.org/wp-json/tribe/events/v1/events",
    ),
    DeVenueConfig(
        slug="the_delaware_contemporary",
        source_name="de_the_delaware_contemporary_events",
        venue_name="The Delaware Contemporary",
        city="Wilmington",
        state="DE",
        list_url="https://www.decontemporary.org/programs",
        default_location="The Delaware Contemporary, Wilmington, DE",
        discovery_mode="tdc_programs_html",
    ),
)

DE_VENUES_BY_SLUG = {venue.slug: venue for venue in DE_VENUES}


class DeBundleAdapter(BaseSourceAdapter):
    source_name = "de_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_de_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_de_bundle_payload/parse_de_events from the script runner.")


async def fetch_html(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    use_playwright_fallback: bool = False,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
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
                return response.text

            if response.status_code in (403, 429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if use_playwright_fallback:
        return await fetch_html_playwright(url)

    if last_exception is not None:
        raise RuntimeError(f"Unable to fetch HTML: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch HTML after retries: {url}")


async def fetch_html_playwright(url: str, *, timeout_ms: int = 90000) -> str:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, args=list(PLAYWRIGHT_LAUNCH_ARGS))
        page = await browser.new_page(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            locale="en-US",
            timezone_id=DE_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def fetch_tribe_events_page(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> dict:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=JSON_HEADERS)

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
                        "DE Tribe endpoint returned non-JSON content: "
                        f"url={response.url} status={response.status_code}"
                    ) from exc
                if isinstance(payload, dict):
                    return payload
                raise RuntimeError(
                    "DE Tribe endpoint returned unexpected payload type: "
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
        raise RuntimeError("Unable to fetch DE Tribe endpoint") from last_exception
    raise RuntimeError("Unable to fetch DE Tribe endpoint after retries")


async def load_de_bundle_payload(
    *,
    venues: list[DeVenueConfig] | tuple[DeVenueConfig, ...] | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict[str, dict]:
    selected = list(venues) if venues is not None else list(DE_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}
    today = datetime.now(ZoneInfo(DE_TIMEZONE)).date().isoformat()

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.discovery_mode == "tribe_api":
                    if venue.api_url is None:
                        raise RuntimeError(f"Missing api_url for venue={venue.slug}")
                    events: list[dict] = []
                    next_url = _with_query(venue.api_url, per_page=per_page, page=1, start_date=today)
                    pages_seen = 0
                    tribe_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=JSON_HEADERS)
                    try:
                        while next_url:
                            if page_limit is not None and pages_seen >= max(page_limit, 1):
                                break
                            page_payload = await fetch_tribe_events_page(next_url, client=tribe_client)
                            pages_seen += 1
                            events.extend(page_payload.get("events") or [])
                            next_url = page_payload.get("next_rest_url")
                    finally:
                        await tribe_client.aclose()
                    payload_by_slug[venue.slug] = {"events": events}
                elif venue.discovery_mode == "tdc_programs_html":
                    html = await fetch_html(venue.list_url, client=client)
                    payload_by_slug[venue.slug] = {"html": html}
                else:
                    raise RuntimeError(f"Unsupported discovery_mode={venue.discovery_mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[de-bundle-fetch] venue={venue.slug} failed: {exc}")

    return {"payload_by_slug": payload_by_slug, "errors_by_slug": errors_by_slug}


def parse_de_events(payload: dict, *, venue: DeVenueConfig) -> list[ExtractedActivity]:
    if venue.discovery_mode == "tribe_api":
        rows = _parse_delart_events(payload, venue=venue)
    elif venue.discovery_mode == "tdc_programs_html":
        rows = _parse_delaware_contemporary_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(DE_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


def get_de_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in DE_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
        if venue.api_url:
            api_parsed = urlparse(venue.api_url)
            prefixes.append(f"{api_parsed.scheme}://{api_parsed.netloc}/")
    return tuple(dict.fromkeys(prefixes))


def _parse_delart_events(payload: dict, *, venue: DeVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for event_obj in payload.get("events") or []:
        row = _build_delart_row(event_obj, venue=venue)
        if row is not None:
            rows.append(row)
    return rows


def _build_delart_row(event_obj: dict, *, venue: DeVenueConfig) -> ExtractedActivity | None:
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

    token_blob = _searchable_blob(" ".join([title, description or "", " ".join(category_names)]))
    title_blob = _searchable_blob(title)
    category_blob = _searchable_blob(" ".join(category_names))
    if not _should_keep_delart_event(title_blob=title_blob, token_blob=token_blob, category_blob=category_blob):
        return None

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
        registration_required=_registration_required(token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=DE_TIMEZONE,
        **price_classification_kwargs(" ".join(part for part in [description or "", price_text] if part)),
    )


def _parse_delaware_contemporary_events(payload: dict, *, venue: DeVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("html") or "", "html.parser")
    current_date = datetime.now(ZoneInfo(DE_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []

    for figure in soup.select("figure"):
        title = _normalize_space(_node_text(figure.select_one(".image-title")))
        description = _normalize_space(_node_text(figure.select_one(".image-subtitle")))
        if not title or not description:
            continue
        if "DATES COMING SOON" in description.upper():
            continue

        if title == "Body of Art: Open Figure Drawing":
            for start_at, end_at in _parse_tdc_open_figure_dates(description, current_date=current_date):
                rows.append(
                    ExtractedActivity(
                        source_url=f"{venue.list_url}#body-of-art-open-figure-drawing",
                        title=title,
                        description=description,
                        venue_name=venue.venue_name,
                        location_text=venue.default_location,
                        city=venue.city,
                        state=venue.state,
                        activity_type="workshop",
                        age_min=_parse_age_range(description)[0],
                        age_max=_parse_age_range(description)[1],
                        drop_in=True,
                        registration_required=_registration_required(description),
                        start_at=start_at,
                        end_at=end_at,
                        timezone=DE_TIMEZONE,
                        **price_classification_kwargs(description),
                    )
                )
        elif title == "Body of Art: Intro to Figure Drawing":
            parsed = _parse_tdc_single_datetime(description, current_date=current_date)
            if parsed is None:
                continue
            start_at, end_at = parsed
            rows.append(
                ExtractedActivity(
                    source_url=f"{venue.list_url}#body-of-art-intro-to-figure-drawing",
                    title=title,
                    description=description,
                    venue_name=venue.venue_name,
                    location_text=venue.default_location,
                    city=venue.city,
                    state=venue.state,
                    activity_type="workshop",
                    age_min=_parse_age_range(description)[0],
                    age_max=_parse_age_range(description)[1],
                    drop_in=None,
                    registration_required=_registration_required(description),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=DE_TIMEZONE,
                    **price_classification_kwargs(description),
                )
            )

    return rows


def _parse_tdc_open_figure_dates(description: str, *, current_date: date) -> list[tuple[datetime, datetime | None]]:
    time_match = TIME_RANGE_RE.search(description)
    if time_match is None:
        return []

    start_hour, start_minute = _parse_time_parts(time_match.group(1), time_match.group(2), time_match.group(3))
    end_hour, end_minute = _parse_time_parts(time_match.group(4), time_match.group(5), time_match.group(6))
    month_day_matches = MONTH_DAY_RE.findall(description)
    rows: list[tuple[datetime, datetime | None]] = []
    for month_text, day_text in month_day_matches:
        month = MONTH_LOOKUP.get(month_text[:3].lower())
        if month is None:
            continue
        try:
            parsed_date = date(current_date.year, month, int(day_text))
        except ValueError:
            continue
        rows.append(
            (
                datetime(parsed_date.year, parsed_date.month, parsed_date.day, start_hour, start_minute),
                datetime(parsed_date.year, parsed_date.month, parsed_date.day, end_hour, end_minute),
            )
        )
    return rows


def _parse_tdc_single_datetime(description: str, *, current_date: date) -> tuple[datetime, datetime | None] | None:
    match = DECONTEMP_SINGLE_DATE_TIME_RE.search(description)
    if match is None:
        return None
    month = MONTH_LOOKUP.get(match.group(1)[:3].lower())
    if month is None:
        return None
    try:
        parsed_date = date(current_date.year, month, int(match.group(2)))
    except ValueError:
        return None
    start_hour, start_minute = _parse_time_parts(match.group(3), match.group(4), match.group(5))
    end_hour, end_minute = _parse_time_parts(match.group(6), None, match.group(7))
    return (
        datetime(parsed_date.year, parsed_date.month, parsed_date.day, start_hour, start_minute),
        datetime(parsed_date.year, parsed_date.month, parsed_date.day, end_hour, end_minute),
    )


def _should_keep_delart_event(*, title_blob: str, token_blob: str, category_blob: str) -> bool:
    if " little listeners " in token_blob:
        return False
    if " powwow " in token_blob and " workshop " not in token_blob and " talk " not in token_blob:
        return False
    if " tour " in title_blob and " talk " not in title_blob:
        return False
    if " tours " in title_blob and " talks " not in title_blob:
        return False
    if " exhibitions " in category_blob and not any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS):
        return False
    if " literary " in category_blob:
        return False
    if any(pattern in title_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if any(pattern in title_blob for pattern in CONTEXTUAL_REJECT_PATTERNS):
        return False

    strong_in_title = any(pattern in title_blob for pattern in STRONG_INCLUDE_PATTERNS)
    strong_anywhere = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS) and not strong_in_title:
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_in_title:
        return False

    if strong_anywhere:
        return True
    if " family " in category_blob and (" art " in token_blob or " create " in token_blob or " studio " in token_blob):
        return True
    return any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS) and " art " in token_blob


def _infer_activity_type(token_blob: str) -> str:
    if any(
        pattern in token_blob
        for pattern in (
            " curator talk ",
            " discussion ",
            " gallery talk ",
            " guided group discussion ",
            " slow art ",
            " talk ",
            " talks ",
        )
    ):
        return "lecture"
    return "workshop"


def _registration_required(text: str) -> bool | None:
    searchable = _searchable_blob(text)
    if " no registration required " in searchable:
        return False
    if any(pattern in searchable for pattern in REGISTRATION_PATTERNS):
        return True
    return None


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    older_match = YEARS_AND_OLDER_RE.search(text)
    if older_match:
        return int(older_match.group(1)), None
    return None, None


def _parse_time_parts(hour_text: str, minute_text: str | None, period_text: str) -> tuple[int, int]:
    parsed = _parse_time_value(f"{hour_text}:{minute_text or '00'} {period_text}")
    if parsed is None:
        return 0, 0
    return parsed.hour, parsed.minute


def _parse_time_value(value: str | None) -> time | None:
    if not value:
        return None
    text = _normalize_space(value).lower().replace(".", "")
    for fmt in ("%I:%M %p", "%I %p", "%I:%M%p", "%I%p"):
        try:
            return datetime.strptime(text.upper(), fmt).time()
        except ValueError:
            continue
    return None


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
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _node_text(node) -> str:
    return _normalize_space(node.get_text(" ", strip=True) if node is not None else "")


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


MONTH_LOOKUP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
