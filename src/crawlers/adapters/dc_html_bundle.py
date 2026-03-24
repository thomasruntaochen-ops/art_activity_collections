import asyncio
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urljoin
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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

STRICT_EXCLUDE_PATTERNS = (
    " admission ",
    " camp ",
    " camps ",
    " concert ",
    " film ",
    " films ",
    " meditation ",
    " mindfulness ",
    " music ",
    " performance ",
    " poem ",
    " poetry ",
    " reading ",
    " shopping ",
    " book signing ",
    " culinary ",
    " storytime ",
    " tour ",
    " tours ",
    " writing ",
    " yoga ",
)
INCLUDE_PATTERNS = (
    " activity ",
    " art ",
    " families ",
    " family ",
    " kids ",
    " lecture ",
    " lectures ",
    " studio ",
    " teen ",
    " teens ",
    " workshop ",
    " workshops ",
)
STRONG_ACTIVITY_PATTERNS = (
    " art making ",
    " collage ",
    " create ",
    " creative ",
    " paint ",
    " studio ",
    " watercolor ",
    " workshop ",
    " workshops ",
)
YOUTH_FOCUS_PATTERNS = (
    " child ",
    " children ",
    " families ",
    " family ",
    " kids ",
    " student ",
    " students ",
    " teen ",
    " teens ",
    " youth ",
)

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*[APap]\.?[Mm]\.?)\s*(?:-|–|—|to)\s*(?P<end>\d{1,2}(?::\d{2})?\s*[APap]\.?[Mm]\.?)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(r"\b(?P<time>\d{1,2}(?::\d{2})?\s*[APap]\.?[Mm]\.?)\b", re.IGNORECASE)

ASIA_EVENT_RE = re.compile(r"/whats-on/events/search/event:\d+")
NPG_DATE_RE = re.compile(
    r"Event Date:\s*(?P<date>[A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2},\s+\d{4})\s+(?P<times>.+?)\s+Event Location:",
    re.IGNORECASE,
)
NPG_LOCATION_RE = re.compile(r"Event Location:\s*(?P<location>.+?)\s+Event Cost:", re.IGNORECASE)
NPG_COST_RE = re.compile(r"Event Cost:\s*(?P<cost>.+?)\s+Get Tickets:", re.IGNORECASE)
NPG_CATEGORY_RE = re.compile(
    r"Event Category:\s*(?P<category>.+?)\s+(?P<description>(?:Calling all artists!|Children and families are invited).+?)\s+Add to Calendar",
    re.IGNORECASE,
)
ASIA_DATE_RE = re.compile(
    r"Date\s+(?P<date>[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4})\s+(?P<times>.+?)\s+Location",
    re.IGNORECASE,
)
ASIA_LOCATION_RE = re.compile(r"Location\s+(?P<location>.+?)\s+Description", re.IGNORECASE)
ASIA_DESCRIPTION_RE = re.compile(r"Description\s+(?P<description>.+?)\s+Image:", re.IGNORECASE)
ASIA_COST_RE = re.compile(r"Cost\s+(?P<cost>.+?)\s+Accessibility", re.IGNORECASE)
ASIA_TOPICS_RE = re.compile(r"Topics\s+(?P<topics>.+?)\s+Explore Art \+ Culture", re.IGNORECASE)
PHILLIPS_DATETIME_RE = re.compile(
    r"(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4}),\s+(?P<times>\d{1,2}(?::\d{2})?\s*[ap]m\s*-\s*\d{1,2}(?::\d{2})?\s*[ap]m)",
    re.IGNORECASE,
)
PHILLIPS_PRICE_RE = re.compile(r"(?P<price>Event included with.+?)(?:\s+General Admission|\s+Bring your family)", re.IGNORECASE)
KREEGER_DATETIME_RE = re.compile(
    r"(?P<weekday>[A-Za-z]+),\s+(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?\s*\|\s*(?P<time>\d{1,2}:\d{2}(?:am|pm))",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class DcHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_urls: tuple[str, ...]


DC_HTML_VENUES: tuple[DcHtmlVenueConfig, ...] = (
    DcHtmlVenueConfig(
        slug="asian_art",
        source_name="asian_art_events",
        venue_name="National Museum of Asian Art",
        city="Washington",
        state="DC",
        list_urls=(
            "https://asia.si.edu/whats-on/events/search/?&edan_fq%5B%5D=p.event.topics:Workshops",
            "https://asia.si.edu/whats-on/events/search/?&edan_fq%5B%5D=p.event.topics:Lectures+%26+Discussions",
            "https://asia.si.edu/whats-on/events/search/?&edan_fq%5B%5D=p.event.topics:Kids+%26+Families",
        ),
    ),
    DcHtmlVenueConfig(
        slug="npg",
        source_name="npg_family_events",
        venue_name="National Portrait Gallery",
        city="Washington",
        state="DC",
        list_urls=("https://npg.si.edu/events/family-programs",),
    ),
    DcHtmlVenueConfig(
        slug="kreeger",
        source_name="kreeger_programs",
        venue_name="The Kreeger Museum",
        city="Washington",
        state="DC",
        list_urls=("https://www.kreegermuseum.org/programs/first-studio",),
    ),
    DcHtmlVenueConfig(
        slug="phillips",
        source_name="phillips_family_events",
        venue_name="The Phillips Collection",
        city="Washington",
        state="DC",
        list_urls=("https://www.phillipscollection.org/families",),
    ),
)

DC_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in DC_HTML_VENUES}


async def fetch_html(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
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

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError(f"Unable to fetch HTML: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch HTML after retries: {url}")


async def load_dc_html_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
) -> dict[str, dict]:
    selected = (
        [DC_HTML_VENUES_BY_SLUG[slug] for slug in venue_slugs]
        if venue_slugs
        else list(DC_HTML_VENUES)
    )
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            listing_urls = list(venue.list_urls)
            detail_items: list[dict] = []

            if venue.slug == "kreeger":
                html = await fetch_html(venue.list_urls[0], client=client)
                detail_items.append({"url": venue.list_urls[0], "list_text": "First Studio", "html": html})
                payload[venue.slug] = {"listing_urls": listing_urls, "detail_items": detail_items}
                continue

            seen_links: set[str] = set()
            link_candidates: list[tuple[str, str]] = []
            for listing_url in venue.list_urls:
                try:
                    listing_html = await fetch_html(listing_url, client=client)
                except Exception:
                    continue
                for link_url, list_text in _extract_listing_links(venue.slug, listing_html, listing_url):
                    if link_url in seen_links:
                        continue
                    seen_links.add(link_url)
                    link_candidates.append((link_url, list_text))

            for link_url, list_text in link_candidates:
                try:
                    detail_html = await fetch_html(link_url, client=client)
                except Exception:
                    continue
                detail_items.append({"url": link_url, "list_text": list_text, "html": detail_html})

            payload[venue.slug] = {"listing_urls": listing_urls, "detail_items": detail_items}

    return payload


def parse_dc_html_events(payload: dict, *, venue: DcHtmlVenueConfig) -> list[ExtractedActivity]:
    detail_items = payload.get("detail_items") or []
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    for item in detail_items:
        row = _build_row(item, venue=venue)
        if row is None or row.start_at.date() < current_date:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class DcHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "dc_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_dc_html_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_dc_html_bundle_payload/parse_dc_html_events from script runner.")


def _extract_listing_links(venue_slug: str, html: str, list_url: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[tuple[str, str]] = []

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        text = _normalize_space(anchor.get_text(" ", strip=True))
        if not text:
            continue

        if venue_slug == "asian_art":
            if not ASIA_EVENT_RE.search(href):
                continue
        elif venue_slug in {"npg", "phillips"}:
            if "/event/" not in href:
                continue
        else:
            continue

        links.append((urljoin(list_url, href), text))

    return links


def _build_row(item: dict, *, venue: DcHtmlVenueConfig) -> ExtractedActivity | None:
    if venue.slug == "asian_art":
        parsed = _parse_asian_art_item(item, venue=venue)
    elif venue.slug == "npg":
        parsed = _parse_npg_item(item, venue=venue)
    elif venue.slug == "kreeger":
        parsed = _parse_kreeger_item(item, venue=venue)
    elif venue.slug == "phillips":
        parsed = _parse_phillips_item(item, venue=venue)
    else:
        return None

    if parsed is None:
        return None

    token_blob = _searchable_blob(
        " ".join(
            [
                parsed.title,
                parsed.description or "",
                parsed.location_text or "",
            ]
        )
    )
    include_hits = [pattern for pattern in INCLUDE_PATTERNS if pattern in token_blob]
    strong_activity = any(pattern in token_blob for pattern in STRONG_ACTIVITY_PATTERNS)
    exclude_hits = [pattern for pattern in STRICT_EXCLUDE_PATTERNS if pattern in token_blob]

    if not include_hits:
        return None
    hard_blockers = {
        " camp ",
        " camps ",
        " concert ",
        " film ",
        " films ",
        " meditation ",
        " mindfulness ",
        " music ",
        " performance ",
        " poem ",
        " poetry ",
        " reading ",
        " shopping ",
        " book signing ",
        " culinary ",
        " writing ",
        " yoga ",
    }
    if any(pattern in hard_blockers for pattern in exclude_hits):
        return None
    if any(pattern in {" admission ", " storytime ", " tour ", " tours "} for pattern in exclude_hits) and not strong_activity:
        return None
    return parsed


def _parse_asian_art_item(item: dict, *, venue: DcHtmlVenueConfig) -> ExtractedActivity | None:
    text = _html_text(item["html"])
    title = _extract_title(item["html"], default=item.get("list_text") or "")
    date_match = ASIA_DATE_RE.search(text)
    if not title or date_match is None:
        return None

    start_at, end_at = _parse_date_and_times(date_match.group("date"), date_match.group("times"))
    if start_at is None:
        return None

    description = _match_group(ASIA_DESCRIPTION_RE, text, "description")
    topics = _match_group(ASIA_TOPICS_RE, text, "topics")
    location = _match_group(ASIA_LOCATION_RE, text, "location") or f"{venue.city}, {venue.state}"
    cost = _match_group(ASIA_COST_RE, text, "cost")

    description_parts = [part for part in [description, f"Topics: {topics}" if topics else None, f"Cost: {cost}" if cost else None] if part]
    full_description = " | ".join(description_parts) if description_parts else None
    token_blob = _searchable_blob(" ".join(part for part in [title, full_description or "", topics] if part))
    if not any(pattern in token_blob for pattern in YOUTH_FOCUS_PATTERNS):
        return None
    is_free, free_verification_status = infer_price_classification(
        " ".join(part for part in [cost, full_description] if part),
        default_is_free=True,
    )

    return ExtractedActivity(
        source_url=item["url"],
        title=title,
        description=full_description,
        venue_name=venue.venue_name,
        location_text=location,
        city=venue.city,
        state=venue.state,
        activity_type="workshop",
        age_min=None,
        age_max=None,
        drop_in=("walk-up" in _searchable_blob(full_description or "")),
        registration_required=("registration" in _searchable_blob(full_description or "") and "no registration" not in _searchable_blob(full_description or "")),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_verification_status,
    )


def _parse_npg_item(item: dict, *, venue: DcHtmlVenueConfig) -> ExtractedActivity | None:
    text = _html_text(item["html"])
    title = _extract_title(item["html"], default=item.get("list_text") or "")
    date_match = NPG_DATE_RE.search(text)
    if not title or date_match is None:
        return None

    start_at, end_at = _parse_date_and_times(date_match.group("date"), date_match.group("times"))
    if start_at is None:
        return None

    location = _match_group(NPG_LOCATION_RE, text, "location") or f"{venue.city}, {venue.state}"
    cost = _match_group(NPG_COST_RE, text, "cost")
    category = _match_group(NPG_CATEGORY_RE, text, "category")
    description = _match_group(NPG_CATEGORY_RE, text, "description")
    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description] if part))
    description_parts = [part for part in [description, f"Category: {category}" if category else None, f"Cost: {cost}" if cost else None] if part]
    full_description = " | ".join(description_parts) if description_parts else None
    price_text = " ".join(part for part in [cost, full_description] if part)
    is_free, free_verification_status = infer_price_classification(price_text, default_is_free=True)
    token_blob = _searchable_blob(price_text)

    return ExtractedActivity(
        source_url=item["url"],
        title=title,
        description=full_description,
        venue_name=venue.venue_name,
        location_text=location,
        city=venue.city,
        state=venue.state,
        activity_type="workshop",
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop in" in token_blob or "drop-in" in token_blob),
        registration_required=("registration required" in token_blob and "no registration required" not in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_verification_status,
    )


def _parse_kreeger_item(item: dict, *, venue: DcHtmlVenueConfig) -> ExtractedActivity | None:
    text = _html_text(item["html"])
    match = KREEGER_DATETIME_RE.search(text)
    title = "First Studio: Art, Story, and Workshop"
    if match is None or title not in text:
        return None

    start_at = _parse_month_day_without_year(
        month_name=match.group("month"),
        day_text=match.group("day"),
        time_text=match.group("time"),
    )
    if start_at is None:
        return None

    description_match = re.search(
        r"First Studio: Art, Story, and Workshop\s+[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?\s*\|\s*\d{1,2}:\d{2}(?:am|pm)\s+Materials fee:\s*\$?\d+(?:\.\d{2})?\s+(?P<description>.+?)\s+Join Our Mailing List",
        text,
        re.IGNORECASE,
    )
    description = _normalize_space(description_match.group("description")) if description_match else None
    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description] if part))
    price_text = "Materials fee: $10"
    is_free, free_verification_status = infer_price_classification(price_text, default_is_free=None)

    return ExtractedActivity(
        source_url=item["url"],
        title=title,
        description=" | ".join(part for part in [description, price_text] if part) if description else price_text,
        venue_name=venue.venue_name,
        location_text=f"{venue.city}, {venue.state}",
        city=venue.city,
        state=venue.state,
        activity_type="workshop",
        age_min=age_min,
        age_max=age_max,
        drop_in=None,
        registration_required=None,
        start_at=start_at,
        end_at=None,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_verification_status,
    )


def _parse_phillips_item(item: dict, *, venue: DcHtmlVenueConfig) -> ExtractedActivity | None:
    text = _html_text(item["html"])
    title_match = re.search(
        r"Breadcrumb\s+Exhibitions and Events\s+(?P<title>The Phillips Plays(?:\s+[A-Za-z][A-Za-z\s]+)?)\s+Drop-in Family Program",
        text,
        re.IGNORECASE,
    )
    title = _normalize_space(title_match.group("title")) if title_match else _extract_title(item["html"], default=item.get("list_text") or "")
    datetime_match = PHILLIPS_DATETIME_RE.search(text)
    if not title or datetime_match is None:
        return None

    start_at, end_at = _parse_date_and_times(datetime_match.group("date"), datetime_match.group("times"))
    if start_at is None:
        return None

    price = _match_group(PHILLIPS_PRICE_RE, text, "price")
    description_match = re.search(
        r"Registration Open\s*/\s*In-Person\s+(?P<description>.+?)\s+Related Events",
        text,
        re.IGNORECASE,
    )
    description = _normalize_space(description_match.group("description")) if description_match else None
    description_parts = [part for part in [description, price] if part]
    full_description = " | ".join(description_parts) if description_parts else None
    price_text = " ".join(description_parts)
    is_free, free_verification_status = infer_price_classification(price_text, default_is_free=None)
    token_blob = _searchable_blob(" ".join(part for part in [title, full_description] if part))

    return ExtractedActivity(
        source_url=item["url"],
        title=title,
        description=full_description,
        venue_name=venue.venue_name,
        location_text=f"{venue.city}, {venue.state}",
        city=venue.city,
        state=venue.state,
        activity_type="workshop",
        age_min=None,
        age_max=None,
        drop_in=("drop in" in token_blob or "drop-in" in token_blob),
        registration_required=("registration open" in token_blob and "drop in" not in token_blob and "drop-in" not in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_verification_status,
    )


def _parse_date_and_times(date_text: str, times_text: str) -> tuple[datetime | None, datetime | None]:
    base_date = _parse_date(date_text)
    if base_date is None:
        return None, None

    time_match = TIME_RANGE_RE.search(times_text)
    if time_match is not None:
        start_time = _parse_time_value(time_match.group("start"))
        end_time = _parse_time_value(time_match.group("end"))
        if start_time is None:
            return None, None
        start_at = datetime.combine(base_date.date(), start_time)
        end_at = datetime.combine(base_date.date(), end_time) if end_time is not None else None
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(times_text)
    if single_match is None:
        return datetime.combine(base_date.date(), datetime.min.time()), None
    start_time = _parse_time_value(single_match.group("time"))
    if start_time is None:
        return None, None
    return datetime.combine(base_date.date(), start_time), None


def _parse_date(value: str) -> datetime | None:
    normalized = value.replace(".", "").strip()
    for fmt in (
        "%a %b %d, %Y",
        "%A, %B %d, %Y",
        "%B %d, %Y",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _parse_time_value(value: str):
    normalized = _normalize_ampm(value)
    for fmt in ("%I:%M %p", "%I %p", "%I:%M%p", "%I%p"):
        try:
            return datetime.strptime(normalized, fmt).time()
        except ValueError:
            continue
    return None


def _parse_month_day_without_year(*, month_name: str, day_text: str, time_text: str) -> datetime | None:
    now = datetime.now(ZoneInfo(NY_TIMEZONE))
    normalized_time = _normalize_ampm(time_text)
    for fmt in ("%B %d %Y %I:%M %p", "%b %d %Y %I:%M %p", "%B %d %Y %I:%M%p", "%b %d %Y %I:%M%p"):
        try:
            candidate = datetime.strptime(
                f"{month_name} {int(day_text)} {now.year} {normalized_time}",
                fmt,
            )
            if candidate.date() < now.date():
                candidate = candidate.replace(year=candidate.year + 1)
            return candidate
        except ValueError:
            continue
    return None


def _parse_age_range(blob: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(blob)
    if match is None:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _extract_title(html: str, *, default: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    heading = soup.find("h1")
    if heading is not None:
        text = _normalize_space(heading.get_text(" ", strip=True))
        if text:
            return text
    if soup.title is not None:
        title_text = _normalize_space(soup.title.get_text(" ", strip=True))
        if " | " in title_text:
            return title_text.split(" | ", 1)[0].strip()
        if " - " in title_text:
            return title_text.split(" - ", 1)[0].strip()
        if title_text:
            return title_text
    return default


def _match_group(pattern: re.Pattern[str], text: str, group_name: str) -> str:
    match = pattern.search(text)
    if match is None:
        return ""
    return _normalize_space(match.group(group_name))


def _html_text(html: str) -> str:
    return _normalize_space(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))


def _normalize_ampm(value: str) -> str:
    normalized = value.replace(".", "").replace("\u202f", " ").replace("\xa0", " ")
    normalized = re.sub(r"(?i)(\d)(am|pm)\b", lambda match: f"{match.group(1)} {match.group(2).upper()}", normalized)
    normalized = re.sub(r"(?i)\bam\b", "AM", normalized)
    normalized = re.sub(r"(?i)\bpm\b", "PM", normalized)
    return " ".join(normalized.split())


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def get_dc_html_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in DC_HTML_VENUES:
        for list_url in venue.list_urls:
            parsed = urlparse(list_url)
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
            break
    return tuple(prefixes)
