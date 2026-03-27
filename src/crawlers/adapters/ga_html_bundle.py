from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urljoin
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None

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

PLAYWRIGHT_LAUNCH_ARGS = (
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
)

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " class ",
    " classes ",
    " discussion ",
    " hand papermaking ",
    " lecture ",
    " lectures ",
    " paper weaving ",
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
    " concert ",
    " exhibition ",
    " exhibitions ",
    " film ",
    " fundraiser ",
    " gala ",
    " music ",
    " performance ",
    " reception ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)
REGISTRATION_PATTERNS = (
    " preregistration ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
)
TIME_RANGE_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)\s*(?:-|–|—|to)\s*(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
DATE_WITH_TIME_RE = re.compile(
    r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{1,2}):(\d{2})\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
DATE_ONLY_RE = re.compile(
    r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+([A-Za-z]{3,9})\s+(\d{1,2})",
    re.IGNORECASE,
)
HIGH_DATE_TIME_RE = re.compile(
    r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s*\|\s*"
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)\s*"
    r"(?:-|–|—|to)\s*"
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
HIGH_DATE_RANGE_TIME_RE = re.compile(
    r"([A-Za-z]{3,9})\s+(\d{1,2})\s*(?:-|–|—|to)\s*"
    r"(?:(?P<end_month>[A-Za-z]{3,9})\s+)?(?P<end_day>\d{1,2}),\s*(?P<year>\d{4})\s*\|\s*"
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)\s*"
    r"(?:-|–|—|to)\s*"
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
HIGH_SINGLE_TIME_RE = re.compile(
    r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s*\|\s*"
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
HIGH_LOCATION_RE = re.compile(r"Location:\s*(.+?)(?:Registration Required|Tickets|$)", re.IGNORECASE)
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


@dataclass(frozen=True, slots=True)
class GaHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    needs_playwright: bool = False


GA_HTML_VENUES: tuple[GaHtmlVenueConfig, ...] = (
    GaHtmlVenueConfig(
        slug="zuckerman",
        source_name="ga_zuckerman_events",
        venue_name="Bernard A. Zuckerman Museum of Art",
        city="Kennesaw",
        state="GA",
        list_url="https://campus.kennesaw.edu/colleges-departments/arts/academics/visual-arts/zuckerman/events-and-programs/index.php",
    ),
    GaHtmlVenueConfig(
        slug="oglethorpe",
        source_name="ga_oglethorpe_events",
        venue_name="Oglethorpe University Museum of Art",
        city="Atlanta",
        state="GA",
        list_url="https://connect.oglethorpe.edu/organization/ouma/events",
        needs_playwright=True,
    ),
    GaHtmlVenueConfig(
        slug="high",
        source_name="ga_high_events",
        venue_name="High Museum of Art",
        city="Atlanta",
        state="GA",
        list_url="https://high.org/events/",
    ),
)

GA_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in GA_HTML_VENUES}


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
            timezone_id=NY_TIMEZONE,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        html = await page.content()
        await browser.close()
        return html


async def load_ga_html_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    max_detail_pages: int = 50,
) -> dict[str, dict]:
    selected = [GA_HTML_VENUES_BY_SLUG[slug] for slug in venue_slugs] if venue_slugs else list(GA_HTML_VENUES)
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            if venue.needs_playwright:
                list_html = await fetch_html_playwright(venue.list_url)
            else:
                list_html = await fetch_html(venue.list_url, client=client)

            detail_pages: dict[str, str] = {}
            api_items: list[dict] = []
            if venue.slug == "oglethorpe":
                detail_urls = _extract_oglethorpe_detail_urls(list_html, venue.list_url)[:max_detail_pages]
                for detail_url in detail_urls:
                    try:
                        detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                    except Exception as exc:
                        print(f"[ga-html-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")
            elif venue.slug == "high":
                api_items = await _load_high_api_items(client)
                for item in api_items[:max_detail_pages]:
                    detail_url = _normalize_space(str(item.get("link") or ""))
                    if not detail_url:
                        continue
                    try:
                        detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                    except Exception as exc:
                        print(f"[ga-html-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")

            payload[venue.slug] = {
                "list_html": list_html,
                "detail_pages": detail_pages,
                "api_items": api_items,
            }

    return payload


def parse_ga_html_events(payload: dict, *, venue: GaHtmlVenueConfig) -> list[ExtractedActivity]:
    if venue.slug == "zuckerman":
        rows = _parse_zuckerman_events(payload, venue=venue)
    elif venue.slug == "oglethorpe":
        rows = _parse_oglethorpe_events(payload, venue=venue)
    elif venue.slug == "high":
        rows = _parse_high_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}
    for row in rows:
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    out = list(deduped.values())
    out.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return out


class GaHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "ga_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_ga_html_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_ga_html_bundle_payload/parse_ga_html_events from script runner.")


def _parse_zuckerman_events(payload: dict, *, venue: GaHtmlVenueConfig) -> list[ExtractedActivity]:
    soup = BeautifulSoup(payload.get("list_html") or "", "html.parser")
    rows: list[ExtractedActivity] = []
    current_year = datetime.now(ZoneInfo(NY_TIMEZONE)).year

    for container in soup.select("div.two_col.is_30.has_gap"):
        title_node = container.select_one(".section_heading p.heading")
        content_cell = container.select_one("td.ou-content")
        if title_node is None or content_cell is None:
            continue

        title = _normalize_space(title_node.get_text(" ", strip=True))
        if not title:
            continue

        paragraphs = content_cell.find_all("p")
        if not paragraphs:
            continue

        date_line = _normalize_space(paragraphs[0].get_text(" ", strip=True))
        description = " ".join(
            _normalize_space(p.get_text(" ", strip=True))
            for p in paragraphs[1:]
            if _normalize_space(p.get_text(" ", strip=True))
        ).strip()
        if not _should_keep_event(title=title, description=description):
            continue

        parsed = _parse_zuckerman_datetime(date_line, year=current_year)
        if parsed is None:
            continue
        start_at, end_at = parsed

        source_url = venue.list_url
        registration_link = content_cell.find("a", href=True)
        if registration_link is not None:
            source_url = urljoin(venue.list_url, registration_link.get("href") or venue.list_url)

        price_text = ""
        if "free" in description.lower():
            price_text = "free"

        is_free, free_status = infer_price_classification(description)
        full_description = description or None

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=full_description,
                venue_name=venue.venue_name,
                location_text=f"{venue.city}, {venue.state}",
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(f"{title} {description}"),
                age_min=None,
                age_max=None,
                drop_in=False,
                registration_required=("rsvp" in description.lower() or registration_link is not None),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    return rows


def _parse_oglethorpe_events(payload: dict, *, venue: GaHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []

    for source_url, html in (payload.get("detail_pages") or {}).items():
        event_obj = _extract_oglethorpe_event_obj(html)
        if not event_obj:
            continue

        title = _normalize_space(event_obj.get("name"))
        description = _html_to_text(event_obj.get("description"))
        if not title or not _should_keep_event(title=title, description=description):
            continue

        start_at = _parse_iso_datetime(event_obj.get("startsOn"))
        if start_at is None:
            continue
        end_at = _parse_iso_datetime(event_obj.get("endsOn"))

        address = event_obj.get("address") or {}
        location_name = _normalize_space(address.get("name")) or f"{venue.city}, {venue.state}"
        categories = [item.get("name") for item in (event_obj.get("categories") or []) if isinstance(item, dict)]
        categories = [_normalize_space(value) for value in categories if _normalize_space(value)]
        description_parts = [description] if description else []
        if categories:
            description_parts.append(f"Categories: {', '.join(categories)}")
        full_description = " | ".join(part for part in description_parts if part) or None
        token_blob = f"{title} {' '.join(categories)} {description}"

        is_free, free_status = infer_price_classification(full_description)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=full_description,
                venue_name=venue.venue_name,
                location_text=location_name,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(token_blob),
                age_min=None,
                age_max=None,
                drop_in=False,
                registration_required=any(marker in token_blob.lower() for marker in REGISTRATION_PATTERNS),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    return rows


async def _load_high_api_items(client: httpx.AsyncClient, *, page_limit: int = 4, per_page: int = 50) -> list[dict]:
    items: list[dict] = []
    for page in range(1, page_limit + 1):
        response = await client.get(
            "https://high.org/wp-json/wp/v2/event",
            params={"per_page": per_page, "page": page},
        )
        response.raise_for_status()
        page_items = response.json()
        if not isinstance(page_items, list) or not page_items:
            break
        items.extend(item for item in page_items if isinstance(item, dict))
        total_pages = int(response.headers.get("X-WP-TotalPages", "1") or "1")
        if page >= total_pages or len(page_items) < per_page:
            break
    return items


def _parse_high_events(payload: dict, *, venue: GaHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    api_items_by_url = {
        _normalize_space(str(item.get("link") or "")): item
        for item in (payload.get("api_items") or [])
        if isinstance(item, dict)
    }

    for source_url, html in (payload.get("detail_pages") or {}).items():
        soup = BeautifulSoup(html, "html.parser")
        api_item = api_items_by_url.get(source_url) or {}

        title = _normalize_space(_html_to_text(((api_item.get("title") or {}) if isinstance(api_item, dict) else {}).get("rendered")))
        if not title:
            title = _normalize_space(soup.select_one("h1.title").get_text(" ", strip=True) if soup.select_one("h1.title") else "")
        if not title:
            continue

        description = _html_to_text(((api_item.get("content") or {}) if isinstance(api_item, dict) else {}).get("rendered"))
        if not description:
            description = _html_to_text(((api_item.get("excerpt") or {}) if isinstance(api_item, dict) else {}).get("rendered"))

        meta_node = soup.select_one("section[aria-label] .description") or soup.select_one(".description")
        meta_text = _normalize_space(meta_node.get_text(" ", strip=True) if meta_node else "")
        if not _should_keep_event(title=title, description=" ".join(part for part in [meta_text, description] if part)):
            continue

        parsed = _parse_high_datetime(meta_text)
        if parsed is None:
            continue
        start_at, end_at = parsed

        price_text = _normalize_space(
            soup.select_one(".at-accordion-price").get_text(" ", strip=True)
            if soup.select_one(".at-accordion-price")
            else ""
        )
        location_text = _extract_high_location(meta_text) or "High Museum of Art, Atlanta, GA"
        registration_required = any(marker in _searchable_blob(f"{meta_text} {description}") for marker in REGISTRATION_PATTERNS)
        if price_text:
            is_free, free_status = infer_price_classification(price_text)
        else:
            is_free, free_status = infer_price_classification(" | ".join(part for part in [description, meta_text] if part))

        description_parts = [part for part in [description or None, meta_text or None, f"Price: {price_text}" if price_text else None] if part]
        full_description = " | ".join(dict.fromkeys(description_parts)) or None

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=full_description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(f"{title} {meta_text} {description}"),
                age_min=None,
                age_max=None,
                drop_in=" drop in " in _searchable_blob(f"{meta_text} {description}"),
                registration_required=registration_required,
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    return rows


def _extract_oglethorpe_detail_urls(html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not re.search(r"/event/\d+", href):
            continue
        absolute = urljoin(list_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        urls.append(absolute)
    return urls


def _extract_oglethorpe_event_obj(html: str) -> dict | None:
    match = re.search(r"window\.initialAppState\s*=\s*(\{.*?\})\s*;</script>", html, re.DOTALL)
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    event_obj = (((data.get("preFetchedData") or {}).get("event")) or None)
    return event_obj if isinstance(event_obj, dict) else None


def _parse_zuckerman_datetime(value: str, *, year: int) -> tuple[datetime, datetime | None] | None:
    normalized = " ".join(value.split())
    match = DATE_WITH_TIME_RE.search(normalized)
    if not match:
        return None

    month_name = match.group(2)
    month = MONTH_LOOKUP.get(month_name[:3].lower())
    if month is None:
        return None
    day = int(match.group(3))
    start_at = datetime(year, month, day, *_parse_time_parts(match.group(4), match.group(5), match.group(6)))

    range_match = TIME_RANGE_RE.search(normalized)
    end_at = None
    if range_match:
        end_at = datetime(year, month, day, *_parse_time_parts(range_match.group(4), range_match.group(5), range_match.group(6)))

    return start_at, end_at


def _parse_time_parts(hour_text: str, minute_text: str | None, meridiem: str) -> tuple[int, int]:
    hour = int(hour_text)
    minute = int(minute_text or "0")
    meridiem_normalized = meridiem.lower().replace(".", "")
    if meridiem_normalized == "pm" and hour != 12:
        hour += 12
    if meridiem_normalized == "am" and hour == 12:
        hour = 0
    return hour, minute


def _parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=None)


def _parse_date_label(value: str) -> tuple[int, int, int] | None:
    try:
        parsed = datetime.strptime(value, "%B %d, %Y")
        return parsed.year, parsed.month, parsed.day
    except ValueError:
        pass

    try:
        parsed = datetime.strptime(value, "%b %d, %Y")
        return parsed.year, parsed.month, parsed.day
    except ValueError:
        return None


def _parse_high_datetime(meta_text: str) -> tuple[datetime, datetime | None] | None:
    date_range_match = HIGH_DATE_RANGE_TIME_RE.search(meta_text)
    if date_range_match:
        start_month = MONTH_LOOKUP.get(date_range_match.group(1)[:3].lower())
        end_month_name = date_range_match.group("end_month") or date_range_match.group(1)
        end_month = MONTH_LOOKUP.get(end_month_name[:3].lower())
        year = int(date_range_match.group("year"))
        if start_month is None or end_month is None:
            return None
        start_at = datetime(
            year,
            start_month,
            int(date_range_match.group(2)),
            *_parse_time_parts(date_range_match.group(6), date_range_match.group(7), date_range_match.group(8)),
        )
        end_at = datetime(
            year,
            end_month,
            int(date_range_match.group("end_day")),
            *_parse_time_parts(date_range_match.group(9), date_range_match.group(10), date_range_match.group(11)),
        )
        return start_at, end_at

    range_match = HIGH_DATE_TIME_RE.search(meta_text)
    if range_match:
        ymd = _parse_date_label(range_match.group(1))
        if ymd is None:
            return None
        start_at = datetime(*ymd, *_parse_time_parts(range_match.group(2), range_match.group(3), range_match.group(4)))
        end_at = datetime(*ymd, *_parse_time_parts(range_match.group(5), range_match.group(6), range_match.group(7)))
        return start_at, end_at

    single_match = HIGH_SINGLE_TIME_RE.search(meta_text)
    if single_match:
        ymd = _parse_date_label(single_match.group(1))
        if ymd is None:
            return None
        start_at = datetime(*ymd, *_parse_time_parts(single_match.group(2), single_match.group(3), single_match.group(4)))
        return start_at, None

    return None


def _extract_high_location(meta_text: str) -> str | None:
    match = HIGH_LOCATION_RE.search(meta_text)
    if not match:
        return None
    return _normalize_space(match.group(1))


def _should_keep_event(*, title: str, description: str) -> bool:
    title_blob = _searchable_blob(title)
    token_blob = _searchable_blob(" ".join([title, description]))
    strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    weak_include = strong_include or any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS)
    if not weak_include:
        return False
    if any(pattern in title_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS) and not strong_include:
        return False
    return True


def _infer_activity_type(text: str) -> str:
    token_blob = _searchable_blob(text)
    if any(pattern in token_blob for pattern in (" lecture ", " lectures ", " talk ", " talks ", " discussion ")):
        return "lecture"
    return "workshop"


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def get_ga_html_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in GA_HTML_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    prefixes.append("https://connect.oglethorpe.edu/")
    return tuple(dict.fromkeys(prefixes))
