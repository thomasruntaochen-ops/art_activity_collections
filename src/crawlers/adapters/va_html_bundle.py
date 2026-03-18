import asyncio
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from urllib.parse import urljoin, urlparse

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
    " closed ",
    " concert ",
    " dinner ",
    " exhibition ",
    " film ",
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
    " poetry ",
    " raffle ",
    " reception ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)
INCLUDE_PATTERNS = (
    " activity ",
    " art making ",
    " class ",
    " classes ",
    " conversation ",
    " crochet ",
    " discussion ",
    " drawing ",
    " family ",
    " homeschool ",
    " kid ",
    " kids ",
    " lab ",
    " lecture ",
    " meetup ",
    " preschool ",
    " presentation ",
    " quilting ",
    " stitch ",
    " studio ",
    " talk ",
    " teen ",
    " teens ",
    " workshop ",
    " workshops ",
    " youth ",
)

AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

DATE_WITH_YEAR_RE = re.compile(r"([A-Za-z]{3,9}\.?\s+\d{1,2},\s*\d{4})")
DATE_RANGE_RE = re.compile(r"([A-Za-z]{3,9}\.?\s+\d{1,2},\s*\d{4})\s*(?:-|–|—|to)\s*([A-Za-z]{3,9}\.?\s+\d{1,2},\s*\d{4})")
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}:\d{2}(?:\s*[APap]\.?M\.?)?)\s*(?:-|–|—|to)\s*(?P<end>\d{1,2}:\d{2}\s*[APap]\.?M\.?)"
)
ICA_DATE_TIME_RE = re.compile(
    r"(?P<weekday>[A-Za-z]+),?\s+"
    r"(?P<month>[A-Za-z]{3,9})\.?\s+"
    r"(?P<day>\d{1,2})"
    r"(?:,\s*(?P<year>\d{4}))?\s+"
    r"(?P<times>\d{1,2}:\d{2}\s*[APap]\.?M\.?\s*[–—-]\s*\d{1,2}:\d{2}\s*[APap]\.?M\.?)"
)
MOCA_DATE_TIME_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*/\s*"
    r"(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2})\s*/\s*"
    r"(?P<times>\d{1,2}:\d{2}\s*[APap]M\s*-\s*\d{1,2}:\d{2}\s*[APap]M)"
)
MUSCARELLE_LISTING_RE = re.compile(
    r"^(?P<title>.+?)\s+"
    r"(?P<date>(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4})\s+"
    r"(?P<start>\d{1,2}:\d{2}\s*[APap]M)"
    r"(?:\s*-\s*(?P<end>\d{1,2}:\d{2}\s*[APap]M))?"
    r"(?:\s+(?P<tail>.+))?$"
)
KLUGE_LISTING_RE = re.compile(
    r"^Upcoming Event\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})(?:\s+until\s+[A-Za-z]+\s+\d{1,2},\s+\d{4})?\s+(?P<title>.+)$"
)


@dataclass(frozen=True, slots=True)
class VaHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str


VA_HTML_VENUES: tuple[VaHtmlVenueConfig, ...] = (
    VaHtmlVenueConfig(
        slug="icavcu",
        source_name="icavcu_events",
        venue_name="Institute for Contemporary Art at Virginia Commonwealth University",
        city="Richmond",
        state="VA",
        list_url="https://icavcu.org/events/",
    ),
    VaHtmlVenueConfig(
        slug="kluge_ruhe",
        source_name="kluge_ruhe_events",
        venue_name="Kluge-Ruhe Aboriginal Art Collection of the University of Virginia",
        city="Charlottesville",
        state="VA",
        list_url="https://kluge-ruhe.org/calendar/",
    ),
    VaHtmlVenueConfig(
        slug="muscarelle",
        source_name="muscarelle_events",
        venue_name="Muscarelle Museum of Art",
        city="Williamsburg",
        state="VA",
        list_url="https://muscarelle.wm.edu/events/",
    ),
    VaHtmlVenueConfig(
        slug="moca_arlington",
        source_name="moca_arlington_events",
        venue_name="Museum of Contemporary Art Arlington",
        city="Arlington",
        state="VA",
        list_url="https://mocaarlington.org/events/",
    ),
    VaHtmlVenueConfig(
        slug="piedmont_arts",
        source_name="piedmont_arts_events",
        venue_name="Piedmont Arts",
        city="Martinsville",
        state="VA",
        list_url="https://www.piedmontarts.org/events.cfm",
    ),
    VaHtmlVenueConfig(
        slug="va_quilt",
        source_name="va_quilt_events",
        venue_name="Virginia Quilt Museum",
        city="Dayton",
        state="VA",
        list_url="https://www.vaquiltmuseum.org/events",
    ),
)

VA_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in VA_HTML_VENUES}


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


async def load_va_html_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    page_limit: int | None = None,
    max_links_per_venue: int = 60,
) -> dict[str, dict]:
    selected = (
        [VA_HTML_VENUES_BY_SLUG[slug] for slug in venue_slugs]
        if venue_slugs
        else list(VA_HTML_VENUES)
    )
    effective_page_limit = max(1, page_limit or 1)
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            listing_urls = _build_listing_urls(venue, page_limit=effective_page_limit)
            link_candidates: list[tuple[str, str]] = []
            seen_links: set[str] = set()

            for listing_url in listing_urls:
                try:
                    listing_html = await fetch_html(listing_url, client=client)
                except Exception:
                    continue
                for link_url, list_text in _extract_listing_links(venue.slug, listing_html, listing_url):
                    if link_url in seen_links:
                        continue
                    seen_links.add(link_url)
                    link_candidates.append((link_url, list_text))
                    if len(link_candidates) >= max_links_per_venue:
                        break
                if len(link_candidates) >= max_links_per_venue:
                    break

            detail_items: list[dict] = []
            for link_url, list_text in link_candidates:
                try:
                    detail_html = await fetch_html(link_url, client=client)
                except Exception:
                    continue
                detail_items.append(
                    {
                        "url": link_url,
                        "list_text": list_text,
                        "html": detail_html,
                    }
                )

            payload[venue.slug] = {
                "listing_urls": listing_urls,
                "detail_items": detail_items,
            }

    return payload


def parse_va_html_events(payload: dict, *, venue: VaHtmlVenueConfig) -> list[ExtractedActivity]:
    detail_items = payload.get("detail_items") or []
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    for item in detail_items:
        row = _build_row(item, venue=venue)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue
        dedupe_key = (row.source_url, row.title, row.start_at)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class VaHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "va_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_va_html_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_va_html_bundle_payload/parse_va_html_events from script runner.")


def _build_row(item: dict, *, venue: VaHtmlVenueConfig) -> ExtractedActivity | None:
    if venue.slug == "icavcu":
        parsed = _parse_icavcu_item(item)
    elif venue.slug == "kluge_ruhe":
        parsed = _parse_kluge_item(item)
    elif venue.slug == "muscarelle":
        parsed = _parse_muscarelle_item(item)
    elif venue.slug == "moca_arlington":
        parsed = _parse_moca_item(item)
    elif venue.slug == "piedmont_arts":
        parsed = _parse_piedmont_item(item)
    elif venue.slug == "va_quilt":
        parsed = _parse_va_quilt_item(item)
    else:
        return None

    if parsed is None:
        return None

    title = parsed["title"]
    source_url = parsed["source_url"]
    start_at = parsed["start_at"]
    end_at = parsed["end_at"]
    description = parsed.get("description")
    signal_text = _normalize_space(parsed.get("signal_text") or "")
    token_blob = _searchable_blob(" ".join(part for part in [title, signal_text] if part))

    if " cancelled " in token_blob or " canceled " in token_blob:
        return None
    if any(pattern in token_blob for pattern in STRICT_EXCLUDE_PATTERNS):
        return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    is_free, free_status = infer_price_classification(description)
    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))
    activity_type = _infer_activity_type(token_blob)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=venue.venue_name,
        location_text=f"{venue.city}, {venue.state}",
        city=venue.city,
        state=venue.state,
        activity_type=activity_type,
        age_min=age_min,
        age_max=age_max,
        drop_in=(" drop-in " in token_blob or " drop in " in token_blob),
        registration_required=(" register " in token_blob or " registration " in token_blob or " ticket " in token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _parse_icavcu_item(item: dict) -> dict | None:
    source_url = _normalize_space(item.get("url"))
    html = item.get("html")
    if not source_url or not isinstance(html, str):
        return None

    soup = BeautifulSoup(html, "html.parser")
    title = _first_non_empty(
        _normalize_space(_node_text(soup.select_one("h1"))),
        _normalize_space(_meta_content(soup, "og:title")),
    )
    if not title:
        return None

    date_line = _normalize_space(_node_text(soup.select_one(".event-date-time-meta.event-single")))
    if not date_line:
        date_bits = _normalize_space(_node_text(soup.select_one(".event-meta-date")))
        time_bits = _normalize_space(_node_text(soup.select_one(".event-meta-time")))
        date_line = " ".join(part for part in [date_bits, time_bits] if part)
    match = ICA_DATE_TIME_RE.search(date_line)
    if not match:
        return None

    year = _parse_int(match.group("year"))
    if year is None:
        year = _extract_year_from_url(source_url) or datetime.now(ZoneInfo(NY_TIMEZONE)).year
    dt = _parse_date_month_day(match.group("month"), match.group("day"), year)
    if dt is None:
        return None
    start_time, end_time = _parse_time_range(match.group("times"))
    if start_time is None:
        return None

    description = _extract_description_from_detail(soup)
    return {
        "source_url": source_url,
        "title": title,
        "description": description,
        "signal_text": date_line,
        "start_at": _combine_date_time(dt, start_time),
        "end_at": _combine_date_time(dt, end_time) if end_time else None,
    }


def _parse_kluge_item(item: dict) -> dict | None:
    source_url = _normalize_space(item.get("url"))
    list_text = _normalize_space(item.get("list_text"))
    html = item.get("html")
    if not source_url or not list_text or not isinstance(html, str):
        return None

    match = KLUGE_LISTING_RE.match(list_text)
    if not match:
        return None

    dt = _parse_full_date(match.group("date"))
    if dt is None:
        return None
    title = _normalize_space(match.group("title"))
    if not title:
        return None

    soup = BeautifulSoup(html, "html.parser")
    description = _extract_description_from_detail(soup)
    return {
        "source_url": source_url,
        "title": title,
        "description": description,
        "signal_text": list_text,
        "start_at": _combine_date_time(dt, None),
        "end_at": None,
    }


def _parse_muscarelle_item(item: dict) -> dict | None:
    source_url = _normalize_space(item.get("url"))
    list_text = _normalize_space(item.get("list_text"))
    html = item.get("html")
    if not source_url or not isinstance(html, str):
        return None

    soup = BeautifulSoup(html, "html.parser")
    title = _first_non_empty(
        _normalize_space(_node_text(soup.select_one("h1"))),
        _normalize_space(_meta_content(soup, "og:title")),
    )
    date_obj: date | None = None
    start_time: time | None = None
    end_time: time | None = None
    extra = ""
    date_line = ""

    listing_match = MUSCARELLE_LISTING_RE.match(list_text)
    if listing_match:
        if not title:
            title = _normalize_space(listing_match.group("title"))
        date_obj = _parse_full_date(listing_match.group("date"))
        start_time = _parse_time_value(listing_match.group("start"))
        end_time = _parse_time_value(listing_match.group("end"))
        extra = _normalize_space(listing_match.group("tail"))

    if date_obj is None:
        date_line = _normalize_space(_node_text(soup.select_one(".event-date-time-meta.event-single")))
        full_date = _first_match(DATE_WITH_YEAR_RE, date_line)
        if full_date:
            date_obj = _parse_full_date(full_date)
        start_time, end_time = _parse_time_range(date_line)

    if not title or date_obj is None:
        return None
    if start_time is None:
        return None

    description = _extract_description_from_detail(soup)
    if extra:
        description = " | ".join(part for part in [extra, description] if part)

    return {
        "source_url": source_url,
        "title": title,
        "description": description,
        "signal_text": list_text or date_line,
        "start_at": _combine_date_time(date_obj, start_time),
        "end_at": _combine_date_time(date_obj, end_time) if end_time else None,
    }


def _parse_moca_item(item: dict) -> dict | None:
    source_url = _normalize_space(item.get("url"))
    html = item.get("html")
    if not source_url or not isinstance(html, str):
        return None

    soup = BeautifulSoup(html, "html.parser")
    text = _normalize_space(soup.get_text(" ", strip=True))
    match = MOCA_DATE_TIME_RE.search(text)
    if not match:
        return None
    year = _extract_year_from_url(source_url)
    if year is None:
        return None

    dt = _parse_date_month_day(match.group("month"), match.group("day"), year)
    if dt is None:
        return None
    start_time, end_time = _parse_time_range(match.group("times"))
    if start_time is None:
        return None

    title = _first_non_empty(
        _normalize_space(_node_text(soup.select_one("h1"))),
        _normalize_space(_meta_content(soup, "og:title")),
    )
    if not title:
        return None
    description = _extract_description_from_detail(soup)

    return {
        "source_url": source_url,
        "title": title,
        "description": description,
        "signal_text": match.group(0),
        "start_at": _combine_date_time(dt, start_time),
        "end_at": _combine_date_time(dt, end_time) if end_time else None,
    }


def _parse_piedmont_item(item: dict) -> dict | None:
    source_url = _normalize_space(item.get("url"))
    html = item.get("html")
    if not source_url or not isinstance(html, str):
        return None

    soup = BeautifulSoup(html, "html.parser")
    title = _first_non_empty(
        _normalize_space(_node_text(soup.select_one("h2"))),
        _normalize_space(_meta_content(soup, "og:title")),
    )
    if not title:
        return None

    date_text = _normalize_space(_node_text(soup.select_one(".date")))
    start_date: date | None = None
    end_date: date | None = None
    if date_text:
        range_match = DATE_RANGE_RE.search(date_text)
        if range_match:
            start_date = _parse_full_date(range_match.group(1))
            end_date = _parse_full_date(range_match.group(2))
        else:
            first_date = _first_match(DATE_WITH_YEAR_RE, date_text)
            if first_date:
                start_date = _parse_full_date(first_date)
    if start_date is None:
        text = _normalize_space(soup.get_text(" ", strip=True))
        first_date = _first_match(DATE_WITH_YEAR_RE, text)
        if first_date:
            start_date = _parse_full_date(first_date)

    if start_date is None:
        return None

    text = _normalize_space(soup.get_text(" ", strip=True))
    start_time, end_time = _parse_time_range(text)
    description = _extract_description_from_detail(soup)

    return {
        "source_url": source_url,
        "title": title,
        "description": description,
        "signal_text": date_text or "",
        "start_at": _combine_date_time(start_date, start_time),
        "end_at": _combine_date_time(start_date, end_time) if end_time else (_combine_date_time(end_date, None) if end_date else None),
    }


def _parse_va_quilt_item(item: dict) -> dict | None:
    source_url = _normalize_space(item.get("url"))
    html = item.get("html")
    if not source_url or not isinstance(html, str):
        return None

    soup = BeautifulSoup(html, "html.parser")
    text = _normalize_space(soup.get_text(" ", strip=True))
    if "CANCELLED" in text.upper() or "SOLD OUT" in text.upper():
        return None

    title = ""
    date_label = soup.find(string=re.compile(r"\bDate:\b", re.IGNORECASE))
    if date_label is not None:
        heading = date_label.find_previous(["h1", "h2", "h3", "h4"])
        if heading is not None:
            title = _normalize_space(heading.get_text(" ", strip=True))
    if not title:
        for heading in soup.find_all(["h1", "h2", "h3", "h4"]):
            candidate = _normalize_space(heading.get_text(" ", strip=True))
            if not candidate:
                continue
            lowered = candidate.lower()
            if "support vqm" in lowered or "your cart" in lowered:
                continue
            title = candidate
            break
    if not title:
        return None

    date_match = re.search(r"Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", text, re.IGNORECASE)
    start_date = _parse_full_date(date_match.group(1)) if date_match else None
    if start_date is None:
        first_date = _first_match(DATE_WITH_YEAR_RE, text)
        if first_date:
            start_date = _parse_full_date(first_date)
    if start_date is None:
        return None

    start_time, end_time = _parse_time_range(text)
    description = _extract_description_from_detail(soup)

    return {
        "source_url": source_url,
        "title": title,
        "description": description,
        "signal_text": f"{title} {description or ''}",
        "start_at": _combine_date_time(start_date, start_time),
        "end_at": _combine_date_time(start_date, end_time) if end_time else None,
    }


def _build_listing_urls(venue: VaHtmlVenueConfig, *, page_limit: int) -> list[str]:
    if venue.slug != "icavcu":
        return [venue.list_url]
    urls = [venue.list_url]
    for page in range(2, page_limit + 1):
        urls.append(urljoin(venue.list_url, f"page/{page}/"))
    return urls


def _extract_listing_links(slug: str, listing_html: str, listing_url: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(listing_html, "html.parser")
    rows: list[tuple[str, str]] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = _normalize_space(anchor.get("href"))
        if not href:
            continue
        url = urljoin(listing_url, href)
        if url in seen:
            continue

        text = _normalize_space(anchor.get_text(" ", strip=True))
        lower_url = url.lower()
        if slug == "icavcu":
            if "/events/" not in lower_url:
                continue
            if "/events/page/" in lower_url:
                continue
            if lower_url.rstrip("/") == "https://icavcu.org/events":
                continue
        elif slug == "kluge_ruhe":
            if "/events/" not in lower_url:
                continue
            if not text.startswith("Upcoming Event"):
                continue
        elif slug == "muscarelle":
            if "/event/" not in lower_url:
                continue
        elif slug == "moca_arlington":
            if "/events/20" not in lower_url:
                continue
        elif slug == "piedmont_arts":
            if "calendar/event.cfm" not in lower_url:
                continue
        elif slug == "va_quilt":
            if "/events/" not in lower_url:
                continue
            if lower_url.rstrip("/") == "https://www.vaquiltmuseum.org/events":
                continue
        else:
            continue

        seen.add(url)
        rows.append((url, text))

    return rows


def _parse_full_date(value: str | None) -> date | None:
    if not value:
        return None
    cleaned = _normalize_space(
        value.replace(".", "")
        .replace(",", ", ")
        .replace("  ", " ")
        .replace("Sept ", "Sep ")
    )
    cleaned = re.sub(r"\s+,", ",", cleaned)
    cleaned = re.sub(
        r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def _parse_date_month_day(month: str, day: str, year: int) -> date | None:
    value = f"{month} {day}, {year}".replace(".", "")
    return _parse_full_date(value)


def _parse_time_range(value: str | None) -> tuple[time | None, time | None]:
    if not value:
        return None, None
    cleaned = _normalize_space(
        value.replace("A.M.", "AM")
        .replace("P.M.", "PM")
        .replace("a.m.", "am")
        .replace("p.m.", "pm")
        .replace("–", "-")
        .replace("—", "-")
    )
    match = TIME_RANGE_RE.search(cleaned)
    if match:
        end_time = _parse_time_value(match.group("end"))
        start_raw = match.group("start")
        if end_time and re.search(r"[APap]\.?M\.?", match.group("end")) and not re.search(r"[APap]\.?M\.?", start_raw):
            start_raw = f"{start_raw} {_extract_meridiem(match.group('end'))}"
        return _parse_time_value(start_raw), end_time

    single = re.findall(r"\d{1,2}:\d{2}\s*[APap]\.?M\.?", cleaned)
    if single:
        start = _parse_time_value(single[0])
        end = _parse_time_value(single[1]) if len(single) > 1 else None
        return start, end
    return None, None


def _parse_time_value(value: str | None) -> time | None:
    if not value:
        return None
    cleaned = (
        _normalize_space(value)
        .replace(".", "")
        .replace("AM", " AM")
        .replace("PM", " PM")
        .replace("  ", " ")
        .strip()
    )
    cleaned = cleaned.replace(" am", " AM").replace(" pm", " PM")
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            return datetime.strptime(cleaned, fmt).time()
        except ValueError:
            continue
    return None


def _combine_date_time(date_value: date, time_value: time | None) -> datetime:
    if time_value is None:
        return datetime.combine(date_value, time.min)
    return datetime.combine(date_value, time_value)


def _extract_year_from_url(url: str) -> int | None:
    match = re.search(r"/(20\d{2})/", url)
    if not match:
        return None
    return _parse_int(match.group(1))


def _extract_description_from_detail(soup: BeautifulSoup) -> str | None:
    paragraphs: list[str] = []
    for node in soup.select("article p, main p, .event-content p, .w-richtext p, p"):
        text = _normalize_space(node.get_text(" ", strip=True))
        if not text:
            continue
        lowered = text.lower()
        if lowered.startswith("skip to content"):
            continue
        if "your cart" in lowered:
            continue
        if "continue to checkout" in lowered:
            continue
        if "gala innovation studio" in lowered:
            continue
        if "menu menu visit exhibitions" in lowered:
            continue
        if text in paragraphs:
            continue
        paragraphs.append(text)
        if len(paragraphs) >= 3:
            break
    if not paragraphs:
        meta_desc = _normalize_space(_meta_content(soup, "description"))
        return meta_desc or None
    return " | ".join(paragraphs)


def _infer_activity_type(token_blob: str) -> str:
    if (
        " talk " in token_blob
        or " lecture " in token_blob
        or " conversation " in token_blob
        or " discussion " in token_blob
    ):
        return "lecture"
    return "workshop"


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        lo, hi = sorted((int(match.group(1)), int(match.group(2))))
        return lo, hi
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _meta_content(soup: BeautifulSoup, name: str) -> str:
    node = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"name": name})
    if node is None:
        return ""
    return _normalize_space(node.get("content"))


def _node_text(node) -> str:
    if node is None:
        return ""
    return _normalize_space(node.get_text(" ", strip=True))


def _first_match(pattern: re.Pattern, text: str) -> str:
    match = pattern.search(text)
    return _normalize_space(match.group(1)) if match else ""


def _first_non_empty(*values: str) -> str:
    for value in values:
        if value:
            return value
    return ""


def _extract_meridiem(value: str) -> str:
    match = re.search(r"([APap])\.?M\.?", value)
    if not match:
        return ""
    return f"{match.group(1).upper()}M"


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def get_va_html_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in VA_HTML_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)
