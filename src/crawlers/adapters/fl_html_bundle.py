import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
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

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " art adventures ",
    " art on the spectrum ",
    " art making ",
    " artist talk ",
    " autism art workshop ",
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " creative connections ",
    " discussion ",
    " discussions ",
    " educator workshop ",
    " educator workshops ",
    " family studio ",
    " family sundays ",
    " future forms ",
    " kids jamm ",
    " lecture ",
    " lectures ",
    " makers ",
    " mini makers ",
    " minimakers ",
    " open studio ",
    " panel ",
    " panels ",
    " professional learning ",
    " second saturday ",
    " sensorial ",
    " sensory friendly ",
    " sketch ",
    " sketching ",
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
ANYWHERE_REJECT_PATTERNS = (
    " book club ",
    " camp ",
    " camps ",
    " fundraiser ",
    " fundraising ",
    " sunday stories ",
    " story time ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
    " meditation ",
    " mindful ",
    " mindfulness ",
    " jazz ",
    " concert ",
    " orchestra ",
)
TITLE_REJECT_PATTERNS = (
    " member event ",
    " member offer ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " admission ",
    " exhibition ",
    " exhibitions ",
    " film ",
    " films ",
    " music ",
    " performance ",
    " performances ",
    " picnic ",
    " poetry ",
    " reception ",
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
    " ticket ",
    " tickets ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
DATE_WITH_YEAR_RE = re.compile(
    r"(?:Mon|Tue|Tues|Wed|Thu|Thur|Thurs|Fri|Sat|Sun)?\.?,?\s*"
    r"(?P<month>[A-Za-z]{3,9})\.?\s+"
    r"(?P<day>\d{1,2}),?\s+"
    r"(?P<year>\d{4})",
    re.IGNORECASE,
)
DATE_WITHOUT_YEAR_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"(?P<month>[A-Za-z]{3,9})\.?\s+"
    r"(?P<day>\d{1,2})",
    re.IGNORECASE,
)
MONTH_DAY_ONLY_RE = re.compile(r"(?P<month>[A-Za-z]{3,9})\.?\s+(?P<day>\d{1,2})", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>[APap]\.?M\.?)?"
    r"\s*(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>[APap]\.?M\.?)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?<!\d)(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?)(?![A-Za-z])",
    re.IGNORECASE,
)
ROLLINS_BLOCK_RE = re.compile(
    r"<h2[^>]*>\s*(?P<title>[^<]+?)\s*</h2>(?P<body>.*?)(?=(?:<h2[^>]*>)|(?:</section>))",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True, slots=True)
class FlHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_urls: tuple[str, ...]


FL_HTML_VENUES: tuple[FlHtmlVenueConfig, ...] = (
    FlHtmlVenueConfig(
        slug="ica_miami",
        source_name="ica_miami_events",
        venue_name="Institute of Contemporary Art, Miami",
        city="Miami",
        state="FL",
        list_urls=("https://icamiami.org/calendar/",),
    ),
    FlHtmlVenueConfig(
        slug="moca_nomi",
        source_name="moca_nomi_events",
        venue_name="Museum of Contemporary Art, North Miami",
        city="North Miami",
        state="FL",
        list_urls=("https://www.mocanomi.org/events",),
    ),
    FlHtmlVenueConfig(
        slug="norton",
        source_name="norton_events",
        venue_name="Norton Museum of Art",
        city="West Palm Beach",
        state="FL",
        list_urls=(
            "https://www.norton.org/programs/family_programs",
            "https://www.norton.org/programs/art-education",
            "https://www.norton.org/programs/speakers",
        ),
    ),
    FlHtmlVenueConfig(
        slug="pamm",
        source_name="pamm_events",
        venue_name="Pérez Art Museum Miami",
        city="Miami",
        state="FL",
        list_urls=("https://www.pamm.org/en/events/",),
    ),
    FlHtmlVenueConfig(
        slug="rollins",
        source_name="rollins_events",
        venue_name="Rollins Museum of Art",
        city="Winter Park",
        state="FL",
        list_urls=(
            "https://www.rollins.edu/rma/learn/families/",
            "https://www.rollins.edu/rma/learn/lectures-tours/",
        ),
    ),
)

FL_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in FL_HTML_VENUES}


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


async def load_fl_html_bundle_payload(
    *,
    venue_slugs: list[str] | None = None,
    max_links_per_venue: int = 30,
) -> dict[str, dict]:
    selected = (
        [FL_HTML_VENUES_BY_SLUG[slug] for slug in venue_slugs]
        if venue_slugs
        else list(FL_HTML_VENUES)
    )
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            listing_pages: list[dict] = []
            for list_url in venue.list_urls:
                html = await fetch_html(list_url, client=client)
                listing_pages.append({"url": list_url, "html": html})

            detail_items: list[dict] = []
            if venue.slug in {"ica_miami", "moca_nomi", "pamm"}:
                link_candidates = _extract_detail_links(venue.slug, listing_pages)
                for link_url, list_text in link_candidates[:max_links_per_venue]:
                    try:
                        detail_html = await fetch_html(link_url, client=client)
                    except Exception:
                        continue
                    detail_items.append({"url": link_url, "list_text": list_text, "html": detail_html})

            payload[venue.slug] = {
                "listing_pages": listing_pages,
                "detail_items": detail_items,
            }

    return payload


def parse_fl_html_events(payload: dict, *, venue: FlHtmlVenueConfig) -> list[ExtractedActivity]:
    if venue.slug == "ica_miami":
        rows = _parse_ica_events(payload, venue=venue)
    elif venue.slug == "moca_nomi":
        rows = _parse_moca_events(payload, venue=venue)
    elif venue.slug == "norton":
        rows = _parse_norton_events(payload, venue=venue)
    elif venue.slug == "pamm":
        rows = _parse_pamm_events(payload, venue=venue)
    elif venue.slug == "rollins":
        rows = _parse_rollins_events(payload, venue=venue)
    else:
        rows = []

    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    deduped: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for row in rows:
        if row.start_at.date() < current_date:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    deduped.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return deduped


class FlHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "fl_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_fl_html_bundle_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_fl_html_bundle_payload/parse_fl_html_events from script runner.")


def _extract_detail_links(venue_slug: str, listing_pages: list[dict]) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    seen: set[str] = set()

    for page in listing_pages:
        list_url = page["url"]
        html = page["html"]
        soup = BeautifulSoup(html, "html.parser")

        if venue_slug == "ica_miami":
            for anchor in soup.find_all("a", href=True):
                href = (anchor.get("href") or "").strip()
                if "/calendar/" not in href or "/page/" in href:
                    continue
                absolute = urljoin(list_url, href)
                parsed = urlparse(absolute)
                if parsed.query or parsed.fragment:
                    continue
                if not re.search(r"/calendar/[^/]+/?$", parsed.path):
                    continue
                if absolute.rstrip("/") == list_url.rstrip("/"):
                    continue
                if absolute in seen:
                    continue
                seen.add(absolute)
                links.append((absolute, _normalize_space(anchor.get_text(" ", strip=True))))
        elif venue_slug == "moca_nomi":
            for anchor in soup.select(".collection-item-2 a[href], .collection-item-2 .h3-post-title_plain[href]"):
                href = (anchor.get("href") or "").strip()
                if not href.startswith("/posts/"):
                    continue
                absolute = urljoin(list_url, href)
                if absolute in seen:
                    continue
                seen.add(absolute)
                links.append((absolute, _normalize_space(anchor.get_text(" ", strip=True))))
        elif venue_slug == "pamm":
            for anchor in soup.find_all("a", href=True):
                href = (anchor.get("href") or "").strip()
                if "/en/events/event/" not in href:
                    continue
                absolute = urljoin(list_url, href)
                if absolute in seen:
                    continue
                seen.add(absolute)
                links.append((absolute, _normalize_space(anchor.get_text(" ", strip=True))))

    return links


def _parse_ica_events(payload: dict, *, venue: FlHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for item in payload.get("detail_items") or []:
        lines = _lines_from_html(item["html"])
        title = _clean_suffix(_title_from_html(item["html"]), " - Institute of Contemporary Art, Miami")
        if title == "Calendar Archive":
            continue
        if not title:
            title = _first_matching_line(lines, lambda line: "ICA Miami" in line)
        title_matches = [index for index, line in enumerate(lines) if title and line == title]
        title_index = title_matches[-1] if title_matches else 0
        date_index, date_obj = _find_date_line_after(lines, start_index=title_index)
        if date_obj is None:
            continue
        time_text = lines[date_index + 1] if date_index >= 0 and date_index + 1 < len(lines) else ""
        time_slots = _parse_time_slots(time_text) or [(_midnight(), None, time_text)]
        description = _collect_description(
            lines,
            start_index=date_index + 2,
            stop_patterns=("Please contact", "Reserve Tickets", "Free Tickets", "Timed Tickets", "First Name", "Last Name"),
        )
        signal_text = f"{title} {description or ''}"
        row_template = _build_row_template(
            source_url=item["url"],
            title=title,
            description=description,
            venue=venue,
            signal_text=signal_text,
        )
        if row_template is None:
            continue
        for start_time, end_time, raw_time in time_slots:
            rows.append(
                _hydrate_row(
                    row_template,
                    start_at=_combine_date_time(date_obj, start_time),
                    end_at=_combine_date_time(date_obj, end_time) if end_time else None,
                    extra_description=f"Time: {raw_time}" if raw_time and raw_time not in (description or "") else None,
                )
            )
    return rows


def _parse_moca_events(payload: dict, *, venue: FlHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for item in payload.get("detail_items") or []:
        lines = _lines_from_html(item["html"])
        title = _clean_suffix(_title_from_html(item["html"]), " - Museum of Contemporary Art North Miami")
        title_matches = [index for index, line in enumerate(lines) if title and line == title]
        title_index = title_matches[-1] if title_matches else 0
        date_index, date_obj = _find_date_line_after(lines, start_index=title_index)
        if date_obj is None:
            continue
        time_text = lines[date_index + 1] if date_index >= 0 and date_index + 1 < len(lines) else ""
        start_end = _parse_time_slots(time_text) or [(_midnight(), None, time_text)]
        description = _collect_description(
            lines,
            start_index=date_index + 2,
            stop_patterns=("North Miami, FL 33161", "Monday:", "Tuesday:", "Wednesday:", "Thursday:", "Friday:", "Saturday:", "Sunday:"),
            max_lines=3,
        )
        signal_text = " ".join(part for part in [title, description or "", item.get("list_text") or ""] if part)
        row_template = _build_row_template(
            source_url=item["url"],
            title=title,
            description=description,
            venue=venue,
            signal_text=signal_text,
        )
        if row_template is None:
            continue
        for start_time, end_time, raw_time in start_end:
            rows.append(
                _hydrate_row(
                    row_template,
                    start_at=_combine_date_time(date_obj, start_time),
                    end_at=_combine_date_time(date_obj, end_time) if end_time else None,
                    extra_description=f"Time: {raw_time}" if raw_time and raw_time not in (description or "") else None,
                )
            )
    return rows


def _parse_norton_events(payload: dict, *, venue: FlHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    current_year = datetime.now(ZoneInfo(NY_TIMEZONE)).year

    for page in payload.get("listing_pages") or []:
        page_url = page["url"]
        page_html = page["html"]
        section_label = _norton_section_label(page_url)
        soup = BeautifulSoup(page_html, "html.parser")
        for anchor in soup.find_all("a", href=True):
            href = urljoin(page_url, (anchor.get("href") or "").strip())
            if "/events/" not in href:
                continue
            text = _normalize_space(anchor.get_text(" ", strip=True))
            if not text or "Learn More" not in text:
                continue
            cleaned = text.rsplit(" Learn More", 1)[0].strip()
            date_match = re.search(
                r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
                r"(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2})(?:,\s*(?P<year>\d{4}))?,?\s*"
                r"(?P<times>.+)$",
                cleaned,
                re.IGNORECASE,
            )
            if date_match is None:
                continue
            title = cleaned[:date_match.start()].strip(" -|/")
            year = int(date_match.group("year") or current_year)
            date_obj = _parse_month_day_year(
                date_match.group("month"),
                int(date_match.group("day")),
                year,
            )
            if date_obj is None:
                continue
            time_slots = _parse_time_slots(date_match.group("times")) or [(_midnight(), None, date_match.group("times"))]
            description = f"Section: {section_label}"
            signal_text = f"{title} {description}"
            row_template = _build_row_template(
                source_url=href,
                title=title,
                description=description,
                venue=venue,
                signal_text=signal_text,
            )
            if row_template is None:
                continue
            for start_time, end_time, raw_time in time_slots:
                rows.append(
                    _hydrate_row(
                        row_template,
                        start_at=_combine_date_time(date_obj, start_time),
                        end_at=_combine_date_time(date_obj, end_time) if end_time else None,
                        extra_description=f"Time: {raw_time}",
                    )
                )
    return rows


def _parse_pamm_events(payload: dict, *, venue: FlHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    for item in payload.get("detail_items") or []:
        lines = _lines_from_html(item["html"])
        title = _clean_suffix(_title_from_html(item["html"]), " • Pérez Art Museum Miami")
        date_index, date_obj = _find_date_line(lines)
        if date_obj is None:
            continue
        time_text = lines[date_index + 1] if date_index >= 0 and date_index + 1 < len(lines) else ""
        time_slots = _parse_time_slots(time_text) or [(_midnight(), None, time_text)]
        description = _collect_description(
            lines,
            start_index=date_index + 2,
            stop_patterns=("PAMM Portraits Blog", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday-Sunday", "1103 Biscayne Blvd.", "Instagram"),
            max_lines=6,
        )
        signal_text = " ".join(part for part in [item.get("list_text") or "", title, description or ""] if part)
        row_template = _build_row_template(
            source_url=item["url"],
            title=title,
            description=description,
            venue=venue,
            signal_text=signal_text,
        )
        if row_template is None:
            continue
        for start_time, end_time, raw_time in time_slots:
            rows.append(
                _hydrate_row(
                    row_template,
                    start_at=_combine_date_time(date_obj, start_time),
                    end_at=_combine_date_time(date_obj, end_time) if end_time else None,
                    extra_description=f"Time: {raw_time}" if raw_time and raw_time not in (description or "") else None,
                )
            )
    return rows


def _parse_rollins_events(payload: dict, *, venue: FlHtmlVenueConfig) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    current_year = datetime.now(ZoneInfo(NY_TIMEZONE)).year

    for page in payload.get("listing_pages") or []:
        page_url = page["url"]
        html = page["html"]
        for match in ROLLINS_BLOCK_RE.finditer(html):
            title = _normalize_space(BeautifulSoup(match.group("title"), "html.parser").get_text(" ", strip=True))
            if not title:
                continue
            body_text = BeautifulSoup(match.group("body"), "html.parser").get_text("\n", strip=True)
            body_lines = [line.strip() for line in body_text.splitlines() if line.strip()]
            description = _collect_rollins_description(body_lines)
            signal_text = " ".join([title, description or ""])
            row_template = _build_row_template(
                source_url=f"{page_url}#{_slugify(title)}",
                title=title,
                description=description,
                venue=venue,
                signal_text=signal_text,
            )
            if row_template is None:
                continue

            for line in body_lines:
                if "|" not in line:
                    continue
                date_obj, raw_times = _parse_rollins_date_time_line(line, current_year=current_year)
                if date_obj is None:
                    continue
                time_slots = _parse_time_slots(raw_times) or [(_midnight(), None, raw_times)]
                for start_time, end_time, raw_time in time_slots:
                    rows.append(
                        _hydrate_row(
                            row_template,
                            start_at=_combine_date_time(date_obj, start_time),
                            end_at=_combine_date_time(date_obj, end_time) if end_time else None,
                            extra_description=f"Time: {raw_time}" if raw_time else None,
                        )
                    )
    return rows


def _build_row_template(
    *,
    source_url: str,
    title: str,
    description: str | None,
    venue: FlHtmlVenueConfig,
    signal_text: str,
) -> ExtractedActivity | None:
    title_blob = _searchable_blob(title)
    token_blob = _searchable_blob(signal_text)
    if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
        return None

    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description or ""] if part))
    is_free, free_status = infer_price_classification(description)
    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=venue.venue_name,
        location_text=f"{venue.city}, {venue.state}",
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(token_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=any(pattern in token_blob for pattern in REGISTRATION_PATTERNS),
        start_at=datetime.combine(date(2000, 1, 1), time(0, 0)),
        end_at=None,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _hydrate_row(
    template: ExtractedActivity,
    *,
    start_at: datetime,
    end_at: datetime | None,
    extra_description: str | None = None,
) -> ExtractedActivity:
    description = template.description
    if extra_description:
        description = " | ".join(part for part in [description, extra_description] if part)
    return ExtractedActivity(
        source_url=template.source_url,
        title=template.title,
        description=description,
        venue_name=template.venue_name,
        location_text=template.location_text,
        city=template.city,
        state=template.state,
        activity_type=template.activity_type,
        age_min=template.age_min,
        age_max=template.age_max,
        drop_in=template.drop_in,
        registration_required=template.registration_required,
        start_at=start_at,
        end_at=end_at,
        timezone=template.timezone,
        is_free=template.is_free,
        free_verification_status=template.free_verification_status,
    )


def _should_keep_event(*, title_blob: str, token_blob: str) -> bool:
    strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    weak_include = strong_include or any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS)
    if not weak_include:
        return False

    if any(pattern in token_blob for pattern in ANYWHERE_REJECT_PATTERNS):
        return False
    if any(pattern in title_blob for pattern in TITLE_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_include:
        return False

    return True


def _infer_activity_type(token_blob: str) -> str:
    if any(pattern in token_blob for pattern in (" lecture ", " lectures ", " talk ", " talks ", " conversation ", " conversations ", " discussion ", " panel ", " panels ")):
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


def _lines_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    return [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]


def _title_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return soup.title.get_text(" ", strip=True) if soup.title else ""


def _find_date_line(lines: list[str]) -> tuple[int, date | None]:
    current_year = datetime.now(ZoneInfo(NY_TIMEZONE)).year
    for index, line in enumerate(lines):
        date_obj = _parse_date_text(line, default_year=current_year)
        if date_obj is not None:
            return index, date_obj
    return -1, None


def _find_date_line_after(lines: list[str], *, start_index: int) -> tuple[int, date | None]:
    current_year = datetime.now(ZoneInfo(NY_TIMEZONE)).year
    for index in range(max(start_index + 1, 0), len(lines)):
        date_obj = _parse_date_text(lines[index], default_year=current_year)
        if date_obj is not None:
            return index, date_obj
    return -1, None


def _parse_date_text(text: str, *, default_year: int) -> date | None:
    cleaned = _normalize_space(text).replace("â", "-")
    match = DATE_WITH_YEAR_RE.search(cleaned)
    if match:
        return _parse_month_day_year(match.group("month"), int(match.group("day")), int(match.group("year")))

    match = DATE_WITHOUT_YEAR_RE.search(cleaned)
    if match:
        return _parse_month_day_year(match.group("month"), int(match.group("day")), default_year)

    match = MONTH_DAY_ONLY_RE.search(cleaned)
    if match and "|" not in cleaned:
        return _parse_month_day_year(match.group("month"), int(match.group("day")), default_year)

    return None


def _parse_month_day_year(month_text: str, day: int, year: int) -> date | None:
    for fmt in ("%b %d %Y", "%B %d %Y"):
        try:
            return datetime.strptime(f"{month_text} {day} {year}", fmt).date()
        except ValueError:
            continue
    return None


def _parse_time_slots(text: str) -> list[tuple[time, time | None, str]]:
    cleaned = _normalize_space(text).replace("â", "-").replace("–", "-").replace("—", "-")
    if not cleaned:
        return []

    segments = [segment.strip(" ,") for segment in re.split(r"\s+or\s+", cleaned, flags=re.IGNORECASE) if segment.strip(" ,")]
    slots: list[tuple[time, time | None, str]] = []
    for segment in segments:
        match = TIME_RANGE_RE.search(segment)
        if match:
            end_meridiem = match.group("end_meridiem")
            start_meridiem = match.group("start_meridiem") or end_meridiem
            start = _to_time(match.group("start_hour"), match.group("start_minute"), start_meridiem)
            end = _to_time(match.group("end_hour"), match.group("end_minute"), end_meridiem)
            if start is not None:
                slots.append((start, end, segment))
            continue

        single_match = TIME_SINGLE_RE.search(segment)
        if single_match:
            single = _to_time(single_match.group("hour"), single_match.group("minute"), single_match.group("meridiem"))
            if single is not None:
                slots.append((single, None, segment))

    return slots


def _to_time(hour_text: str, minute_text: str | None, meridiem_text: str | None) -> time | None:
    if not meridiem_text:
        return None
    hour = int(hour_text)
    minute = int(minute_text or "0")
    meridiem = meridiem_text.lower().replace(".", "")
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    if hour >= 24 or minute >= 60:
        return None
    return time(hour=hour, minute=minute)


def _combine_date_time(value_date: date, value_time: time) -> datetime:
    return datetime.combine(value_date, value_time)


def _midnight() -> time:
    return time(0, 0)


def _collect_description(
    lines: list[str],
    *,
    start_index: int,
    stop_patterns: tuple[str, ...],
    max_lines: int = 5,
) -> str | None:
    description_lines: list[str] = []
    for line in lines[start_index:]:
        if any(line.startswith(pattern) for pattern in stop_patterns):
            break
        if len(line) <= 2:
            continue
        if description_lines and line == description_lines[-1]:
            continue
        description_lines.append(line)
        if len(description_lines) >= max_lines:
            break
    if not description_lines:
        return None
    return " ".join(description_lines)


def _collect_rollins_description(lines: list[str]) -> str | None:
    description_parts: list[str] = []
    for line in lines:
        if "|" in line:
            continue
        if line.startswith("Register") or line.startswith("Learn More"):
            continue
        if re.search(r"^\d", line):
            continue
        if len(line) < 8:
            continue
        description_parts.append(line)
        if len(description_parts) >= 4:
            break
    return " ".join(description_parts) if description_parts else None


def _parse_rollins_date_time_line(line: str, *, current_year: int) -> tuple[date | None, str]:
    cleaned = _normalize_space(line).replace("â", "-").replace("–", "-")
    parts = [part.strip() for part in cleaned.split("|") if part.strip()]
    if len(parts) < 2:
        return None, ""
    date_obj = _parse_date_text(parts[0], default_year=current_year)
    if date_obj is None:
        return None, ""
    return date_obj, parts[1]


def _norton_section_label(page_url: str) -> str:
    if page_url.endswith("family_programs"):
        return "Family Programs"
    if page_url.endswith("art-education"):
        return "Art Classes and Workshops"
    if page_url.endswith("speakers"):
        return "Lectures and Conversations"
    return "Programs"


def _first_matching_line(lines: list[str], predicate) -> str:
    for line in lines:
        if predicate(line):
            return line
    return ""


def _clean_suffix(text: str, suffix: str) -> str:
    if text.endswith(suffix):
        return text[: -len(suffix)].strip()
    return text.strip()


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return normalized or "event"


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def get_fl_html_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in FL_HTML_VENUES:
        for url in venue.list_urls:
            parsed = urlparse(url)
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(dict.fromkeys(prefixes))
