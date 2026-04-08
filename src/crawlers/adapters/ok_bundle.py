from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

OK_TIMEZONE = "America/Chicago"
OK_TZ = ZoneInfo(OK_TIMEZONE)
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

OK_INCLUDE_PATTERNS = (
    " activity ",
    " activities ",
    " babies at the museum ",
    " ceramics ",
    " class ",
    " classes ",
    " collage ",
    " conversation ",
    " conversations ",
    " creative ",
    " discussion ",
    " discussions ",
    " draw ",
    " drawing ",
    " drop in art ",
    " drop-in art ",
    " family ",
    " families ",
    " fused glass ",
    " hands on ",
    " hands-on ",
    " lecture ",
    " lectures ",
    " monoprint ",
    " monoprints ",
    " mosaic ",
    " mosaics ",
    " paint ",
    " painting ",
    " playdate ",
    " printmaking ",
    " speaker series ",
    " studio ",
    " studio school ",
    " talk ",
    " talks ",
    " teen ",
    " teens ",
    " workshop ",
    " workshops ",
    " youth ",
)
OK_OVERRIDE_PATTERNS = (
    " babies at the museum ",
    " ceramics ",
    " class ",
    " classes ",
    " conversation ",
    " drawing ",
    " drop in art ",
    " drop-in art ",
    " family ",
    " families ",
    " fused glass ",
    " monoprint ",
    " monoprints ",
    " mosaic ",
    " mosaics ",
    " paint ",
    " painting ",
    " playdate ",
    " printmaking ",
    " speaker series ",
    " studio ",
    " workshop ",
    " workshops ",
)
OK_REJECT_PATTERNS = (
    " bbq ",
    " cocktail ",
    " cocktails ",
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
    " music ",
    " performance ",
    " performances ",
    " reception ",
    " screening ",
    " screenings ",
    " social ",
    " sold out ",
    " tour ",
    " tours ",
    " vinyl ",
    " wellness ",
    " writing ",
    " yoga ",
)
OK_HARD_REJECT_PATTERNS = (
    " writing across genres ",
)

GILCREASE_ICC_TITLE = "Indigenous Collections Care (ICC) Speaker Series"
GILCREASE_UNCREASE_TITLE = "Uncrease"
GILCREASE_OCCURRENCE_RE = re.compile(
    r"(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\s*,\s*"
    r"(?P<time>\d{1,2}:\d{2}\s*[APap][.]?M[.]?)\s*,\s*"
    r"(?P<subtitle>.*?)\s*,\s*Register",
    re.IGNORECASE,
)
GILCREASE_UNCREASE_OCCURRENCE_RE = re.compile(
    r"(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\s*,\s*"
    r"(?P<time>\d{1,2}:\d{2}\s*[APap][.]?M[.]?)\s*,\s*,\s*Register",
    re.IGNORECASE,
)
MGMOA_CLASS_RE = re.compile(
    r"2026 Spring Class Schedule\s+"
    r"(?P<title>After School Art Ceramic Class)\s+"
    r"(?P<body>.*?)"
    r"Week 1 \|\s+(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}):.*?"
    r"Times:\s+(?P<time>.*?)\s+"
    r"Cost:\s+\$(?P<cost>\d+(?:\.\d{2})?)",
    re.IGNORECASE | re.DOTALL,
)
OKCMOA_DATE_TIME_RE = re.compile(
    r"(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\s+"
    r"(?P<start>\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?|am|pm))"
    r"(?:\s*-\s*(?P<end>\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?|am|pm)))?",
    re.IGNORECASE,
)
OKCONTEMP_DATE_RE = re.compile(
    r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})",
    re.IGNORECASE,
)
OKCONTEMP_TIME_RE = re.compile(
    r"\|\s*(?P<time>\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?|am|pm)\s*-\s*"
    r"\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
OKCONTEMP_COST_RE = re.compile(r"Cost\s+\$(?P<cost>\d+(?:\.\d{2})?)", re.IGNORECASE)
OKCONTEMP_AGE_RE = re.compile(r"Age Requirements\s+(?P<age>[^$]+?)\s+Cost", re.IGNORECASE)
OKCONTEMP_TIME_RANGE_RE = re.compile(
    r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?\s*-\s*"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
PHILBROOK_LINK_DATE_RE = re.compile(r"-(?P<date>\d{4}-\d{2}-\d{2})/?$")


@dataclass(frozen=True, slots=True)
class OkVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    mode: str
    source_prefixes: tuple[str, ...]
    detail_url: str | None = None


OK_VENUES: tuple[OkVenueConfig, ...] = (
    OkVenueConfig(
        slug="gilcrease",
        source_name="ok_gilcrease_events",
        venue_name="Gilcrease Museum",
        city="Tulsa",
        state="OK",
        list_url="https://my.gilcrease.org/events?view=list&span=month&k=public%20events",
        default_location="Gilcrease Museum, Tulsa, OK",
        mode="gilcrease_playwright",
        source_prefixes=("https://my.gilcrease.org/events",),
    ),
    OkVenueConfig(
        slug="mgmoa",
        source_name="ok_mgmoa_events",
        venue_name="Mabee-Gerrer Museum of Art",
        city="Shawnee",
        state="OK",
        list_url="https://www.mgmoa.org/calendar/",
        default_location="Mabee-Gerrer Museum of Art, Shawnee, OK",
        mode="mgmoa_html",
        source_prefixes=(
            "https://www.mgmoa.org/calendar/",
            "https://www.mgmoa.org/asa-class/",
        ),
        detail_url="https://www.mgmoa.org/asa-class/",
    ),
    OkVenueConfig(
        slug="okcmoa",
        source_name="ok_okcmoa_events",
        venue_name="Oklahoma City Museum of Art",
        city="Oklahoma City",
        state="OK",
        list_url="https://www.okcmoa.com/events/",
        default_location="Oklahoma City Museum of Art, Oklahoma City, OK",
        mode="okcmoa_html",
        source_prefixes=(
            "https://www.okcmoa.com/events/",
            "https://www.okcmoa.com/visit/events/",
        ),
    ),
    OkVenueConfig(
        slug="okcontemporary",
        source_name="ok_oklahoma_contemporary_events",
        venue_name="Oklahoma Contemporary",
        city="Oklahoma City",
        state="OK",
        list_url="https://oklahomacontemporary.org/events/calendar/",
        default_location="Oklahoma Contemporary, Oklahoma City, OK",
        mode="okcontemporary_html",
        source_prefixes=(
            "https://oklahomacontemporary.org/events/calendar/",
            "https://oklahomacontemporary.org/course/",
            "https://oklahomacontemporary.org/event/",
        ),
    ),
)

OK_VENUES_BY_SLUG = {venue.slug: venue for venue in OK_VENUES}


def get_ok_source_prefixes() -> tuple[str, ...]:
    return tuple(
        prefix
        for venue in OK_VENUES
        for prefix in venue.source_prefixes
    )


async def fetch_playwright_body_text(url: str, *, timeout_ms: int = 90000) -> str:
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
            timezone_id=OK_TIMEZONE,
        )
        await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
        await page.wait_for_timeout(5000)
        body_text = await page.locator("body").inner_text()
        await browser.close()
        return body_text


async def load_ok_bundle_payload(
    *,
    venues: list[OkVenueConfig] | tuple[OkVenueConfig, ...] | None = None,
) -> dict[str, dict[str, object]]:
    payload_by_slug: dict[str, dict[str, object]] = {}
    errors_by_slug: dict[str, str] = {}
    venues_to_load = list(venues) if venues is not None else list(OK_VENUES)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in venues_to_load:
            try:
                if venue.mode == "gilcrease_playwright":
                    payload_by_slug[venue.slug] = {
                        "body_text": await fetch_playwright_body_text(venue.list_url),
                    }
                    continue

                if venue.mode == "mgmoa_html":
                    detail_url = venue.detail_url or venue.list_url
                    payload_by_slug[venue.slug] = {
                        "html": await fetch_html(detail_url, referer=venue.list_url, client=client),
                    }
                    continue

                if venue.mode == "okcmoa_html":
                    list_html = await fetch_html(venue.list_url, client=client)
                    detail_urls = _extract_okcmoa_detail_urls(list_html, venue.list_url)
                    detail_html_by_url: dict[str, str] = {}
                    for detail_url in detail_urls:
                        detail_html_by_url[detail_url] = await fetch_html(detail_url, referer=venue.list_url, client=client)
                    payload_by_slug[venue.slug] = {
                        "list_html": list_html,
                        "detail_html_by_url": detail_html_by_url,
                    }
                    continue

                if venue.mode == "okcontemporary_html":
                    list_html = await fetch_html(venue.list_url, client=client)
                    occurrences = _extract_okcontemporary_occurrences(list_html, venue.list_url)
                    detail_html_by_url: dict[str, str] = {}
                    for detail_url in sorted({item["source_url"] for item in occurrences}):
                        detail_html_by_url[detail_url] = await fetch_html(detail_url, referer=venue.list_url, client=client)
                    payload_by_slug[venue.slug] = {
                        "list_html": list_html,
                        "occurrences": occurrences,
                        "detail_html_by_url": detail_html_by_url,
                    }
                    continue

                raise RuntimeError(f"Unsupported OK venue mode: {venue.mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)
                print(f"[ok-bundle] venue={venue.slug} failed: {exc}")

    return {"payload_by_slug": payload_by_slug, "errors_by_slug": errors_by_slug}


def parse_ok_events(
    payload: dict[str, object],
    *,
    venue: OkVenueConfig,
    start_date: str | None = None,
) -> list[ExtractedActivity]:
    current_date = _resolve_start_date(start_date)

    if venue.mode == "gilcrease_playwright":
        return _parse_gilcrease(payload, venue=venue, current_date=current_date)
    if venue.mode == "mgmoa_html":
        return _parse_mgmoa(payload, venue=venue, current_date=current_date)
    if venue.mode == "okcmoa_html":
        return _parse_okcmoa(payload, venue=venue, current_date=current_date)
    if venue.mode == "okcontemporary_html":
        return _parse_okcontemporary(payload, venue=venue, current_date=current_date)
    raise RuntimeError(f"Unsupported OK venue mode: {venue.mode}")


class OkBundleAdapter(BaseSourceAdapter):
    source_name = "ok_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_ok_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_ok_bundle_payload/parse_ok_events from the script runner.")


def _parse_gilcrease(
    payload: dict[str, object],
    *,
    venue: OkVenueConfig,
    current_date: date,
) -> list[ExtractedActivity]:
    body_text = normalize_space(str(payload.get("body_text") or ""))
    if not body_text:
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    icc_section = _slice_text(body_text, GILCREASE_ICC_TITLE, GILCREASE_UNCREASE_TITLE)
    for match in GILCREASE_OCCURRENCE_RE.finditer(icc_section):
        start_date_text = match.group("date")
        base_date = parse_date_text(start_date_text)
        if base_date is None or base_date < current_date:
            continue
        start_at, end_at = parse_time_range(base_date=base_date, time_text=match.group("time"))
        if start_at is None:
            continue
        subtitle = normalize_space(match.group("subtitle"))
        title = f"{GILCREASE_ICC_TITLE}: {subtitle}" if subtitle else GILCREASE_ICC_TITLE
        description = subtitle or GILCREASE_ICC_TITLE
        key = (venue.list_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            ExtractedActivity(
                source_url=venue.list_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type="talk",
                age_min=None,
                age_max=None,
                drop_in=False,
                registration_required=True,
                start_at=start_at,
                end_at=end_at,
                timezone=OK_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=True),
            )
        )

    uncrease_section = _slice_text(body_text, GILCREASE_UNCREASE_TITLE, "Thomas Gilcrease Institute of American History and Art")
    description = _slice_text(
        uncrease_section,
        GILCREASE_UNCREASE_TITLE,
        "April 11, 2026",
    ).replace(GILCREASE_UNCREASE_TITLE, "", 1)
    description = normalize_space(description)
    for match in GILCREASE_UNCREASE_OCCURRENCE_RE.finditer(uncrease_section):
        base_date = parse_date_text(match.group("date"))
        if base_date is None or base_date < current_date:
            continue
        start_at, end_at = parse_time_range(base_date=base_date, time_text=match.group("time"))
        if start_at is None:
            continue
        key = (venue.list_url, GILCREASE_UNCREASE_TITLE, start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            ExtractedActivity(
                source_url=venue.list_url,
                title=GILCREASE_UNCREASE_TITLE,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type="activity",
                age_min=None,
                age_max=None,
                drop_in=False,
                registration_required=True,
                start_at=start_at,
                end_at=end_at,
                timezone=OK_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=True),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_mgmoa(
    payload: dict[str, object],
    *,
    venue: OkVenueConfig,
    current_date: date,
) -> list[ExtractedActivity]:
    html = str(payload.get("html") or "")
    text = normalize_space(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
    match = MGMOA_CLASS_RE.search(text)
    if not match:
        return []

    event_date = parse_date_text(f"{match.group('month')} {match.group('day')}, 2026")
    if event_date is None or event_date < current_date:
        return []
    start_at, end_at = parse_time_range(base_date=event_date, time_text=match.group("time"))
    if start_at is None:
        return []

    title = normalize_space(match.group("title"))
    description = normalize_space(match.group("body"))
    age_min, age_max = parse_age_range(description)
    price_text = f"${match.group('cost')}"

    return [
        ExtractedActivity(
            source_url=venue.detail_url or venue.list_url,
            title=title,
            description=description,
            venue_name=venue.venue_name,
            location_text=venue.default_location,
            city=venue.city,
            state=venue.state,
            activity_type="class",
            age_min=age_min,
            age_max=age_max,
            drop_in=False,
            registration_required=True,
            start_at=start_at,
            end_at=end_at,
            timezone=OK_TIMEZONE,
            **price_classification_kwargs(f"{price_text} {description}", default_is_free=None),
        )
    ]


def _parse_okcmoa(
    payload: dict[str, object],
    *,
    venue: OkVenueConfig,
    current_date: date,
) -> list[ExtractedActivity]:
    detail_html_by_url = payload.get("detail_html_by_url") or {}
    if not isinstance(detail_html_by_url, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for source_url, html in detail_html_by_url.items():
        soup = BeautifulSoup(str(html), "html.parser")
        text = normalize_space(soup.get_text(" ", strip=True))
        title = _extract_okcmoa_title(soup)
        if not title:
            continue
        if not _ok_should_include(title=title, description=text):
            continue

        match = OKCMOA_DATE_TIME_RE.search(text)
        if not match:
            continue
        base_date = parse_date_text(match.group("date"))
        if base_date is None or base_date < current_date:
            continue
        time_text = match.group("start")
        if match.group("end"):
            time_text = f"{time_text}-{match.group('end')}"
        start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
        if start_at is None:
            continue

        description = _extract_okcmoa_description(soup)
        age_min, age_max = parse_age_range(text)
        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=infer_activity_type(title, description),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in title.lower() or "drop in" in title.lower()),
                registration_required=("register now" in text.lower() or "buy tickets" in text.lower()),
                start_at=start_at,
                end_at=end_at,
                timezone=OK_TIMEZONE,
                **price_classification_kwargs(text, default_is_free=None),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _parse_okcontemporary(
    payload: dict[str, object],
    *,
    venue: OkVenueConfig,
    current_date: date,
) -> list[ExtractedActivity]:
    occurrences = payload.get("occurrences") or []
    detail_html_by_url = payload.get("detail_html_by_url") or {}
    if not isinstance(occurrences, list) or not isinstance(detail_html_by_url, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for occurrence in occurrences:
        if not isinstance(occurrence, dict):
            continue
        title = normalize_space(str(occurrence.get("title") or ""))
        source_url = normalize_space(str(occurrence.get("source_url") or ""))
        date_text = normalize_space(str(occurrence.get("date_text") or ""))
        if not title or not source_url or not date_text:
            continue
        base_date = parse_date_text(date_text)
        if base_date is None or base_date < current_date:
            continue

        detail_html = str(detail_html_by_url.get(source_url) or "")
        detail_soup = BeautifulSoup(detail_html, "html.parser")
        detail_text = normalize_space(detail_soup.get_text(" ", strip=True))
        if not _ok_should_include(title=title, description=detail_text):
            continue
        time_text = _extract_okcontemporary_time(detail_soup)
        start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
        if start_at is None:
            continue
        description = _extract_okcontemporary_description(detail_soup)
        age_min, age_max = parse_age_range(_extract_okcontemporary_age_text(detail_soup))
        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type="class",
                age_min=age_min,
                age_max=age_max,
                drop_in=False,
                registration_required=True,
                start_at=start_at,
                end_at=end_at,
                timezone=OK_TIMEZONE,
                **price_classification_kwargs(detail_text, default_is_free=None),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _resolve_start_date(start_date: str | None) -> date:
    if start_date:
        parsed = parse_date_text(start_date)
        if parsed is not None:
            return parsed
    return datetime.now(OK_TZ).date()


def _extract_okcmoa_detail_urls(list_html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for article in soup.select("article.ae-post-item"):
        text = normalize_space(article.get_text(" ", strip=True))
        if not _ok_should_include(title=text, description=text):
            continue
        title_link = article.select_one("a[href*='/visit/events/']")
        if title_link is None:
            continue
        detail_url = absolute_url(list_url, title_link.get("href"))
        if detail_url in seen:
            continue
        seen.add(detail_url)
        urls.append(detail_url)
    return urls


def _extract_okcontemporary_occurrences(list_html: str, list_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(list_html, "html.parser")
    occurrences: list[dict[str, str]] = []
    for article in soup.select("article.calendar-item"):
        text = normalize_space(article.get_text(" ", strip=True))
        title_link = article.select_one("a[href]")
        if title_link is None:
            continue
        title = normalize_space(title_link.get_text(" ", strip=True))
        if not title or not _ok_should_include(title=title, description=text):
            continue
        date_match = OKCONTEMP_DATE_RE.search(text)
        if date_match is None:
            continue
        occurrences.append(
            {
                "title": title,
                "source_url": absolute_url(list_url, title_link.get("href")),
                "date_text": date_match.group(0),
            }
        )
    return occurrences


def _ok_should_include(*, title: str, description: str | None = None) -> bool:
    combined = f" {normalize_space(title).lower()} {normalize_space(description).lower() if description else ''} "
    hard_reject_hit = any(marker in combined for marker in OK_HARD_REJECT_PATTERNS)
    include_hit = any(marker in combined for marker in OK_INCLUDE_PATTERNS)
    override_hit = any(marker in combined for marker in OK_OVERRIDE_PATTERNS)
    reject_hit = any(marker in combined for marker in OK_REJECT_PATTERNS)
    if hard_reject_hit:
        return False
    if reject_hit and not override_hit:
        return False
    return include_hit or override_hit


def _slice_text(text: str, start_marker: str, end_marker: str) -> str:
    start_index = text.find(start_marker)
    if start_index == -1:
        return ""
    end_index = text.find(end_marker, start_index + len(start_marker))
    if end_index == -1:
        return text[start_index:]
    return text[start_index:end_index]


def _extract_okcmoa_title(soup: BeautifulSoup) -> str:
    heading = soup.select_one("h1.elementor-heading-title")
    if heading is not None:
        return normalize_space(heading.get_text(" ", strip=True))
    return ""


def _extract_okcmoa_description(soup: BeautifulSoup) -> str | None:
    content = soup.select_one(".elementor-widget-theme-post-content .elementor-widget-container")
    if content is not None:
        cleaned = normalize_space(content.get_text(" ", strip=True))
        return cleaned or None
    return None


def _extract_okcontemporary_time(soup: BeautifulSoup) -> str | None:
    candidates = [
        soup.select_one("div.col-span-full.md\\:col-span-4.lg\\:col-span-3"),
        soup.select_one("p.font-medium"),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        text = normalize_space(candidate.get_text(" ", strip=True))
        match = OKCONTEMP_TIME_RANGE_RE.search(text)
        if match:
            return normalize_space(match.group("time"))
    match = OKCONTEMP_TIME_RANGE_RE.search(normalize_space(soup.get_text(" ", strip=True)))
    if match:
        return normalize_space(match.group("time"))
    return None


def _extract_okcontemporary_age_text(soup: BeautifulSoup) -> str:
    summary = soup.select_one("div.col-span-full.md\\:col-span-4.lg\\:col-span-3")
    text = normalize_space(summary.get_text(" ", strip=True)) if summary is not None else ""
    match = OKCONTEMP_AGE_RE.search(text)
    if match:
        return normalize_space(match.group("age"))
    return ""


def _extract_okcontemporary_description(soup: BeautifulSoup) -> str | None:
    prose = soup.select_one("div.prose.md\\:text-lg")
    if prose is None:
        return None
    cleaned = normalize_space(prose.get_text(" ", strip=True))
    return cleaned or None


def parse_philbrook_feed(
    xml_text: str,
    *,
    venue_name: str,
    city: str,
    state: str,
    default_location: str,
    current_date: date,
) -> list[ExtractedActivity]:
    root = ET.fromstring(xml_text)
    channel = root.find("channel")
    if channel is None:
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for item in channel.findall("item"):
        title = normalize_space(item.findtext("title") or "")
        link = normalize_space(item.findtext("link") or "")
        description = normalize_space(
            BeautifulSoup(item.findtext("description") or "", "html.parser").get_text(" ", strip=True)
        )
        categories = ", ".join(normalize_space(category.text or "") for category in item.findall("category"))
        if not _ok_should_include(title=title, description=f"{description} {categories}"):
            continue
        link_match = PHILBROOK_LINK_DATE_RE.search(link)
        if link_match is None:
            continue
        base_date = parse_date_text(link_match.group("date"))
        if base_date is None or base_date < current_date:
            continue
        start_at, end_at = parse_time_range(base_date=base_date, time_text=None)
        key = (link, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            ExtractedActivity(
                source_url=link,
                title=title,
                description=normalize_space(f"{description} | Categories: {categories}") or None,
                venue_name=venue_name,
                location_text=default_location,
                city=city,
                state=state,
                activity_type=infer_activity_type(title, description, categories),
                age_min=None,
                age_max=None,
                drop_in=False,
                registration_required=("register" in description.lower() or "ticket" in description.lower()),
                start_at=start_at,
                end_at=end_at,
                timezone=OK_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=None),
            )
        )
    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows
