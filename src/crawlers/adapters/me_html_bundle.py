from __future__ import annotations

import asyncio
from html import unescape
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from urllib.parse import quote
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_month_day_year
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
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

DATE_WITH_WEEKDAY_RE = re.compile(
    r"(?P<date>"
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+\d{4}"
    r")"
    r"(?:\s+[—-]\s+(?P<time>[^|]+))?",
    re.IGNORECASE,
)
MONTH_DAY_TIME_RE = re.compile(
    r"(?P<month>"
    r"January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
    r")"
    r"\s+(?P<day>\d{1,2})"
    r"(?:\s*\|\s*(?P<time>[^|]+))?",
    re.IGNORECASE,
)
TIME_RANGE_INLINE_RE = re.compile(
    r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)\s*(?:-|–|—|to)\s*"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
TIME_SINGLE_INLINE_RE = re.compile(
    r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))",
    re.IGNORECASE,
)
CMCA_DETAIL_LINK_HINTS = ("makerspace", "workshop", "vacation", "family")

TITLE_REJECT_PATTERNS = (
    " admission ",
    " art in bloom ",
    " camp ",
    " concert ",
    " gala ",
    " film ",
    " films ",
    " mindfulness ",
    " music ",
    " opening day ",
    " opening: ",
    " opening reception ",
    " pma films",
    " performance ",
    " poem ",
    " poetry ",
    " reading ",
    " reception ",
    " sale ",
    " screening ",
    " films:",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)
BODY_REJECT_PATTERNS = (
    " admission ",
    " camp ",
    " camps ",
    " concert ",
    " concerts ",
    " film ",
    " films ",
    " fundraising ",
    " gala ",
    " meditation ",
    " mindfulness ",
    " music ",
    " orchestra ",
    " performance ",
    " poem ",
    " poetry ",
    " reading ",
    " reception ",
    " sale ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)
INCLUDE_PATTERNS = (
    " activity ",
    " art making ",
    " art-making ",
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " discussion ",
    " discussions ",
    " lab ",
    " labs ",
    " lecture ",
    " lectures ",
    " makerspace ",
    " panel ",
    " panels ",
    " studio ",
    " symposium ",
    " talk ",
    " talks ",
    " with dr. ",
    " workshop ",
    " workshops ",
)


@dataclass(frozen=True, slots=True)
class MeVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    mode: str
    api_url: str | None = None
    source_prefixes: tuple[str, ...] = ()


ME_HTML_VENUES: tuple[MeVenueConfig, ...] = (
    MeVenueConfig(
        slug="bates",
        source_name="me_bates_events",
        venue_name="Bates College Museum of Art",
        city="Lewiston",
        state="ME",
        list_url="https://www.bates.edu/museum/upcoming-events/",
        default_location="Bates College Museum of Art, Lewiston, ME",
        mode="bates",
        source_prefixes=("https://www.bates.edu/museum/events/",),
    ),
    MeVenueConfig(
        slug="bowdoin",
        source_name="me_bowdoin_events",
        venue_name="Bowdoin College Museum of Art",
        city="Brunswick",
        state="ME",
        list_url="https://www.bowdoin.edu/art-museum/events/events-index.html",
        default_location="Bowdoin College Museum of Art, Brunswick, ME",
        mode="bowdoin",
        source_prefixes=("https://calendar.bowdoin.edu/event/",),
    ),
    MeVenueConfig(
        slug="cmca",
        source_name="me_cmca_events",
        venue_name="Center for Maine Contemporary Art",
        city="Rockland",
        state="ME",
        list_url="https://www.cmcanow.org/upcoming-events",
        default_location="Center for Maine Contemporary Art, Rockland, ME",
        mode="cmca",
        source_prefixes=("https://www.cmcanow.org/",),
    ),
    MeVenueConfig(
        slug="farnsworth",
        source_name="me_farnsworth_events",
        venue_name="Farnsworth Art Museum",
        city="Rockland",
        state="ME",
        list_url="https://www.farnsworthmuseum.org/events-calendar/",
        default_location="Farnsworth Art Museum, Rockland, ME",
        mode="tribe_json",
        api_url="https://www.farnsworthmuseum.org/wp-json/tribe/events/v1/events",
        source_prefixes=("https://www.farnsworthmuseum.org/event/",),
    ),
    MeVenueConfig(
        slug="ica_meca",
        source_name="me_ica_meca_events",
        venue_name="Institute of Contemporary Art at Maine College of Art & Design",
        city="Portland",
        state="ME",
        list_url="https://meca.edu/newsevent/ica/",
        default_location="Institute of Contemporary Art at Maine College of Art & Design, Portland, ME",
        mode="ica",
        source_prefixes=("https://meca.edu/event/",),
    ),
    MeVenueConfig(
        slug="ogunquit",
        source_name="me_ogunquit_events",
        venue_name="Ogunquit Museum of American Art",
        city="Ogunquit",
        state="ME",
        list_url="https://ogunquitmuseum.org/about-us/events/",
        default_location="Ogunquit Museum of American Art, Ogunquit, ME",
        mode="tribe_json",
        api_url="https://ogunquitmuseum.org/wp-json/tribe/events/v1/events",
        source_prefixes=("https://ogunquitmuseum.org/event/",),
    ),
    MeVenueConfig(
        slug="pma",
        source_name="me_portland_museum_events",
        venue_name="Portland Museum of Art",
        city="Portland",
        state="ME",
        list_url="https://www.portlandmuseum.org/events-calendar/",
        default_location="Portland Museum of Art, Portland, ME",
        mode="tribe_json",
        api_url="https://www.portlandmuseum.org/wp-json/tribe/events/v1/events",
        source_prefixes=("https://www.portlandmuseum.org/event/",),
    ),
    MeVenueConfig(
        slug="zillman",
        source_name="me_zillman_events",
        venue_name="Zillman Art Museum",
        city="Bangor",
        state="ME",
        list_url="https://zam.umaine.edu/events/list/",
        default_location="Zillman Art Museum, Bangor, ME",
        mode="zillman",
        source_prefixes=("https://zam.umaine.edu/event/",),
    ),
)

ME_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in ME_HTML_VENUES}


class MeHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "me_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_me_html_bundle_payload()
        return [json.dumps(payload, ensure_ascii=False)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_me_html_bundle_payload/parse_me_html_events from the script runner.")


async def fetch_html(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    referer: str | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    headers = dict(DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer

    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers)

    last_exception: Exception | None = None
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(url, headers=headers)
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


async def fetch_json(
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
                return response.json()

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError(f"Unable to fetch JSON: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch JSON after retries: {url}")


async def load_me_html_bundle_payload(
    *,
    venues: list[MeVenueConfig] | None = None,
) -> dict[str, dict]:
    selected = venues or list(ME_HTML_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            try:
                if venue.mode == "tribe_json":
                    assert venue.api_url is not None
                    today = datetime.now().date().isoformat()
                    api_url = f"{venue.api_url}?per_page=100&start_date={quote(today)}"
                    payload_by_slug[venue.slug] = await fetch_json(api_url, client=client)
                elif venue.mode == "bowdoin":
                    widget_url = (
                        "https://calendar.bowdoin.edu/widget/view"
                        "?schools=bowdoin"
                        "&departments=bowdoin_college_museum_of_art"
                        "&days=250"
                        "&num=110"
                        "&hide_past=1"
                        "&template=flex-width-right-img"
                    )
                    payload_by_slug[venue.slug] = {
                        "widget_js": await fetch_html(widget_url, client=client, referer=venue.list_url),
                    }
                elif venue.mode == "cmca":
                    listing_html = await fetch_html(venue.list_url, client=client)
                    detail_urls = _extract_cmca_detail_urls(listing_html=listing_html, list_url=venue.list_url)
                    detail_pages: list[dict[str, str]] = []
                    for detail_url in detail_urls:
                        try:
                            detail_pages.append(
                                {
                                    "url": detail_url,
                                    "html": await fetch_html(detail_url, client=client, referer=venue.list_url),
                                }
                            )
                        except Exception:
                            continue
                    payload_by_slug[venue.slug] = {
                        "listing_html": listing_html,
                        "detail_pages": detail_pages,
                    }
                elif venue.mode == "bates":
                    listing_html = await fetch_html(venue.list_url, client=client)
                    detail_urls = _extract_bates_detail_urls(listing_html=listing_html, list_url=venue.list_url)
                    detail_pages: list[dict[str, str]] = []
                    for detail_url in detail_urls:
                        try:
                            detail_pages.append(
                                {
                                    "url": detail_url,
                                    "html": await fetch_html(detail_url, client=client, referer=venue.list_url),
                                }
                            )
                        except Exception:
                            continue
                    payload_by_slug[venue.slug] = {
                        "listing_html": listing_html,
                        "detail_pages": detail_pages,
                    }
                elif venue.mode == "ica":
                    payload_by_slug[venue.slug] = {
                        "listing_html": await fetch_html(venue.list_url, client=client),
                    }
                elif venue.mode == "zillman":
                    payload_by_slug[venue.slug] = {
                        "listing_html": await fetch_html(venue.list_url, client=client),
                    }
                else:
                    raise RuntimeError(f"Unsupported Maine venue mode: {venue.mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)

    return {
        "payload_by_slug": payload_by_slug,
        "errors_by_slug": errors_by_slug,
    }


def parse_me_html_events(payload: dict, *, venue: MeVenueConfig) -> list[ExtractedActivity]:
    now = datetime.now()

    if venue.mode == "tribe_json":
        return _parse_tribe_json_events(payload, venue=venue, now=now)
    if venue.mode == "bowdoin":
        return _parse_bowdoin_widget(payload, venue=venue, now=now)
    if venue.mode == "cmca":
        return _parse_cmca_detail_pages(payload, venue=venue, now=now)
    if venue.mode == "bates":
        return _parse_bates_detail_pages(payload, venue=venue, now=now)
    if venue.mode == "ica":
        return _parse_ica_listing_html(payload, venue=venue, now=now)
    if venue.mode == "zillman":
        return _parse_zillman_listing_html(payload, venue=venue, now=now)

    raise RuntimeError(f"Unsupported Maine venue mode: {venue.mode}")


def get_me_source_prefixes() -> list[str]:
    prefixes: list[str] = []
    for venue in ME_HTML_VENUES:
        prefixes.extend(venue.source_prefixes)
    return prefixes


def _parse_tribe_json_events(payload: dict, *, venue: MeVenueConfig, now: datetime) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event in payload.get("events") or []:
        title = normalize_space(event.get("title"))
        categories = ", ".join(
            normalize_space(category.get("name"))
            for category in (event.get("categories") or [])
            if normalize_space(category.get("name"))
        ) or None
        description = join_non_empty(
            [
                _strip_html(event.get("excerpt")),
                _strip_html(event.get("description")),
                f"Categories: {categories}" if categories else None,
            ]
        )
        if not _should_keep_event(title=title, description=description, category=categories):
            continue

        start_at = _parse_isoish_datetime(event.get("start_date"))
        if start_at is None or start_at.date() < now.date():
            continue
        end_at = _parse_isoish_datetime(event.get("end_date"))

        source_url = normalize_space(event.get("url")) or venue.list_url
        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        text_blob = join_non_empty([title, description, categories, normalize_space(event.get("cost"))])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=_tribe_location_text(event, venue=venue),
                city=venue.city,
                state=venue.state,
                activity_type=_infer_me_activity_type(title=title, description=description, category=categories),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **price_classification_kwargs(
                    join_non_empty([normalize_space(event.get("cost")), description, categories]),
                    default_is_free=None,
                ),
            )
        )

    return rows


def _parse_bowdoin_widget(payload: dict, *, venue: MeVenueConfig, now: datetime) -> list[ExtractedActivity]:
    widget_js = payload.get("widget_js") or ""
    html = _decode_document_write(widget_js)
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in soup.select("ul.localist-widget-eds > li"):
        title_anchor = item.select_one(".localist-title a[href]")
        if title_anchor is None:
            continue

        title = normalize_space(title_anchor.get_text(" ", strip=True))
        if not title:
            continue

        description = normalize_space(
            item.select_one(".localist-description").get_text(" ", strip=True)
            if item.select_one(".localist-description")
            else ""
        ) or None
        if not _should_keep_event(title=title, description=description, category=None):
            continue

        month_text = normalize_space(item.select_one(".localist-month").get_text(" ", strip=True))
        day_text = normalize_space(item.select_one(".localist-day").get_text(" ", strip=True))
        base_date = _resolve_month_day_year(month_text=month_text, day_text=day_text, now=now)
        if base_date is None or base_date < now.date():
            continue

        daytime_blob = normalize_space(
            item.select_one(".localist-daytime").get_text(" ", strip=True)
            if item.select_one(".localist-daytime")
            else ""
        )
        time_text = _extract_inline_time(daytime_blob)
        start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
        if start_at is None:
            continue

        source_url = normalize_space(title_anchor.get("href")) or venue.list_url
        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        text_blob = join_non_empty([title, description])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_me_activity_type(title=title, description=description, category=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=None),
            )
        )

    return rows


def _parse_cmca_detail_pages(payload: dict, *, venue: MeVenueConfig, now: datetime) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in payload.get("detail_pages") or []:
        source_url = normalize_space(item.get("url")) or venue.list_url
        soup = BeautifulSoup(item.get("html") or "", "html.parser")
        title = _first_non_empty(
            [
                normalize_space(tag.get_text(" ", strip=True))
                for tag in soup.select("h1, h2")
            ]
        )
        if not title:
            continue

        lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
        date_text, time_text = _extract_cmca_date_time(lines)
        if date_text is None:
            continue
        start_at, end_at = parse_time_range(base_date=date_text, time_text=time_text)
        if start_at is None or start_at.date() < now.date():
            continue

        description = join_non_empty(_cmca_description_lines(lines))
        if not _should_keep_event(title=title, description=description, category=None):
            continue

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        text_blob = join_non_empty([title, description])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_me_activity_type(title=title, description=description, category=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=None),
            )
        )

    return rows


def _parse_bates_detail_pages(payload: dict, *, venue: MeVenueConfig, now: datetime) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in payload.get("detail_pages") or []:
        source_url = normalize_space(item.get("url")) or venue.list_url
        soup = BeautifulSoup(item.get("html") or "", "html.parser")
        lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
        title = _extract_bates_title(soup)
        if not title:
            continue

        date_line = _first_matching_line(lines, DATE_WITH_WEEKDAY_RE)
        if date_line is None:
            continue
        match = DATE_WITH_WEEKDAY_RE.search(date_line)
        if match is None:
            continue
        base_date = parse_date_text(match.group("date"))
        if base_date is None or base_date < now.date():
            continue

        start_at, end_at = parse_time_range(base_date=base_date, time_text=match.group("time"))
        if start_at is None:
            continue

        for index, line in enumerate(lines):
            if "until" in line.lower():
                end_time = normalize_space(line.replace("until", "", 1))
                _, parsed_end = parse_time_range(base_date=base_date, time_text=end_time)
                if parsed_end is not None:
                    end_at = parsed_end
                break

        description = join_non_empty(_bates_description_lines(lines, title=title))
        if not _should_keep_event(title=title, description=description, category=None):
            continue

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        text_blob = join_non_empty([title, description])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=_extract_bates_location(lines) or venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_me_activity_type(title=title, description=description, category=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=None),
            )
        )

    return rows


def _parse_ica_listing_html(payload: dict, *, venue: MeVenueConfig, now: datetime) -> list[ExtractedActivity]:
    html = payload.get("listing_html") or ""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for anchor in soup.find_all("a", href=True):
        href = normalize_space(anchor.get("href"))
        if not href.startswith("/event/"):
            continue

        title_blob = normalize_space(anchor.get_text(" ", strip=True))
        title, date_text = _split_ica_title_and_date(title_blob)
        if not title or date_text is None:
            continue

        base_date = parse_date_text(date_text)
        if base_date is None or base_date < now.date():
            continue

        start_at, end_at = parse_time_range(base_date=base_date, time_text=None)
        if start_at is None:
            continue

        source_url = urljoin(venue.list_url, href)
        if not _should_keep_event(title=title, description=None, category=None):
            continue

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=None,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_me_activity_type(title=title, description=None, category=None),
                age_min=None,
                age_max=None,
                drop_in=None,
                registration_required=None,
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **price_classification_kwargs(None, default_is_free=None),
            )
        )

    return rows


def _parse_zillman_listing_html(payload: dict, *, venue: MeVenueConfig, now: datetime) -> list[ExtractedActivity]:
    html = payload.get("listing_html") or ""
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in _extract_ld_json_event_objects(html):
        title = normalize_space(event_obj.get("name"))
        description = _strip_html(event_obj.get("description"))
        if not _should_keep_event(title=title, description=description, category=None):
            continue

        start_at = _parse_isoish_datetime(event_obj.get("startDate"))
        if start_at is None or start_at.date() < now.date():
            continue
        end_at = _parse_isoish_datetime(event_obj.get("endDate"))
        source_url = normalize_space(event_obj.get("url")) or venue.list_url

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        offer = event_obj.get("offers")
        price_text = None
        price_amount = None
        if isinstance(offer, dict):
            price_text = normalize_space(str(offer.get("price") or "")) or None
            raw_price = offer.get("price")
            if isinstance(raw_price, (int, float)):
                price_amount = raw_price
            elif isinstance(raw_price, str):
                normalized_price = raw_price.replace("$", "").strip()
                if normalized_price and normalized_price.replace(".", "", 1).isdigit():
                    price_amount = float(normalized_price)

        text_blob = join_non_empty([title, description, price_text])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=_extract_schema_location_text(event_obj.get("location")) or venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_me_activity_type(title=title, description=description, category=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **price_classification_kwargs_from_amount(
                    price_amount,
                    text=text_blob,
                    default_is_free=None,
                ),
            )
        )

    return rows


def _tribe_location_text(event: dict, *, venue: MeVenueConfig) -> str:
    venue_info = event.get("venue") or {}
    parts = [
        normalize_space(venue_info.get("venue")),
        normalize_space(venue_info.get("address")),
        normalize_space(venue_info.get("city")),
        normalize_space(venue_info.get("state")),
    ]
    location_text = ", ".join(part for part in parts if part)
    return location_text or venue.default_location


def _should_keep_event(*, title: str, description: str | None, category: str | None) -> bool:
    title_blob = f" {normalize_space(title).lower()} "
    body_blob = f" {normalize_space(join_non_empty([description, category]) or '').lower()} "
    include_hit = any(pattern in title_blob or pattern in body_blob for pattern in INCLUDE_PATTERNS)

    if not include_hit:
        return False

    if any(pattern in title_blob for pattern in TITLE_REJECT_PATTERNS):
        return False

    if any(pattern in body_blob for pattern in BODY_REJECT_PATTERNS) and not include_hit:
        return False

    if "sold out" in title_blob and "workshop" not in title_blob and "lecture" not in title_blob:
        return False

    return True


def _infer_me_activity_type(*, title: str, description: str | None, category: str | None) -> str:
    blob = normalize_space(join_non_empty([title, description, category]) or "").lower()
    if "makerspace" in blob or "art making" in blob or "art-making" in blob:
        return "workshop"
    if "workshop" in blob or "class" in blob:
        return infer_activity_type(title=title, description=description, category=category)
    if "panel" in blob or "symposium" in blob:
        return "talk"
    return infer_activity_type(title=title, description=description, category=category)


def _infer_registration_required(text: str | None) -> bool | None:
    blob = normalize_space(text).lower()
    if not blob:
        return None
    if "registration is required" in blob or "register" in blob or "ticket" in blob or "space is limited" in blob:
        return True
    if "drop-in" in blob or "drop in" in blob:
        return False
    return None


def _infer_drop_in(text: str | None) -> bool | None:
    blob = normalize_space(text).lower()
    if not blob:
        return None
    if "drop-in" in blob or "drop in" in blob:
        return True
    return None


def _parse_isoish_datetime(value: str | None) -> datetime | None:
    text = normalize_space(value)
    if not text:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _decode_document_write(value: str) -> str:
    text = normalize_space(value)
    prefix = "document.write("
    if text.startswith(prefix) and text.endswith(");"):
        text = text[len(prefix) : -2]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _resolve_month_day_year(*, month_text: str, day_text: str, now: datetime) -> date | None:
    current_year = now.year
    candidate = parse_month_day_year(month_text, day_text, current_year)
    if candidate is None:
        return None
    if candidate < now.date():
        next_candidate = parse_month_day_year(month_text, day_text, current_year + 1)
        if next_candidate is not None:
            return next_candidate
    return candidate


def _extract_inline_time(text: str | None) -> str | None:
    blob = normalize_space(text)
    if not blob:
        return None
    range_match = TIME_RANGE_INLINE_RE.search(blob)
    if range_match:
        return range_match.group("time")
    single_match = TIME_SINGLE_INLINE_RE.search(blob)
    if single_match:
        return single_match.group("time")
    return None


def _extract_cmca_detail_urls(*, listing_html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(listing_html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = normalize_space(anchor.get("href"))
        text = normalize_space(anchor.get_text(" ", strip=True)).lower()
        if not href.startswith("/"):
            continue
        if href in {"/upcoming-events", "/events", "/signature-events"}:
            continue
        if not any(hint in f"{href.lower()} {text}" for hint in CMCA_DETAIL_LINK_HINTS):
            continue
        if href.endswith("-1"):
            href = href[:-2]
        detail_url = urljoin(list_url, href)
        if detail_url in seen:
            continue
        seen.add(detail_url)
        urls.append(detail_url)

    return urls


def _extract_bates_detail_urls(*, listing_html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(listing_html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = normalize_space(anchor.get("href"))
        if "/museum/events/" not in href:
            continue
        detail_url = urljoin(list_url, href)
        if detail_url in seen:
            continue
        seen.add(detail_url)
        urls.append(detail_url)

    return urls


def _cmca_description_lines(lines: list[str]) -> list[str]:
    description: list[str] = []
    capture = False
    for line in lines:
        normalized = normalize_space(line)
        if normalized.startswith("Sunday,") or normalized.startswith("Saturday,") or normalized.startswith("Friday,"):
            capture = True
            continue
        if not capture:
            continue
        if normalized in {"First Name", "Last Name"}:
            break
        if normalized.lower().startswith("photography policy"):
            break
        description.append(normalized)
    return description


def _extract_cmca_date_time(lines: list[str]) -> tuple[date | None, str | None]:
    for line in lines:
        normalized = normalize_space(line)
        match = MONTH_DAY_TIME_RE.search(normalized)
        if match:
            base_date = parse_month_day_year(match.group("month"), match.group("day"), datetime.now().year)
            return base_date, match.group("time")
    for line in lines:
        normalized = normalize_space(line)
        base_date = parse_date_text(normalized)
        if base_date is not None:
            return base_date, _extract_inline_time(normalized)
    return None, None


def _bates_description_lines(lines: list[str], *, title: str) -> list[str]:
    description: list[str] = []
    capture = False
    for line in lines:
        normalized = normalize_space(line)
        if normalized == title:
            capture = True
            continue
        if not capture:
            continue
        if normalized in {"Search these pages", "Bates College Museum of Art"}:
            break
        if DATE_WITH_WEEKDAY_RE.search(normalized):
            continue
        if normalized.startswith("until"):
            continue
        if normalized == "70 Campus Avenue":
            continue
        description.append(normalized)
    return description


def _extract_bates_location(lines: list[str]) -> str | None:
    for line in lines:
        normalized = normalize_space(line)
        if "museum of art" in normalized.lower():
            return normalized
        if re.search(r"\d+\s+.+", normalized):
            return normalized
    return None


def _extract_bates_title(soup: BeautifulSoup) -> str | None:
    title_tag = soup.title.get_text(" ", strip=True) if soup.title else ""
    if title_tag:
        return normalize_space(title_tag.split("|", 1)[0])
    for tag in soup.select("h2"):
        value = normalize_space(tag.get_text(" ", strip=True))
        if value and value != "Museum of Art":
            return value
    return None


def _first_matching_line(lines: list[str], pattern: re.Pattern[str]) -> str | None:
    for line in lines:
        if pattern.search(line):
            return line
    return None


def _strip_html(value: str | None) -> str | None:
    text = normalize_space(BeautifulSoup(unescape(value or ""), "html.parser").get_text(" ", strip=True))
    return text or None


def _first_non_empty(values: list[str]) -> str | None:
    for value in values:
        normalized = normalize_space(value)
        if normalized:
            return normalized
    return None


def _split_ica_title_and_date(value: str) -> tuple[str | None, str | None]:
    text = normalize_space(value)
    match = re.search(
        r"(?P<title>.+?)\s+"
        r"(?P<date>(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})$",
        text,
    )
    if match is None:
        return text or None, None
    return normalize_space(match.group("title")), normalize_space(match.group("date"))


def _extract_ld_json_event_objects(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    events: list[dict] = []

    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for obj in _iter_json_objects(data):
            if isinstance(obj, dict) and obj.get("@type") == "Event":
                events.append(obj)

    return events


def _iter_json_objects(data: object) -> list[object]:
    if isinstance(data, list):
        objects: list[object] = []
        for item in data:
            objects.extend(_iter_json_objects(item))
        return objects
    if isinstance(data, dict):
        objects = [data]
        for value in data.values():
            objects.extend(_iter_json_objects(value))
        return objects
    return []


def _extract_schema_location_text(location: object) -> str | None:
    if not isinstance(location, dict):
        return None
    address = location.get("address")
    if isinstance(address, dict):
        parts = [
            normalize_space(location.get("name")),
            normalize_space(address.get("streetAddress")),
            normalize_space(address.get("addressLocality")),
            normalize_space(address.get("addressRegion")),
        ]
        location_text = ", ".join(part for part in parts if part)
        return location_text or None
    return normalize_space(location.get("name")) or None
