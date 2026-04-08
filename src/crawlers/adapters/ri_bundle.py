from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from html import unescape
from urllib.parse import parse_qs
from urllib.parse import quote
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from bs4 import Tag

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

REGISTRATION_MARKERS = (
    "add to cart",
    "register",
    "registration",
    "reserve your spot",
    "reserve",
    "rsvp",
    "sign up",
)
INCLUDE_MARKERS = (
    "acrylic",
    "art making",
    "artist talk",
    "career in the arts",
    "class",
    "classes",
    "collage",
    "conversation",
    "conversations",
    "discussion",
    "discussions",
    "drawing",
    "gallery conversation",
    "hands on",
    "hands-on",
    "lecture",
    "lectures",
    "lesson planning",
    "make art",
    "open studio",
    "paint",
    "painting",
    "panel",
    "panels",
    "portrait",
    "printmaking",
    "sketch",
    "sketching",
    "speaker",
    "speakers",
    "studio",
    "talk",
    "talks",
    "watercolor",
    "workshop",
    "workshops",
)
EXCLUDE_MARKERS = (
    "annual meeting",
    "book group",
    "camp",
    "celebration",
    "concert",
    "film",
    "festival",
    "fundraiser",
    "fundraising",
    "gala",
    "member event",
    "member tour",
    "memoir",
    "murder",
    "opening celebration",
    "opening reception",
    "party",
    "performance",
    "poetry",
    "reading",
    "reception",
    "screening",
    "soiree",
    "storytime",
    "tour",
    "tours",
    "vacation camp",
    "writing workshop",
)
DATE_WITH_YEAR_RE = re.compile(
    r"(?P<date>"
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+\d{4}"
    r")",
    re.IGNORECASE,
)
DATE_WITHOUT_YEAR_RE = re.compile(
    r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"(?P<month>"
    r"January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
    r")\s+"
    r"(?P<day>\d{1,2})",
    re.IGNORECASE,
)
TIME_TEXT_RE = re.compile(
    r"(?P<time>"
    r"(?:"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?\s*(?:-|–|—|to)\s*"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)"
    r"|"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)"
    r")"
    r")",
    re.IGNORECASE,
)
MONEY_RE = re.compile(r"\$\s*(\d+(?:\.\d{1,2})?)")
AGES_RANGE_RE = re.compile(r"\b(?:ages?|aged)\s*(\d{1,2})\s*(?:-|–|—|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGES_PLUS_RE = re.compile(r"\b(?:ages?|aged)\s*(\d{1,2})(?:\s*\+|\s+and up)", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class RiVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    mode: str
    source_prefixes: tuple[str, ...]


RI_VENUES: tuple[RiVenueConfig, ...] = (
    RiVenueConfig(
        slug="jamestown",
        source_name="ri_jamestown_events",
        venue_name="Jamestown Arts Center",
        city="Jamestown",
        state="RI",
        list_url="https://www.jamestownartcenter.org/events",
        default_location="Jamestown Arts Center, Jamestown, RI",
        mode="jamestown",
        source_prefixes=("https://www.jamestownartcenter.org/events/",),
    ),
    RiVenueConfig(
        slug="newport",
        source_name="ri_newport_events",
        venue_name="Newport Art Museum",
        city="Newport",
        state="RI",
        list_url="https://newportartmuseum.org/events/",
        default_location="Newport Art Museum, Newport, RI",
        mode="newport",
        source_prefixes=(
            "https://newportartmuseum.org/events/",
            "https://newportartmuseum.org/?post_type=classes",
        ),
    ),
    RiVenueConfig(
        slug="risd",
        source_name="ri_risd_events",
        venue_name="RISD Museum",
        city="Providence",
        state="RI",
        list_url="https://risdmuseum.org/exhibitions-events/events",
        default_location="RISD Museum, Providence, RI",
        mode="risd",
        source_prefixes=("https://risdmuseum.org/exhibitions-events/events/",),
    ),
    RiVenueConfig(
        slug="warwick",
        source_name="ri_warwick_events",
        venue_name="Warwick Center for the Arts",
        city="Warwick",
        state="RI",
        list_url="https://warwickcfa.org/calendar-of-events/",
        default_location="Warwick Center for the Arts, Warwick, RI",
        mode="warwick",
        source_prefixes=("https://site.corsizio.com/event/",),
    ),
)

RI_VENUES_BY_SLUG = {venue.slug: venue for venue in RI_VENUES}


class RiBundleAdapter(BaseSourceAdapter):
    source_name = "ri_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_ri_bundle_payload()
        return [json.dumps(payload, ensure_ascii=False)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_ri_bundle_payload/parse_ri_events from the script runner.")


def get_ri_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in RI_VENUES:
        prefixes.extend(venue.source_prefixes)
    return tuple(dict.fromkeys(prefixes))


async def load_ri_bundle_payload(
    *,
    venues: list[RiVenueConfig] | tuple[RiVenueConfig, ...] = RI_VENUES,
    month_limit: int = 4,
) -> dict[str, object]:
    payload_by_slug: dict[str, dict[str, object]] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers=DEFAULT_HEADERS,
    ) as client:
        results = await asyncio.gather(
            *[_load_venue_payload(client=client, venue=venue, month_limit=month_limit) for venue in venues],
            return_exceptions=True,
        )

    for venue, result in zip(venues, results):
        if isinstance(result, Exception):
            errors_by_slug[venue.slug] = str(result)
            continue
        payload_by_slug[venue.slug] = result

    return {
        "payload_by_slug": payload_by_slug,
        "errors_by_slug": errors_by_slug,
    }


def parse_ri_events(
    payload: dict[str, object],
    *,
    venue: RiVenueConfig,
    start_date: str | date | datetime | None = None,
) -> list[ExtractedActivity]:
    start_day = _coerce_start_date(start_date)

    if venue.mode == "jamestown":
        rows = _parse_jamestown_events(payload, venue=venue, start_day=start_day)
    elif venue.mode == "newport":
        rows = _parse_newport_events(payload, venue=venue, start_day=start_day)
    elif venue.mode == "risd":
        rows = _parse_risd_events(payload, venue=venue, start_day=start_day)
    elif venue.mode == "warwick":
        rows = _parse_warwick_events(payload, venue=venue, start_day=start_day)
    else:
        raise ValueError(f"Unsupported RI venue mode: {venue.mode}")

    deduped: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for row in sorted(rows, key=lambda item: (item.start_at, item.title, item.source_url)):
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


async def _load_venue_payload(
    *,
    client: httpx.AsyncClient,
    venue: RiVenueConfig,
    month_limit: int,
) -> dict[str, object]:
    if venue.mode == "jamestown":
        list_html = await fetch_html(venue.list_url, client=client)
        detail_urls = _extract_jamestown_detail_urls(list_html, venue.list_url)
        details = await _fetch_detail_pages(detail_urls, client=client, referer=venue.list_url)
        return {"list_html": list_html, "details": details}

    if venue.mode == "newport":
        list_html = await fetch_html(venue.list_url, client=client)
        event_urls = _extract_newport_event_urls(list_html, venue.list_url)
        details = await _fetch_detail_pages(event_urls, client=client, referer=venue.list_url)
        return {"list_html": list_html, "details": details}

    if venue.mode == "risd":
        month_pages = await _load_risd_month_pages(client=client, list_url=venue.list_url, month_limit=month_limit)
        detail_urls: list[str] = []
        for page in month_pages:
            soup = BeautifulSoup(str(page["html"] or ""), "html.parser")
            for anchor in soup.select(".calendar-view-day__row a[href]"):
                href = normalize_space(anchor.get("href"))
                if not href:
                    continue
                detail_urls.append(absolute_url(venue.list_url, href))
        details = await _fetch_detail_pages(sorted(set(detail_urls)), client=client, referer=venue.list_url)
        return {"months": month_pages, "details": details}

    if venue.mode == "warwick":
        list_html = await fetch_html(venue.list_url, client=client)
        portal_url = _extract_warwick_portal_url(list_html, venue.list_url)
        portal_html = await fetch_html(portal_url, client=client, referer=venue.list_url)
        detail_urls = _extract_warwick_event_urls(portal_html, portal_url)
        details = await _fetch_detail_pages(detail_urls, client=client, referer=portal_url)
        return {"list_html": list_html, "portal_url": portal_url, "portal_html": portal_html, "details": details}

    raise ValueError(f"Unsupported RI venue mode: {venue.mode}")


async def _load_risd_month_pages(
    *,
    client: httpx.AsyncClient,
    list_url: str,
    month_limit: int,
) -> list[dict[str, str]]:
    pages: list[dict[str, str]] = []
    seen: set[str] = set()
    current_url = list_url

    for _ in range(max(month_limit, 1)):
        if current_url in seen:
            break
        seen.add(current_url)
        html = await fetch_html(current_url, client=client, referer=list_url)
        pages.append({"url": current_url, "html": html})

        soup = BeautifulSoup(html, "html.parser")
        next_link = soup.select_one(".pager__item.pager__next a[href]")
        if next_link is None:
            break
        href = normalize_space(next_link.get("href"))
        if not href:
            break
        current_url = absolute_url(current_url, href)

    return pages


async def _fetch_detail_pages(
    urls: list[str],
    *,
    client: httpx.AsyncClient,
    referer: str | None = None,
) -> dict[str, str]:
    semaphore = asyncio.Semaphore(8)

    async def _fetch_one(url: str) -> tuple[str, str] | None:
        async with semaphore:
            try:
                return url, await fetch_html(url, client=client, referer=referer or url)
            except Exception:
                return None

    results = await asyncio.gather(*[_fetch_one(url) for url in urls])
    return {url: html for item in results if item is not None for url, html in [item]}


def _parse_jamestown_events(
    payload: dict[str, object],
    *,
    venue: RiVenueConfig,
    start_day: date,
) -> list[ExtractedActivity]:
    details = payload.get("details") or {}
    rows: list[ExtractedActivity] = []

    for source_url, html in sorted(details.items()):
        soup = BeautifulSoup(str(html or ""), "html.parser")
        title = normalize_space(_text_or_empty(soup.select_one("h1.entry-title")))
        if not title:
            continue

        meta_description = _meta_content(soup, "description") or _meta_property_content(soup, "og:description")
        body_text = _jamestown_body_text(soup)
        signal_text = join_non_empty([meta_description, body_text]) or ""
        if not _should_include_event(title=title, description=signal_text):
            continue

        event_date = _extract_date_with_year(join_non_empty([body_text, meta_description]) or "")
        if event_date is None or event_date < start_day:
            continue

        time_text = _extract_time_text(join_non_empty([body_text, meta_description]) or "")
        start_at, end_at = parse_time_range(base_date=event_date, time_text=time_text)
        price_text = _extract_price_text(body_text, meta_description)
        age_min, age_max = _parse_age_range_extended(signal_text)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=body_text or meta_description or None,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=signal_text),
                age_min=age_min,
                age_max=age_max,
                drop_in="walk-ins welcome" in signal_text.lower() or "drop-in" in signal_text.lower(),
                registration_required=_has_registration_marker(signal_text),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **_price_kwargs(price_text or signal_text, default_is_free=None),
            )
        )

    return rows


def _parse_newport_events(
    payload: dict[str, object],
    *,
    venue: RiVenueConfig,
    start_day: date,
) -> list[ExtractedActivity]:
    list_html = str(payload.get("list_html") or "")
    details = payload.get("details") or {}
    rows: list[ExtractedActivity] = []

    for source_url, html in sorted(details.items()):
        soup = BeautifulSoup(str(html or ""), "html.parser")
        title = normalize_space(_text_or_empty(soup.select_one(".event-single-title")))
        if not title:
            continue

        short_description = normalize_space(_text_or_empty(soup.select_one(".event-single-short-description")))
        date_text = normalize_space(_text_or_empty(soup.select_one(".event-date")))
        event_date = parse_date_text(date_text)
        if event_date is None or event_date < start_day:
            continue

        time_text = normalize_space(_text_or_empty(soup.select_one(".event-time")))
        start_at, end_at = parse_time_range(base_date=event_date, time_text=time_text)
        location_text = normalize_space(_text_or_empty(soup.select_one(".event-location"))) or venue.default_location
        description = _text_or_empty(soup.select_one(".rich-text.fc-rich-text-container.wysiwyg"))
        price_text = _join_texts(soup.select(".single-sidebar-event .price, .single-sidebar-event .member-price-col"))
        signal_text = join_non_empty([short_description, description, price_text]) or ""
        if not _should_include_event(title=title, description=signal_text):
            continue

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=join_non_empty([short_description, description]),
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=signal_text),
                age_min=None,
                age_max=None,
                drop_in=False,
                registration_required=True,
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **_price_kwargs(
                    price_text or signal_text,
                    amount=_extract_amount(price_text),
                    default_is_free=None,
                ),
            )
        )

    rows.extend(_parse_newport_class_cards(list_html, venue=venue, start_day=start_day))
    return rows


def _parse_newport_class_cards(
    list_html: str,
    *,
    venue: RiVenueConfig,
    start_day: date,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(list_html, "html.parser")
    rows: list[ExtractedActivity] = []

    for card in soup.select(".card.card-type-class"):
        title = normalize_space(_text_or_empty(card.select_one(".card-text-title")))
        if not title:
            continue

        dates_text = normalize_space(_text_or_empty(card.select_one(".card-text-class-dates")))
        days_text = normalize_space(_text_or_empty(card.select_one(".card-text-class-days")))
        instructor_text = normalize_space(_text_or_empty(card.select_one(".card-text-class-instructor")))
        signal_text = join_non_empty([dates_text, days_text, instructor_text]) or ""
        if not _should_include_event(title=title, description=signal_text):
            continue

        start_text = dates_text.split(" - ", 1)[0]
        event_date = parse_date_text(start_text)
        if event_date is None or event_date < start_day:
            continue

        time_text = _extract_time_text(days_text)
        start_at, _ = parse_time_range(base_date=event_date, time_text=time_text)
        anchor = card.select_one("a[href]")
        href = normalize_space(anchor.get("href")) if anchor is not None else ""
        source_url = absolute_url(venue.list_url, href) if href else f"{venue.list_url}#class-{quote(title, safe='')}"

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=join_non_empty([dates_text, days_text, instructor_text]),
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=signal_text),
                age_min=None,
                age_max=None,
                drop_in=False,
                registration_required=True,
                start_at=start_at,
                end_at=None,
                timezone=NY_TIMEZONE,
                **_price_kwargs(signal_text, default_is_free=None),
            )
        )

    return rows


def _parse_risd_events(
    payload: dict[str, object],
    *,
    venue: RiVenueConfig,
    start_day: date,
) -> list[ExtractedActivity]:
    details = payload.get("details") or {}
    month_pages = payload.get("months") or []
    candidates: dict[str, dict[str, object]] = {}

    for page in month_pages:
        page_url = str(page.get("url") or venue.list_url)
        page_html = str(page.get("html") or "")
        page_year, page_month = _calendar_context_from_url(page_url)
        soup = BeautifulSoup(page_html, "html.parser")

        for item in soup.select(".calendar-view-day__row"):
            anchor = item.select_one("a[href]")
            if anchor is None:
                continue

            href = normalize_space(anchor.get("href"))
            if not href:
                continue
            source_url = absolute_url(venue.list_url, href)
            title = normalize_space(anchor.get_text(" ", strip=True))
            subtitle = normalize_space(_text_or_empty(item.select_one(".field-content")))
            row_text = normalize_space(item.get_text(" ", strip=True))
            if not _should_include_event(title=title, description=join_non_empty([subtitle, row_text]) or ""):
                continue

            event_date = _extract_date_without_year(row_text, page_year=page_year, page_month=page_month)
            if event_date is None or event_date < start_day:
                continue

            time_text = _extract_time_text(row_text)
            start_at, end_at = parse_time_range(base_date=event_date, time_text=time_text)
            candidates[source_url] = {
                "title": title,
                "subtitle": subtitle,
                "start_at": start_at,
                "end_at": end_at,
            }

    rows: list[ExtractedActivity] = []
    for source_url, candidate in sorted(candidates.items()):
        soup = BeautifulSoup(str(details.get(source_url) or ""), "html.parser")
        description = normalize_space(_text_or_empty(soup.select_one(".field--name-field-description")))
        location_text = normalize_space(_text_or_empty(soup.select_one(".field--name-field-place"))) or venue.default_location
        signal_text = join_non_empty(
            [
                str(candidate.get("subtitle") or ""),
                description,
            ]
        ) or ""
        title = str(candidate["title"])
        if not _should_include_event(title=title, description=signal_text):
            continue

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or str(candidate.get("subtitle") or "") or None,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=signal_text),
                age_min=_parse_age_range_extended(signal_text)[0],
                age_max=_parse_age_range_extended(signal_text)[1],
                drop_in=_is_drop_in(signal_text),
                registration_required=_has_registration_marker(signal_text),
                start_at=candidate["start_at"],
                end_at=candidate["end_at"],
                timezone=NY_TIMEZONE,
                **_price_kwargs(signal_text, default_is_free=None),
            )
        )

    return rows


def _parse_warwick_events(
    payload: dict[str, object],
    *,
    venue: RiVenueConfig,
    start_day: date,
) -> list[ExtractedActivity]:
    details = payload.get("details") or {}
    rows: list[ExtractedActivity] = []

    for source_url, html in sorted(details.items()):
        soup = BeautifulSoup(str(html or ""), "html.parser")
        event_json = _extract_corsizio_event_jsonld(soup)
        if not event_json:
            continue

        title = normalize_space(str(event_json.get("name") or ""))
        description = normalize_space(str(event_json.get("description") or ""))
        if not title or not _should_include_event(title=title, description=description):
            continue

        start_at = _parse_iso_datetime(str(event_json.get("startDate") or ""))
        if start_at is None or start_at.date() < start_day:
            continue

        end_at = _parse_iso_datetime(str(event_json.get("endDate") or ""))
        location_name = ""
        location = event_json.get("location")
        if isinstance(location, dict):
            location_name = normalize_space(str(location.get("name") or ""))

        offer_amount = _extract_offer_amount(event_json.get("offers"))
        age_min, age_max = _parse_age_range_extended(description)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description or None,
                venue_name=venue.venue_name,
                location_text=location_name or venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description),
                age_min=age_min,
                age_max=age_max,
                drop_in=False,
                registration_required=True,
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **_price_kwargs(description, amount=offer_amount, default_is_free=None),
            )
        )

    return rows


def _extract_jamestown_detail_urls(list_html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    urls: set[str] = set()
    for anchor in soup.select("a.blog-more-link[href], a.image-wrapper[href]"):
        href = normalize_space(anchor.get("href"))
        if not href or "/events/" not in href or href.endswith("/next"):
            continue
        urls.add(absolute_url(base_url, href))
    return sorted(urls)


def _extract_newport_event_urls(list_html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    urls: set[str] = set()
    for card in soup.select(".card.card-type-event"):
        anchor = card.select_one("a[href]")
        if anchor is None:
            continue
        href = normalize_space(anchor.get("href"))
        if not href:
            continue
        urls.add(absolute_url(base_url, href))
    return sorted(urls)


def _extract_warwick_portal_url(list_html: str, base_url: str) -> str:
    soup = BeautifulSoup(list_html, "html.parser")
    iframe = soup.select_one('iframe[src*="site.corsizio.com/portal/"]')
    if iframe is None:
        raise RuntimeError("Warwick Center for the Arts page is missing the Corsizio portal iframe.")
    href = normalize_space(iframe.get("src"))
    if not href:
        raise RuntimeError("Warwick Center for the Arts iframe did not include a src URL.")
    return absolute_url(base_url, href)


def _extract_warwick_event_urls(portal_html: str, portal_url: str) -> list[str]:
    soup = BeautifulSoup(portal_html, "html.parser")
    urls: set[str] = set()
    for anchor in soup.select('a[href*="/event/"]'):
        href = normalize_space(anchor.get("href"))
        if not href or "/event/" not in href:
            continue
        urls.add(absolute_url(portal_url, href))
    return sorted(urls)


def _jamestown_body_text(soup: BeautifulSoup) -> str:
    blocks = soup.select(".blog-item-content .sqs-html-content")
    if not blocks:
        return ""
    return join_non_empty([block.get_text(" ", strip=True) for block in blocks]) or ""


def _calendar_context_from_url(url: str) -> tuple[int, int]:
    parsed = urlparse(url)
    timestamp_values = parse_qs(parsed.query).get("calendar_timestamp")
    if timestamp_values:
        try:
            dt = datetime.fromtimestamp(int(timestamp_values[0]), ZoneInfo(NY_TIMEZONE))
            return dt.year, dt.month
        except (TypeError, ValueError, OverflowError):
            pass
    now = datetime.now(ZoneInfo(NY_TIMEZONE))
    return now.year, now.month


def _extract_date_with_year(text: str) -> date | None:
    match = DATE_WITH_YEAR_RE.search(text)
    if not match:
        return None
    return parse_date_text(match.group("date").replace("  ", " "))


def _extract_date_without_year(text: str, *, page_year: int, page_month: int) -> date | None:
    match = DATE_WITHOUT_YEAR_RE.search(text)
    if not match:
        return None

    month_name = normalize_space(match.group("month"))
    day_text = normalize_space(match.group("day"))
    month_number = _month_number(month_name)
    if month_number is None:
        return None

    year = page_year
    if page_month == 12 and month_number == 1:
        year += 1
    elif page_month == 1 and month_number == 12:
        year -= 1

    weekday = normalize_space(match.group("weekday"))
    return parse_date_text(f"{weekday}, {month_name} {day_text}, {year}")


def _extract_time_text(text: str) -> str | None:
    match = TIME_TEXT_RE.search(text)
    if not match:
        return None
    return normalize_space(match.group("time").replace("—", "-").replace("–", "-"))


def _extract_price_text(*parts: str | None) -> str:
    markers = ("free", "pay-what-you-can", "pay what you can", "$", "member price", "non-member")
    for part in parts:
        normalized = normalize_space(part)
        if not normalized:
            continue
        lowered = normalized.lower()
        if any(marker in lowered for marker in markers):
            return normalized
    return ""


def _extract_amount(text: str | None) -> Decimal | None:
    if not text:
        return None
    amounts: list[Decimal] = []
    for match in MONEY_RE.finditer(text):
        try:
            amount = Decimal(match.group(1))
        except (InvalidOperation, ValueError):
            continue
        if amount > 0:
            amounts.append(amount)
    if not amounts:
        return Decimal("0") if "$0" in text.replace(" ", "") else None
    return min(amounts)


def _extract_offer_amount(value: object) -> Decimal | None:
    if isinstance(value, dict):
        price = value.get("price")
        if price is not None:
            try:
                return Decimal(str(price))
            except (InvalidOperation, ValueError):
                return None
    if isinstance(value, list):
        amounts = [_extract_offer_amount(item) for item in value]
        positive = [amount for amount in amounts if amount is not None]
        return min(positive) if positive else None
    return None


def _extract_corsizio_event_jsonld(soup: BeautifulSoup) -> dict[str, object] | None:
    for node in soup.select('script[type="application/ld+json"]'):
        raw = node.string or node.get_text()
        normalized = normalize_space(raw)
        if not normalized or '"@type": "Event"' not in normalized:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and str(payload.get("@type") or "").lower() == "event":
            return payload
    return None


def _parse_iso_datetime(value: str) -> datetime | None:
    normalized = normalize_space(value)
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(ZoneInfo(NY_TIMEZONE)).replace(tzinfo=None)


def _parse_age_range_extended(text: str | None) -> tuple[int | None, int | None]:
    blob = normalize_space(text)
    if not blob:
        return None, None

    match = AGES_RANGE_RE.search(blob)
    if match:
        return int(match.group(1)), int(match.group(2))

    match = AGES_PLUS_RE.search(blob)
    if match:
        return int(match.group(1)), None

    return None, None


def _should_include_event(*, title: str, description: str | None = None) -> bool:
    blob = _token_blob(join_non_empty([title, description]) or "")
    include_hit = any(_contains_marker(blob, marker) for marker in INCLUDE_MARKERS)
    exclude_hit = any(_contains_marker(blob, marker) for marker in EXCLUDE_MARKERS)

    if not include_hit:
        return False
    if exclude_hit:
        return False
    return True


def _infer_activity_type(*, title: str, description: str | None = None) -> str:
    blob = _token_blob(join_non_empty([title, description]) or "")
    if any(_contains_marker(blob, marker) for marker in ("lecture", "speaker")):
        return "lecture"
    if any(_contains_marker(blob, marker) for marker in ("talk", "conversation", "discussion", "panel")):
        return "talk"
    if any(_contains_marker(blob, marker) for marker in ("class", "classes")):
        return "class"
    if any(
        _contains_marker(blob, marker)
        for marker in (
            "workshop",
            "studio",
            "hands on",
            "paint",
            "painting",
            "drawing",
            "sketch",
            "portrait",
            "watercolor",
            "acrylic",
        )
    ):
        return "workshop"
    return "activity"


def _price_kwargs(
    text: str | None,
    *,
    amount: Decimal | None = None,
    default_is_free: bool | None = None,
) -> dict[str, bool | None | str]:
    normalized = _token_blob(text or "")
    if " pay what you can " in normalized:
        return {"is_free": True, "free_verification_status": "confirmed"}
    if " free " in normalized:
        return {"is_free": True, "free_verification_status": "confirmed"}
    if amount is not None:
        return price_classification_kwargs_from_amount(amount, text=text, default_is_free=default_is_free)
    return price_classification_kwargs(text, default_is_free=default_is_free)


def _has_registration_marker(text: str | None) -> bool:
    lowered = normalize_space(text).lower()
    return any(marker in lowered for marker in REGISTRATION_MARKERS)


def _is_drop_in(text: str | None) -> bool:
    normalized = _token_blob(text or "")
    return " drop in " in normalized


def _coerce_start_date(value: str | date | datetime | None) -> date:
    if value is None:
        return datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _month_number(value: str) -> int | None:
    mapping = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }
    return mapping.get(value.strip().lower())


def _join_texts(nodes: list[Tag]) -> str:
    return join_non_empty([node.get_text(" ", strip=True) for node in nodes]) or ""


def _meta_content(soup: BeautifulSoup, name: str) -> str:
    node = soup.select_one(f'meta[name="{name}"]')
    if node is None:
        return ""
    return normalize_space(unescape(str(node.get("content") or "")))


def _meta_property_content(soup: BeautifulSoup, name: str) -> str:
    node = soup.select_one(f'meta[property="{name}"]')
    if node is None:
        return ""
    return normalize_space(unescape(str(node.get("content") or "")))


def _text_or_empty(node: Tag | None) -> str:
    if node is None:
        return ""
    return node.get_text(" ", strip=True)


def _token_blob(text: str) -> str:
    normalized = normalize_space(text).lower()
    normalized = normalized.replace("’", "'")
    normalized = normalized.replace("-", " ")
    normalized = re.sub(r"[^a-z0-9$+ ]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return f" {normalized} "


def _contains_marker(blob: str, marker: str) -> bool:
    return f" {marker.lower()} " in blob
