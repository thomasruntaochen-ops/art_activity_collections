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
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlparse
from urllib.parse import urlunparse
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from bs4 import Tag

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import clean_html_fragment
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

KS_TIMEZONE = "America/Chicago"
TRIBE_API_DATE_FORMAT = "%Y-%m-%d"
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
    " activities ",
    " art cart ",
    " art project ",
    " art projects ",
    " art talk ",
    " art talks ",
    " artsmart ",
    " button making ",
    " class ",
    " classes ",
    " conversation ",
    " conversations ",
    " craft ",
    " crafts ",
    " curator talk ",
    " cyanotype ",
    " discussion ",
    " discussions ",
    " draw ",
    " drawing ",
    " family free day ",
    " gallery talk ",
    " hands on ",
    " hands-on ",
    " homeschool ",
    " image making ",
    " image-making ",
    " lecture ",
    " lectures ",
    " make art ",
    " make buttons ",
    " makers masterpieces ",
    " material world ",
    " panel ",
    " panels ",
    " photogram ",
    " pre k ",
    " pre-k ",
    " presentation ",
    " presentations ",
    " printmaking ",
    " prints ",
    " senior session ",
    " senior wednesday ",
    " sketch ",
    " slow art ",
    " sculpt ",
    " studio ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
    " youth art classes ",
)
WEAK_INCLUDE_PATTERNS = (
    " art ",
    " artist ",
    " child ",
    " children ",
    " families ",
    " family ",
    " kid ",
    " kids ",
    " older adults ",
    " senior ",
    " teen ",
    " teens ",
    " youth ",
)
ALWAYS_REJECT_PATTERNS = (
    " annual meeting ",
    " book sale ",
    " camp ",
    " camps ",
    " concert ",
    " concerts ",
    " festival ",
    " film screening ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " giving day ",
    " market ",
    " member meeting ",
    " opening reception ",
    " performance ",
    " performances ",
    " runway ",
    " screening ",
    " screenings ",
    " spoken word ",
    " storytime ",
    " story time ",
    " tarot ",
    " yoga ",
    " art fair ",
    " closed ",
    " fair ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " book club ",
    " exhibition ",
    " exhibitions ",
    " parade ",
    " party ",
    " poetry ",
    " reception ",
    " tour ",
    " tours ",
)
ARTMAKING_OVERRIDE_PATTERNS = (
    " art making ",
    " art project ",
    " art projects ",
    " artmaking ",
    " button making ",
    " hands on ",
    " hands-on ",
    " image making ",
    " image-making ",
    " make buttons ",
    " printmaking ",
    " workshop ",
    " workshops ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
)
REGISTRATION_PATTERNS = (
    " buy tickets ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
    " sign up ",
    " space is limited ",
)
MONEY_RE = re.compile(r"\$\s*(\d+(?:\.\d{1,2})?)")
NERMAN_DATE_RANGE_RE = re.compile(
    r"^(?P<start>[A-Za-z]+\s+\d{1,2},\s+\d{4})\s*-\s*(?P<end>[A-Za-z]+\s+\d{1,2},\s+\d{4})$"
)
VERNON_DATE_RE = re.compile(r"(?P<start>\d{2}/\d{2}/\d{2})(?:\s*-\s*(?P<end>\d{2}/\d{2}/\d{2}))?")
TIME_RANGE_RE = re.compile(
    r"(?P<range>"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?"
    r")",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class KsVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    mode: str
    source_prefixes: tuple[str, ...]
    api_url: str | None = None
    widget_url: str | None = None


KS_VENUES: tuple[KsVenueConfig, ...] = (
    KsVenueConfig(
        slug="beach",
        source_name="ks_beach_events",
        venue_name="Marianna Kistler Beach Museum of Art",
        city="Manhattan",
        state="KS",
        list_url="https://beach.k-state.edu/visit/events/",
        default_location="Marianna Kistler Beach Museum of Art, Manhattan, KS",
        mode="beach_widget",
        widget_url=(
            "https://events.k-state.edu/widget/view?"
            "schools=k-state&template=main-column-2020&departments=beach_museum_of_art"
            "&num=35&days=365&exclude_types=133295,139747"
        ),
        source_prefixes=(
            "https://events.k-state.edu/event/",
            "https://beach.k-state.edu/visit/events/",
        ),
    ),
    KsVenueConfig(
        slug="mulvane",
        source_name="ks_mulvane_events",
        venue_name="Mulvane Art Museum",
        city="Topeka",
        state="KS",
        list_url="https://www.mulvaneartmuseum.org/calendar/",
        default_location="Mulvane Art Museum, Topeka, KS",
        mode="mulvane_xml",
        source_prefixes=("https://www.mulvaneartmuseum.org/calendar/",),
    ),
    KsVenueConfig(
        slug="artlight",
        source_name="ks_artlight_events",
        venue_name="Museum of Art + Light",
        city="Manhattan",
        state="KS",
        list_url="https://artlightmuseum.org/events/",
        default_location="Museum of Art + Light, Manhattan, KS",
        mode="tribe_json",
        api_url="https://artlightmuseum.org/wp-json/tribe/events/v1/events",
        source_prefixes=("https://artlightmuseum.org/event/",),
    ),
    KsVenueConfig(
        slug="nerman",
        source_name="ks_nerman_events",
        venue_name="Nerman Museum of Contemporary Art",
        city="Overland Park",
        state="KS",
        list_url="https://www.nermanmuseum.org/calendar/",
        default_location="Nerman Museum of Contemporary Art, Overland Park, KS",
        mode="nerman_html",
        source_prefixes=(
            "https://www.nermanmuseum.org/calendar/events/",
            "https://www.jccc.edu/calendar/events/",
        ),
    ),
    KsVenueConfig(
        slug="spencer",
        source_name="ks_spencer_events",
        venue_name="Spencer Museum of Art",
        city="Lawrence",
        state="KS",
        list_url="https://spencerart.ku.edu/exhibitions-and-events/events",
        default_location="Spencer Museum of Art, Lawrence, KS",
        mode="spencer_api",
        source_prefixes=("https://spencerart.ku.edu/exhibitions-and-events/events#event-",),
    ),
    KsVenueConfig(
        slug="ulrich",
        source_name="ks_ulrich_events",
        venue_name="Ulrich Museum of Art",
        city="Wichita",
        state="KS",
        list_url="https://ulrich.wichita.edu/programs/",
        default_location="Ulrich Museum of Art, Wichita, KS",
        mode="tribe_json",
        api_url="https://ulrich.wichita.edu/wp-json/tribe/events/v1/events",
        source_prefixes=("https://ulrich.wichita.edu/event/",),
    ),
    KsVenueConfig(
        slug="vernon_filley",
        source_name="ks_vernon_filley_events",
        venue_name="Vernon Filley Art Museum",
        city="Pratt",
        state="KS",
        list_url="https://www.vernonfilleyartmuseum.org/calendar-of-events.cfm",
        default_location="Vernon Filley Art Museum, Pratt, KS",
        mode="vernon_filley_html",
        source_prefixes=("https://www.vernonfilleyartmuseum.org/calendar-of-events.cfm",),
    ),
    KsVenueConfig(
        slug="wam",
        source_name="ks_wam_events",
        venue_name="Wichita Art Museum",
        city="Wichita",
        state="KS",
        list_url="https://wam.org/events/",
        default_location="Wichita Art Museum, Wichita, KS",
        mode="wam_html",
        source_prefixes=("https://wam.org/event/",),
    ),
)

KS_VENUES_BY_SLUG = {venue.slug: venue for venue in KS_VENUES}


class KsBundleAdapter(BaseSourceAdapter):
    source_name = "ks_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_ks_bundle_payload()
        return [json.dumps(payload, ensure_ascii=False)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_ks_bundle_payload/parse_ks_events from the script runner.")


def get_ks_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in KS_VENUES:
        prefixes.extend(venue.source_prefixes)
    return tuple(dict.fromkeys(prefixes))


async def load_ks_bundle_payload(
    *,
    venues: list[KsVenueConfig] | tuple[KsVenueConfig, ...] = KS_VENUES,
    month_limit: int = 4,
    per_page: int = 50,
) -> dict[str, object]:
    payload_by_slug: dict[str, dict[str, object]] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers=DEFAULT_HEADERS,
    ) as client:
        results = await asyncio.gather(
            *[
                _load_venue_payload(
                    client=client,
                    venue=venue,
                    month_limit=month_limit,
                    per_page=per_page,
                )
                for venue in venues
            ],
            return_exceptions=True,
        )

    for venue, result in zip(venues, results):
        if isinstance(result, Exception):
            errors_by_slug[venue.slug] = str(result)
            continue
        payload_by_slug[venue.slug] = result

    return {"payload_by_slug": payload_by_slug, "errors_by_slug": errors_by_slug}


def parse_ks_events(
    payload: dict[str, object],
    *,
    venue: KsVenueConfig,
    start_date: str | None = None,
) -> list[ExtractedActivity]:
    cutoff = _resolve_start_date(start_date)

    if venue.mode == "beach_widget":
        rows = _parse_beach_widget(payload, venue=venue, cutoff=cutoff)
    elif venue.mode == "tribe_json":
        rows = _parse_tribe_json_events(payload, venue=venue, cutoff=cutoff)
    elif venue.mode == "mulvane_xml":
        rows = _parse_mulvane_events(payload, venue=venue, cutoff=cutoff)
    elif venue.mode == "nerman_html":
        rows = _parse_nerman_events(payload, venue=venue, cutoff=cutoff)
    elif venue.mode == "spencer_api":
        rows = _parse_spencer_events(payload, venue=venue, cutoff=cutoff)
    elif venue.mode == "vernon_filley_html":
        rows = _parse_vernon_filley_events(payload, venue=venue, cutoff=cutoff)
    elif venue.mode == "wam_html":
        rows = _parse_wam_events(payload, venue=venue, cutoff=cutoff)
    else:  # pragma: no cover - config guard
        raise ValueError(f"Unsupported KS mode: {venue.mode}")

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
    venue: KsVenueConfig,
    month_limit: int,
    per_page: int,
) -> dict[str, object]:
    if venue.mode == "beach_widget":
        if not venue.widget_url:
            raise RuntimeError(f"Missing widget URL for {venue.slug}")
        widget_js = await fetch_html(venue.widget_url, referer=venue.list_url, client=client)
        return {"widget_js": widget_js}

    if venue.mode == "tribe_json":
        if not venue.api_url:
            raise RuntimeError(f"Missing API URL for {venue.slug}")
        events = await _load_tribe_events(api_url=venue.api_url, client=client, per_page=per_page)
        return {"events": events}

    if venue.mode == "mulvane_xml":
        return await _load_mulvane_payload(client=client, venue=venue, month_limit=month_limit)

    if venue.mode == "nerman_html":
        return await _load_nerman_payload(client=client, venue=venue)

    if venue.mode == "spencer_api":
        return await _load_spencer_payload(client=client, venue=venue, month_limit=month_limit)

    if venue.mode == "vernon_filley_html":
        html = await fetch_html(venue.list_url, client=client)
        if normalize_space(html):
            return {"html": html, "used_playwright": False}
        html = await _fetch_html_with_playwright(venue.list_url)
        return {"html": html, "used_playwright": True}

    if venue.mode == "wam_html":
        html = await fetch_html(venue.list_url, client=client)
        return {"html": html}

    raise RuntimeError(f"Unsupported KS mode: {venue.mode}")


async def _load_tribe_events(
    *,
    api_url: str,
    client: httpx.AsyncClient,
    per_page: int,
) -> list[dict]:
    events: list[dict] = []
    next_url = _with_per_page(api_url, per_page)

    while next_url:
        response = await client.get(next_url, headers={**DEFAULT_HEADERS, "Accept": "application/json,text/plain,*/*"})
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected tribe payload type for {api_url}")
        events.extend(payload.get("events") or [])
        next_url = payload.get("next_rest_url")

    return events


async def _load_mulvane_payload(
    *,
    client: httpx.AsyncClient,
    venue: KsVenueConfig,
    month_limit: int,
) -> dict[str, object]:
    events: list[dict[str, object]] = []
    detail_urls: list[str] = []

    for year, month in _month_sequence(month_limit):
        xml_url = absolute_url(venue.list_url, f"{year}/{month:02d}/index.xml")
        xml_text = await fetch_html(xml_url, referer=venue.list_url, client=client)
        root = ET.fromstring(xml_text)

        for node in root.findall(".//event"):
            path = normalize_space(node.findtext("path"))
            detail_url = _mulvane_detail_url(path) if path else venue.list_url
            events.append(
                {
                    "title": normalize_space(node.findtext("title")),
                    "summary": normalize_space(node.findtext("summary")),
                    "location": normalize_space(node.findtext("location")),
                    "start_iso": normalize_space(node.findtext("startISO")),
                    "end_iso": normalize_space(node.findtext("endISO")),
                    "detail_url": detail_url,
                    "categories": [
                        normalize_space(category.text)
                        for category in node.findall("./categories/category")
                        if normalize_space(category.text)
                    ],
                }
            )
            if detail_url != venue.list_url:
                detail_urls.append(detail_url)

    unique_detail_urls = list(dict.fromkeys(detail_urls))
    detail_pages = await asyncio.gather(
        *[
            fetch_html(detail_url, referer=venue.list_url, client=client)
            for detail_url in unique_detail_urls
        ],
        return_exceptions=True,
    )

    detail_html_by_url: dict[str, str] = {}
    for detail_url, detail_html in zip(unique_detail_urls, detail_pages):
        if isinstance(detail_html, Exception):
            continue
        detail_html_by_url[detail_url] = detail_html

    return {"events": events, "detail_html_by_url": detail_html_by_url}


async def _load_nerman_payload(
    *,
    client: httpx.AsyncClient,
    venue: KsVenueConfig,
) -> dict[str, object]:
    list_html = await fetch_html(venue.list_url, client=client)
    soup = BeautifulSoup(list_html, "html.parser")
    detail_urls: list[str] = []

    for anchor in soup.select("a[href]"):
        href = normalize_space(anchor.get("href"))
        if not href:
            continue
        if not (href.startswith("events/") or "/calendar/events/" in href):
            continue
        detail_urls.append(absolute_url(venue.list_url, href))

    unique_detail_urls = list(dict.fromkeys(detail_urls))
    detail_pages = await asyncio.gather(
        *[
            fetch_html(detail_url, referer=venue.list_url, client=client)
            for detail_url in unique_detail_urls
        ],
        return_exceptions=True,
    )

    detail_html_by_url: dict[str, str] = {}
    for detail_url, detail_html in zip(unique_detail_urls, detail_pages):
        if isinstance(detail_html, Exception):
            continue
        detail_html_by_url[detail_url] = detail_html

    return {"list_html": list_html, "detail_html_by_url": detail_html_by_url}


async def _load_spencer_payload(
    *,
    client: httpx.AsyncClient,
    venue: KsVenueConfig,
    month_limit: int,
) -> dict[str, object]:
    all_events: list[dict] = []
    for year, month in _month_sequence(month_limit):
        response = await client.get(f"https://sma-search-api.ku.edu/event/{year}/{month}")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected Spencer payload for {year}-{month}")
        all_events.extend(item for item in payload if isinstance(item, dict))
    return {"events": all_events}


def _parse_beach_widget(
    payload: dict[str, object],
    *,
    venue: KsVenueConfig,
    cutoff: date,
) -> list[ExtractedActivity]:
    widget_js = payload.get("widget_js") or ""
    html = _decode_document_write(widget_js if isinstance(widget_js, str) else "")
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []

    for item in soup.select("div.event"):
        anchor = item.select_one(".event__title a[href]")
        start_node = item.select_one(".dtstart")
        if anchor is None or start_node is None:
            continue

        title = normalize_space(anchor.get_text(" ", strip=True))
        source_url = _strip_tracking_params(normalize_space(anchor.get("href")) or venue.list_url)
        start_at = _parse_iso_datetime(start_node.get("title"))
        end_at = _parse_iso_datetime(item.select_one(".dtend").get("title") if item.select_one(".dtend") else None)
        if not title or start_at is None or start_at.date() < cutoff:
            continue

        description = clean_html_fragment(str(item.select_one(".event__description"))) or None
        location_text = (
            normalize_space(item.select_one(".event__location").get_text(" ", strip=True))
            if item.select_one(".event__location")
            else venue.default_location
        ) or venue.default_location

        if not _should_keep_event(
            venue_slug=venue.slug,
            title=title,
            description=description,
            category=None,
            event_type=None,
        ):
            continue

        text_blob = join_non_empty([title, description])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description, category=None, event_type=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=KS_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=None),
            )
        )

    return rows


def _parse_mulvane_events(
    payload: dict[str, object],
    *,
    venue: KsVenueConfig,
    cutoff: date,
) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    detail_html_by_url = payload.get("detail_html_by_url") or {}

    for event in payload.get("events") or []:
        if not isinstance(event, dict):
            continue

        title = normalize_space(event.get("title"))
        start_at = _parse_naive_datetime(event.get("start_iso"))
        end_at = _parse_naive_datetime(event.get("end_iso"))
        if not title or start_at is None or start_at.date() < cutoff:
            continue

        source_url = normalize_space(event.get("detail_url")) or venue.list_url
        category = ", ".join(event.get("categories") or [])
        detail_html = detail_html_by_url.get(source_url) if isinstance(detail_html_by_url, dict) else None
        description = _first_non_empty(
            normalize_space(event.get("summary")),
            _extract_meta_description(detail_html),
            _extract_mulvane_lead(detail_html),
        )
        location_text = normalize_space(event.get("location")) or venue.default_location

        if not _should_keep_event(
            venue_slug=venue.slug,
            title=title,
            description=description,
            category=category,
            event_type=None,
        ):
            continue

        text_blob = join_non_empty([title, description, category])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description, category=category, event_type=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=KS_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=None),
            )
        )

    return rows


def _parse_tribe_json_events(
    payload: dict[str, object],
    *,
    venue: KsVenueConfig,
    cutoff: date,
) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []

    for event_obj in payload.get("events") or []:
        if not isinstance(event_obj, dict):
            continue

        title = clean_html_fragment(event_obj.get("title")) or normalize_space(event_obj.get("title"))
        source_url = normalize_space(event_obj.get("url")) or venue.list_url
        start_at = _parse_naive_datetime(event_obj.get("start_date"))
        if not title or start_at is None or start_at.date() < cutoff:
            continue

        end_at = _parse_naive_datetime(event_obj.get("end_date"))
        category_names = [
            clean_html_fragment(category.get("name")) or normalize_space(category.get("name"))
            for category in (event_obj.get("categories") or [])
            if isinstance(category, dict)
        ]
        category_names = [name for name in category_names if name]
        category = ", ".join(category_names) or None

        excerpt = clean_html_fragment(event_obj.get("excerpt"))
        description_html = clean_html_fragment(event_obj.get("description"))
        price_text = normalize_space(event_obj.get("cost"))
        description = join_non_empty(
            [
                excerpt or None,
                description_html or None,
                f"Categories: {category}" if category else None,
                f"Price: {price_text}" if price_text else None,
            ]
        )

        if not _should_keep_event(
            venue_slug=venue.slug,
            title=title,
            description=description,
            category=category,
            event_type=None,
        ):
            continue

        text_blob = join_non_empty([title, description, category, price_text])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=_tribe_location_text(event_obj, venue=venue),
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description, category=category, event_type=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=KS_TIMEZONE,
                **price_classification_kwargs_from_amount(
                    _extract_amount(event_obj.get("cost_details")),
                    text=join_non_empty([price_text, description]),
                    default_is_free=None,
                ),
            )
        )

    return rows


def _parse_nerman_events(
    payload: dict[str, object],
    *,
    venue: KsVenueConfig,
    cutoff: date,
) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    detail_html_by_url = payload.get("detail_html_by_url") or {}
    soup = BeautifulSoup(list_html if isinstance(list_html, str) else "", "html.parser")
    rows: list[ExtractedActivity] = []

    for anchor in soup.select("a[href]"):
        href = normalize_space(anchor.get("href"))
        if not href or not (href.startswith("events/") or "/calendar/events/" in href):
            continue

        title = _first_non_empty(
            normalize_space(node.get_text(" ", strip=True))
            for node in anchor.select(".media-heading, .h4")
        )
        if not title:
            continue

        source_url = absolute_url(venue.list_url, href)
        category = (
            normalize_space(anchor.select_one(".category").get_text(" ", strip=True))
            if anchor.select_one(".category")
            else None
        )
        price_text = (
            normalize_space(anchor.select_one(".price").get_text(" ", strip=True))
            if anchor.select_one(".price")
            else None
        )
        date_time_text = (
            normalize_space(anchor.select_one(".date-time").get_text(" ", strip=True))
            if anchor.select_one(".date-time")
            else ""
        )
        start_at, end_at, location_text = _parse_nerman_date_time(date_time_text, default_location=venue.default_location)
        if start_at is None or start_at.date() < cutoff:
            continue

        detail_html = detail_html_by_url.get(source_url) if isinstance(detail_html_by_url, dict) else None
        description = _first_non_empty(
            _extract_nerman_description(detail_html),
            _extract_meta_description(detail_html),
            price_text,
        )

        if not _should_keep_event(
            venue_slug=venue.slug,
            title=title,
            description=description,
            category=category,
            event_type=None,
        ):
            continue

        text_blob = join_non_empty([title, description, category, price_text])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description, category=category, event_type=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=KS_TIMEZONE,
                **price_classification_kwargs(join_non_empty([price_text, description]), default_is_free=None),
            )
        )

    return rows


def _parse_spencer_events(
    payload: dict[str, object],
    *,
    venue: KsVenueConfig,
    cutoff: date,
) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []

    for event_obj in payload.get("events") or []:
        if not isinstance(event_obj, dict):
            continue

        title = normalize_space(event_obj.get("title"))
        source_url = f"{venue.list_url}#event-{event_obj.get('id')}"
        start_at = _parse_spencer_datetime(event_obj.get("startDate"), event_obj.get("startTime"))
        if not title or start_at is None or start_at.date() < cutoff:
            continue

        description = join_non_empty(
            [
                normalize_space(event_obj.get("description")),
                f"Series: {normalize_space(event_obj.get('seriesTitle'))}" if normalize_space(event_obj.get("seriesTitle")) else None,
                f"Presented by: {', '.join(normalize_space(name) for name in (event_obj.get('presentedBy') or []) if normalize_space(name))}"
                if event_obj.get("presentedBy")
                else None,
                f"Type: {normalize_space(event_obj.get('type'))}" if normalize_space(event_obj.get("type")) else None,
            ]
        )
        category = normalize_space(event_obj.get("type")) or None
        if not _should_keep_event(
            venue_slug=venue.slug,
            title=title,
            description=description,
            category=category,
            event_type=category,
        ):
            continue

        end_at = _parse_spencer_datetime(event_obj.get("endDate"), event_obj.get("endTime"))
        location_text = (
            normalize_space(event_obj.get("specificLocation"))
            or normalize_space(event_obj.get("location"))
            or venue.default_location
        )
        text_blob = join_non_empty([title, description, category])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description, category=category, event_type=category),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=KS_TIMEZONE,
                **price_classification_kwargs("free", default_is_free=None),
            )
        )

    return rows


def _parse_vernon_filley_events(
    payload: dict[str, object],
    *,
    venue: KsVenueConfig,
    cutoff: date,
) -> list[ExtractedActivity]:
    html = payload.get("html") or ""
    soup = BeautifulSoup(html if isinstance(html, str) else "", "html.parser")
    rows: list[ExtractedActivity] = []

    for row in soup.select("table tr"):
        anchor = row.find("a", href=True)
        if anchor is None:
            continue
        href = normalize_space(anchor.get("href"))
        if "calendar-of-events.cfm?t=events" not in href:
            continue

        cells = row.find_all("td")
        if not cells:
            continue

        title = normalize_space(anchor.get_text(" ", strip=True))
        description = clean_html_fragment(str(cells[0])) or None
        date_time_text = normalize_space(cells[-1].get_text(" ", strip=True))
        start_at, end_at = _parse_vernon_date_time(date_time_text)
        if not title or start_at is None or start_at.date() < cutoff:
            continue

        location_text = normalize_space(cells[1].get_text(" ", strip=True)) if len(cells) > 1 else venue.default_location
        source_url = absolute_url(venue.list_url, href)
        if not _should_keep_event(
            venue_slug=venue.slug,
            title=title,
            description=description,
            category=None,
            event_type=None,
        ):
            continue

        text_blob = join_non_empty([title, description])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=location_text or venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description, category=None, event_type=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=KS_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=None),
            )
        )

    return rows


def _parse_wam_events(
    payload: dict[str, object],
    *,
    venue: KsVenueConfig,
    cutoff: date,
) -> list[ExtractedActivity]:
    html = payload.get("html") or ""
    soup = BeautifulSoup(html if isinstance(html, str) else "", "html.parser")
    rows: list[ExtractedActivity] = []

    for event_obj in _extract_event_json_ld_objects(soup):
        title = clean_html_fragment(event_obj.get("name")) or normalize_space(event_obj.get("name"))
        source_url = normalize_space(event_obj.get("url")) or venue.list_url
        start_at = _parse_iso_datetime(event_obj.get("startDate"))
        if not title or start_at is None or start_at.date() < cutoff:
            continue

        description = clean_html_fragment(event_obj.get("description")) or None
        if description:
            description = clean_html_fragment(unescape(description)) or description
        if not _should_keep_event(
            venue_slug=venue.slug,
            title=title,
            description=description,
            category=None,
            event_type=None,
        ):
            continue

        end_at = _parse_iso_datetime(event_obj.get("endDate"))
        text_blob = join_non_empty([title, description])
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=_extract_schema_location_text(event_obj.get("location")) or venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_activity_type(title=title, description=description, category=None, event_type=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=KS_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=None),
            )
        )

    return rows


def _resolve_start_date(start_date: str | None) -> date:
    if not normalize_space(start_date):
        return datetime.now().date()
    return datetime.strptime(normalize_space(start_date), TRIBE_API_DATE_FORMAT).date()


def _month_sequence(month_limit: int) -> list[tuple[int, int]]:
    total = max(month_limit, 1)
    current = datetime.now()
    year = current.year
    month = current.month
    pairs: list[tuple[int, int]] = []

    for _ in range(total):
        pairs.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1

    return pairs


def _decode_document_write(value: str) -> str:
    text = normalize_space(value)
    prefix = "document.write("
    if text.startswith(prefix) and text.endswith(");"):
        text = text[len(prefix) : -2]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


async def _fetch_html_with_playwright(url: str) -> str:
    if async_playwright is None:
        raise RuntimeError("Playwright is required to fetch this KS museum page.")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, args=list(PLAYWRIGHT_LAUNCH_ARGS))
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until="networkidle", timeout=60_000)
            return await page.content()
        finally:
            await browser.close()


def _parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = normalize_space(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _parse_naive_datetime(value: object) -> datetime | None:
    text = normalize_space(value if isinstance(value, str) else "")
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y-%m-%d":
                return datetime.combine(parsed.date(), datetime.min.time())
            return parsed
        except ValueError:
            continue
    return _parse_iso_datetime(text)


def _parse_spencer_datetime(date_obj: object, time_text: object) -> datetime | None:
    if not isinstance(date_obj, dict):
        return None
    date_blob = normalize_space(date_obj.get("date")).split(" ", 1)[0]
    if not date_blob:
        return None
    time_blob = normalize_space(time_text if isinstance(time_text, str) else "")
    if not time_blob:
        time_blob = "00:00:00"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(f"{date_blob} {time_blob}", fmt)
        except ValueError:
            continue
    return None


def _parse_nerman_date_time(
    value: str,
    *,
    default_location: str,
) -> tuple[datetime | None, datetime | None, str]:
    parts = [normalize_space(part) for part in value.split("•") if normalize_space(part)]
    if not parts:
        return None, None, default_location

    date_part = parts[0]
    time_part: str | None = None
    location_text = default_location

    if len(parts) >= 2 and _looks_like_time(parts[1]):
        time_part = parts[1]
        if len(parts) >= 3:
            location_text = parts[2]
    elif len(parts) >= 2:
        location_text = parts[1]

    range_match = NERMAN_DATE_RANGE_RE.match(date_part)
    base_date_text = range_match.group("start") if range_match else date_part
    base_date = _parse_long_date(base_date_text)
    if base_date is None:
        return None, None, location_text

    start_at = _parse_time_on_date(base_date, time_part, default_midnight=True)
    end_at = _parse_time_on_date(base_date, _extract_end_time(time_part), default_midnight=False) if time_part else None
    return start_at, end_at, location_text


def _parse_vernon_date_time(value: str) -> tuple[datetime | None, datetime | None]:
    blob = normalize_space(value)
    if not blob:
        return None, None

    date_match = VERNON_DATE_RE.search(blob)
    if date_match is None:
        return None, None

    base_date = _parse_short_us_date(date_match.group("start"))
    if base_date is None:
        return None, None

    remainder = normalize_space(blob[date_match.end() :])
    time_match = TIME_RANGE_RE.search(remainder)
    time_text = time_match.group("range") if time_match else None
    start_at = _parse_time_on_date(base_date, time_text, default_midnight=True)
    end_at = _parse_time_on_date(base_date, _extract_end_time(time_text), default_midnight=False) if time_text else None
    return start_at, end_at


def _parse_time_on_date(base_date: date, time_text: str | None, *, default_midnight: bool) -> datetime | None:
    blob = normalize_space(time_text)
    if not blob:
        return datetime.combine(base_date, datetime.min.time()) if default_midnight else None

    range_match = re.search(
        r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?)"
        r"\s*(?:-|–|—|to)\s*"
        r"(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?)",
        blob,
        re.IGNORECASE,
    )
    if range_match:
        first = range_match.group("start")
        second = range_match.group("end")
        return _parse_clock(base_date, first, fallback_meridiem=_extract_meridiem(second))

    return _parse_clock(base_date, blob)


def _extract_end_time(time_text: str | None) -> str | None:
    blob = normalize_space(time_text)
    if not blob:
        return None
    match = re.search(
        r"(?:-|–|—|to)\s*(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?)",
        blob,
        re.IGNORECASE,
    )
    return normalize_space(match.group("end")) if match else None


def _parse_clock(base_date: date, value: str | None, *, fallback_meridiem: str | None = None) -> datetime | None:
    blob = normalize_space(value)
    if not blob:
        return None

    meridiem = _extract_meridiem(blob) or fallback_meridiem
    normalized = blob.lower().replace(".", "")
    if meridiem and "am" not in normalized and "pm" not in normalized:
        normalized = f"{normalized} {meridiem}"

    normalized = re.sub(r"\s+", " ", normalized.strip())
    bare_time_match = re.fullmatch(r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?", normalized)
    if bare_time_match and meridiem is None:
        hour = int(bare_time_match.group("hour"))
        minute = int(bare_time_match.group("minute") or 0)
        if 1 <= hour <= 7:
            hour += 12
        return datetime.combine(base_date, datetime.min.time().replace(hour=hour % 24, minute=minute))

    for fmt in ("%I:%M %p", "%I %p", "%H:%M:%S", "%H:%M"):
        try:
            clock = datetime.strptime(normalized.upper(), fmt).time()
            return datetime.combine(base_date, clock)
        except ValueError:
            continue

    return None


def _extract_meridiem(value: str | None) -> str | None:
    blob = normalize_space(value).lower().replace(".", "")
    if "am" in blob:
        return "am"
    if "pm" in blob:
        return "pm"
    return None


def _parse_long_date(value: str | None) -> date | None:
    text = normalize_space(value)
    if not text:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_short_us_date(value: str | None) -> date | None:
    text = normalize_space(value)
    if not text:
        return None
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _looks_like_time(value: str) -> bool:
    blob = normalize_space(value).lower()
    return "am" in blob or "pm" in blob or bool(re.search(r"\d{1,2}:\d{2}", blob))


def _strip_tracking_params(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return url
    kept_query = [
        (key, val)
        for key, val in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_")
    ]
    return urlunparse(parsed._replace(query=urlencode(kept_query)))


def _mulvane_detail_url(path: str | None) -> str:
    href = normalize_space(path)
    if not href:
        return "https://www.mulvaneartmuseum.org/calendar/"
    if href.startswith("/"):
        return absolute_url("https://www.mulvaneartmuseum.org/", href)
    return absolute_url("https://www.mulvaneartmuseum.org/", f"/{href}")


def _searchable_blob(*parts: str | None) -> str:
    raw = " ".join(normalize_space(part).lower() for part in parts if normalize_space(part))
    compact = re.sub(r"[^a-z0-9]+", " ", raw)
    return f" {' '.join(compact.split())} "


def _should_keep_event(
    *,
    venue_slug: str,
    title: str,
    description: str | None,
    category: str | None,
    event_type: str | None,
) -> bool:
    token_blob = _searchable_blob(title, description, category, event_type)
    title_blob = _searchable_blob(title)

    if " cancelled " in token_blob or " canceled " in token_blob or " sold out " in token_blob:
        return False

    strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    if event_type and _searchable_blob(event_type) in {
        " class ",
        " discussion ",
        " lecture ",
        " talk ",
        " workshop ",
    }:
        strong_include = True

    weak_include = strong_include or any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS)
    if not weak_include:
        return False

    if any(pattern in title_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if venue_slug == "spencer" and any(marker in title_blob for marker in (" open friday ", " one day one ku ")):
        return False

    contextual_reject = any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS)
    artmaking_override = any(pattern in token_blob for pattern in ARTMAKING_OVERRIDE_PATTERNS)
    if contextual_reject and not (artmaking_override or strong_include):
        return False

    if venue_slug == "artlight" and " poets prints " in token_blob and artmaking_override:
        return True
    if venue_slug == "wam" and " pop up pop art " in token_blob and artmaking_override:
        return True

    return True


def _infer_activity_type(
    *,
    title: str,
    description: str | None,
    category: str | None,
    event_type: str | None,
) -> str:
    blob = _searchable_blob(title, description, category, event_type)
    if any(
        marker in blob
        for marker in (
            " activity ",
            " art cart ",
            " art making ",
            " artmaking ",
            " art project ",
            " button making ",
            " class ",
            " classes ",
            " craft ",
            " crafts ",
            " hands on ",
            " hands-on ",
            " model ",
            " modeling ",
            " sculpt ",
            " sketch ",
            " workshop ",
            " workshops ",
        )
    ):
        return "workshop"
    if any(
        marker in blob
        for marker in (
            " conversation ",
            " discussion ",
            " lecture ",
            " panel ",
            " presentation ",
            " senior session ",
            " senior wednesday ",
            " talk ",
            " talks ",
        )
    ):
        return "lecture"
    return "activity"


def _infer_drop_in(value: str | None) -> bool | None:
    blob = _searchable_blob(value)
    if any(pattern in blob for pattern in DROP_IN_PATTERNS):
        return True
    return None


def _infer_registration_required(value: str | None) -> bool | None:
    blob = _searchable_blob(value)
    if any(pattern in blob for pattern in REGISTRATION_PATTERNS):
        return True
    return None


def _extract_amount(value: object) -> Decimal | None:
    if isinstance(value, dict):
        values = value.get("values")
        if isinstance(values, list):
            for item in values:
                try:
                    return Decimal(str(item))
                except InvalidOperation:
                    continue
        return None
    if isinstance(value, str):
        match = MONEY_RE.search(value)
        if match:
            return Decimal(match.group(1))
    return None


def _tribe_location_text(event_obj: dict, *, venue: KsVenueConfig) -> str:
    venue_obj = event_obj.get("venue")
    if isinstance(venue_obj, dict):
        city = normalize_space(venue_obj.get("city"))
        state = normalize_space(venue_obj.get("state") or venue_obj.get("province")) or (venue.state if city else "")
        pieces = [
            normalize_space(venue_obj.get("venue")),
            city,
            state,
        ]
        joined = ", ".join(part for part in pieces if part)
        if joined:
            return joined
    return venue.default_location


def _extract_meta_description(html: object) -> str | None:
    if not isinstance(html, str) or not normalize_space(html):
        return None
    soup = BeautifulSoup(html, "html.parser")
    for selector in (
        "meta[name='description']",
        "meta[property='og:description']",
    ):
        node = soup.select_one(selector)
        if node is None:
            continue
        content = normalize_space(node.get("content"))
        if content:
            return content
    return None


def _extract_mulvane_lead(html: object) -> str | None:
    if not isinstance(html, str) or not normalize_space(html):
        return None
    soup = BeautifulSoup(html, "html.parser")
    for selector in (".entry-content p", ".event-description p", "article p"):
        node = soup.select_one(selector)
        if node is None:
            continue
        text = normalize_space(node.get_text(" ", strip=True))
        if text:
            return text
    return None


def _extract_nerman_description(html: object) -> str | None:
    if not isinstance(html, str) or not normalize_space(html):
        return None
    soup = BeautifulSoup(html, "html.parser")
    body = soup.select_one("main") or soup
    paragraphs = [
        normalize_space(node.get_text(" ", strip=True))
        for node in body.select(".lead, .event-content p, article p, .content p")
    ]
    paragraphs = [text for text in paragraphs if text]
    return join_non_empty(paragraphs[:4]) if paragraphs else None


def _extract_event_json_ld_objects(soup: BeautifulSoup) -> list[dict]:
    events: list[dict] = []
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text("\n", strip=True)
        if not normalize_space(raw):
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for candidate in _flatten_json(payload):
            if isinstance(candidate, dict) and normalize_space(candidate.get("@type")).lower() == "event":
                events.append(candidate)
    return events


def _flatten_json(value: object) -> list[object]:
    if isinstance(value, list):
        items: list[object] = []
        for item in value:
            items.extend(_flatten_json(item))
        return items
    if isinstance(value, dict):
        if "@graph" in value:
            return _flatten_json(value.get("@graph"))
        return [value]
    return []


def _extract_schema_location_text(location: object) -> str | None:
    if isinstance(location, dict):
        name = normalize_space(location.get("name"))
        address = location.get("address")
        if isinstance(address, dict):
            address_blob = join_non_empty(
                [
                    normalize_space(address.get("streetAddress")),
                    normalize_space(address.get("addressLocality")),
                    normalize_space(address.get("addressRegion")),
                ]
            )
            return join_non_empty([name, address_blob])
        return name or None
    return None


def _with_per_page(url: str, per_page: int) -> str:
    delimiter = "&" if "?" in url else "?"
    return f"{url}{delimiter}per_page={per_page}"


def _first_non_empty(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str):
            cleaned = normalize_space(value)
            if cleaned:
                return cleaned
        elif value is not None:
            for item in value:
                cleaned = normalize_space(item) if isinstance(item, str) else ""
                if cleaned:
                    return cleaned
    return None
