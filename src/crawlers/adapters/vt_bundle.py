from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from html import unescape
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from bs4 import Tag

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import clean_html_fragment
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

TRIBE_API_DATE_FORMAT = "%Y-%m-%d"
VT_TZ = ZoneInfo(NY_TIMEZONE)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

INCLUDE_MARKERS = (
    "art talk",
    "artmaking",
    "art making",
    "class",
    "classes",
    "collage",
    "coloring",
    "community day",
    "conversation",
    "conversations",
    "discussion",
    "discussions",
    "family day",
    "gallery drawing",
    "hands-on",
    "hands on",
    "homeschool",
    "homeschool day",
    "lecture",
    "lectures",
    "make art",
    "museum abcs",
    "panel",
    "panels",
    "portrait",
    "presentations",
    "research presentation",
    "sketch",
    "sketching",
    "studio",
    "talk",
    "talks",
    "theatrical exploration",
    "webinar",
    "workshop",
    "workshops",
)
EXCLUDE_MARKERS = (
    "admission",
    "artravel",
    "benefactor",
    "brunch",
    "camp",
    "concert",
    "dinner",
    "exhibition opening",
    "film",
    "fundraiser",
    "fundraising",
    "gala",
    "live music",
    "member season preview day",
    "members event",
    "member preview",
    "music",
    "open house",
    "party",
    "performance",
    "poetry",
    "reading",
    "reception",
    "sold out",
    "stargazing",
    "tour",
    "tours",
    "travel",
    "volunteer",
    "wine tasting",
)
HARD_REJECT_TITLE_MARKERS = (
    "artravel",
    "concert",
    "guided tour",
    "open house",
    "pop-up exhibit",
    "sold out",
    "tour",
    "trip",
    "travel",
    "volunteer",
    "wine tasting",
)
HARD_REJECT_BODY_MARKERS = (
    "benefactor level",
    "member season preview day",
    "members at the benefactor level",
)
REGISTRATION_MARKERS = (
    "buy tickets",
    "register",
    "registration",
    "reservation",
    "reserve",
    "rsvp",
    "sign up",
)
MIDDLEBURY_DATE_RE = re.compile(
    r"^(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})(?:,\s*(?P<year>\d{4}))?"
    r"(?:\s*\|\s*(?P<time>.+))?$",
    re.IGNORECASE,
)
MIDDLEBURY_RANGE_RE = re.compile(
    r"^(?P<weekday1>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"(?P<month1>[A-Za-z]+)\s+(?P<day1>\d{1,2})"
    r"(?:\u2013|-)"
    r"(?P<weekday2>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"(?P<month2>[A-Za-z]+)\s+(?P<day2>\d{1,2})(?:,\s*(?P<year>\d{4}))?",
    re.IGNORECASE,
)
BRATTLEBORO_DATE_RE = re.compile(
    r"^(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})"
    r"(?:-\d{1,2})?"
    r",\s*(?P<weekday>[A-Za-z]+)[\.,]?\s*(?P<time>.+)?$",
    re.IGNORECASE,
)
BRATTLEBORO_YEAR_RE = re.compile(
    r"^(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})-\d{1,2},\s*(?P<year>\d{4})$",
    re.IGNORECASE,
)
LOCALIST_INNER_HTML_RE = re.compile(
    r'innerHTML=(?P<html>"(?:\\.|[^"])*")',
    re.DOTALL,
)


@dataclass(frozen=True, slots=True)
class VtVenueConfig:
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


VT_VENUES: tuple[VtVenueConfig, ...] = (
    VtVenueConfig(
        slug="bennington",
        source_name="vt_bennington_events",
        venue_name="Bennington Museum",
        city="Bennington",
        state="VT",
        list_url="https://benningtonmuseum.org/events-and-programs/",
        default_location="Bennington Museum, Bennington, VT",
        mode="tribe_json",
        api_url="https://benningtonmuseum.org/wp-json/tribe/events/v1/events",
        source_prefixes=("https://benningtonmuseum.org/event/",),
    ),
    VtVenueConfig(
        slug="brattleboro",
        source_name="vt_brattleboro_events",
        venue_name="Brattleboro Museum & Art Center",
        city="Brattleboro",
        state="VT",
        list_url="https://www.brattleboromuseum.org/calendar/",
        default_location="Brattleboro Museum & Art Center, Brattleboro, VT",
        mode="brattleboro_html",
        source_prefixes=("https://www.brattleboromuseum.org/",),
    ),
    VtVenueConfig(
        slug="fleming",
        source_name="vt_fleming_events",
        venue_name="Fleming Museum of Art",
        city="Burlington",
        state="VT",
        list_url="https://www.uvm.edu/fleming/events",
        default_location="Fleming Museum of Art, Burlington, VT",
        mode="fleming_localist",
        widget_url=(
            "https://events.uvm.edu/widget/view?"
            "schools=uvm&departments=fleming_museum&days=100&num=50"
            "&experience=inperson&container=localist-widget-13572409&template=uvm-template-card"
        ),
        source_prefixes=("https://events.uvm.edu/event/",),
    ),
    VtVenueConfig(
        slug="middlebury",
        source_name="vt_middlebury_events",
        venue_name="Middlebury College Museum of Art",
        city="Middlebury",
        state="VT",
        list_url="https://www.middlebury.edu/museum/events",
        default_location="Middlebury College Museum of Art, Middlebury, VT",
        mode="middlebury_html",
        source_prefixes=("https://www.middlebury.edu/museum/events",),
    ),
    VtVenueConfig(
        slug="shelburne",
        source_name="vt_shelburne_events",
        venue_name="Shelburne Museum",
        city="Shelburne",
        state="VT",
        list_url="https://shelburnemuseum.org/calendar/",
        default_location="Shelburne Museum, Shelburne, VT",
        mode="tribe_json",
        api_url="https://shelburnemuseum.org/wp-json/tribe/events/v1/events",
        source_prefixes=("https://shelburnemuseum.org/event/",),
    ),
    VtVenueConfig(
        slug="twwood",
        source_name="vt_twwood_events",
        venue_name="T.W. Wood Museum",
        city="Montpelier",
        state="VT",
        list_url="https://twwoodmuseum.org/events/",
        default_location="T.W. Wood Museum, Montpelier, VT",
        mode="tribe_json",
        api_url="https://twwoodmuseum.org/wp-json/tribe/events/v1/events",
        source_prefixes=("https://twwoodmuseum.org/event/",),
    ),
)

VT_VENUES_BY_SLUG = {venue.slug: venue for venue in VT_VENUES}


class VtBundleAdapter(BaseSourceAdapter):
    source_name = "vt_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_vt_bundle_payload()
        return [json.dumps(payload, ensure_ascii=False)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_vt_bundle_payload/parse_vt_events from the script runner.")


def get_vt_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in VT_VENUES:
        prefixes.extend(venue.source_prefixes)
    return tuple(dict.fromkeys(prefixes))


async def load_vt_bundle_payload(
    *,
    venues: list[VtVenueConfig] | tuple[VtVenueConfig, ...] = VT_VENUES,
    start_date: str | date | datetime | None = None,
) -> dict[str, object]:
    start_day = _coerce_start_date(start_date)
    payload_by_slug: dict[str, dict[str, object]] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        results = await asyncio.gather(
            *[_load_venue_payload(client=client, venue=venue, start_day=start_day) for venue in venues],
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


def parse_vt_events(
    payload: dict[str, object],
    *,
    venue: VtVenueConfig,
    start_date: str | date | datetime | None = None,
) -> list[ExtractedActivity]:
    start_day = _coerce_start_date(start_date)
    now_dt = datetime.combine(start_day, datetime.min.time())

    if venue.mode == "tribe_json":
        rows = _parse_tribe_events(payload, venue=venue, start_day=start_day)
    elif venue.mode == "brattleboro_html":
        rows = _parse_brattleboro_events(payload, venue=venue, start_day=start_day)
    elif venue.mode == "fleming_localist":
        rows = _parse_fleming_events(payload, venue=venue, start_day=start_day)
    elif venue.mode == "middlebury_html":
        rows = _parse_middlebury_events(payload, venue=venue, now=now_dt)
    else:
        raise ValueError(f"Unsupported VT venue mode: {venue.mode}")

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
    venue: VtVenueConfig,
    start_day: date,
) -> dict[str, object]:
    if venue.mode == "tribe_json":
        if venue.api_url is None:
            raise ValueError(f"Missing VT tribe api_url for {venue.slug}")
        params = {
            "per_page": "50",
            "start_date": start_day.strftime(TRIBE_API_DATE_FORMAT),
        }
        data = await _fetch_json(venue.api_url, client=client, params=params)
        return {"events_json": data}

    if venue.mode == "brattleboro_html":
        list_html = await fetch_html(venue.list_url, client=client)
        detail_urls = _extract_brattleboro_detail_urls(list_html, venue.list_url)
        details = await _fetch_detail_pages(detail_urls, client=client, referer=venue.list_url)
        return {"list_html": list_html, "details": details}

    if venue.mode == "fleming_localist":
        if venue.widget_url is None:
            raise ValueError(f"Missing VT widget_url for {venue.slug}")
        widget_js = await fetch_html(venue.widget_url, client=client, referer=venue.list_url)
        detail_urls = _extract_fleming_detail_urls(widget_js, venue=venue)
        details = await _fetch_detail_pages(detail_urls, client=client, referer=venue.list_url)
        return {"widget_js": widget_js, "details": details}

    if venue.mode == "middlebury_html":
        page_html = await fetch_html(venue.list_url, client=client)
        return {"page_html": page_html}

    raise ValueError(f"Unsupported VT venue mode: {venue.mode}")


async def _fetch_json(
    url: str,
    *,
    client: httpx.AsyncClient,
    params: dict[str, str] | None = None,
    referer: str | None = None,
) -> dict:
    headers: dict[str, str] = dict(DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer
    response = await client.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


async def _fetch_detail_pages(
    urls: list[str],
    *,
    client: httpx.AsyncClient,
    referer: str | None = None,
) -> list[dict[str, str]]:
    unique_urls = list(dict.fromkeys(urls))
    if not unique_urls:
        return []

    pages = await asyncio.gather(
        *[fetch_html(url, client=client, referer=referer) for url in unique_urls],
    )
    return [{"url": url, "html": html} for url, html in zip(unique_urls, pages)]


def _parse_tribe_events(payload: dict[str, object], *, venue: VtVenueConfig, start_day: date) -> list[ExtractedActivity]:
    data = payload.get("events_json") or {}
    rows: list[ExtractedActivity] = []

    for event in data.get("events") or []:
        title = normalize_space(event.get("title"))
        source_url = normalize_space(event.get("url")) or venue.list_url
        description = clean_html_fragment(event.get("description"))
        categories = ", ".join(normalize_space(cat.get("name")) for cat in event.get("categories") or [] if normalize_space(cat.get("name")))

        start_at = _parse_iso_datetime(event.get("start_date"))
        if start_at is None or start_at.date() < start_day:
            continue
        end_at = _parse_iso_datetime(event.get("end_date"))

        if not _should_keep_event(title=title, description=description, category=categories):
            continue

        venue_info = event.get("venue") or {}
        location_text = join_non_empty(
            [
                normalize_space(venue_info.get("venue")),
                normalize_space(venue_info.get("address")),
                normalize_space(venue_info.get("city")),
                normalize_space(venue_info.get("state")),
            ]
        ) or venue.default_location
        text_blob = join_non_empty([title, description, categories])

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=join_non_empty([description, f"Category: {categories}" if categories else None]),
                venue_name=venue.venue_name,
                location_text=location_text,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_vt_activity_type(title=title, description=description, category=categories),
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


def _parse_brattleboro_events(
    payload: dict[str, object],
    *,
    venue: VtVenueConfig,
    start_day: date,
) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    detail_map = {
        normalize_space(item.get("url")): item.get("html") or ""
        for item in payload.get("details") or []
    }
    soup = BeautifulSoup(list_html, "html.parser")
    rows: list[ExtractedActivity] = []

    for card in soup.select("div.kutu"):
        title_anchor = card.select_one("h2 a[href]")
        date_tag = card.select_one("div.event_date")
        if title_anchor is None or date_tag is None:
            continue

        source_url = absolute_url(venue.list_url, title_anchor.get("href"))
        if "brattleboromuseum.org" not in httpx.URL(source_url).host:
            continue

        title = normalize_space(title_anchor.get_text(" ", strip=True))
        excerpt = normalize_space(
            card.select_one("div.events_excerpt").get_text(" ", strip=True)
            if card.select_one("div.events_excerpt")
            else ""
        )
        detail_html = detail_map.get(source_url, "")
        detail_description = _extract_brattleboro_detail_description(detail_html)
        description = join_non_empty([detail_description, excerpt])

        if not _should_keep_event(title=title, description=description, category=None):
            continue

        start_at, end_at = _parse_brattleboro_start_end(
            normalize_space(date_tag.get_text(" ", strip=True)),
            start_day=start_day,
        )
        if start_at is None or start_at.date() < start_day:
            continue

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
                activity_type=_infer_vt_activity_type(title=title, description=description, category=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **price_classification_kwargs(text_blob, default_is_free=None),
            )
        )

    return rows


def _parse_fleming_events(payload: dict[str, object], *, venue: VtVenueConfig, start_day: date) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []

    for item in payload.get("details") or []:
        source_url = normalize_space(item.get("url")) or venue.list_url
        html = item.get("html") or ""
        soup = BeautifulSoup(html, "html.parser")
        for event_obj in _extract_ld_json_event_objects(soup):
            title = normalize_space(event_obj.get("name"))
            description = normalize_space(event_obj.get("description"))
            if not _should_keep_event(title=title, description=description, category=None):
                continue

            start_at = _parse_iso_datetime(event_obj.get("startDate"))
            if start_at is None or start_at.date() < start_day:
                continue
            end_at = _parse_iso_datetime(event_obj.get("endDate"))

            text_blob = join_non_empty([title, description])
            location_text = _extract_schema_location_text(event_obj.get("location")) or venue.default_location
            rows.append(
                ExtractedActivity(
                    source_url=normalize_space(event_obj.get("url")) or source_url,
                    title=title,
                    description=description,
                    venue_name=venue.venue_name,
                    location_text=location_text,
                    city=venue.city,
                    state=venue.state,
                    activity_type=_infer_vt_activity_type(title=title, description=description, category=None),
                    age_min=parse_age_range(text_blob)[0],
                    age_max=parse_age_range(text_blob)[1],
                    drop_in=_infer_drop_in(text_blob),
                    registration_required=_infer_registration_required(text_blob),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=NY_TIMEZONE,
                    **price_classification_kwargs(text_blob, default_is_free=None),
                )
            )

    return rows


def _parse_middlebury_events(payload: dict[str, object], *, venue: VtVenueConfig, now: datetime) -> list[ExtractedActivity]:
    html = payload.get("page_html") or ""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []

    for block in soup.select("div.typography.paragraph--text[data-digest-content]"):
        heading = block.find("h2")
        date_heading = block.find("h3")
        if heading is None or date_heading is None:
            continue

        title = normalize_space(heading.get_text(" ", strip=True))
        date_text = normalize_space(date_heading.get_text(" ", strip=True))
        paragraphs = [
            normalize_space(tag.get_text(" ", strip=True))
            for tag in block.find_all("p")
        ]
        description = join_non_empty(paragraphs)

        if not _should_keep_event(title=title, description=description, category=None):
            continue

        start_at, end_at = _parse_middlebury_start_end(date_text, now=now)
        if start_at is None or start_at.date() < now.date():
            continue

        text_blob = join_non_empty([title, description, date_text])
        rows.append(
            ExtractedActivity(
                source_url=venue.list_url,
                title=title,
                description=description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type=_infer_vt_activity_type(title=title, description=description, category=None),
                age_min=parse_age_range(text_blob)[0],
                age_max=parse_age_range(text_blob)[1],
                drop_in=_infer_drop_in(text_blob),
                registration_required=_infer_registration_required(text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **price_classification_kwargs(text_blob, default_is_free=None),
            )
        )

    return rows


def _extract_brattleboro_detail_urls(list_html: str, list_url: str) -> list[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    urls: list[str] = []
    for anchor in soup.select("h2 a[href]"):
        href = normalize_space(anchor.get("href"))
        if not href:
            continue
        url = absolute_url(list_url, href)
        if "brattleboromuseum.org" not in httpx.URL(url).host:
            continue
        urls.append(url)
    return urls


def _extract_brattleboro_detail_description(html: str) -> str | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    parts: list[str] = []
    for selector in (".entry p", ".post p", "article p"):
        for tag in soup.select(selector):
            text = normalize_space(tag.get_text(" ", strip=True))
            if not text:
                continue
            parts.append(text)
    return join_non_empty(parts)


def _extract_fleming_detail_urls(widget_js: str, *, venue: VtVenueConfig) -> list[str]:
    html = _decode_localist_widget_html(widget_js)
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    for anchor in soup.select(".em-card-sm_title a[href]"):
        href = normalize_space(anchor.get("href"))
        if not href:
            continue
        urls.append(absolute_url(venue.list_url, href.split("?", 1)[0]))
    return urls


def _decode_localist_widget_html(widget_js: str) -> str:
    match = LOCALIST_INNER_HTML_RE.search(widget_js)
    if match is None:
        return widget_js
    try:
        return json.loads(match.group("html"))
    except json.JSONDecodeError:
        return widget_js


def _extract_ld_json_event_objects(soup: BeautifulSoup) -> list[dict]:
    event_objects: list[dict] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.string or script.get_text() or ""
        if not text.strip():
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue
        event_objects.extend(_iter_event_objects(data))
    return event_objects


def _iter_event_objects(value: object) -> list[dict]:
    found: list[dict] = []
    stack: list[object] = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            kind = current.get("@type")
            if kind == "Event" or (isinstance(kind, list) and "Event" in kind):
                found.append(current)
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return found


def _extract_schema_location_text(value: object) -> str | None:
    if isinstance(value, str):
        return normalize_space(value) or None
    if isinstance(value, dict):
        return join_non_empty(
            [
                normalize_space(value.get("name")),
                normalize_space(value.get("address")),
                normalize_space(value.get("streetAddress")),
                normalize_space(value.get("addressLocality")),
                normalize_space(value.get("addressRegion")),
            ]
        )
    return None


def _parse_middlebury_start_end(date_text: str, *, now: datetime) -> tuple[datetime | None, datetime | None]:
    blob = normalize_space(unescape(date_text)).replace("\xa0", " ")
    range_match = MIDDLEBURY_RANGE_RE.match(blob)
    if range_match:
        year = int(range_match.group("year") or now.year)
        base_date = parse_date_text(
            f"{range_match.group('month1')} {range_match.group('day1')}, {year}"
        )
        if base_date is None:
            return None, None
        return parse_time_range(base_date=base_date, time_text=None)

    match = MIDDLEBURY_DATE_RE.match(blob)
    if match is None:
        return None, None

    year = int(match.group("year") or now.year)
    base_date = parse_date_text(f"{match.group('month')} {match.group('day')}, {year}")
    if base_date is None:
        return None, None

    return parse_time_range(base_date=base_date, time_text=match.group("time"))


def _parse_brattleboro_start_end(date_text: str, *, start_day: date) -> tuple[datetime | None, datetime | None]:
    blob = normalize_space(unescape(date_text)).replace("\xa0", " ")
    year_match = BRATTLEBORO_YEAR_RE.match(blob)
    if year_match is not None:
        base_date = parse_date_text(
            f"{year_match.group('month')} {year_match.group('day')}, {year_match.group('year')}"
        )
        if base_date is None:
            return None, None
        return parse_time_range(base_date=base_date, time_text=None)

    match = BRATTLEBORO_DATE_RE.match(blob)
    if match is None:
        return None, None

    base_date = parse_date_text(
        f"{match.group('month')} {match.group('day')}, {start_day.year}"
    )
    if base_date is None:
        return None, None
    if base_date < start_day:
        next_candidate = parse_date_text(
            f"{match.group('month')} {match.group('day')}, {start_day.year + 1}"
        )
        if next_candidate is not None:
            base_date = next_candidate

    return parse_time_range(base_date=base_date, time_text=match.group("time"))


def _coerce_start_date(value: str | date | datetime | None) -> date:
    if value is None:
        return datetime.now(VT_TZ).date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = parse_date_text(value)
    if parsed is None:
        raise ValueError(f"Invalid VT start date: {value}")
    return parsed


def _parse_iso_datetime(value: object) -> datetime | None:
    text = normalize_space(str(value or ""))
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(VT_TZ).replace(tzinfo=None)
    return parsed


def _should_keep_event(*, title: str, description: str | None, category: str | None) -> bool:
    title_blob = f" {normalize_space(unescape(title)).lower()} "
    body_blob = f" {normalize_space(unescape(join_non_empty([description, category]) or '')).lower()} "
    combined = f"{title_blob}{body_blob}"

    if any(_has_marker(title_blob, marker) for marker in HARD_REJECT_TITLE_MARKERS):
        return False
    if any(_has_marker(body_blob, marker) for marker in HARD_REJECT_BODY_MARKERS):
        return False

    include_hit = any(_has_marker(combined, marker) for marker in INCLUDE_MARKERS)
    exclude_hit = any(_has_marker(title_blob, marker) for marker in EXCLUDE_MARKERS)
    strong_body_exclude_hit = any(_has_marker(body_blob, marker) for marker in EXCLUDE_MARKERS)

    if "cancelled" in combined or "canceled" in combined:
        return False
    if exclude_hit and not include_hit:
        return False
    if strong_body_exclude_hit and not include_hit:
        return False
    return include_hit


def _infer_vt_activity_type(*, title: str, description: str | None, category: str | None) -> str:
    blob = " ".join(
        normalize_space(unescape(part)).lower()
        for part in (title, description, category)
        if normalize_space(unescape(part if part is not None else ""))
    )
    if _has_marker(blob, "museum abcs") or _has_marker(blob, "family day") or _has_marker(blob, "coloring"):
        return "workshop"
    if _has_marker(blob, "lecture"):
        return "lecture"
    if (
        _has_marker(blob, "talk")
        or _has_marker(blob, "conversation")
        or _has_marker(blob, "discussion")
        or _has_marker(blob, "presentation")
        or _has_marker(blob, "presentations")
        or _has_marker(blob, "webinar")
    ):
        return "talk"
    if _has_marker(blob, "class") or _has_marker(blob, "classes") or _has_marker(blob, "homeschool"):
        return "class"
    if _has_marker(blob, "workshop") or _has_marker(blob, "studio"):
        return "workshop"
    return infer_activity_type(title, description=description, category=category)


def _infer_drop_in(text: str | None) -> bool | None:
    blob = f" {normalize_space(unescape(text)).lower()} "
    if not blob.strip():
        return None
    if "drop-in" in blob or "drop in" in blob or "all are welcome" in blob:
        return True
    return False


def _infer_registration_required(text: str | None) -> bool | None:
    blob = f" {normalize_space(unescape(text)).lower()} "
    if not blob.strip():
        return None
    if "no registration" in blob or "registration is encouraged" in blob or "no admission or reservation is required" in blob:
        return False
    return any(_has_marker(blob, marker) for marker in REGISTRATION_MARKERS)


def _has_marker(blob: str, marker: str) -> bool:
    pattern = re.escape(normalize_space(marker).lower()).replace(r"\ ", r"\s+")
    return re.search(rf"(?<!\w){pattern}(?!\w)", normalize_space(blob).lower()) is not None
