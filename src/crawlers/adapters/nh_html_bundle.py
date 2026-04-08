from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from urllib.parse import quote
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

NY_TIMEZONE = "America/New_York"

CURRIER_SINGLE_DATE_RE = re.compile(
    r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})"
    r"(?:\s+and\s+(?:(?P<second_month>[A-Za-z]+)\s+)?(?P<second_day>\d{1,2}))?",
    re.IGNORECASE,
)
CURRIER_RANGE_RE = re.compile(
    r"(?P<month1>[A-Za-z]+)\s+(?P<day1>\d{1,2})\s+through\s+(?P<month2>[A-Za-z]+)\s+(?P<day2>\d{1,2})",
    re.IGNORECASE,
)
MONTH_DAY_SHORT_RE = re.compile(
    r"^(?P<month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2})$",
    re.IGNORECASE,
)
INLINE_TIME_RE = re.compile(
    r"(?P<time>"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)\s+to\s+(?:Noon|Midnight|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))"
    r")",
    re.IGNORECASE,
)
HOOD_NEXT_OFFSET_RE = re.compile(r"(?:\?|&)offset=\d+", re.IGNORECASE)
NHAA_EVENT_PATH_RE = re.compile(r"^/events/[^?#]+$", re.IGNORECASE)

EXTRA_INCLUDE_MARKERS = (
    " art venture ",
    " artmaking ",
    " art making ",
    " creative practice ",
    " community day ",
    " gallery talk ",
    " hands-on ",
    " hands on ",
    " maker ",
    " maker drop-in ",
)
HARD_REJECT_MARKERS = (
    " opening reception ",
    " last day to view ",
    " exhibition tour ",
    " hood after 5 ",
    " benefit concert ",
    " concert ",
    " fundraiser ",
    " fundraising ",
    " guided hike ",
    " mimosa ",
    " mimosas ",
    " poetry after hours ",
    " storytime ",
)


@dataclass(frozen=True, slots=True)
class NhHtmlVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    default_location: str
    mode: str
    fetch_url: str | None = None
    source_prefixes: tuple[str, ...] = ()


NH_HTML_VENUES: tuple[NhHtmlVenueConfig, ...] = (
    NhHtmlVenueConfig(
        slug="currier",
        source_name="nh_currier_events",
        venue_name="Currier Museum of Art",
        city="Manchester",
        state="NH",
        list_url="https://www.currier.org/events-calendar",
        fetch_url="https://www.currier.org/blank-3-1",
        default_location="Currier Museum of Art, Manchester, NH",
        mode="currier",
        source_prefixes=(
            "https://www.currier.org/blank-3-1",
            "https://www.currier.org/events-calendar",
        ),
    ),
    NhHtmlVenueConfig(
        slug="hood",
        source_name="nh_hood_events",
        venue_name="Hood Museum of Art",
        city="Hanover",
        state="NH",
        list_url="https://hoodmuseum.dartmouth.edu/events",
        default_location="Hood Museum of Art, Hanover, NH",
        mode="hood",
        source_prefixes=(
            "https://hoodmuseum.dartmouth.edu/events",
            "https://hoodmuseum.dartmouth.edu/events/event",
        ),
    ),
    NhHtmlVenueConfig(
        slug="nhaa",
        source_name="nh_nhaa_events",
        venue_name="New Hampshire Art Association",
        city="Portsmouth",
        state="NH",
        list_url="https://www.nhartassociation.org/events",
        default_location="136 State St, Portsmouth, NH",
        mode="nhaa",
        source_prefixes=("https://www.nhartassociation.org/events",),
    ),
)

NH_HTML_VENUES_BY_SLUG = {venue.slug: venue for venue in NH_HTML_VENUES}


class NhHtmlBundleAdapter(BaseSourceAdapter):
    source_name = "nh_html_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_nh_html_bundle_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_nh_html_bundle_payload/parse_nh_html_events from the script runner.")


async def load_nh_html_bundle_payload(
    *,
    venues: list[NhHtmlVenueConfig] | None = None,
    page_limit: int | None = None,
) -> dict:
    selected_venues = venues or list(NH_HTML_VENUES)
    payload_by_slug: dict[str, dict] = {}
    errors_by_slug: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for venue in selected_venues:
            try:
                if venue.mode == "currier":
                    payload_by_slug[venue.slug] = await _load_currier_payload(venue=venue, client=client)
                elif venue.mode == "hood":
                    payload_by_slug[venue.slug] = await _load_hood_payload(
                        venue=venue,
                        client=client,
                        page_limit=page_limit,
                    )
                elif venue.mode == "nhaa":
                    payload_by_slug[venue.slug] = await _load_nhaa_payload(venue=venue, client=client)
                else:
                    raise RuntimeError(f"Unsupported NH venue mode: {venue.mode}")
            except Exception as exc:
                errors_by_slug[venue.slug] = str(exc)

    return {
        "payload_by_slug": payload_by_slug,
        "errors_by_slug": errors_by_slug,
    }


def parse_nh_html_events(
    payload: dict,
    *,
    venue: NhHtmlVenueConfig,
) -> list[ExtractedActivity]:
    if venue.mode == "currier":
        return _parse_currier_events(payload, venue=venue)
    if venue.mode == "hood":
        return _parse_hood_events(payload, venue=venue)
    if venue.mode == "nhaa":
        return _parse_nhaa_events(payload, venue=venue)
    raise RuntimeError(f"Unsupported NH venue mode: {venue.mode}")


def get_nh_source_prefixes() -> list[str]:
    prefixes = {
        prefix.rstrip("/")
        for venue in NH_HTML_VENUES
        for prefix in venue.source_prefixes
    }
    return sorted(prefixes)


async def _load_currier_payload(
    *,
    venue: NhHtmlVenueConfig,
    client: httpx.AsyncClient,
) -> dict:
    fetch_url = venue.fetch_url or venue.list_url
    html = await fetch_html(fetch_url, referer=venue.list_url, client=client)
    return {
        "html": html,
        "fetch_url": fetch_url,
    }


async def _load_hood_payload(
    *,
    venue: NhHtmlVenueConfig,
    client: httpx.AsyncClient,
    page_limit: int | None,
) -> dict:
    max_pages = page_limit or 12
    cards: list[dict] = []
    seen_pages: set[str] = set()
    next_url: str | None = venue.list_url

    while next_url and next_url not in seen_pages and len(seen_pages) < max_pages:
        seen_pages.add(next_url)
        html = await fetch_html(next_url, referer=venue.list_url, client=client)
        soup = BeautifulSoup(html, "html.parser")
        cards.extend(_extract_hood_cards(soup, base_url=venue.list_url))
        next_url = _extract_hood_next_url(soup, current_url=next_url)

    return {"cards": cards}


async def _load_nhaa_payload(
    *,
    venue: NhHtmlVenueConfig,
    client: httpx.AsyncClient,
) -> dict:
    list_html = await fetch_html(venue.list_url, client=client)
    soup = BeautifulSoup(list_html, "html.parser")

    detail_html_by_url: dict[str, str] = {}
    for detail_url in _extract_nhaa_detail_urls(soup, venue.list_url):
        detail_html_by_url[detail_url] = await fetch_html(detail_url, referer=venue.list_url, client=client)

    return {
        "list_html": list_html,
        "detail_html_by_url": detail_html_by_url,
    }


def _parse_currier_events(payload: dict, *, venue: NhHtmlVenueConfig) -> list[ExtractedActivity]:
    html = payload.get("html") or ""
    fetch_url = str(payload.get("fetch_url") or venue.fetch_url or venue.list_url)
    soup = BeautifulSoup(html, "html.parser")
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for section in soup.find_all("section"):
        register_links = [
            anchor for anchor in section.find_all("a", href=True)
            if "blackbaudhosting.com" in (anchor.get("href") or "")
        ]
        if not register_links:
            continue

        if any("ages-" in (anchor.get("href") or "").lower() for anchor in register_links):
            section_rows = _parse_currier_kids_section(
                section=section,
                venue=venue,
                fetch_url=fetch_url,
                today=today,
            )
        else:
            row = _parse_currier_adult_section(
                section=section,
                venue=venue,
                fetch_url=fetch_url,
                today=today,
            )
            section_rows = [row] if row is not None else []

        for row in section_rows:
            if row.start_at.date() < today:
                continue
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _parse_currier_adult_section(
    *,
    section,
    venue: NhHtmlVenueConfig,
    fetch_url: str,
    today: date,
) -> ExtractedActivity | None:
    text = normalize_space(section.get_text(" ", strip=True))
    if not text:
        return None

    title = normalize_space(section.find("h4").get_text(" ", strip=True) if section.find("h4") else "")
    if not title or not _should_keep_event(title=title, description=text):
        return None

    paragraphs = [
        normalize_space(node.get_text(" ", strip=True))
        for node in section.find_all("p")
        if normalize_space(node.get_text(" ", strip=True))
    ]
    if len(paragraphs) < 3:
        return None

    date_text = paragraphs[0]
    time_text = _normalize_time_text(paragraphs[1])
    tag_text = paragraphs[2]
    description_parts = paragraphs[3:]
    description = join_non_empty(description_parts)

    date_match = CURRIER_SINGLE_DATE_RE.search(date_text)
    if date_match is None:
        return None
    start_date = _build_inferred_date(
        month_text=date_match.group("month"),
        day_text=date_match.group("day"),
        today=today,
    )
    if start_date is None:
        return None

    start_at, end_at = parse_time_range(base_date=start_date, time_text=time_text)
    if start_at is None:
        return None

    extra_notes: list[str] = []
    second_day = date_match.group("second_day")
    if second_day:
        second_month = date_match.group("second_month") or date_match.group("month")
        extra_notes.append(f"Additional session date: {normalize_space(second_month)} {second_day}, {start_date.year}")
    extra_notes.append(f"Format: {tag_text}")

    register_url = next(
        (
            (anchor.get("href") or "").strip()
            for anchor in section.find_all("a", href=True)
            if "blackbaudhosting.com" in (anchor.get("href") or "")
        ),
        "",
    )
    if register_url:
        extra_notes.append(f"Registration: {register_url}")

    full_description = join_non_empty([description, *extra_notes])
    return ExtractedActivity(
        source_url=_fragment_url(fetch_url, title),
        title=title,
        description=full_description,
        venue_name=venue.venue_name,
        location_text=venue.default_location,
        city=venue.city,
        state=venue.state,
        activity_type="workshop" if "workshop" in tag_text.lower() else infer_activity_type(title, full_description, tag_text),
        age_min=None,
        age_max=None,
        drop_in=False,
        registration_required=True,
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **price_classification_kwargs(join_non_empty([text, register_url]), default_is_free=None),
    )


def _parse_currier_kids_section(
    *,
    section,
    venue: NhHtmlVenueConfig,
    fetch_url: str,
    today: date,
) -> list[ExtractedActivity]:
    children = [child for child in section.children if getattr(child, "name", None)]
    content_blocks = [child for child in children if normalize_space(child.get_text(" ", strip=True))]
    if len(content_blocks) < 4:
        return []

    header_text = normalize_space(content_blocks[0].get_text(" ", strip=True))
    range_match = CURRIER_RANGE_RE.search(header_text)
    if range_match is None:
        return []

    after_range = header_text[range_match.end():].strip()
    if not after_range or " Cost:" not in after_range:
        return []

    subtitle_and_description, _, cost_tail = after_range.partition(" Cost:")
    subtitle, _, description = subtitle_and_description.partition(" Get ready")
    subtitle = normalize_space(subtitle)
    description = normalize_space(f"Get ready{description}") if description else normalize_space(subtitle_and_description)
    base_title = normalize_space(
        header_text[:range_match.start()].strip() or "April Vacation Art Ventures"
    )
    shared_title = join_non_empty([base_title, subtitle]) or base_title
    shared_cost = f"Cost: {normalize_space(cost_tail)}"
    start_date = _build_inferred_date(
        month_text=range_match.group("month1"),
        day_text=range_match.group("day1"),
        today=today,
    )
    end_date_note = f"Runs through {normalize_space(range_match.group('month2'))} {range_match.group('day2')}, {start_date.year}" if start_date else None
    if start_date is None:
        return []

    rows: list[ExtractedActivity] = []
    for block in content_blocks[1:]:
        register_link = block.find("a", href=True)
        if register_link is None:
            continue
        href = (register_link.get("href") or "").strip()
        if "blackbaudhosting.com" not in href:
            continue

        block_text = normalize_space(block.get_text(" ", strip=True))
        age_text = normalize_space(block.find("h3").get_text(" ", strip=True) if block.find("h3") else "")
        time_match = INLINE_TIME_RE.search(block_text)
        time_text = _normalize_time_text(time_match.group("time")) if time_match else None
        start_at, end_at = parse_time_range(base_date=start_date, time_text=time_text)
        if start_at is None:
            continue

        age_min, age_max = parse_age_range(age_text)
        title = shared_title if not age_text else f"{shared_title} ({age_text})"
        full_description = join_non_empty(
            [
                description,
                shared_cost,
                end_date_note,
                f"Schedule: {age_text} | {normalize_space(time_text)}" if age_text and time_text else None,
                f"Registration: {href}",
            ]
        )
        text_blob = join_non_empty([title, full_description])
        if not _should_keep_event(title=title, description=text_blob):
            continue

        rows.append(
            ExtractedActivity(
                source_url=_fragment_url(fetch_url, f"{title}-{age_min or 'all'}"),
                title=title,
                description=full_description,
                venue_name=venue.venue_name,
                location_text=venue.default_location,
                city=venue.city,
                state=venue.state,
                activity_type="workshop",
                age_min=age_min,
                age_max=age_max,
                drop_in=False,
                registration_required=True,
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **price_classification_kwargs(join_non_empty([text_blob, href]), default_is_free=None),
            )
        )

    return rows


def _extract_hood_cards(soup: BeautifulSoup, *, base_url: str) -> list[dict]:
    cards: list[dict] = []
    for article in soup.select("article.listing-item--event"):
        anchor = article.select_one("a[href*='/events/event?event=']")
        if anchor is None:
            continue
        detail_url = absolute_url(base_url, anchor.get("href"))
        title = normalize_space(
            article.select_one(".event-item__desc").get_text(" ", strip=True)
            if article.select_one(".event-item__desc")
            else ""
        )
        date_spans = [
            normalize_space(span.get_text(" ", strip=True))
            for span in article.select(".event-item__date span")
            if normalize_space(span.get_text(" ", strip=True))
        ]
        summary = normalize_space(article.select_one("p").get_text(" ", strip=True) if article.select_one("p") else "")
        if not title or len(date_spans) < 2:
            continue
        cards.append(
            {
                "detail_url": detail_url,
                "title": title,
                "date_text": date_spans[0],
                "time_text": _normalize_time_text(date_spans[1]),
                "summary": summary,
            }
        )
    return cards


def _extract_hood_next_url(soup: BeautifulSoup, *, current_url: str) -> str | None:
    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        label = normalize_space(anchor.get_text(" ", strip=True))
        if "next" not in label.lower():
            continue
        if not HOOD_NEXT_OFFSET_RE.search(href):
            continue
        return absolute_url(current_url, href)
    return None


def _parse_hood_events(payload: dict, *, venue: NhHtmlVenueConfig) -> list[ExtractedActivity]:
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in payload.get("cards") or []:
        base_date = _parse_month_day_short(card.get("date_text"), today=today)
        if base_date is None:
            continue
        start_at, end_at = parse_time_range(base_date=base_date, time_text=card.get("time_text"))
        if start_at is None or start_at.date() < today:
            continue

        description = card.get("summary") or None
        if not _should_keep_event(title=card.get("title"), description=description):
            continue

        text_blob = join_non_empty([card.get("title"), description])
        row = ExtractedActivity(
            source_url=card["detail_url"],
            title=card["title"],
            description=description,
            venue_name=venue.venue_name,
            location_text=venue.default_location,
            city=venue.city,
            state=venue.state,
            activity_type=infer_activity_type(card["title"], description),
            age_min=None,
            age_max=None,
            drop_in=("drop-in" in normalize_space(text_blob).lower() if text_blob else False),
            registration_required=False,
            start_at=start_at,
            end_at=end_at,
            timezone=NY_TIMEZONE,
            **price_classification_kwargs(text_blob, default_is_free=None),
        )
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _extract_nhaa_detail_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select("article.eventlist-event a.eventlist-title-link[href]"):
        href = (anchor.get("href") or "").strip()
        if not NHAA_EVENT_PATH_RE.match(href):
            continue
        detail_url = absolute_url(base_url, href)
        if detail_url in seen:
            continue
        seen.add(detail_url)
        urls.append(detail_url)
    return urls


def _parse_nhaa_events(payload: dict, *, venue: NhHtmlVenueConfig) -> list[ExtractedActivity]:
    list_html = payload.get("list_html") or ""
    detail_html_by_url = payload.get("detail_html_by_url") or {}
    soup = BeautifulSoup(list_html, "html.parser")
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for article in soup.select("article.eventlist-event--upcoming"):
        title = normalize_space(
            article.select_one("a.eventlist-title-link").get_text(" ", strip=True)
            if article.select_one("a.eventlist-title-link")
            else ""
        )
        detail_href = article.select_one("a.eventlist-title-link[href]")
        if not title or detail_href is None:
            continue
        detail_url = absolute_url(venue.list_url, detail_href.get("href"))

        date_node = article.select_one("time.event-date")
        date_text = normalize_space(date_node.get_text(" ", strip=True) if date_node else "")
        base_date = parse_date_text(date_text)
        if base_date is None or base_date < today:
            continue

        start_node = article.select_one("time.event-time-localized-start")
        end_node = article.select_one("time.event-time-localized-end")
        time_text = join_non_empty(
            [
                normalize_space(start_node.get_text(" ", strip=True) if start_node else ""),
                normalize_space(end_node.get_text(" ", strip=True) if end_node else ""),
            ]
        )
        time_text = _normalize_time_text(time_text.replace(" | ", " to ") if time_text else "")
        start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
        if start_at is None:
            continue

        excerpt = normalize_space(
            article.select_one(".eventlist-excerpt").get_text(" ", strip=True)
            if article.select_one(".eventlist-excerpt")
            else ""
        )
        detail_html = detail_html_by_url.get(detail_url, "")
        detail_soup = BeautifulSoup(detail_html, "html.parser") if detail_html else None
        detail_body = normalize_space(
            detail_soup.select_one(".eventitem-column-content").get_text(" ", strip=True)
            if detail_soup and detail_soup.select_one(".eventitem-column-content")
            else ""
        )
        register_url = _extract_nhaa_register_url(detail_soup) if detail_soup else None
        description = join_non_empty(
            [
                detail_body or excerpt,
                f"Registration: {register_url}" if register_url else None,
            ]
        )

        if not _should_keep_event(title=title, description=description or excerpt):
            continue

        location_text = normalize_space(
            detail_soup.select(".eventitem-meta")[1].get_text(" ", strip=True)
            if detail_soup and len(detail_soup.select(".eventitem-meta")) > 1
            else venue.default_location
        )

        row = ExtractedActivity(
            source_url=detail_url,
            title=title,
            description=description,
            venue_name=venue.venue_name,
            location_text=location_text,
            city=venue.city,
            state=venue.state,
            activity_type=infer_activity_type(title, description or excerpt),
            age_min=None,
            age_max=None,
            drop_in=False,
            registration_required=bool(register_url or "reservation" in (description or excerpt).lower()),
            start_at=start_at,
            end_at=end_at,
            timezone=NY_TIMEZONE,
            **price_classification_kwargs(join_non_empty([title, description, excerpt]), default_is_free=None),
        )
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _extract_nhaa_register_url(detail_soup: BeautifulSoup | None) -> str | None:
    if detail_soup is None:
        return None
    for anchor in detail_soup.find_all("a", href=True):
        label = normalize_space(anchor.get_text(" ", strip=True)).lower()
        href = (anchor.get("href") or "").strip()
        if "register" in label or "reservation" in label:
            return href
    return None


def _should_keep_event(
    *,
    title: str | None,
    description: str | None = None,
    category: str | None = None,
) -> bool:
    normalized_title = normalize_space(title)
    if not normalized_title:
        return False

    combined = f" {normalize_space(join_non_empty([normalized_title, description, category]) or '').lower()} "
    if any(marker in combined for marker in HARD_REJECT_MARKERS):
        return False

    if should_include_event(title=normalized_title, description=description, category=category):
        return True

    return any(marker in combined for marker in EXTRA_INCLUDE_MARKERS)


def _parse_month_day_short(value: str | None, *, today: date) -> date | None:
    text = normalize_space(value)
    if not text:
        return None
    match = MONTH_DAY_SHORT_RE.match(text)
    if match is None:
        return None
    return _build_inferred_date(match.group("month"), match.group("day"), today=today)


def _build_inferred_date(month_text: str, day_text: str, *, today: date) -> date | None:
    try:
        candidate = datetime.strptime(
            f"{normalize_space(month_text)} {normalize_space(day_text)} {today.year}",
            "%B %d %Y",
        ).date()
    except ValueError:
        try:
            candidate = datetime.strptime(
                f"{normalize_space(month_text)} {normalize_space(day_text)} {today.year}",
                "%b %d %Y",
            ).date()
        except ValueError:
            return None

    if candidate < today and (today - candidate).days > 45:
        return candidate.replace(year=candidate.year + 1)
    return candidate


def _normalize_time_text(value: str | None) -> str | None:
    text = normalize_space(value)
    if not text:
        return None
    return (
        text.replace("Noon", "12 PM")
        .replace("noon", "12 PM")
        .replace("Midnight", "12 AM")
        .replace("midnight", "12 AM")
    )


def _fragment_url(base_url: str, value: str) -> str:
    slug = quote(
        normalize_space(value).lower().replace(" ", "-"),
        safe="-",
    )
    return f"{base_url}#{slug}"


def _source_roots() -> list[str]:
    roots: set[str] = set()
    for venue in NH_HTML_VENUES:
        for url in {venue.list_url, venue.fetch_url, *venue.source_prefixes}:
            if not url:
                continue
            parsed = urlparse(url)
            if parsed.scheme and parsed.netloc:
                roots.add(f"{parsed.scheme}://{parsed.netloc}")
    return sorted(roots)


NH_SOURCE_ROOTS = _source_roots()
