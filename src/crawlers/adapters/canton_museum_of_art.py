import json
import re
from datetime import date
from datetime import datetime
from datetime import timedelta
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import httpx

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import DEFAULT_HEADERS
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


CANTON_EVENTS_URL = "https://www.cantonart.org/calendar"
CANTON_VENUE_NAME = "Canton Museum of Art"
CANTON_CITY = "Canton"
CANTON_STATE = "OH"
CANTON_LOCATION = "Canton Museum of Art, Canton, OH"
DATE_RANGE_RE = re.compile(
    r"Begins\s+(?P<start>\d{2}/\d{2}/\d{4}),\s+Ends\s+(?P<end>\d{2}/\d{2}/\d{4})",
    re.IGNORECASE,
)
TIME_BLOCK_RE = re.compile(
    r"(?:(?:\d+)\s+[A-Za-z]+s?,\s*)?(?P<time>\d{1,2}:\d{2}\s*[AP]M-\d{1,2}:\d{2}\s*[AP]M)",
    re.IGNORECASE,
)
# "Begins 09/04/2026, Ends 12/04/2026, no class 11/13/2026,11/20/2026"
NO_CLASS_RE = re.compile(
    r"no\s+class\s+(?P<dates>\d{2}/\d{2}/\d{4}(?:\s*,\s*\d{2}/\d{2}/\d{4})*)",
    re.IGNORECASE,
)
# The days field either names the weekday a series meets on ("6 Wednesdays") or
# spells the sessions out ("1 09/02/26,09/09/26,09/16/26, 06:00 PM-08:00 PM").
SESSION_DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{2})(?!\d)")
WEEKDAY_RE = re.compile(
    r"\b(?P<weekday>sunday|monday|tuesday|wednesday|thursday|friday|saturday)s?\b",
    re.IGNORECASE,
)
WEEKDAY_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
# Sanity bound on a Begins/Ends range before expanding it week by week.
MAX_SERIES_DAYS = 365


async def load_canton_museum_of_art_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_html(CANTON_EVENTS_URL, client=client)
        detail_urls = _extract_detail_urls(listing_html)
        detail_pages: dict[str, str] = {}
        for detail_url in detail_urls:
            detail_pages[detail_url] = await fetch_html(detail_url, referer=CANTON_EVENTS_URL, client=client)

    return {
        "listing_html": listing_html,
        "detail_pages": detail_pages,
    }


def parse_canton_museum_of_art_payload(payload: dict) -> list[ExtractedActivity]:
    listing_html = payload.get("listing_html") or ""
    detail_pages = payload.get("detail_pages") or {}
    listing_items = _parse_listing_items(listing_html)
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for detail_url, html in detail_pages.items():
        for row in _build_rows(
            html,
            detail_url=detail_url,
            listing_item=listing_items.get(detail_url),
            today=today,
        ):
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class CantonMuseumOfArtAdapter(BaseSourceAdapter):
    source_name = "canton_museum_of_art_events"

    async def fetch(self) -> list[str]:
        payload = await load_canton_museum_of_art_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_canton_museum_of_art_payload(json.loads(payload))


def _extract_detail_urls(listing_html: str) -> list[str]:
    soup = BeautifulSoup(listing_html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    for anchor in soup.select("div.grid-item h3.eventsummary a[href]"):
        detail_url = absolute_url(CANTON_EVENTS_URL, anchor.get("href"))
        if detail_url in seen:
            continue
        seen.add(detail_url)
        urls.append(detail_url)

    return urls


def _parse_listing_items(listing_html: str) -> dict[str, dict]:
    soup = BeautifulSoup(listing_html, "html.parser")
    items: dict[str, dict] = {}

    for item in soup.select("div.grid-item"):
        link = item.select_one("h3.eventsummary a[href]")
        if link is None:
            continue
        detail_url = absolute_url(CANTON_EVENTS_URL, link.get("href"))
        items[detail_url] = {
            "title": normalize_space(link.get_text(" ", strip=True)),
            "event_type": normalize_space(item.select_one(".event-type").get_text(" ", strip=True) if item.select_one(".event-type") else ""),
            "date_text": normalize_space(item.select_one(".event-date").get_text(" ", strip=True) if item.select_one(".event-date") else ""),
        }

    return items


def _build_rows(
    html: str,
    *,
    detail_url: str,
    listing_item: dict | None,
    today: date,
) -> list[ExtractedActivity]:
    """One row per session still to come in a class series.

    Canton lists a multi-week class as a Begins/Ends range plus the weekday it
    meets on, and /calendar only shows the current month. Keying off the series
    start date alone therefore dropped every class the moment week one passed --
    0 rows on 07/29-07/31 and again on 08/30-08/31, while 11 classes running
    through October sat on the page.
    """
    soup = BeautifulSoup(html, "html.parser")
    title = normalize_space(
        soup.select_one("h1#page-title").get_text(" ", strip=True)
        if soup.select_one("h1#page-title")
        else (listing_item or {}).get("title")
    )
    skill_text = normalize_space(
        soup.select_one(".field-name-field-skill-level").get_text(" ", strip=True)
        if soup.select_one(".field-name-field-skill-level")
        else ""
    )
    description_text = normalize_space(
        soup.select_one(".field-name-body").get_text(" ", strip=True)
        if soup.select_one(".field-name-body")
        else ""
    )
    event_type = normalize_space((listing_item or {}).get("event_type"))
    candidate_blob = f" {normalize_space(join_non_empty([title, event_type, skill_text, description_text]) or '').lower()} "
    if " camp " in candidate_blob:
        return []
    if not should_include_event(title=title, description=description_text, category=join_non_empty([event_type, skill_text])):
        return []

    begin_text = normalize_space(
        soup.select_one(".cuscls_data_begin").get_text(" ", strip=True)
        if soup.select_one(".cuscls_data_begin")
        else ""
    )
    date_match = DATE_RANGE_RE.search(begin_text)
    if date_match is None:
        return []
    start_date = datetime.strptime(date_match.group("start"), "%m/%d/%Y").date()
    end_date = datetime.strptime(date_match.group("end"), "%m/%d/%Y").date()
    if end_date < start_date:
        end_date = start_date

    day_time_text = normalize_space(
        soup.select_one(".cuscls_data_days").get_text(" ", strip=True)
        if soup.select_one(".cuscls_data_days")
        else ""
    )
    skipped_dates = _no_class_dates(begin_text)
    session_dates = [
        session_date
        for session_date in _session_dates(
            start_date=start_date,
            end_date=end_date,
            day_text=day_time_text,
        )
        if session_date >= today and session_date not in skipped_dates
    ]
    if not session_dates:
        return []

    time_match = TIME_BLOCK_RE.search(day_time_text)
    time_text = time_match.group("time") if time_match else None

    room_text = normalize_space(
        soup.select_one(".cuscls_data_room").get_text(" ", strip=True)
        if soup.select_one(".cuscls_data_room")
        else ""
    )
    instructor_text = normalize_space(
        soup.select_one(".cuscls_data_ins").get_text(" ", strip=True)
        if soup.select_one(".cuscls_data_ins")
        else ""
    )
    main_price = normalize_space(
        soup.select_one(".mainprice").get_text(" ", strip=True)
        if soup.select_one(".mainprice")
        else ""
    )
    member_price = normalize_space(
        soup.select_one(".memberprice").get_text(" ", strip=True)
        if soup.select_one(".memberprice")
        else ""
    )
    price_text = join_non_empty([main_price, member_price])

    age_min, age_max = parse_age_range(join_non_empty([skill_text, description_text]))
    location_text = CANTON_LOCATION
    if room_text:
        location_text = f"{room_text}, {CANTON_LOCATION}"

    full_description = join_non_empty(
        [
            skill_text,
            instructor_text,
            description_text,
            price_text,
        ]
    )

    rows: list[ExtractedActivity] = []
    for session_date in session_dates:
        start_at, end_at = parse_time_range(base_date=session_date, time_text=time_text)
        if start_at is None:
            continue
        rows.append(
            ExtractedActivity(
                source_url=detail_url,
                title=title,
                description=full_description,
                venue_name=CANTON_VENUE_NAME,
                location_text=location_text,
                city=CANTON_CITY,
                state=CANTON_STATE,
                activity_type=infer_activity_type(title, full_description, event_type),
                age_min=age_min,
                age_max=age_max,
                drop_in=False,
                registration_required=True,
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                audience_segment=infer_audience_segment(
                    title=title,
                    description=full_description,
                    category=event_type,
                    age_min=age_min,
                    age_max=age_max,
                ),
                **price_classification_kwargs(full_description),
            )
        )

    return rows


def _no_class_dates(begin_text: str) -> set[date]:
    """Dates the Begins/Ends line explicitly excludes ("no class 11/13/2026")."""
    match = NO_CLASS_RE.search(begin_text or "")
    if match is None:
        return set()

    skipped: set[date] = set()
    for raw in match.group("dates").split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            skipped.add(datetime.strptime(raw, "%m/%d/%Y").date())
        except ValueError:
            continue
    return skipped


def _session_dates(*, start_date: date, end_date: date, day_text: str) -> list[date]:
    """Every session a class meets, from either shape the days field takes.

    Canton either spells the dates out ("1 09/02/26,09/09/26, 06:00 PM-08:00 PM")
    or names the weekday and leaves the range implied ("6 Wednesdays"). Anything
    it cannot read falls back to a single session on the start date.
    """
    explicit = _explicit_session_dates(day_text)
    if explicit:
        return explicit

    weekday_index = _weekday_index(day_text)
    if weekday_index is None:
        return [start_date]

    last_date = min(end_date, start_date + timedelta(days=MAX_SERIES_DAYS))
    # Keep start_date itself when it already falls on the meeting weekday.
    cursor = start_date + timedelta(days=(weekday_index - start_date.weekday()) % 7)
    dates: list[date] = []
    while cursor <= last_date:
        dates.append(cursor)
        cursor += timedelta(weeks=1)

    return dates or [start_date]


def _explicit_session_dates(day_text: str) -> list[date]:
    dates: list[date] = []
    for raw in SESSION_DATE_RE.findall(day_text or ""):
        try:
            dates.append(datetime.strptime(raw, "%m/%d/%y").date())
        except ValueError:
            continue
    return sorted(set(dates))


def _weekday_index(day_text: str) -> int | None:
    match = WEEKDAY_RE.search(day_text or "")
    if match is None:
        return None
    return WEEKDAY_INDEX.get(match.group("weekday").lower())
