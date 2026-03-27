import json
import re
from datetime import date
from datetime import datetime
from datetime import timedelta

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


CLEVELAND_EVENTS_URL_TEMPLATE = "https://www.clevelandart.org/whats-on/all?page={page}&when=Upcoming"
CLEVELAND_DEFAULT_LOCATION = "Cleveland Museum of Art, Cleveland, OH"
CLEVELAND_VENUE_NAME = "Cleveland Museum of Art"
CLEVELAND_CITY = "Cleveland"
CLEVELAND_STATE = "OH"
TAG_INCLUDE_MARKERS = {"class", "lecture", "talk", "workshop"}
TAG_EXCLUDE_MARKERS = {"performance", "tour"}
WEEKDAY_INDEX = {
    "mon": 0,
    "tue": 1,
    "wed": 2,
    "thu": 3,
    "fri": 4,
    "sat": 5,
    "sun": 6,
}
RECURRING_EVENT_RE = re.compile(
    r"^(?P<frequency>Daily|Weekly on (?P<weekday>[A-Za-z]{3}))"
    r",\s*(?P<time>.+?)\s+from\s+[A-Za-z]{3},\s+(?P<start>[A-Za-z]{3} \d{1,2}, \d{4})"
    r"\s+until\s+[A-Za-z]{3},\s+(?P<end>[A-Za-z]{3} \d{1,2}, \d{4})$"
)
SINGLE_EVENT_RE = re.compile(r"^(?P<date>[A-Za-z]{3} \d{1,2}, \d{4}),\s*(?P<time>.+)$")
DATE_RANGE_RE = re.compile(r"^(?P<month>[A-Za-z]{3})\s+(?P<start_day>\d{1,2})[–-](?P<end_day>\d{1,2}),\s*(?P<year>\d{4})$")


async def load_cleveland_art_museum_payload(*, max_pages: int | None = None) -> dict:
    first_html = await fetch_html(CLEVELAND_EVENTS_URL_TEMPLATE.format(page=0))
    first_page = _extract_page_payload(first_html)
    page_count = int(first_page.get("pageCount") or 1)
    if max_pages is not None:
        page_count = min(page_count, max_pages)

    pages = {0: first_page}
    for page_index in range(1, page_count):
        html = await fetch_html(CLEVELAND_EVENTS_URL_TEMPLATE.format(page=page_index))
        pages[page_index] = _extract_page_payload(html)

    return {"pages": pages}


def parse_cleveland_art_museum_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    today = datetime.now().date()

    for page_payload in payload.get("pages", {}).values():
        for item in page_payload.get("initialItems", []):
            row = _build_row(item)
            if row is None:
                continue
            if row.start_at.date() < today:
                continue
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class ClevelandArtMuseumAdapter(BaseSourceAdapter):
    source_name = "cleveland_art_museum_events"

    async def fetch(self) -> list[str]:
        payload = await load_cleveland_art_museum_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_cleveland_art_museum_payload(json.loads(payload))


def _extract_page_payload(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if script is None or not script.string:
        raise RuntimeError("Cleveland Museum of Art page is missing __NEXT_DATA__")
    data = json.loads(script.string)
    return data["props"]["pageProps"]


def _build_row(item: dict) -> ExtractedActivity | None:
    title = normalize_space(item.get("title"))
    description = normalize_space(item.get("body"))
    tag_names = _extract_tag_names(item)
    tag_text = ", ".join(tag_names)
    if not _should_include_item(title=title, description=description, tag_names=tag_names):
        return None

    start_at, end_at = _parse_event_date(item.get("eventDate"))
    if start_at is None:
        return None

    source_url = f"https://www.clevelandart.org{normalize_space(item.get('cta'))}"
    location_text = join_non_empty([item_location for item_location in item.get("eventLocation", [])]) or CLEVELAND_DEFAULT_LOCATION
    ticket_info = normalize_space(item.get("ticketInfo"))
    full_description = join_non_empty([description, f"Tags: {tag_text}" if tag_text else None, ticket_info])
    price_text = join_non_empty([ticket_info, full_description])

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=CLEVELAND_VENUE_NAME,
        location_text=location_text,
        city=CLEVELAND_CITY,
        state=CLEVELAND_STATE,
        activity_type=infer_activity_type(title, description, tag_text),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in (full_description or "").lower() or "drop in" in (full_description or "").lower()),
        registration_required=(
            ("ticket required" in price_text.lower() and "no ticket required" not in price_text.lower())
            or "register" in price_text.lower()
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **price_classification_kwargs(price_text),
    )


def _parse_event_date(value: str | None) -> tuple[datetime | None, datetime | None]:
    lines = _normalize_event_date_lines(value)
    if not lines:
        return None, None

    today = datetime.now().date()
    upcoming: list[tuple[datetime, datetime | None]] = []
    fallback: list[tuple[datetime, datetime | None]] = []

    for line in lines:
        parsed = _parse_event_date_line(line, today=today)
        if parsed is None or parsed[0] is None:
            continue
        fallback.append(parsed)
        if parsed[0].date() >= today:
            upcoming.append(parsed)

    if upcoming:
        return min(upcoming, key=lambda pair: pair[0])
    if fallback:
        return min(fallback, key=lambda pair: pair[0])
    return None, None


def _extract_tag_names(item: dict) -> list[str]:
    return [
        normalize_space(tag.get("text"))
        for tag in item.get("tags", [])
        if isinstance(tag, dict) and normalize_space(tag.get("text"))
    ]


def _should_include_item(*, title: str, description: str | None, tag_names: list[str]) -> bool:
    tag_set = {tag.lower() for tag in tag_names}
    if "sold out" in tag_set:
        return False
    if tag_set & TAG_INCLUDE_MARKERS:
        return True
    if tag_set & TAG_EXCLUDE_MARKERS:
        return False
    return should_include_event(title=title, description=description, category=", ".join(tag_names))


def _normalize_event_date_lines(value: str | None) -> list[str]:
    if not value:
        return []
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text("\n", strip=True)
    return [normalize_space(line) for line in text.splitlines() if normalize_space(line)]


def _parse_event_date_line(
    text: str,
    *,
    today: date,
) -> tuple[datetime | None, datetime | None] | None:
    recurring_match = RECURRING_EVENT_RE.match(text)
    if recurring_match:
        start_date = datetime.strptime(recurring_match.group("start"), "%b %d, %Y").date()
        end_date = datetime.strptime(recurring_match.group("end"), "%b %d, %Y").date()
        occurrence_date = _next_occurrence_date(
            frequency=recurring_match.group("frequency"),
            weekday=recurring_match.group("weekday"),
            start_date=start_date,
            end_date=end_date,
            today=today,
        )
        if occurrence_date is None:
            return None
        return parse_time_range(base_date=occurrence_date, time_text=recurring_match.group("time"))

    single_match = SINGLE_EVENT_RE.match(text)
    if single_match:
        base_date = datetime.strptime(single_match.group("date"), "%b %d, %Y").date()
        return parse_time_range(base_date=base_date, time_text=single_match.group("time"))

    range_match = DATE_RANGE_RE.match(text)
    if range_match:
        base_date = datetime.strptime(
            f"{range_match.group('month')} {range_match.group('start_day')}, {range_match.group('year')}",
            "%b %d, %Y",
        ).date()
        return parse_time_range(base_date=base_date, time_text=None)

    return None


def _next_occurrence_date(
    *,
    frequency: str,
    weekday: str | None,
    start_date: date,
    end_date: date,
    today: date,
) -> date | None:
    candidate = max(start_date, today)
    if frequency == "Daily":
        return candidate if candidate <= end_date else None

    if weekday is None:
        return None

    weekday_index = WEEKDAY_INDEX.get(weekday.lower())
    if weekday_index is None:
        return None

    offset = (weekday_index - candidate.weekday()) % 7
    candidate += timedelta(days=offset)
    if candidate < start_date:
        candidate = start_date
    return candidate if candidate <= end_date else None
