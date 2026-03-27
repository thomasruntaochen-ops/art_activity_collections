import json
import re
from datetime import date
from datetime import datetime
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
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


DAYTON_EVENTS_URL = "https://www.daytonartinstitute.org/upcoming-events/calendar/"
DAYTON_VENUE_NAME = "Dayton Art Institute"
DAYTON_CITY = "Dayton"
DAYTON_STATE = "OH"
DAYTON_LOCATION = "Dayton Art Institute, Dayton, OH"
DETAIL_PATH_FRAGMENT = "/events/"
TIME_IN_DATE_RE = re.compile(r"\bfrom\s+(.+)$", re.IGNORECASE)
LABEL_PATTERNS = {
    "date": re.compile(r"\bDate:\s*(.+?)(?=\b(?:Time|Cost|Where|Location|Ages):|$)", re.IGNORECASE | re.DOTALL),
    "time": re.compile(r"\bTime:\s*(.+?)(?=\b(?:Date|Cost|Where|Location|Ages):|$)", re.IGNORECASE | re.DOTALL),
    "cost": re.compile(r"\bCost:\s*(.+?)(?=\b(?:Date|Time|Where|Location|Ages):|$)", re.IGNORECASE | re.DOTALL),
    "where": re.compile(r"\bWhere:\s*(.+?)(?=\b(?:Date|Time|Cost|Location|Ages):|$)", re.IGNORECASE | re.DOTALL),
    "location": re.compile(r"\bLocation:\s*(.+?)(?=\b(?:Date|Time|Cost|Where|Ages):|$)", re.IGNORECASE | re.DOTALL),
    "ages": re.compile(r"\bAges:\s*(.+?)(?=\b(?:Date|Time|Cost|Where|Location):|$)", re.IGNORECASE | re.DOTALL),
}


async def load_dayton_art_institute_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_html(DAYTON_EVENTS_URL, client=client)
        listing_items = _extract_listing_items(listing_html)
        detail_pages: dict[str, str] = {}
        for detail_url in listing_items:
            detail_pages[detail_url] = await fetch_html(detail_url, referer=DAYTON_EVENTS_URL, client=client)

    return {
        "listing_items": listing_items,
        "detail_pages": detail_pages,
    }


def parse_dayton_art_institute_payload(payload: dict) -> list[ExtractedActivity]:
    listing_items = payload.get("listing_items") or {}
    detail_pages = payload.get("detail_pages") or {}
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for detail_url, html in detail_pages.items():
        row = _build_row(
            html,
            detail_url=detail_url,
            listing_item=listing_items.get(detail_url),
            today=today,
        )
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class DaytonArtInstituteAdapter(BaseSourceAdapter):
    source_name = "dayton_art_institute_events"

    async def fetch(self) -> list[str]:
        payload = await load_dayton_art_institute_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_dayton_art_institute_payload(json.loads(payload))


def _extract_listing_items(listing_html: str) -> dict[str, dict]:
    soup = BeautifulSoup(listing_html, "html.parser")
    items: dict[str, dict] = {}

    for article in soup.select("article.mec-event-article"):
        link = article.select_one(".mec-event-title a[href]")
        if link is None:
            continue
        detail_url = absolute_url(DAYTON_EVENTS_URL, link.get("href"))
        if DETAIL_PATH_FRAGMENT not in detail_url:
            continue
        title = normalize_space(link.get_text(" ", strip=True))
        if not _listing_title_is_candidate(title):
            continue
        items[detail_url] = {
            "title": title,
            "time_text": normalize_space(
                article.select_one(".mec-event-time").get_text(" ", strip=True)
                if article.select_one(".mec-event-time")
                else ""
            ),
        }

    return items


def _build_row(
    html: str,
    *,
    detail_url: str,
    listing_item: dict | None,
    today: date,
) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    title = normalize_space(
        soup.select_one(".mec-single-title").get_text(" ", strip=True)
        if soup.select_one(".mec-single-title")
        else (listing_item or {}).get("title")
    )
    description_block = soup.select_one(".mec-single-event-description")
    description_raw = description_block.get_text("\n", strip=True) if description_block else ""
    description_text = normalize_space(description_raw)
    if not _listing_title_is_candidate(normalize_space((listing_item or {}).get("title")) or title):
        return None

    event_obj = _extract_event_object(soup)
    start_at = _parse_iso_datetime((event_obj or {}).get("startDate"))
    end_at = _parse_iso_datetime((event_obj or {}).get("endDate"))

    field_lines = _extract_field_lines(description_raw)
    date_field = field_lines.get("date")
    time_field = field_lines.get("time")
    cost_field = field_lines.get("cost")
    location_field = field_lines.get("location") or field_lines.get("where")
    ages_field = field_lines.get("ages")

    base_date = start_at.date() if start_at is not None else _parse_date_from_field(date_field)
    inline_time = None
    if time_field:
        inline_time = time_field
    elif date_field:
        time_match = TIME_IN_DATE_RE.search(date_field)
        if time_match:
            inline_time = time_match.group(1)
    elif (listing_item or {}).get("time_text"):
        inline_time = normalize_space((listing_item or {}).get("time_text")).replace("i ", "", 1)

    if base_date is not None and inline_time:
        parsed_start, parsed_end = parse_time_range(base_date=base_date, time_text=inline_time)
        start_at = parsed_start or start_at
        end_at = parsed_end or end_at

    if start_at is None or start_at.date() < today:
        return None

    full_description = join_non_empty(
        [
            description_text,
            f"Cost: {cost_field}" if cost_field else None,
        ]
    )
    age_min, age_max = parse_age_range(join_non_empty([ages_field, description_text]))
    location_text = normalize_space(location_field) or DAYTON_LOCATION

    return ExtractedActivity(
        source_url=detail_url,
        title=title,
        description=full_description,
        venue_name=DAYTON_VENUE_NAME,
        location_text=location_text,
        city=DAYTON_CITY,
        state=DAYTON_STATE,
        activity_type=infer_activity_type(title, full_description),
        age_min=age_min,
        age_max=age_max,
        drop_in="drop-in" in description_text.lower() or "drop ins encouraged" in description_text.lower() or "no signup necessary" in description_text.lower(),
        registration_required=(
            "no signup necessary" not in description_text.lower()
            and (
                "register" in description_text.lower()
                or "sign up" in description_text.lower()
                or "signup" in description_text.lower()
            )
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **price_classification_kwargs(full_description),
    )


def _extract_event_object(soup: BeautifulSoup) -> dict | None:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = (script.get_text() or "").strip()
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        found = _find_event_object(data)
        if found is not None:
            return found
    return None


def _find_event_object(value: object) -> dict | None:
    if isinstance(value, dict):
        value_type = value.get("@type")
        if value_type == "Event" or (isinstance(value_type, list) and "Event" in value_type):
            return value
        for nested in value.values():
            found = _find_event_object(nested)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _find_event_object(nested)
            if found is not None:
                return found
    return None


def _extract_labeled_field(text: str, label: str) -> str | None:
    match = LABEL_PATTERNS[label].search(text)
    if match is None:
        return None
    return normalize_space(match.group(1))


def _extract_field_lines(raw_text: str) -> dict[str, str]:
    lines = [normalize_space(line) for line in raw_text.splitlines() if normalize_space(line)]
    values: dict[str, str] = {}
    for index, line in enumerate(lines):
        for label in ("date", "time", "cost", "where", "location", "ages"):
            prefix = f"{label.capitalize()}:"
            if line.lower() == prefix.lower():
                if index + 1 < len(lines):
                    values[label] = lines[index + 1]
                break
            if line.lower().startswith(prefix.lower()):
                values[label] = normalize_space(line[len(prefix) :])
                break
    return values


def _listing_title_is_candidate(title: str) -> bool:
    title_text = normalize_space(title)
    if not title_text:
        return False
    lowered = title_text.lower()
    if "family open studio" in lowered:
        return True
    if any(marker in lowered for marker in ("talk", "lecture", "workshop", "class", "studio", "discussion")):
        return True
    return should_include_event(title=title_text)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = normalize_space(value)
    if not text:
        return None
    if len(text) > 5 and (text[-5] in {"+", "-"}) and text[-3] != ":":
        text = f"{text[:-2]}:{text[-2:]}"
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        if len(text) == 10:
            try:
                return datetime.strptime(text, "%Y-%m-%d")
            except ValueError:
                return None
        return None


def _parse_date_from_field(value: str | None) -> date | None:
    text = normalize_space(value)
    if not text or text.lower().startswith("every "):
        return None
    cleaned = re.sub(r"\bfrom\s+.+$", "", text, flags=re.IGNORECASE).strip(" ,")
    return parse_date_text(cleaned)
