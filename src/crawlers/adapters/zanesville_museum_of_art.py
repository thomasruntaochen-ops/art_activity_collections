import calendar
import json
from datetime import date, datetime
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
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


# The /calendar/ widget page was removed in Sept 2026, so read the zma-calendar
# events collection's Squarespace JSON instead. Each month view holds only that
# month, and late in a month every event is already past -- so pool a few months.
ZANESVILLE_EVENTS_URL = "https://www.zanesvilleart.org/zma-calendar"
ZANESVILLE_MONTHS_AHEAD = 3
ZANESVILLE_VENUE_NAME = "Zanesville Museum of Art"
ZANESVILLE_CITY = "Zanesville"
ZANESVILLE_STATE = "OH"
ZANESVILLE_LOCATION = "Zanesville Museum of Art, Zanesville, OH"
EXPLICIT_INCLUDE_MARKERS = (
    "book apart",
    "minute with a masterpiece",
    "photo hunt",
)
EXPLICIT_EXCLUDE_MARKERS = (
    "concert",
    "puzzler",
)


async def load_zanesville_museum_of_art_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        detail_urls: list[str] = []
        for month_url in _build_month_urls(datetime.now(ZoneInfo(NY_TIMEZONE)).date()):
            month_payload = json.loads(await fetch_html(month_url, referer=ZANESVILLE_EVENTS_URL, client=client))
            for item in month_payload.get("items") or []:
                full_url = normalize_space(item.get("fullUrl"))
                if not full_url:
                    continue
                detail_url = absolute_url(ZANESVILLE_EVENTS_URL, full_url)
                if detail_url not in detail_urls:
                    detail_urls.append(detail_url)

        detail_pages: dict[str, str] = {}
        for detail_url in detail_urls:
            detail_pages[detail_url] = await fetch_html(detail_url, referer=ZANESVILLE_EVENTS_URL, client=client)

    return {"detail_pages": detail_pages}


def _build_month_urls(today: date) -> list[str]:
    urls: list[str] = []
    year, month = today.year, today.month
    for _ in range(ZANESVILLE_MONTHS_AHEAD):
        urls.append(f"{ZANESVILLE_EVENTS_URL}?format=json&month={calendar.month_name[month].lower()}-{year}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return urls


def parse_zanesville_museum_of_art_payload(payload: dict) -> list[ExtractedActivity]:
    detail_pages = payload.get("detail_pages") or {}
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for detail_url, html in detail_pages.items():
        row = _build_row(html, detail_url=detail_url, today=today)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class ZanesvilleMuseumOfArtAdapter(BaseSourceAdapter):
    source_name = "zanesville_museum_of_art_events"

    async def fetch(self) -> list[str]:
        payload = await load_zanesville_museum_of_art_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_zanesville_museum_of_art_payload(json.loads(payload))


def _build_row(html: str, *, detail_url: str, today) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    event_obj = _extract_event_object(soup)
    title = normalize_space(
        soup.select_one(".eventitem-title").get_text(" ", strip=True)
        if soup.select_one(".eventitem-title")
        else (event_obj or {}).get("name")
    )
    description_text = normalize_space(
        soup.select_one(".sqs-html-content").get_text("\n", strip=True)
        if soup.select_one(".sqs-html-content")
        else (
            (soup.find("meta", attrs={"property": "og:description"}) or {}).get("content")
            or ""
        )
    )
    category_text = ", ".join(
        normalize_space(link.get_text(" ", strip=True))
        for link in soup.select(".eventitem-meta-cats-tags-container a")
    )
    title_blob = title.lower()
    combined_blob = join_non_empty([title, description_text, category_text]) or ""
    combined_lower = combined_blob.lower()

    explicit_include = any(marker in combined_lower for marker in EXPLICIT_INCLUDE_MARKERS)
    if not (should_include_event(title=title, description=description_text, category=category_text) or explicit_include):
        return None
    if any(marker in title_blob or marker in combined_lower for marker in EXPLICIT_EXCLUDE_MARKERS):
        return None

    start_at = _parse_iso_datetime((event_obj or {}).get("startDate"))
    end_at = _parse_iso_datetime((event_obj or {}).get("endDate"))
    if start_at is None or start_at.date() < today:
        return None

    location_text = ZANESVILLE_LOCATION
    if "online anytime" in category_text.lower():
        location_text = "Online"

    full_description = join_non_empty(
        [
            description_text,
            f"Categories: {category_text}" if category_text else None,
        ]
    )

    return ExtractedActivity(
        source_url=detail_url,
        title=title,
        description=full_description,
        venue_name=ZANESVILLE_VENUE_NAME,
        location_text=location_text,
        city=ZANESVILLE_CITY,
        state=ZANESVILLE_STATE,
        activity_type=infer_activity_type(title, full_description, category_text),
        age_min=None,
        age_max=None,
        drop_in=False,
        registration_required="register" in combined_lower,
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


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = normalize_space(value)
    if not text:
        return None
    if len(text) > 5 and (text[-5] in {"+", "-"}) and text[-3] != ":":
        text = f"{text[:-2]}:{text[-2:]}"
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        return None
