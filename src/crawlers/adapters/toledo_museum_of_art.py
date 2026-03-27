from datetime import datetime

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_month_day_year
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


TOLEDO_EVENTS_URL = "https://toledomuseum.org/events"
TOLEDO_VENUE_NAME = "Toledo Museum of Art"
TOLEDO_CITY = "Toledo"
TOLEDO_STATE = "OH"
TOLEDO_LOCATION = "Toledo Museum of Art, Toledo, OH"


async def load_toledo_museum_of_art_payload() -> str:
    return await fetch_html(TOLEDO_EVENTS_URL)


def parse_toledo_museum_of_art_payload(html: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in soup.select("li.item"):
        title = normalize_space(item.select_one("h3.sub-md").get_text(" ", strip=True) if item.select_one("h3.sub-md") else "")
        link = item.select_one(".cta a[href]")
        time_text = normalize_space(item.select_one(".event-details li.strong").get_text(" ", strip=True) if item.select_one(".event-details li.strong") else "")
        month_text = normalize_space(item.select_one(".cal-date-mo").get_text(" ", strip=True) if item.select_one(".cal-date-mo") else "")
        day_text = normalize_space(item.select_one(".cal-date-da").get_text(" ", strip=True) if item.select_one(".cal-date-da") else "")
        category = ", ".join(normalize_space(li.get_text(" ", strip=True)) for li in item.select(".event-details li.min"))
        location_bits = [
            normalize_space(li.get_text(" ", strip=True))
            for li in item.select(".event-details li")
            if "Free for All" not in normalize_space(li.get_text(" ", strip=True))
            and "AM" not in normalize_space(li.get_text(" ", strip=True))
            and "PM" not in normalize_space(li.get_text(" ", strip=True))
            and "Adults" not in normalize_space(li.get_text(" ", strip=True))
            and "Families" not in normalize_space(li.get_text(" ", strip=True))
            and "Kids" not in normalize_space(li.get_text(" ", strip=True))
            and "Teens" not in normalize_space(li.get_text(" ", strip=True))
        ]
        description = normalize_space(item.select_one(".notification").get_text(" ", strip=True) if item.select_one(".notification") else "")
        if not title or link is None:
            continue
        if not should_include_event(title=title, description=description, category=category):
            continue

        year = _extract_year(item.get("class", []))
        if year is None:
            continue
        base_date = parse_month_day_year(month_text, day_text, year)
        if base_date is None:
            continue
        start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
        if start_at is None:
            continue

        full_description = join_non_empty([description, category])
        row = ExtractedActivity(
            source_url=absolute_url(TOLEDO_EVENTS_URL, link.get("href")),
            title=title,
            description=full_description,
            venue_name=TOLEDO_VENUE_NAME,
            location_text=join_non_empty(location_bits) or TOLEDO_LOCATION,
            city=TOLEDO_CITY,
            state=TOLEDO_STATE,
            activity_type=infer_activity_type(title, description, category),
            age_min=None,
            age_max=None,
            drop_in=False,
            registration_required=("register" in normalize_space(link.get_text(" ", strip=True)).lower()),
            start_at=start_at,
            end_at=end_at,
            timezone=NY_TIMEZONE,
            **price_classification_kwargs(join_non_empty([full_description, normalize_space(link.get_text(" ", strip=True))])),
        )
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class ToledoMuseumOfArtAdapter(BaseSourceAdapter):
    source_name = "toledo_museum_of_art_events"

    async def fetch(self) -> list[str]:
        return [await load_toledo_museum_of_art_payload()]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_toledo_museum_of_art_payload(payload)


def _extract_year(class_names: list[str]) -> int | None:
    for class_name in class_names:
        if class_name.startswith("event-") and class_name.endswith("-date"):
            parts = class_name.split("-")
            if len(parts) >= 4 and parts[1].isdigit():
                return int(parts[1])
    return None
