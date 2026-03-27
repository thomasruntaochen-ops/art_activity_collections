from datetime import datetime

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_datetime_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


CINCINNATI_EVENTS_URL = "https://www.cincinnatiartmuseum.org/events-programs/events-list/"
CINCINNATI_VENUE_NAME = "Cincinnati Art Museum"
CINCINNATI_CITY = "Cincinnati"
CINCINNATI_STATE = "OH"
CINCINNATI_LOCATION = "Cincinnati Art Museum, Cincinnati, OH"


async def load_cincinnati_art_museum_payload() -> str:
    return await fetch_html(CINCINNATI_EVENTS_URL)


def parse_cincinnati_art_museum_payload(html: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in soup.select(".eventResults .eventResult"):
        title = normalize_space(item.select_one("h3").get_text(" ", strip=True) if item.select_one("h3") else "")
        link = item.select_one("a[href]")
        date_text = normalize_space(item.select_one(".bod.date").get_text(" ", strip=True) if item.select_one(".bod.date") else "")
        category = normalize_space(item.select_one(".bod.audienceAndProgram").get_text(" ", strip=True) if item.select_one(".bod.audienceAndProgram") else "")
        description = normalize_space(item.select_one(".bod p").get_text(" ", strip=True) if item.select_one(".bod p") else "")
        if not title or link is None:
            continue
        if not should_include_event(title=title, description=description, category=category):
            continue

        if " at " not in date_text:
            continue
        date_part, time_part = date_text.split(" at ", 1)
        start_at, end_at = parse_datetime_range(date_text=date_part, time_text=time_part)
        if start_at is None:
            continue

        buttons_text = " ".join(normalize_space(button.get_text(" ", strip=True)) for button in item.select(".buttons a"))
        full_description = join_non_empty([description, category, buttons_text])
        row = ExtractedActivity(
            source_url=absolute_url(CINCINNATI_EVENTS_URL, link.get("href")),
            title=title,
            description=full_description,
            venue_name=CINCINNATI_VENUE_NAME,
            location_text=CINCINNATI_LOCATION,
            city=CINCINNATI_CITY,
            state=CINCINNATI_STATE,
            activity_type=infer_activity_type(title, description, category),
            age_min=None,
            age_max=None,
            drop_in=False,
            registration_required=("register" in buttons_text.lower() or "purchase tickets" in buttons_text.lower()),
            start_at=start_at,
            end_at=end_at,
            timezone=NY_TIMEZONE,
            **price_classification_kwargs(full_description),
        )
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class CincinnatiArtMuseumAdapter(BaseSourceAdapter):
    source_name = "cincinnati_art_museum_events"

    async def fetch(self) -> list[str]:
        return [await load_cincinnati_art_museum_payload()]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_cincinnati_art_museum_payload(payload)
