from datetime import datetime

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_datetime_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


ALLEN_EVENTS_URL = "https://amam.oberlin.edu/exhibitions-events/events"
ALLEN_VENUE_NAME = "Allen Memorial Art Museum"
ALLEN_CITY = "Oberlin"
ALLEN_STATE = "OH"
ALLEN_LOCATION = "Allen Memorial Art Museum, Oberlin, OH"


async def load_allen_memorial_art_museum_payload() -> str:
    return await fetch_html(ALLEN_EVENTS_URL)


def parse_allen_memorial_art_museum_payload(html: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for anchor in soup.select("#events-list a.event[href]"):
        title = normalize_space(anchor.select_one("h5").get_text(" ", strip=True) if anchor.select_one("h5") else "")
        description = normalize_space(anchor.select_one("p:not(.heading)").get_text(" ", strip=True) if anchor.select_one("p:not(.heading)") else "")
        heading = normalize_space(anchor.select_one("p.heading").get_text(" ", strip=True) if anchor.select_one("p.heading") else "")
        if not should_include_event(title=title, description=description):
            continue

        if " at " not in heading:
            continue
        date_text, time_text = heading.split(" at ", 1)
        start_at, end_at = parse_datetime_range(date_text=date_text, time_text=time_text)
        if start_at is None:
            continue

        row = ExtractedActivity(
            source_url=absolute_url("https://amam.oberlin.edu/", anchor.get("href")),
            title=title,
            description=description or None,
            venue_name=ALLEN_VENUE_NAME,
            location_text=ALLEN_LOCATION,
            city=ALLEN_CITY,
            state=ALLEN_STATE,
            activity_type=infer_activity_type(title, description),
            age_min=None,
            age_max=None,
            drop_in=False,
            registration_required=("register" in description.lower()),
            start_at=start_at,
            end_at=end_at,
            timezone=NY_TIMEZONE,
            **price_classification_kwargs(description),
        )
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class AllenMemorialArtMuseumAdapter(BaseSourceAdapter):
    source_name = "allen_memorial_art_museum_events"

    async def fetch(self) -> list[str]:
        return [await load_allen_memorial_art_museum_payload()]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_allen_memorial_art_museum_payload(payload)
