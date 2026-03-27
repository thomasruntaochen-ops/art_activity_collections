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


AKRON_EVENTS_URL = "https://akronartmuseum.org/calendar/"
AKRON_VENUE_NAME = "Akron Art Museum"
AKRON_CITY = "Akron"
AKRON_STATE = "OH"
AKRON_LOCATION = "Akron Art Museum, Akron, OH"


async def load_akron_art_museum_payload() -> str:
    return await fetch_html(AKRON_EVENTS_URL)


def parse_akron_art_museum_payload(html: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in soup.select("div.me-event-list-item"):
        link = item.select_one("a.me-event-list-item__link[href]")
        title = normalize_space(item.select_one("h2.me-event-list-item__title").get_text(" ", strip=True) if item.select_one("h2.me-event-list-item__title") else "")
        date_block = item.select_one(".me-event-list-item__text-column > p")
        categories = ", ".join(
            normalize_space(li.get_text(" ", strip=True))
            for li in item.select(".me-event-list-item__text-column ul li")
        )
        if not title or link is None:
            continue
        if not should_include_event(title=title, description=categories, category=categories):
            continue

        lines = [normalize_space(line) for line in date_block.stripped_strings] if date_block else []
        if not lines:
            continue
        date_text = lines[0]
        time_text = lines[1] if len(lines) > 1 else None
        start_at, end_at = parse_datetime_range(date_text=date_text, time_text=time_text)
        if start_at is None:
            continue

        description = join_non_empty([categories])
        row = ExtractedActivity(
            source_url=absolute_url(AKRON_EVENTS_URL, link.get("href")),
            title=title,
            description=description,
            venue_name=AKRON_VENUE_NAME,
            location_text=AKRON_LOCATION,
            city=AKRON_CITY,
            state=AKRON_STATE,
            activity_type=infer_activity_type(title, description, categories),
            age_min=None,
            age_max=None,
            drop_in=False,
            registration_required=("register" in (description or "").lower()),
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


class AkronArtMuseumAdapter(BaseSourceAdapter):
    source_name = "akron_art_museum_events"

    async def fetch(self) -> list[str]:
        return [await load_akron_art_museum_payload()]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_akron_art_museum_payload(payload)
