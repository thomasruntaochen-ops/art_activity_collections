import json
from datetime import datetime

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


MOCA_EVENTS_URL = "https://www.mocacleveland.org/events"
MOCA_VENUE_NAME = "Museum of Contemporary Art Cleveland"
MOCA_CITY = "Cleveland"
MOCA_STATE = "OH"
MOCA_LOCATION = "Museum of Contemporary Art Cleveland, Cleveland, OH"


async def load_moca_cleveland_payload() -> str:
    return await fetch_html(MOCA_EVENTS_URL)


def parse_moca_cleveland_payload(html: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.select_one("script#wix-warmup-data")
    if script is None or not script.string:
        raise RuntimeError("moCa Cleveland page is missing wix-warmup-data")

    data = json.loads(script.string)
    records = data["appsWarmupData"]["dataBinding"]["dataStore"]["recordsByCollectionId"]["moCaEvents"]

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    today = datetime.now().date()

    for item in records.values():
        row = _build_row(item)
        if row is None or row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class MocaClevelandAdapter(BaseSourceAdapter):
    source_name = "moca_cleveland_events"

    async def fetch(self) -> list[str]:
        return [await load_moca_cleveland_payload()]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_moca_cleveland_payload(payload)


def _build_row(item: dict) -> ExtractedActivity | None:
    title = normalize_space(item.get("title"))
    short_description = normalize_space(item.get("shortDecription"))
    long_description = normalize_space(_rich_text_to_text(item.get("longDescription")))
    category = ", ".join(normalize_space(value) for value in item.get("arraystring", []) if normalize_space(value))
    description = join_non_empty([short_description, long_description, f"Series: {category}" if category else None, item.get("cost")])
    combined = " ".join(part.lower() for part in (title, short_description, long_description, category) if part)
    if title.lower().startswith("ciff:"):
        return None
    if "donor event" in combined or "donor members" in combined or "members only" in combined:
        return None
    if " film " in f" {combined} " and not any(marker in combined for marker in ("artist talk", "workshop", "art-making", "artmaking")):
        return None
    if not should_include_event(title=title, description=description, category=category):
        return None

    base_date = parse_date_text(item.get("eventDate"))
    if base_date is None:
        return None
    start_at, end_at = parse_time_range(base_date=base_date, time_text=item.get("time"))
    if start_at is None:
        return None

    location_text = normalize_space(item.get("location")) or MOCA_LOCATION
    full_description = join_non_empty(
        [
            short_description,
            long_description,
            item.get("cost"),
            f"Series: {category}" if category else None,
            f"Registration: {item.get('registrationLInk')}" if normalize_space(item.get("registrationLInk")) else None,
        ]
    )
    price_text = join_non_empty([item.get("cost"), full_description])

    return ExtractedActivity(
        source_url=normalize_space(item.get("url")),
        title=title,
        description=full_description,
        venue_name=MOCA_VENUE_NAME,
        location_text=location_text,
        city=MOCA_CITY,
        state=MOCA_STATE,
        activity_type=infer_activity_type(title, full_description, category),
        age_min=None,
        age_max=None,
        drop_in=False,
        registration_required=bool(normalize_space(item.get("registrationLInk"))) or "registration" in (full_description or "").lower(),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **price_classification_kwargs(price_text),
    )


def _rich_text_to_text(value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and "textData" in value:
        return normalize_space(value["textData"].get("text"))
    if isinstance(value, dict):
        return join_non_empty([_rich_text_to_text(node) for node in value.get("nodes", [])]) or ""
    if isinstance(value, list):
        return join_non_empty([_rich_text_to_text(node) for node in value]) or ""
    if isinstance(value, (int, float)):
        return str(value)
    if hasattr(value, "get"):
        if "textData" in value:
            return normalize_space(value["textData"].get("text"))
        return join_non_empty([_rich_text_to_text(node) for node in value.get("nodes", [])]) or ""
    return ""
