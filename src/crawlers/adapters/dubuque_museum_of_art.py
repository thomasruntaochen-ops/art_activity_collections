from __future__ import annotations

import json
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import DEFAULT_HEADERS
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_date_text
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

DUBUQUE_EXPERIENCES_URL = "https://dbqart.org/experiences"
DUBUQUE_SITE_ROOT = "https://dbqart.org/"
DUBUQUE_VENUE_NAME = "Dubuque Museum of Art"
DUBUQUE_CITY = "Dubuque"
DUBUQUE_STATE = "IA"
DUBUQUE_TIMEZONE = "America/Chicago"
DUBUQUE_LOCATION = "Dubuque Museum of Art, Dubuque, IA"

SCRIPT_URL_RE = re.compile(r'<script src="(?P<url>https://dbqart\.org/experiences/script\?v=[^"]+)"')
DATA_ARRAY_RE = re.compile(r"data:\s*(\[\s*.*?\])\s*,\s*selectedExperienceType", re.DOTALL)
UNQUOTED_KEY_RE = re.compile(r'(?<=[{,])\s*([A-Za-z_][A-Za-z0-9_]*)\s*:')
EXCLUDE_MARKERS = (
    "concert",
    "dinner",
    "film",
    "fundraiser",
    "music",
    "performance",
    "poetry",
    "reception",
    "screening",
    "tour",
    "yoga",
)


async def load_dubuque_museum_of_art_payload() -> dict:
    events_payload: list[dict] = []

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        experiences_html = await fetch_html(client, DUBUQUE_EXPERIENCES_URL, referer=DUBUQUE_EXPERIENCES_URL)
        script_url = _extract_script_url(experiences_html)
        script_text = await fetch_html(client, script_url, referer=DUBUQUE_EXPERIENCES_URL)

        for event in _extract_events(script_text):
            if not event.get("published", True) or event.get("channel") != "experiences":
                continue
            source_url = urljoin(DUBUQUE_SITE_ROOT, f"{event['channel']}/{event['url_title']}")
            detail_html = await fetch_html(client, source_url, referer=DUBUQUE_EXPERIENCES_URL)
            events_payload.append(
                {
                    "event": event,
                    "source_url": source_url,
                    "detail_html": detail_html,
                }
            )

    return {"events": events_payload}


class DubuqueMuseumOfArtAdapter(BaseSourceAdapter):
    source_name = "dubuque_museum_of_art_events"

    async def fetch(self) -> list[str]:
        payload = await load_dubuque_museum_of_art_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_dubuque_museum_of_art_payload(json.loads(payload))


def parse_dubuque_museum_of_art_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in payload.get("events", []):
        row = _build_row(item)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _build_row(item: dict) -> ExtractedActivity | None:
    event = item.get("event") or {}
    title = normalize_space(event.get("title"))
    if not title:
        return None

    soup = BeautifulSoup(item.get("detail_html", ""), "html.parser")
    about_node = soup.select_one(".about")
    about_text = normalize_space(about_node.get_text(" ", strip=True)) if about_node else ""
    highlight_text = "; ".join(
        normalize_space(node.get_text(" ", strip=True))
        for node in soup.select(".highlights .highlight")
        if normalize_space(node.get_text(" ", strip=True))
    )
    type_text = " | ".join(
        normalize_space(type_item.get("title"))
        for type_item in event.get("types", [])
        if normalize_space(type_item.get("title"))
    )
    category_text = " | ".join(
        normalize_space(category.get("title"))
        for category in event.get("categories", [])
        if normalize_space(category.get("title"))
    )
    subhead = normalize_space(event.get("subhead"))
    description = join_non_empty([about_text, f"Highlights: {highlight_text}" if highlight_text else None, subhead])
    if not _should_keep(title, description or "", type_text, category_text):
        return None

    start_at = _parse_datetime_value(event.get("start_date"))
    if start_at is None:
        return None
    end_at = _parse_datetime_value(event.get("end_date"))

    location_text = normalize_space(event.get("event_location")) or DUBUQUE_LOCATION
    price_text = normalize_space(event.get("price"))
    tickets_link = normalize_space(event.get("tickets_link"))
    blob = " ".join(part for part in [title, description or "", type_text, category_text, price_text] if part)

    return ExtractedActivity(
        source_url=item["source_url"],
        title=title,
        description=description,
        venue_name=DUBUQUE_VENUE_NAME,
        location_text=location_text,
        city=DUBUQUE_CITY,
        state=DUBUQUE_STATE,
        activity_type=_infer_activity_type(blob),
        age_min=None,
        age_max=None,
        drop_in=not bool(tickets_link),
        registration_required=bool(tickets_link),
        start_at=start_at,
        end_at=end_at,
        timezone=DUBUQUE_TIMEZONE,
        **price_classification_kwargs(" ".join(part for part in [price_text, description] if part)),
    )


async def fetch_html(client: httpx.AsyncClient, url: str, *, referer: str | None = None) -> str:
    headers = dict(DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer
    response = await client.get(url, headers=headers)
    response.raise_for_status()
    return response.text


def _extract_script_url(html: str) -> str:
    match = SCRIPT_URL_RE.search(html)
    if match is None:
        raise RuntimeError("Unable to locate Dubuque experiences script URL.")
    return match.group("url")


def _extract_events(script_text: str) -> list[dict]:
    match = DATA_ARRAY_RE.search(script_text)
    if match is None:
        return []

    json_like = UNQUOTED_KEY_RE.sub(lambda matched: f' "{matched.group(1)}":', match.group(1))
    json_like = re.sub(r",\s*([}\]])", r"\1", json_like)
    payload = json.loads(json_like)
    return [item for item in payload if isinstance(item, dict)]


def _parse_datetime_value(value: str | None) -> datetime | None:
    text = normalize_space(value)
    if not text:
        return None

    for fmt in ("%A, %B %d, %Y %I:%M %p", "%A, %b %d, %Y %I:%M %p"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    base_date = parse_date_text(text)
    if base_date is None:
        return None
    return datetime.combine(base_date, datetime.min.time())


def _should_keep(title: str, description: str, type_text: str, category_text: str) -> bool:
    blob = f" {title.lower()} {description.lower()} {type_text.lower()} {category_text.lower()} "
    has_exclude = any(marker in blob for marker in EXCLUDE_MARKERS)
    strong_include = any(
        marker in blob
        for marker in (
            "art activit",
            "self-guided art activit",
            "hands-on",
            "make art",
        )
    )
    has_include = strong_include or any(
        marker in blob
        for marker in (
            "activity",
            "activities",
            "class",
            "conversation",
            "lecture",
            "talk",
            "workshop",
        )
    )
    if has_exclude and not strong_include:
        return False
    return has_include


def _infer_activity_type(text: str) -> str:
    blob = f" {text.lower()} "
    if "talk" in blob or "lecture" in blob or "conversation" in blob:
        return "talk"
    if "class" in blob:
        return "class"
    return "activity"
