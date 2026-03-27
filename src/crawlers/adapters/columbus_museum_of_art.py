import json
import re
from datetime import date
from datetime import datetime
from datetime import timedelta

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import DEFAULT_HEADERS
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import absolute_url
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


COLUMBUS_EVENTS_URL = "https://www.columbusmuseum.org/events"
COLUMBUS_VENUE_NAME = "Columbus Museum of Art"
COLUMBUS_CITY = "Columbus"
COLUMBUS_STATE = "OH"
COLUMBUS_LOCATION = "Columbus Museum of Art, Columbus, OH"
SITE_ID_RE = re.compile(r"siteId:\s*([0-9]+)")
PAGE_PART_ID_RE = re.compile(r"pagePartId:\s*([0-9]+)")
API_TOKEN_RE = re.compile(r"apiToken:\s*'([^']+)'")
DATE_TIME_TEXT_RE = re.compile(
    r"(?P<weekday>[A-Za-z]+),\s+(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})\s*\|\s*(?P<time>.+)$"
)


async def load_columbus_museum_of_art_payload(*, max_pages: int | None = None) -> dict:
    landing_html = await fetch_html(COLUMBUS_EVENTS_URL)
    site_id = _require_match(SITE_ID_RE, landing_html, "siteId")
    page_part_id = _require_match(PAGE_PART_ID_RE, landing_html, "pagePartId")
    api_token = _require_match(API_TOKEN_RE, landing_html, "apiToken")
    today = datetime.now().date()

    list_cards: list[dict] = []
    detail_pages: dict[str, str] = {}
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        page_number = 1
        while True:
            if max_pages is not None and page_number > max_pages:
                break
            content = await _fetch_render_page(
                client=client,
                site_id=site_id,
                page_part_id=page_part_id,
                api_token=api_token,
                start_date=today,
                end_date=today + timedelta(days=120),
                page_number=page_number,
            )
            cards = _extract_listing_cards(content)
            if not cards:
                break
            list_cards.extend(cards)
            if len(cards) < 30:
                break
            page_number += 1

        for card in list_cards:
            if not _should_include_listing(card):
                continue
            detail_pages[card["source_url"]] = await fetch_html(card["source_url"], client=client, referer=COLUMBUS_EVENTS_URL)

    return {
        "cards": list_cards,
        "detail_pages": detail_pages,
    }


def parse_columbus_museum_of_art_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    today = datetime.now().date()

    for card in payload.get("cards", []):
        if not _should_include_listing(card):
            continue
        row = _build_row(card=card, detail_html=payload.get("detail_pages", {}).get(card["source_url"]), today=today)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class ColumbusMuseumOfArtAdapter(BaseSourceAdapter):
    source_name = "columbus_museum_of_art_events"

    async def fetch(self) -> list[str]:
        payload = await load_columbus_museum_of_art_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_columbus_museum_of_art_payload(json.loads(payload))


async def _fetch_render_page(
    *,
    client: httpx.AsyncClient,
    site_id: str,
    page_part_id: str,
    api_token: str,
    start_date: date,
    end_date: date,
    page_number: int,
) -> str:
    response = await client.get(
        f"https://api.sitewrench.com/pageparts/calendars/{page_part_id}/render",
        params={
            "token": api_token,
            "siteid": site_id,
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "sortBy": "startDate",
            "page": page_number,
            "pageSize": 30,
            "days": (end_date - start_date).days,
        },
    )
    response.raise_for_status()
    return response.json()["Content"]


def _extract_listing_cards(content: str) -> list[dict]:
    soup = BeautifulSoup(content, "html.parser")
    cards: list[dict] = []
    for card in soup.select(".excerpt-event"):
        title = normalize_space(card.select_one(".excerpt-event__title a").get_text(" ", strip=True) if card.select_one(".excerpt-event__title a") else "")
        href = card.select_one(".excerpt-event__title a[href]")
        if not title or href is None:
            continue
        tags = [normalize_space(node.get_text(" ", strip=True)) for node in card.select(".excerpt-event__tags a")]
        cards.append(
            {
                "title": title,
                "source_url": absolute_url(COLUMBUS_EVENTS_URL, href.get("href")),
                "month_text": normalize_space(
                    card.select_one(".excerpt-event__date--month").get_text(" ", strip=True)
                    if card.select_one(".excerpt-event__date--month")
                    else ""
                ),
                "day_text": normalize_space(
                    card.select_one(".excerpt-event__date--day").get_text(" ", strip=True)
                    if card.select_one(".excerpt-event__date--day")
                    else ""
                ),
                "weekday_text": normalize_space(
                    card.select_one(".excerpt-event__day").get_text(" ", strip=True)
                    if card.select_one(".excerpt-event__day")
                    else ""
                ),
                "time_text": normalize_space(
                    card.select_one(".excerpt-event__time").get_text(" ", strip=True)
                    if card.select_one(".excerpt-event__time")
                    else ""
                ),
                "summary": normalize_space(
                    card.select_one(".excerpt-event__description").get_text(" ", strip=True)
                    if card.select_one(".excerpt-event__description")
                    else ""
                ),
                "tags": tags,
            }
        )
    return cards


def _should_include_listing(card: dict) -> bool:
    return should_include_event(
        title=card["title"],
        description=card.get("summary"),
        category=", ".join(card.get("tags", [])),
    )


def _build_row(*, card: dict, detail_html: str | None, today: date) -> ExtractedActivity | None:
    title = card["title"]
    tags = card.get("tags", [])
    description = card.get("summary")
    detail_text = description
    location_text = COLUMBUS_LOCATION
    start_at: datetime | None = None
    end_at: datetime | None = None

    if detail_html:
        soup = BeautifulSoup(detail_html, "html.parser")
        title = normalize_space(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else title)
        detail_text = normalize_space(
            soup.select_one(".event-detail").get_text(" ", strip=True)
            if soup.select_one(".event-detail")
            else description
        )
        date_time_text = normalize_space(
            soup.select_one(".event-date-time").get_text(" ", strip=True)
            if soup.select_one(".event-date-time")
            else ""
        )
        parsed = _parse_detail_date_time(date_time_text=date_time_text, today=today)
        if parsed is not None:
            start_at, end_at = parsed
        location_text = _extract_location_from_soup(soup) or COLUMBUS_LOCATION

    if start_at is None:
        base_date = _infer_listing_date(card["month_text"], card["day_text"], today=today)
        start_at, end_at = parse_time_range(base_date=base_date, time_text=card.get("time_text"))

    if start_at is None or start_at.date() < today:
        return None

    full_description = join_non_empty([detail_text, f"Tags: {', '.join(tags)}" if tags else None])
    price_text = full_description
    return ExtractedActivity(
        source_url=card["source_url"],
        title=title,
        description=full_description,
        venue_name=COLUMBUS_VENUE_NAME,
        location_text=location_text,
        city=COLUMBUS_CITY,
        state=COLUMBUS_STATE,
        activity_type=infer_activity_type(title, full_description, ", ".join(tags)),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in (full_description or "").lower() or "drop in" in (full_description or "").lower()),
        registration_required="register" in (full_description or "").lower(),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        **price_classification_kwargs(price_text),
    )


def _parse_detail_date_time(*, date_time_text: str, today: date) -> tuple[datetime | None, datetime | None] | None:
    match = DATE_TIME_TEXT_RE.match(normalize_space(date_time_text))
    if match is None:
        return None
    base_date = _infer_listing_date(match.group("month"), match.group("day"), today=today)
    return parse_time_range(base_date=base_date, time_text=match.group("time"))


def _extract_location_from_soup(soup: BeautifulSoup) -> str | None:
    location_paragraph = soup.find("p", string=lambda value: value and normalize_space(value).startswith("Location:"))
    if location_paragraph is None:
        return None
    location = normalize_space(location_paragraph.get_text(" ", strip=True).replace("Location:", "", 1))
    return location or None


def _infer_listing_date(month_text: str, day_text: str, *, today: date) -> date:
    month = datetime.strptime(normalize_space(month_text), "%b" if len(normalize_space(month_text)) <= 3 else "%B").month
    day = int(day_text)
    candidate = date(today.year, month, day)
    if (today - candidate).days > 180:
        return date(today.year + 1, month, day)
    return candidate


def _require_match(pattern: re.Pattern[str], text: str, label: str) -> str:
    match = pattern.search(text)
    if match is None:
        raise RuntimeError(f"Columbus Museum of Art page missing {label}")
    return match.group(1)
