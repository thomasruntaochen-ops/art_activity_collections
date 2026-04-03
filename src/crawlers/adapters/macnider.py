from __future__ import annotations

import json
import re
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import DEFAULT_HEADERS
from src.crawlers.adapters.oh_common import AGE_PLUS_RE
from src.crawlers.adapters.oh_common import AGE_RANGE_RE
from src.crawlers.adapters.oh_common import fetch_html
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_month_day_year
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.pipeline.pricing import price_classification_kwargs_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

MACNIDER_CALENDAR_URL = "https://macniderart.org/events-calendar/"
MACNIDER_CATEGORY_URLS = (
    "https://macniderart.org/product-category/multiage-art-classes/",
    "https://macniderart.org/product-category/youth-art-classes/",
)
MACNIDER_VENUE_NAME = "Charles H. MacNider Art Museum"
MACNIDER_CITY = "Mason City"
MACNIDER_STATE = "IA"
MACNIDER_TIMEZONE = "America/Chicago"
MACNIDER_LOCATION = "Charles H. MacNider Art Museum, Mason City, IA"

DATE_LINE_RE = re.compile(
    r"(?:Mon|Tue|Tues|Wed|Thu|Thur|Thurs|Fri|Sat|Sun)\.?,?\s+"
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})(?:,\s*(?P<year>\d{4}))?"
    r"(?:\s+from\s+(?P<time>[^\n\r]+))?",
    re.IGNORECASE,
)
PRICE_AMOUNT_RE = re.compile(r"\$\s*(\d+(?:\.\d{1,2})?)")
AGE_PLUS_FALLBACK_RE = re.compile(r"\bages?\s*(\d{1,2})\+", re.IGNORECASE)
SKIP_TITLE_MARKERS = (
    "donation",
    "membership",
)


async def load_macnider_payload() -> dict:
    items: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for category_url in MACNIDER_CATEGORY_URLS:
            category_html = await fetch_html(category_url, referer=MACNIDER_CALENDAR_URL, client=client)
            soup = BeautifulSoup(category_html, "html.parser")
            for product in soup.select("li.product"):
                link = product.select_one("a.woocommerce-LoopProduct-link[href]")
                title_node = product.select_one("h2.woocommerce-loop-product__title")
                if link is None or title_node is None:
                    continue

                title = normalize_space(title_node.get_text(" ", strip=True))
                if not title or _should_skip_listing(title):
                    continue

                source_url = urljoin(category_url, link.get("href", "").strip())
                if source_url in seen_urls:
                    continue
                seen_urls.add(source_url)

                detail_html = await fetch_html(source_url, referer=category_url, client=client)
                items.append(
                    {
                        "source_url": source_url,
                        "category_url": category_url,
                        "detail_html": detail_html,
                    }
                )

    return {"items": items}


class MacNiderAdapter(BaseSourceAdapter):
    source_name = "macnider_events"

    async def fetch(self) -> list[str]:
        payload = await load_macnider_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_macnider_payload(json.loads(payload))


def parse_macnider_payload(payload: dict) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in payload.get("items", []):
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


def _build_row(item: dict[str, str]) -> ExtractedActivity | None:
    soup = BeautifulSoup(item.get("detail_html", ""), "html.parser")
    title_node = soup.select_one("h1.product_title")
    if title_node is None:
        return None

    title = normalize_space(title_node.get_text(" ", strip=True))
    if not title or _should_skip_listing(title):
        return None

    description_node = soup.select_one(".woocommerce-product-details__short-description")
    raw_description = description_node.get_text("\n", strip=True) if description_node else ""
    description_text = normalize_space(raw_description)
    if not description_text:
        return None

    start_at, end_at = _extract_datetime(raw_description)
    if start_at is None:
        return None

    price_node = soup.select_one("p.price")
    price_text = normalize_space(price_node.get_text(" ", strip=True)) if price_node else ""
    amount = _extract_price_amount(price_text)
    category_text = normalize_space(soup.select_one(".product_meta .posted_in").get_text(" ", strip=True) if soup.select_one(".product_meta .posted_in") else "")
    age_min, age_max = _parse_age_range(title, description_text)

    description = join_non_empty(
        [
            description_text,
            category_text,
            f"Price: {price_text}" if price_text else None,
        ]
    )

    return ExtractedActivity(
        source_url=item["source_url"],
        title=title,
        description=description,
        venue_name=MACNIDER_VENUE_NAME,
        location_text=MACNIDER_LOCATION,
        city=MACNIDER_CITY,
        state=MACNIDER_STATE,
        activity_type="class",
        age_min=age_min,
        age_max=age_max,
        drop_in=False,
        registration_required=True,
        start_at=start_at,
        end_at=end_at,
        timezone=MACNIDER_TIMEZONE,
        **price_classification_kwargs_from_amount(amount, text=price_text or description_text),
    )


def _should_skip_listing(title: str) -> bool:
    lowered = f" {title.lower()} "
    return any(marker in lowered for marker in SKIP_TITLE_MARKERS)


def _extract_datetime(description_text: str) -> tuple[datetime | None, datetime | None]:
    match = DATE_LINE_RE.search(description_text)
    if match is None:
        return None, None

    year_text = match.group("year")
    if year_text:
        event_date = parse_month_day_year(match.group("month"), match.group("day"), int(year_text))
    else:
        event_date = _infer_event_date(match.group("month"), match.group("day"))
    if event_date is None:
        return None, None

    return parse_time_range(base_date=event_date, time_text=match.group("time"))


def _infer_event_date(month_text: str, day_text: str) -> datetime.date | None:
    today = datetime.now().date()
    candidate = parse_month_day_year(month_text, day_text, today.year)
    if candidate is None:
        return None
    if candidate < today - timedelta(days=30):
        return parse_month_day_year(month_text, day_text, today.year + 1)
    return candidate


def _extract_price_amount(price_text: str) -> Decimal | None:
    match = PRICE_AMOUNT_RE.search(price_text)
    if match is None:
        return None
    return Decimal(match.group(1))


def _parse_age_range(*parts: str | None) -> tuple[int | None, int | None]:
    text = " ".join(normalize_space(part) for part in parts if normalize_space(part))
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = AGE_PLUS_RE.search(text)
    if match:
        return int(match.group(1)), None
    match = AGE_PLUS_FALLBACK_RE.search(text)
    if match:
        return int(match.group(1)), None
    return None, None
