import asyncio
import re
from datetime import datetime
from datetime import time
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

BUFFALO_AKG_FIND_EVENT_URL = "https://buffaloakg.org/find-event"

NY_TIMEZONE = "America/New_York"
BUFFALO_AKG_VENUE_NAME = "Buffalo AKG Art Museum"
BUFFALO_AKG_CITY = "Buffalo"
BUFFALO_AKG_STATE = "NY"
BUFFALO_AKG_DEFAULT_LOCATION = "Buffalo, NY"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BUFFALO_AKG_FIND_EVENT_URL,
}

PAGE_NUMBER_RE = re.compile(r"[?&]page=(\d+)")
DATE_RANGE_RE = re.compile(
    r"(?P<date>(?:[A-Za-z]+,\s+)?[A-Za-z]+\s+\d{1,2},\s+\d{4})\s+"
    r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))(?:\s+[A-Z]{2,4})?",
    re.IGNORECASE,
)
DATE_ONLY_RE = re.compile(r"((?:[A-Za-z]+,\s+)?[A-Za-z]+\s+\d{1,2},\s+\d{4})")
TIME_SINGLE_RE = re.compile(r"\b(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))\b", re.IGNORECASE)
ISO_DATETIME_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\b")
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)(?:\b|(?=[^\w]))", re.IGNORECASE)

INCLUDE_KEYWORDS = (
    "activity",
    "art class",
    "art classes",
    "artmaking",
    "art-making",
    "class",
    "classes",
    "clay",
    "conversation",
    "conversations",
    "craft",
    "crafts",
    "drawing",
    "drop-in",
    "drop in",
    "kids' studio",
    "lab",
    "lecture",
    "marbling",
    "printmaking",
    "studio",
    "talk",
    "teen",
    "teens",
    "workshop",
)
EXCLUDE_KEYWORDS = (
    "after hours",
    "band",
    "cornelia",
    "dessert",
    "dinner",
    "film",
    "fundraiser",
    "fundraising",
    "meet and greet",
    "music",
    "orchestra",
    "performance",
    "poetry",
    "pop-up",
    "public tour",
    "public tours",
    "recipe card",
    "reception",
    "requiem",
    "rockin",
    "storytime",
    "tour",
    "tribute",
    "tv",
    "virtual",
    "yoga",
)
LOCATION_HINTS = (
    "auditorium",
    "atrium",
    "building",
    "gallery",
    "hall",
    "knox",
    "lipsey",
    "room",
    "square",
    "studio",
    "theater",
    "theatre",
    "town square",
    "wilmers",
    "wilson",
)


async def fetch_buffalo_akg_page(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS)

    last_exception: Exception | None = None
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            if response.status_code < 400:
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Buffalo AKG event pages") from last_exception
    raise RuntimeError("Unable to fetch Buffalo AKG event pages after retries")


def build_buffalo_akg_find_event_url(*, page_number: int) -> str:
    if page_number <= 0:
        return BUFFALO_AKG_FIND_EVENT_URL
    return f"{BUFFALO_AKG_FIND_EVENT_URL}?page={page_number}"


async def load_buffalo_akg_payload(*, page_limit: int | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        first_html = await fetch_buffalo_akg_page(BUFFALO_AKG_FIND_EVENT_URL, client=client)
        page_total = _parse_page_total(first_html)
        if page_limit is not None:
            page_total = min(page_total, max(page_limit, 1))

        page_payloads: list[tuple[str, str]] = [(BUFFALO_AKG_FIND_EVENT_URL, first_html)]
        page_number = 1
        while page_number < page_total:
            page_url = build_buffalo_akg_find_event_url(page_number=page_number)
            page_html = await fetch_buffalo_akg_page(page_url, client=client)
            page_payloads.append((page_url, page_html))

            discovered_total = _parse_page_total(page_html)
            if page_limit is not None:
                discovered_total = min(discovered_total, max(page_limit, 1))
            if discovered_total > page_total:
                page_total = discovered_total

            page_number += 1

        listings_by_url: dict[str, dict] = {}
        for list_url, html in page_payloads:
            for listing in _parse_listing_items(html, list_url=list_url):
                listings_by_url.setdefault(listing["source_url"], listing)

        detail_urls = sorted(listings_by_url)
        detail_pages = await asyncio.gather(*(fetch_buffalo_akg_page(url, client=client) for url in detail_urls))

    return {
        "listing_url": BUFFALO_AKG_FIND_EVENT_URL,
        "listings": list(listings_by_url.values()),
        "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
    }


def parse_buffalo_akg_payload(payload: dict) -> list[ExtractedActivity]:
    listings_by_url = {listing["source_url"]: listing for listing in payload["listings"]}
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, html in payload["detail_pages"].items():
        detail = _parse_detail_page(html, source_url=source_url)
        if detail is None or detail["start_at"] is None:
            continue
        if detail["start_at"].date() < current_date:
            continue

        listing = listings_by_url.get(source_url)
        if not _should_include_event(listing=listing, detail=detail):
            continue

        key = (source_url, detail["title"], detail["start_at"])
        if key in seen:
            continue
        seen.add(key)

        description_parts = [detail["description"]]
        if detail.get("category_tags"):
            description_parts.append(f"Tags: {', '.join(detail['category_tags'])}")
        if detail.get("price_text"):
            description_parts.append(f"Price: {detail['price_text']}")
        full_description = " | ".join(part for part in description_parts if part) or None
        text_blob = " ".join(
            filter(
                None,
                [
                    detail["title"],
                    detail.get("description"),
                    detail.get("price_text"),
                    detail.get("location_text"),
                    " ".join(detail.get("category_tags") or []),
                ],
            )
        ).lower()
        age_min, age_max = _parse_age_range(" ".join(filter(None, [detail["title"], detail.get("description")])))
        is_free, free_status = infer_price_classification(detail.get("price_text"))

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=detail["title"],
                description=full_description,
                venue_name=BUFFALO_AKG_VENUE_NAME,
                location_text=detail.get("location_text") or BUFFALO_AKG_DEFAULT_LOCATION,
                city=BUFFALO_AKG_CITY,
                state=BUFFALO_AKG_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=detail["registration_required"],
                start_at=detail["start_at"],
                end_at=detail["end_at"],
                timezone=NY_TIMEZONE,
                is_free=is_free,
                free_verification_status=free_status,
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class BuffaloAkgEventsAdapter(BaseSourceAdapter):
    source_name = "buffalo_akg_events"

    def __init__(self, url: str = BUFFALO_AKG_FIND_EVENT_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_buffalo_akg_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        listings = _parse_listing_items(payload, list_url=self.url)
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
            detail_pages = await asyncio.gather(
                *(fetch_buffalo_akg_page(listing["source_url"], client=client) for listing in listings)
            )
        return parse_buffalo_akg_payload(
            {
                "listing_url": self.url,
                "listings": listings,
                "detail_pages": {
                    listing["source_url"]: html for listing, html in zip(listings, detail_pages, strict=True)
                },
            }
        )


def _parse_page_total(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    page_numbers = [0]
    for anchor in soup.select(".pager a[href]"):
        href = (anchor.get("href") or "").strip()
        match = PAGE_NUMBER_RE.search(href)
        if match is None:
            continue
        page_numbers.append(int(match.group(1)))
    return max(page_numbers) + 1


def _parse_listing_items(html: str, *, list_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    listings: list[dict] = []

    for item in soup.select("main div.content-results.content-view-grid li.c-view-list__item"):
        link = item.select_one('a[href*="/events/"]')
        if link is None:
            continue

        source_url = urljoin(list_url, (link.get("href") or "").strip())
        if not source_url:
            continue

        listing_text = _normalize_space(item.get_text(" ", strip=True))
        if not listing_text:
            continue

        listings.append(
            {
                "source_url": source_url,
                "listing_text": listing_text,
            }
        )

    return listings


def _parse_detail_page(html: str, *, source_url: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    article = soup.select_one("main article.basic-page.node-event") or soup.select_one("main article")
    if article is None:
        return None

    content_root = article.select_one("section.main-content") or article
    title_node = article.select_one("h1.c-hero__title") or article.select_one("h1")
    title = _normalize_space(title_node.get_text(" ", strip=True) if title_node is not None else "")
    if not title:
        return None

    date_text = _normalize_space(
        article.select_one("div.c-hero__date").get_text(" ", strip=True)
        if article.select_one("div.c-hero__date")
        else ""
    )
    start_at, end_at = _parse_schedule_text(date_text)
    if start_at is None:
        start_at, end_at = _parse_iso_datetimes(content_root.get_text(" ", strip=True))

    paragraphs = [_normalize_space(node.get_text(" ", strip=True)) for node in content_root.select("p")]
    strong_texts = [_normalize_space(node.get_text(" ", strip=True)) for node in content_root.select("strong")]
    list_items = [_normalize_space(node.get_text(" ", strip=True)) for node in content_root.select("li")]
    link_texts = [
        _normalize_space(node.get_text(" ", strip=True))
        for node in content_root.select("a[href]")
        if _normalize_space(node.get_text(" ", strip=True))
    ]

    description = " ".join(_extract_description_paragraphs(paragraphs)) or None
    price_text = _extract_price_text(paragraphs=paragraphs, strong_texts=strong_texts, link_texts=link_texts)
    location_text = _extract_location_text(paragraphs=paragraphs, strong_texts=strong_texts, list_items=list_items)
    category_tags = _extract_category_tags(list_items=list_items, location_text=location_text)
    registration_required = any(
        keyword in " ".join(link_texts).lower()
        for keyword in ("register", "tickets", "ticket", "reserve", "buy now", "buy")
    )

    return {
        "source_url": source_url,
        "title": title,
        "description": description,
        "price_text": price_text,
        "location_text": location_text,
        "category_tags": category_tags,
        "registration_required": registration_required,
        "start_at": start_at,
        "end_at": end_at,
    }


def _should_include_event(*, listing: dict | None, detail: dict) -> bool:
    listing_blob = _normalize_space((listing or {}).get("listing_text"))
    title_blob = _normalize_space(detail["title"]).lower()
    detail_blob = _normalize_space(
        " ".join(
            filter(
                None,
                [
                    detail["title"],
                    detail.get("description"),
                    detail.get("price_text"),
                    " ".join(detail.get("category_tags") or []),
                ],
            )
        )
    )
    blob = f"{listing_blob} {detail_blob}".strip().lower()
    if not blob:
        return False

    if "cancel" in title_blob:
        return False
    if any(keyword in title_blob for keyword in EXCLUDE_KEYWORDS):
        return False
    if any(keyword in blob for keyword in ("musical performance", "poetry workshop", "poet laureate", "yoga")):
        return False

    return any(keyword in blob for keyword in INCLUDE_KEYWORDS)


def _infer_activity_type(text_blob: str) -> str:
    if any(keyword in text_blob for keyword in ("conversation", "lecture", "talk")):
        return "talk"
    return "workshop"


def _parse_schedule_text(text: str | None) -> tuple[datetime | None, datetime | None]:
    normalized = _normalize_space(text)
    if not normalized:
        return None, None

    match = DATE_RANGE_RE.search(normalized)
    if match is not None:
        base_day = _parse_date_token(match.group("date"))
        if base_day is None:
            return None, None
        start_time = _parse_time_token(match.group("start"))
        end_time = _parse_time_token(match.group("end"))
        if start_time is None:
            return None, None
        start_at = datetime.combine(base_day.date(), start_time)
        end_at = datetime.combine(base_day.date(), end_time) if end_time is not None else None
        return start_at, end_at

    date_match = DATE_ONLY_RE.search(normalized)
    if date_match is None:
        return None, None

    base_day = _parse_date_token(date_match.group(1))
    if base_day is None:
        return None, None

    time_match = TIME_SINGLE_RE.search(normalized)
    if time_match is None:
        return datetime.combine(base_day.date(), datetime.min.time()), None

    start_time = _parse_time_token(time_match.group(1))
    if start_time is None:
        return datetime.combine(base_day.date(), datetime.min.time()), None
    return datetime.combine(base_day.date(), start_time), None


def _parse_iso_datetimes(text: str) -> tuple[datetime | None, datetime | None]:
    matches = ISO_DATETIME_RE.findall(text)
    if not matches:
        return None, None

    parsed = []
    for value in matches:
        try:
            parsed.append(datetime.strptime(value, "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            continue

    if not parsed:
        return None, None
    if len(parsed) == 1:
        return parsed[0], None
    return parsed[0], parsed[-1]


def _parse_date_token(value: str) -> datetime | None:
    normalized = _normalize_space(value).removeprefix("Monday, ").removeprefix("Tuesday, ").removeprefix(
        "Wednesday, "
    ).removeprefix("Thursday, ").removeprefix("Friday, ").removeprefix("Saturday, ").removeprefix("Sunday, ")
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _parse_time_token(value: str) -> time | None:
    normalized = _normalize_space(value).lower().replace(".", "")
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            return datetime.strptime(normalized.upper(), fmt).time()
        except ValueError:
            continue
    return None


def _extract_description_paragraphs(paragraphs: list[str]) -> list[str]:
    descriptions: list[str] = []
    for text in paragraphs:
        text = _clean_description_text(text)
        if not text:
            continue
        lowered = text.lower()
        if lowered.startswith("photo:"):
            continue
        if DATE_ONLY_RE.search(text) or TIME_SINGLE_RE.search(text):
            if len(text.split()) <= 8:
                continue
        if text.isupper():
            continue
        if infer_price_classification(text)[1] == "confirmed":
            continue
        if _looks_like_location_line(text):
            continue
        if len(text) < 40:
            continue
        descriptions.append(text)
    return descriptions


def _extract_price_text(*, paragraphs: list[str], strong_texts: list[str], link_texts: list[str]) -> str | None:
    candidates = []
    for text in strong_texts + paragraphs + link_texts:
        if not text:
            continue
        lowered = text.lower()
        if "$" in text or "free" in lowered or "admission" in lowered or "member" in lowered:
            candidates.append(text)
    for text in candidates:
        if infer_price_classification(text)[1] != "uncertain":
            return text
    return candidates[0] if candidates else None


def _extract_location_text(*, paragraphs: list[str], strong_texts: list[str], list_items: list[str]) -> str | None:
    for text in strong_texts + paragraphs:
        if _looks_like_location_line(text):
            return text

    location_parts = [text for text in list_items if _looks_like_location_fragment(text)]
    if not location_parts:
        return None

    location_parts.sort(key=lambda value: ("building" in value.lower() or "gallery" in value.lower(), value.lower()))
    return ", ".join(location_parts)


def _extract_category_tags(*, list_items: list[str], location_text: str | None) -> list[str]:
    tags: list[str] = []
    normalized_location = _normalize_space(location_text).lower()
    for text in list_items:
        normalized = _normalize_space(text)
        if not normalized:
            continue
        if normalized.lower() == normalized_location:
            continue
        if _looks_like_location_fragment(normalized):
            continue
        tags.append(normalized)
    return tags


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match is not None:
        return int(match.group(1)), int(match.group(2))

    match = AGE_PLUS_RE.search(text)
    if match is not None:
        return int(match.group(1)), None

    return None, None


def _looks_like_location_line(text: str) -> bool:
    normalized = _normalize_space(text)
    if not normalized or "$" in normalized:
        return False
    lowered = normalized.lower()
    return any(keyword in lowered for keyword in LOCATION_HINTS) and len(normalized) <= 80


def _looks_like_location_fragment(text: str) -> bool:
    normalized = _normalize_space(text)
    if not normalized:
        return False
    lowered = normalized.lower()
    if any(keyword in lowered for keyword in LOCATION_HINTS):
        return True
    return normalized.count(",") == 0 and len(normalized.split()) <= 4 and normalized != normalized.upper()


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def _clean_description_text(text: str) -> str:
    cleaned = re.sub(r"^.*?\bPhoto:\s+[A-Za-z .'\-]+\s*", "", text)
    cleaned = re.sub(r"\b(?:REGISTER|TICKETS?)\b", "", cleaned, flags=re.IGNORECASE)
    return _normalize_space(cleaned)
