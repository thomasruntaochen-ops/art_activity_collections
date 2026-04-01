import json
import re
import asyncio
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

ANCHORAGE_CATEGORY_URLS = {
    "family_classes": "https://www.anchoragemuseum.org/visit/calendar/categories/family-classes/",
    "art_classes": "https://www.anchoragemuseum.org/visit/calendar/categories/art-classes/",
    "art_lab": "https://www.anchoragemuseum.org/visit/calendar/categories/art-lab/",
    "gallery_talk": "https://www.anchoragemuseum.org/visit/calendar/categories/gallery-talk/",
    "school_break_workshops": "https://www.anchoragemuseum.org/visit/calendar/categories/school-break-workshops/",
}

ANCHORAGE_TIMEZONE = "America/Anchorage"
ANCHORAGE_VENUE_NAME = "Anchorage Museum"
ANCHORAGE_CITY = "Anchorage"
ANCHORAGE_STATE = "AK"
ANCHORAGE_LOCATION = "Anchorage, AK"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

CARD_DATE_RE = re.compile(
    r"\b(?P<month>[A-Z][a-z]{2,8})\s+(?P<day>\d{1,2})\s+(?P<year>\d{4})\s+[A-Za-z]{3}\b"
)
DETAIL_DATE_RE = re.compile(
    r"^(?P<time>.+?)\s+(?P<weekday>[A-Za-z-]+),\s+"
    r"(?P<month>[A-Za-z]+)\s+(?P<start_day>\d{1,2})(?:-(?P<end_day>\d{1,2}))?$",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*"
    r"(?P<start_meridiem>a\.?m\.?|p\.?m\.?|am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*"
    r"(?P<end_meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
TIME_TOKEN_RE = re.compile(
    r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?(?:\s*(?P<meridiem>a\.?m\.?|p\.?m\.?|am|pm))?\b",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+", re.IGNORECASE)


async def fetch_anchorage_museum_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[anchorage-museum-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[anchorage-museum-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[anchorage-museum-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            print(f"[anchorage-museum-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[anchorage-museum-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Anchorage Museum page") from last_exception
    raise RuntimeError("Unable to fetch Anchorage Museum page after retries")


async def load_anchorage_museum_payload() -> dict[str, object]:
    list_pages: dict[str, str] = {}
    for url in ANCHORAGE_CATEGORY_URLS.values():
        list_pages[url] = await fetch_anchorage_museum_page(url)

    listing_meta: dict[str, dict[str, object]] = {}
    for page_url, html in list_pages.items():
        category_slug = next(slug for slug, candidate in ANCHORAGE_CATEGORY_URLS.items() if candidate == page_url)
        for detail_url, card in _extract_listing_meta(html, list_url=page_url, category_slug=category_slug).items():
            existing = listing_meta.setdefault(detail_url, {"category_slugs": [], "default_year": card["default_year"]})
            categories = existing.setdefault("category_slugs", [])
            if isinstance(categories, list) and category_slug not in categories:
                categories.append(category_slug)
            if "card_text" not in existing:
                existing["card_text"] = card["card_text"]
            if "list_url" not in existing:
                existing["list_url"] = page_url
            if "card_date_hint" not in existing:
                existing["card_date_hint"] = card.get("card_date_hint")
            if not existing.get("default_year"):
                existing["default_year"] = card["default_year"]

    detail_pages: dict[str, str] = {}
    for detail_url in listing_meta:
        detail_pages[detail_url] = await fetch_anchorage_museum_page(detail_url)

    return {
        "list_pages": list_pages,
        "detail_pages": detail_pages,
        "listing_meta": listing_meta,
    }


class AnchorageMuseumAdapter(BaseSourceAdapter):
    source_name = "anchorage_museum_events"

    async def fetch(self) -> list[str]:
        payload = await load_anchorage_museum_payload()
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_anchorage_museum_payload(data)


def parse_anchorage_museum_payload(payload: dict[str, object]) -> list[ExtractedActivity]:
    detail_pages = payload.get("detail_pages") or {}
    listing_meta = payload.get("listing_meta") or {}
    if not isinstance(detail_pages, dict) or not isinstance(listing_meta, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for detail_url, meta in listing_meta.items():
        if not isinstance(detail_url, str) or not isinstance(meta, dict):
            continue
        detail_html = detail_pages.get(detail_url)
        if not isinstance(detail_html, str):
            continue
        item = _parse_detail_page(
            detail_html,
            source_url=detail_url,
            category_slugs=[slug for slug in meta.get("category_slugs", []) if isinstance(slug, str)],
            default_year=int(meta.get("default_year") or datetime.now().year),
            card_date_hint=meta.get("card_date_hint") if isinstance(meta.get("card_date_hint"), str) else None,
        )
        if item is None:
            continue
        key = (item.source_url, item.title, item.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(item)

    return rows


def _extract_listing_meta(html: str, *, list_url: str, category_slug: str) -> dict[str, dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    items: dict[str, dict[str, object]] = {}
    for anchor in soup.find_all("a", href=True):
        href = normalize_space(anchor.get("href"))
        if "/visit/calendar/event-details/" not in href:
            continue
        detail_url = urljoin(list_url, href)
        container = anchor.find_parent("div", class_=lambda value: value and "col-" in " ".join(value))
        card_text = normalize_space(container.get_text(" ", strip=True) if container is not None else anchor.get_text(" ", strip=True))
        year = _extract_card_year(card_text, detail_url)
        if detail_url not in items:
            items[detail_url] = {
                "card_text": card_text,
                "default_year": year,
                "category_slug": category_slug,
                "card_date_hint": CARD_DATE_RE.search(card_text).group(0) if CARD_DATE_RE.search(card_text) else None,
            }
    return items


def _extract_card_year(card_text: str, detail_url: str) -> int:
    match = CARD_DATE_RE.search(card_text)
    if match is not None:
        return int(match.group("year"))
    id_match = re.search(r"_(\d{4})\d{4}T", detail_url)
    if id_match is not None:
        return int(id_match.group(1))
    return datetime.now().year


def _parse_detail_page(
    html: str,
    *,
    source_url: str,
    category_slugs: list[str],
    default_year: int,
    card_date_hint: str | None,
) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    page_text = normalize_space(soup.get_text(" ", strip=True))
    if "specified event cannot be found" in page_text.lower():
        return None

    detail = soup.select_one(".calendarItem.calendarDetail")
    if detail is None:
        return None

    title = normalize_space(detail.select_one("h2").get_text(" ", strip=True) if detail.select_one("h2") else "")
    date_text = normalize_space(
        detail.select_one(".dateContainer").get_text(" ", strip=True) if detail.select_one(".dateContainer") else ""
    )
    location_text = normalize_space(
        detail.select_one(".addressContainer").get_text(" ", strip=True) if detail.select_one(".addressContainer") else ""
    )
    description_box = soup.select_one(".col-80 .grid-item--boxed")
    description_parts: list[str] = []
    seen_description_parts: set[str] = set()
    if description_box is not None:
        for node in description_box.find_all(["p", "li"], recursive=False):
            text = normalize_space(node.get_text(" ", strip=True))
            if not text or text == "REGISTER" or text in seen_description_parts:
                continue
            seen_description_parts.add(text)
            description_parts.append(text)
    description = " | ".join(description_parts) if description_parts else None

    if not _should_keep_event(title=title, description=description, category_slugs=category_slugs):
        return None

    start_at, end_at = _parse_detail_datetimes(
        date_text=date_text,
        default_year=default_year,
        card_date_hint=card_date_hint,
    )
    if start_at is None:
        return None

    age_min, age_max = _parse_age_range(" ".join([title, description or ""]))
    category_text = ", ".join(category_slugs)
    is_free, free_status = _classify_price(" | ".join([title, description or "", page_text]))
    text_blob = " ".join([title, description or "", category_text, page_text]).lower()

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=ANCHORAGE_VENUE_NAME,
        location_text=location_text or ANCHORAGE_LOCATION,
        city=ANCHORAGE_CITY,
        state=ANCHORAGE_STATE,
        activity_type=_infer_activity_type(title, category_slugs=category_slugs),
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=("registration required" in text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=ANCHORAGE_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _should_keep_event(*, title: str, description: str | None, category_slugs: list[str]) -> bool:
    text = " ".join([title, description or "", " ".join(category_slugs)]).lower()
    if "camp" in text:
        return False
    if "film" in text and "planetarium" not in text:
        return False
    if "reception" in text and "class" not in text and "workshop" not in text:
        return False

    include_markers = (
        "class",
        "classes",
        "workshop",
        "workshops",
        "lab",
        "talk",
        "hands-on",
        "hands on",
        "family",
        "gallery",
        "planetarium",
        "creative",
    )
    trusted_categories = {"family_classes", "art_classes", "art_lab", "gallery_talk"}
    return any(marker in text for marker in include_markers) or any(slug in trusted_categories for slug in category_slugs)


def _parse_detail_datetimes(
    *,
    date_text: str,
    default_year: int,
    card_date_hint: str | None,
) -> tuple[datetime | None, datetime | None]:
    normalized = normalize_space(date_text)
    match = DETAIL_DATE_RE.match(normalized)
    if match is None:
        return _parse_detail_datetimes_from_card_hint(date_text=normalized, card_date_hint=card_date_hint)

    try:
        start_date = datetime.strptime(
            f"{match.group('month')} {match.group('start_day')} {default_year}",
            "%B %d %Y",
        )
    except ValueError:
        return None, None

    start_at, same_day_end = _parse_time_text(base_date=start_date, time_text=match.group("time"))
    if match.group("end_day") is None:
        return start_at, same_day_end

    try:
        end_date = datetime.strptime(
            f"{match.group('month')} {match.group('end_day')} {default_year}",
            "%B %d %Y",
        )
    except ValueError:
        return start_at, same_day_end

    _, end_at = _parse_time_text(base_date=end_date, time_text=match.group("time"))
    return start_at, end_at


def _parse_detail_datetimes_from_card_hint(
    *,
    date_text: str,
    card_date_hint: str | None,
) -> tuple[datetime | None, datetime | None]:
    if not card_date_hint:
        return None, None

    card_match = CARD_DATE_RE.search(card_date_hint)
    if card_match is None:
        return None, None

    try:
        base_date = datetime.strptime(
            f"{card_match.group('month')} {card_match.group('day')} {card_match.group('year')}",
            "%b %d %Y",
        )
    except ValueError:
        try:
            base_date = datetime.strptime(
                f"{card_match.group('month')} {card_match.group('day')} {card_match.group('year')}",
                "%B %d %Y",
            )
        except ValueError:
            return None, None

    time_prefix = date_text.split(" Saturday", 1)[0].split(" Sunday", 1)[0]
    return _parse_time_text(base_date=base_date, time_text=time_prefix)


def _parse_time_text(*, base_date: datetime, time_text: str) -> tuple[datetime | None, datetime | None]:
    normalized = normalize_space(time_text).replace("–", "-").replace("—", "-").replace("&", "")
    range_match = TIME_RANGE_RE.search(normalized)
    if range_match is not None:
        start_meridiem = range_match.group("start_meridiem") or range_match.group("end_meridiem")
        end_meridiem = range_match.group("end_meridiem")
        start_at = _build_datetime(
            base_date=base_date,
            hour_text=range_match.group("start_hour"),
            minute_text=range_match.group("start_minute"),
            meridiem=start_meridiem,
        )
        end_at = _build_datetime(
            base_date=base_date,
            hour_text=range_match.group("end_hour"),
            minute_text=range_match.group("end_minute"),
            meridiem=end_meridiem,
        )
        return start_at, end_at

    start_match = TIME_TOKEN_RE.search(normalized)
    if start_match is None:
        return base_date.replace(hour=0, minute=0, second=0, microsecond=0), None

    meridiem = start_match.group("meridiem")
    if meridiem is None:
        meridiem_candidates = TIME_TOKEN_RE.findall(normalized)
        for _, _, candidate in reversed(meridiem_candidates):
            if candidate:
                meridiem = candidate
                break

    start_at = _build_datetime(
        base_date=base_date,
        hour_text=start_match.group("hour"),
        minute_text=start_match.group("minute"),
        meridiem=meridiem,
    )
    return start_at, None


def _build_datetime(
    *,
    base_date: datetime,
    hour_text: str | None,
    minute_text: str | None,
    meridiem: str | None,
) -> datetime | None:
    if hour_text is None or meridiem is None:
        return None
    hour = int(hour_text)
    minute = int(minute_text or "0")
    meridiem_text = meridiem.lower().replace(".", "")
    if meridiem_text == "pm" and hour != 12:
        hour += 12
    if meridiem_text == "am" and hour == 12:
        hour = 0
    return base_date.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    normalized = normalize_space(text)
    range_match = AGE_RANGE_RE.search(normalized)
    if range_match is not None:
        return int(range_match.group(1)), int(range_match.group(2))
    plus_match = AGE_PLUS_RE.search(normalized)
    if plus_match is not None:
        return int(plus_match.group(1)), None
    return None, None


def _infer_activity_type(title: str, *, category_slugs: list[str]) -> str:
    text = " ".join([title, " ".join(category_slugs)]).lower()
    if "talk" in text:
        return "talk"
    if "class" in text:
        return "class"
    if "lab" in text:
        return "lab"
    return "workshop"


def _classify_price(text: str) -> tuple[bool | None, str]:
    normalized = f" {normalize_space(text).lower()} "
    if "included with admission" in normalized and "$" not in normalized:
        return None, "uncertain"
    return infer_price_classification(text)
