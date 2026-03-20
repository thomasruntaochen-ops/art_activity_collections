import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

ROSE_PROGRAMS_URL = "https://www.brandeis.edu/rose/programs/index.html"
ROSE_TIMEZONE = "America/New_York"
ROSE_VENUE_NAME = "Rose Art Museum"
ROSE_CITY = "Waltham"
ROSE_STATE = "MA"
ROSE_DEFAULT_LOCATION = "Rose Art Museum, 415 South Street, Waltham, MA 02453"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": ROSE_PROGRAMS_URL,
}

SINGLE_DATE_RE = re.compile(r"^(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})$")
SINGLE_DATETIME_RE = re.compile(
    r"^(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4}),\s*"
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?)$",
    re.IGNORECASE,
)
DATE_RANGE_RE = re.compile(
    r"^(?P<start_month>[A-Za-z]+)\s+(?P<start_day>\d{1,2}),\s*(?P<start_year>\d{4})\s*[–-]\s*"
    r"(?P<end_month>[A-Za-z]+)\s+(?P<end_day>\d{1,2}),\s*(?P<end_year>\d{4})$"
)


async def fetch_rose_page(
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
        raise RuntimeError("Unable to fetch Rose Art Museum programs page") from last_exception
    raise RuntimeError("Unable to fetch Rose Art Museum programs page after retries")


async def load_rose_payload() -> dict:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_rose_page(ROSE_PROGRAMS_URL, client=client)
        listing_entries = _extract_listing_entries(listing_html)
        detail_urls = sorted({entry["source_url"] for entry in listing_entries})
        detail_pages = await asyncio.gather(*(fetch_rose_page(url, client=client) for url in detail_urls))

    return {
        "listing_html": listing_html,
        "entries": listing_entries,
        "detail_pages": {url: html for url, html in zip(detail_urls, detail_pages, strict=True)},
    }


def parse_rose_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(ROSE_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    detail_pages = payload.get("detail_pages", {})
    for entry in payload.get("entries") or []:
        row = _build_row_from_entry(
            entry=entry,
            detail_html=detail_pages.get(entry["source_url"]),
        )
        if row is None:
            continue

        last_date = row.end_at.date() if row.end_at is not None else row.start_at.date()
        if last_date < current_date:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class RoseProgramsAdapter(BaseSourceAdapter):
    source_name = "rose_programs"

    async def fetch(self) -> list[str]:
        html = await fetch_rose_page(ROSE_PROGRAMS_URL)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_rose_payload(
            {
                "entries": _extract_listing_entries(payload),
                "detail_pages": {},
            }
        )


def _extract_listing_entries(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    entries: list[dict[str, str]] = []
    section = soup.select_one("section#listFeed1")
    if section is None:
        return entries

    for block in section.select("div.block"):
        text_block = block.select_one("div.block__text")
        if text_block is None:
            continue

        title_link = text_block.select_one("h3 a[href]")
        date_nodes = text_block.select("h3")
        summary_node = text_block.select_one("p:not(.caption)")
        if title_link is None or len(date_nodes) < 2:
            continue

        title = _normalize_space(title_link.get_text(" ", strip=True))
        source_url = urljoin(ROSE_PROGRAMS_URL, title_link.get("href", "").strip())
        date_text = _normalize_space(date_nodes[1].get_text(" ", strip=True))
        summary = _normalize_space(summary_node.get_text(" ", strip=True) if summary_node else "")
        if not title or not source_url or not date_text:
            continue

        entries.append(
            {
                "title": title,
                "source_url": source_url,
                "date_text": date_text,
                "summary": summary,
            }
        )

    return entries


def _build_row_from_entry(
    *,
    entry: dict[str, str],
    detail_html: str | None,
) -> ExtractedActivity | None:
    detail = _parse_detail_page(detail_html) if detail_html else {}
    title = entry["title"]
    source_url = entry["source_url"]
    date_text = detail.get("date_text") or entry["date_text"]
    if not isinstance(date_text, str):
        return None

    start_at, end_at = _parse_date_window(date_text)
    if start_at is None:
        return None

    summary = _normalize_space(entry.get("summary"))
    detail_summary = _normalize_space(detail.get("summary"))
    hero_text = _normalize_space(detail.get("hero_text"))
    description = _join_unique_parts(
        [
            summary,
            detail_summary if detail_summary and detail_summary != summary else "",
            f"Format: {hero_text}" if hero_text and hero_text.lower() == "virtual program" else "",
            f"Schedule: {hero_text}" if _looks_like_schedule_text(hero_text) else "",
        ]
    )

    text_blob = _normalize_space(" ".join([title, description or "", hero_text])).lower()
    registration_required = bool(detail.get("registration_required") or "register" in text_blob)
    location_text = _build_location_text(hero_text)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=ROSE_VENUE_NAME,
        location_text=location_text,
        city=ROSE_CITY,
        state=ROSE_STATE,
        activity_type=_infer_activity_type(text_blob),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=registration_required,
        start_at=start_at,
        end_at=end_at,
        timezone=ROSE_TIMEZONE,
        **price_classification_kwargs("All programs are free to attend."),
    )


def _parse_detail_page(html: str) -> dict[str, str | bool | None]:
    soup = BeautifulSoup(html, "html.parser")

    date_node = soup.select_one("span.hero__content__heading--date")
    hero_text_node = soup.select_one("div.hero__content--text")
    main_section = soup.select_one("div.main div.section")
    paragraphs: list[str] = []
    if main_section is not None:
        for child in main_section.find_all("p", recursive=False):
            text = _normalize_space(child.get_text(" ", strip=True))
            if not text:
                continue
            if "register now" in text.lower():
                break
            if text.lower().startswith("about the artist") or text.lower().startswith("program schedule"):
                break
            if text == "Program":
                continue
            paragraphs.append(text)

    return {
        "date_text": _normalize_space(date_node.get_text(" ", strip=True) if date_node else ""),
        "hero_text": _normalize_space(hero_text_node.get_text(" ", strip=True) if hero_text_node else ""),
        "summary": _join_unique_parts(paragraphs[:2]),
        "registration_required": soup.find("a", string=re.compile(r"register now", re.IGNORECASE)) is not None,
    }


def _build_location_text(hero_text: str | None) -> str:
    text = _normalize_space(hero_text)
    if not text:
        return ROSE_DEFAULT_LOCATION
    lowered = text.lower()
    if "virtual program" in lowered or "zoom" in lowered or "webinar" in lowered:
        return "Online"
    if _looks_like_schedule_text(text):
        return ROSE_DEFAULT_LOCATION
    return text


def _looks_like_schedule_text(text: str | None) -> bool:
    normalized = _normalize_space(text).lower()
    if not normalized:
        return False
    return any(day in normalized for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"))


def _infer_activity_type(text_blob: str) -> str:
    if any(keyword in text_blob for keyword in (" lecture ", " talk ", " conversation ", " discussion ", " forum ")):
        return "lecture"
    return "workshop"


def _parse_date_window(value: str) -> tuple[datetime | None, datetime | None]:
    text = _normalize_space(value).replace(" ,", ",")

    match = SINGLE_DATETIME_RE.match(text)
    if match:
        hour = int(match.group("hour"))
        minute = int(match.group("minute") or "0")
        meridiem = match.group("meridiem").lower().replace(".", "")
        if meridiem == "pm" and hour != 12:
            hour += 12
        if meridiem == "am" and hour == 12:
            hour = 0
        return (
            datetime.strptime(
                f"{match.group('month')} {match.group('day')} {match.group('year')}",
                "%B %d %Y",
            ).replace(hour=hour, minute=minute, second=0, microsecond=0),
            None,
        )

    match = DATE_RANGE_RE.match(text)
    if match:
        start_at = datetime.strptime(
            f"{match.group('start_month')} {match.group('start_day')} {match.group('start_year')}",
            "%B %d %Y",
        ).replace(hour=0, minute=0, second=0, microsecond=0)
        end_at = datetime.strptime(
            f"{match.group('end_month')} {match.group('end_day')} {match.group('end_year')}",
            "%B %d %Y",
        ).replace(hour=0, minute=0, second=0, microsecond=0)
        return start_at, end_at

    match = SINGLE_DATE_RE.match(text)
    if match:
        return (
            datetime.strptime(
                f"{match.group('month')} {match.group('day')} {match.group('year')}",
                "%B %d %Y",
            ).replace(hour=0, minute=0, second=0, microsecond=0),
            None,
        )

    return None, None


def _join_unique_parts(parts: list[str]) -> str | None:
    seen: set[str] = set()
    ordered: list[str] = []
    for part in parts:
        normalized = _normalize_space(part)
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(normalized)
    if not ordered:
        return None
    return " | ".join(ordered)


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())
