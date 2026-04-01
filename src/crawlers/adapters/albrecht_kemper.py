import asyncio
import re
from datetime import datetime
from datetime import timezone
from urllib.parse import parse_qs
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.parse import unquote

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

ALBRECHT_KEMPER_EVENTS_URL = "https://www.albrecht-kemper.org/calendar"

MO_TIMEZONE = "America/Chicago"
ALBRECHT_KEMPER_VENUE_NAME = "Albrecht-Kemper Museum of Art"
ALBRECHT_KEMPER_CITY = "St. Joseph"
ALBRECHT_KEMPER_STATE = "MO"
ALBRECHT_KEMPER_DEFAULT_LOCATION = "2818 Frederick Avenue, Saint Joseph, MO 64506"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.albrecht-kemper.org/",
}

INCLUDE_PATTERNS = (
    " class ",
    " classes ",
    " fused glass ",
    " kids ",
    " knitting ",
    " media mix up ",
    " painting ",
    " pastel ",
    " studio basics ",
    " workshop ",
    " workshops ",
)
HARD_EXCLUDE_PATTERNS = (
    " after hours ",
    " auction ",
    " camp ",
    " camps ",
    " exhibition ",
    " exhibitions ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " market ",
    " music ",
    " opening ",
    " performance ",
    " reception ",
    " tea ",
    " vendor ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|to|\u2013)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)


async def fetch_albrecht_kemper_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
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

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Albrecht-Kemper events page") from last_exception
    raise RuntimeError("Unable to fetch Albrecht-Kemper events page after retries")


class AlbrechtKemperAdapter(BaseSourceAdapter):
    source_name = "albrecht_kemper_events"

    def __init__(self, url: str = ALBRECHT_KEMPER_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_albrecht_kemper_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_albrecht_kemper_events_html(payload, list_url=self.url)


def parse_albrecht_kemper_events_html(html: str, *, list_url: str) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    current_date = datetime.now(ZoneInfo(MO_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for article in soup.select("article.eventlist-event.eventlist-event--upcoming"):
        row = _build_row(article, list_url=list_url)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _build_row(article, *, list_url: str) -> ExtractedActivity | None:
    title_node = article.select_one(".eventlist-title-link")
    title = _normalize_space(title_node.get_text(" ", strip=True) if title_node else "")
    href = title_node.get("href") if title_node else ""
    source_url = urljoin(list_url, href or "")
    if not title or not source_url:
        return None

    categories = [_normalize_space(node.get_text(" ", strip=True)) for node in article.select(".eventlist-cats a")]
    categories = [value for value in categories if value]
    description = _normalize_space(_html_to_text(article.select_one(".eventlist-excerpt")))
    location_text = _extract_location_text(article)

    start_at: datetime | None = None
    end_at: datetime | None = None
    google_calendar_url = article.select_one(".eventlist-meta-export-google")
    if google_calendar_url is not None and google_calendar_url.get("href"):
        start_at, end_at = _parse_google_calendar_datetimes(google_calendar_url["href"])

    if start_at is None:
        start_at, end_at = _parse_visible_datetimes(article)
    if start_at is None:
        return None

    token_blob = _searchable_blob(" ".join([title, description, " ".join(categories)]))
    if not _should_include_event(token_blob=token_blob, title=title):
        return None

    age_min, age_max = _parse_age_range(title=title, description=description)
    registration_required = any(
        marker in token_blob for marker in (" register ", " registration ", " reserve ", " signup ", " sign up ")
    )

    description_parts = [part for part in [description] if part]
    if categories:
        description_parts.append(f"Categories: {', '.join(categories)}")
    description_text = " | ".join(description_parts) if description_parts else None

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description_text,
        venue_name=ALBRECHT_KEMPER_VENUE_NAME,
        location_text=location_text or ALBRECHT_KEMPER_DEFAULT_LOCATION,
        city=ALBRECHT_KEMPER_CITY,
        state=ALBRECHT_KEMPER_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=(" drop in " in token_blob or " drop-in " in token_blob),
        registration_required=registration_required,
        start_at=start_at,
        end_at=end_at,
        timezone=MO_TIMEZONE,
        **price_classification_kwargs(description_text),
    )


def _should_include_event(*, token_blob: str, title: str) -> bool:
    if any(pattern in token_blob for pattern in HARD_EXCLUDE_PATTERNS):
        return False
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return False

    normalized_title = title.lower()
    if "camp" in normalized_title or "gala" in normalized_title:
        return False
    if "exhibition opening" in normalized_title:
        return False

    return True


def _parse_google_calendar_datetimes(url: str) -> tuple[datetime | None, datetime | None]:
    parsed = urlparse(url)
    dates = parse_qs(parsed.query).get("dates") or []
    if not dates:
        return None, None
    parts = dates[0].split("/")
    if len(parts) != 2:
        return None, None
    return _parse_google_datetime(parts[0]), _parse_google_datetime(parts[1])


def _parse_google_datetime(value: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None

    try:
        if text.endswith("Z"):
            dt = datetime.strptime(text, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            return dt.astimezone(ZoneInfo(MO_TIMEZONE)).replace(tzinfo=None)
        if "T" in text:
            return datetime.strptime(text, "%Y%m%dT%H%M%S")
        return datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return None


def _parse_visible_datetimes(article) -> tuple[datetime | None, datetime | None]:
    date_node = article.select_one(".event-date")
    date_value = _normalize_space((date_node.get("datetime") if date_node else "") or (date_node.get_text(" ", strip=True) if date_node else ""))
    if not date_value:
        return None, None

    try:
        event_date = datetime.strptime(date_value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None, None

    start_text = _normalize_space(_html_to_text(article.select_one(".event-time-localized-start")))
    end_text = _normalize_space(_html_to_text(article.select_one(".event-time-localized-end")))
    if not start_text:
        return datetime.combine(event_date, datetime.min.time()), None

    start_at = _combine_date_and_time(event_date, start_text)
    end_at = _combine_date_and_time(event_date, end_text) if end_text else None
    return start_at, end_at


def _combine_date_and_time(event_date, time_text: str) -> datetime | None:
    normalized = (
        time_text.replace("\u202f", " ")
        .replace("\xa0", " ")
        .replace(".", "")
        .strip()
        .upper()
    )
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            parsed_time = datetime.strptime(normalized, fmt).time()
            return datetime.combine(event_date, parsed_time)
        except ValueError:
            continue
    return None


def _extract_location_text(article) -> str | None:
    node = article.select_one(".eventlist-meta-address")
    if node is None:
        return None
    map_link = node.select_one("a[href]")
    if map_link is not None:
        parsed = urlparse(map_link.get("href", ""))
        query_location = (parse_qs(parsed.query).get("q") or [""])[0].strip()
        if query_location:
            return _normalize_space(unquote(query_location))
    pieces: list[str] = []
    for text in node.stripped_strings:
        cleaned = _normalize_space(text)
        if cleaned and cleaned != "(map)":
            pieces.append(cleaned)
    if not pieces:
        return None
    return ", ".join(dict.fromkeys(pieces))


def _parse_age_range(*, title: str, description: str) -> tuple[int | None, int | None]:
    combined = " ".join(part for part in [title, description] if part)
    match = AGE_RANGE_RE.search(combined)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = AGE_PLUS_RE.search(combined)
    if match:
        return int(match.group(1)), None
    return None, None


def _infer_activity_type(token_blob: str) -> str:
    if any(marker in token_blob for marker in (" lecture ", " talk ", " conversation ")):
        return "lecture"
    return "workshop"


def _html_to_text(node) -> str:
    if node is None:
        return ""
    if isinstance(node, str):
        return _normalize_space(BeautifulSoup(node, "html.parser").get_text(" ", strip=True))
    return _normalize_space(node.get_text(" ", strip=True))


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "
