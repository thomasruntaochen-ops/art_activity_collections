import asyncio
import re
from datetime import date
from datetime import datetime
from datetime import time
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

DIXON_EVENTS_URL = "https://www.dixon.org/events"

NY_TIMEZONE = "America/New_York"
MAX_FUTURE_DAYS = 365
DIXON_VENUE_NAME = "Dixon Gallery & Gardens"
DIXON_CITY = "Memphis"
DIXON_STATE = "TN"
DIXON_DEFAULT_LOCATION = "Dixon Gallery & Gardens, Memphis, TN"
MAX_PAGE_SCAN = 30
DETAIL_FETCH_CONCURRENCY = 8

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

INCLUDE_PATTERNS = (
    " adult workshop ",
    " art for all: the arts ",
    " classes and workshops ",
    " creative aging ",
    " drawing ",
    " fiber arts ",
    " hobby kick-start ",
    " kaleidoscope club ",
    " lecture ",
    " lectures ",
    " mini masters ",
    " munch and learn ",
    " open studio ",
    " painting ",
    " photography ",
    " project pop-up ",
    " sketch ",
    " spring to art ",
    " stage & sketch ",
    " stitching ",
    " studio courses ",
    " workshop ",
    " workshops ",
)
HARD_EXCLUDE_PATTERNS = (
    " book club ",
    " club de lectura ",
    " easter egg hunt ",
    " festival ",
    " film screening ",
    " food truck ",
    " garden fair ",
    " gardening ",
    " hydrangea ",
    " member event ",
    " opening reception ",
    " plant sale ",
    " preview party ",
    " project grow ",
    " qigong ",
    " reception ",
    " sale ",
    " secrets in the garden ",
    " speaker event ",
    " symphony ",
    " taijiquan ",
    " ticket includes two complimentary beverages ",
    " tour ",
    " tours ",
    " wrestling ",
    " yoga ",
)
LECTURE_TOPIC_EXCLUDE_PATTERNS = (
    " clematis ",
    " flowering shrub ",
    " flowering shrubs ",
    " gardening ",
    " hydrangea ",
    " plant sale ",
    " shrubs ",
    " wrestling ",
)
OBVIOUS_LISTING_EXCLUDE_PATTERNS = (
    " art for all festival ",
    " book club ",
    " club de lectura ",
    " easter egg hunt ",
    " film screening ",
    " food truck ",
    " garden fair ",
    " hydrangea society ",
    " masquerade ",
    " opening reception ",
    " plant sale ",
    " preview party ",
    " qigong ",
    " reception ",
    " sale ",
    " speaker event ",
    " symphony ",
    " taijiquan ",
    " tour ",
    " yoga ",
)

MULTI_SPACE_RE = re.compile(r"\s+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9+]+")
INSTANCE_DATE_RE = re.compile(r"/event/\d+/(\d{4})/(\d{2})/(\d{2})/?$")
DATE_TEXT_RE = re.compile(r"^[A-Za-z]+\s+\d{1,2},\s+\d{4}$")
AGE_RANGE_RE = re.compile(r"\b(?:ages?|teens?)\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\b(?:ages?\s*)?(\d{1,2})\+(?!\d)", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*[ap]\.?\s*m\.?)\s*[-–]\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?\s*[ap]\.?\s*m\.?)",
    re.IGNORECASE,
)
SINGLE_TIME_RE = re.compile(r"(?P<time>\d{1,2}(?::\d{2})?\s*[ap]\.?\s*m\.?)", re.IGNORECASE)


async def fetch_dixon_page(
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
        raise RuntimeError("Unable to fetch Dixon Gallery & Gardens page") from last_exception
    raise RuntimeError("Unable to fetch Dixon Gallery & Gardens page after retries")


async def load_dixon_payload(*, page_limit: int | None = None) -> dict:
    events: list[dict] = []
    page_signatures: set[tuple[str, ...]] = set()
    pages_seen = 0
    max_pages = max(page_limit, 1) if page_limit is not None else MAX_PAGE_SCAN

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for page_number in range(1, max_pages + 1):
            page_url = DIXON_EVENTS_URL if page_number == 1 else f"{DIXON_EVENTS_URL}?page={page_number}"
            html = await fetch_dixon_page(page_url, client=client)
            cards = _parse_listing_page(html, page_url)
            signature = tuple(card["source_url"] for card in cards)
            if not cards or signature in page_signatures:
                break

            page_signatures.add(signature)
            events.extend(cards)
            pages_seen += 1

            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break

        semaphore = asyncio.Semaphore(DETAIL_FETCH_CONCURRENCY)

        async def _fetch_detail(event: dict) -> None:
            if not _should_fetch_detail(event.get("title")):
                return
            async with semaphore:
                event["detail_html"] = await fetch_dixon_page(event["source_url"], client=client)

        await asyncio.gather(*[_fetch_detail(event) for event in events])

    return {"events": events}


def parse_dixon_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        row = _build_row(event_obj)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue
        if (row.start_at.date() - current_date).days > MAX_FUTURE_DAYS:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class DixonEventsAdapter(BaseSourceAdapter):
    source_name = "dixon_events"

    def __init__(self, url: str = DIXON_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_dixon_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_dixon_payload/parse_dixon_payload from script runner.")


def _parse_listing_page(html: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict] = []
    seen_urls: set[str] = set()

    for article in soup.select("article"):
        link = article.select_one('a[href*="/events/event/"]')
        title_node = article.select_one("h1")
        time_node = article.select_one("p.text-1p4.font-semibold")
        if link is None or title_node is None:
            continue

        source_url = urljoin(page_url, link.get("href") or "")
        title = _normalize_space(title_node.get_text(" ", strip=True))
        if not source_url or not title or source_url in seen_urls:
            continue

        seen_urls.add(source_url)
        cards.append(
            {
                "title": title,
                "source_url": source_url,
                "listing_time_text": _normalize_space(time_node.get_text(" ", strip=True) if time_node else None),
                "instance_date": _extract_instance_date(source_url),
            }
        )

    return cards


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _normalize_space(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("source_url"))
    detail_html = event_obj.get("detail_html")
    if not title or not source_url or not isinstance(detail_html, str):
        return None

    detail_soup = BeautifulSoup(detail_html, "html.parser")
    article = detail_soup.select_one("article.relative")
    if article is None:
        return None

    detail_title_node = article.find("h1")
    if detail_title_node is not None:
        title = _normalize_space(detail_title_node.get_text(" ", strip=True)) or title

    description_parts = _extract_description_parts(article)
    description = _join_non_empty(description_parts)
    tags = _extract_tags(article)
    time_values = [_normalize_space(node.get_text(" ", strip=True)) for node in article.find_all("time")]
    date_text, time_text = _extract_temporal_texts(
        time_values=time_values,
        listing_instance_date=event_obj.get("instance_date"),
        listing_time_text=event_obj.get("listing_time_text"),
    )
    start_at, end_at = _parse_event_datetimes(date_text, time_text)
    if start_at is None:
        return None

    age_text = " ".join(part for part in [title, description or "", " ".join(tags)] if part)
    signal_text = _normalize_signal_text(age_text)
    token_blob = f" {signal_text} "
    if any(pattern in token_blob for pattern in HARD_EXCLUDE_PATTERNS):
        return None
    if _is_non_art_lecture(token_blob):
        return None
    if not any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return None

    full_description = _join_non_empty(
        [
            description,
            f"Tags: {', '.join(tags)}" if tags else None,
        ]
    )
    is_free, free_status = infer_price_classification(full_description)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=DIXON_VENUE_NAME,
        location_text=_extract_location(article) or DIXON_DEFAULT_LOCATION,
        city=DIXON_CITY,
        state=DIXON_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=_extract_age_min(age_text),
        age_max=_extract_age_max(age_text),
        drop_in=(" drop-in " in token_blob or " drop in " in token_blob),
        registration_required=_requires_registration(token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_temporal_texts(
    *,
    time_values: list[str],
    listing_instance_date: object,
    listing_time_text: object,
) -> tuple[str | None, str | None]:
    date_text: str | None = None
    time_text: str | None = None

    for value in time_values:
        normalized = _normalize_space(value)
        if not normalized:
            continue
        if DATE_TEXT_RE.match(normalized):
            date_text = normalized
            continue
        if SINGLE_TIME_RE.search(normalized):
            time_text = normalized

    if date_text is None and isinstance(listing_instance_date, date):
        date_text = listing_instance_date.isoformat()
    if time_text is None:
        time_text = _normalize_space(listing_time_text)
    return date_text, time_text


def _parse_event_datetimes(date_text: str | None, time_text: str | None) -> tuple[datetime | None, datetime | None]:
    event_date = _parse_date_text(date_text)
    if event_date is None:
        return None, None
    if not time_text:
        start_at = datetime.combine(event_date, datetime.min.time())
        return start_at, None

    match = TIME_RANGE_RE.search(time_text)
    if match is not None:
        start_time = _parse_time_value(match.group("start"))
        end_time = _parse_time_value(match.group("end"))
        if start_time is None:
            return None, None
        start_at = datetime.combine(event_date, start_time)
        end_at = datetime.combine(event_date, end_time) if end_time is not None else None
        return start_at, end_at

    single_match = SINGLE_TIME_RE.search(time_text)
    if single_match is None:
        return datetime.combine(event_date, datetime.min.time()), None

    start_time = _parse_time_value(single_match.group("time"))
    if start_time is None:
        return None, None
    return datetime.combine(event_date, start_time), None


def _parse_date_text(value: str | None) -> date | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None

    try:
        return date.fromisoformat(normalized)
    except ValueError:
        pass

    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue
    return None


def _parse_time_value(value: str) -> time | None:
    normalized = _normalize_space(value).lower().replace(".", "")
    normalized = normalized.replace(" ", "")
    for fmt in ("%I:%M%p", "%I%p"):
        try:
            return datetime.strptime(normalized, fmt).time()
        except ValueError:
            continue
    return None


def _extract_description_parts(article: BeautifulSoup) -> list[str]:
    container = _find_detail_body_container(article)
    if container is None:
        return []
    parts: list[str] = []
    for paragraph in container.find_all("p"):
        text = _normalize_space(paragraph.get_text(" ", strip=True))
        if text:
            parts.append(text)
    return parts


def _find_detail_body_container(article: BeautifulSoup):
    for div in article.find_all("div"):
        classes = div.get("class") or []
        if "pt-4" in classes and "border-t" in classes:
            return div
    return None


def _extract_location(article: BeautifulSoup) -> str | None:
    venue_text = _normalize_space(_text_or_none(article.select_one("p.font-bold")))
    address_text = _normalize_space(_text_or_none(article.select_one("p.mb-1")))
    return _join_non_empty([venue_text, address_text], separator=", ")


def _extract_tags(article: BeautifulSoup) -> list[str]:
    tags: list[str] = []
    for node in article.select("div.space-y-p5 a"):
        value = _normalize_space(node.get_text(" ", strip=True))
        if value:
            tags.append(value)
    return tags


def _should_fetch_detail(title: object) -> bool:
    normalized = _normalized_token_blob(title)
    if not normalized:
        return False
    return not any(pattern in normalized for pattern in OBVIOUS_LISTING_EXCLUDE_PATTERNS)


def _infer_activity_type(token_blob: str) -> str | None:
    if " lecture " in token_blob or " lectures " in token_blob or " munch and learn " in token_blob:
        return "lecture"
    if (
        " workshop " in token_blob
        or " workshops " in token_blob
        or " class " in token_blob
        or " classes " in token_blob
        or " mini masters " in token_blob
        or " kaleidoscope club " in token_blob
        or " project pop-up " in token_blob
        or " stage & sketch " in token_blob
        or " open studio " in token_blob
        or " creative aging " in token_blob
    ):
        return "workshop"
    return None


def _requires_registration(token_blob: str) -> bool | None:
    if " no registration required " in token_blob:
        return False
    return (
        " register at " in token_blob
        or " registration required " in token_blob
        or " click here to register " in token_blob
        or " tickets must be purchased in advance " in token_blob
        or " space limited " in token_blob
    )


def _is_non_art_lecture(token_blob: str) -> bool:
    if " munch and learn " not in token_blob and " lecture " not in token_blob and " lectures " not in token_blob:
        return False
    return any(pattern in token_blob for pattern in LECTURE_TOPIC_EXCLUDE_PATTERNS)


def _extract_age_min(value: str | None) -> int | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None
    match = AGE_RANGE_RE.search(normalized)
    if match is not None:
        return int(match.group(1))
    match = AGE_PLUS_RE.search(normalized)
    if match is not None:
        return int(match.group(1))
    return None


def _extract_age_max(value: str | None) -> int | None:
    normalized = _normalize_space(value)
    if not normalized:
        return None
    match = AGE_RANGE_RE.search(normalized)
    if match is not None:
        return int(match.group(2))
    return None


def _extract_instance_date(source_url: str) -> date | None:
    match = INSTANCE_DATE_RE.search(source_url)
    if match is None:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def _normalized_token_blob(value: object) -> str:
    if value is None:
        return ""
    text = _normalize_signal_text(str(value))
    return f" {text} " if text else ""


def _normalize_signal_text(value: str) -> str:
    lowered = value.lower()
    cleaned = NON_ALNUM_RE.sub(" ", lowered)
    return _normalize_space(cleaned)


def _join_non_empty(parts: list[str | None], *, separator: str = "\n\n") -> str | None:
    values = [value for value in (_normalize_space(part) for part in parts) if value]
    if not values:
        return None
    return separator.join(values)


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    return MULTI_SPACE_RE.sub(" ", str(value)).strip()


def _text_or_none(node: object) -> str | None:
    if node is None or not hasattr(node, "get_text"):
        return None
    return node.get_text(" ", strip=True)
