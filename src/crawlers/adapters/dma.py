import asyncio
import json
import re
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

DMA_EVENTS_URL = "https://dma.org/visit/calendar"
DMA_SEARCH_URL = "https://sitesearch.dma.org/api/search/calendar"
DMA_CONTENT_BASE_URL = "https://files.dma.org/cms/production/content/en"
DMA_TIMEZONE = "America/Chicago"
DMA_VENUE_NAME = "Dallas Museum of Art"
DMA_CITY = "Dallas"
DMA_STATE = "TX"
DMA_LOCATION = "1717 N Harwood St, Dallas, TX"
DMA_PAGE_SIZE = 100

DMA_INCLUDE_KEYWORDS = (
    "workshop",
    "class",
    "lecture",
    "talk",
    "conversation",
    "activity",
    "lab",
    "studio",
)
DMA_EXCLUDE_KEYWORDS = (
    "arts & letters live",
    "author",
    "novel",
    "book",
    "fiction",
    "tour",
    "camp",
    "free first sundays",
    "free admission",
    "admission",
    "film",
    "jazz",
    "music",
    "performance",
    "reading",
    "writing",
    "poetry",
    "storytime",
    "open house",
    "reception",
    "meditation",
    "mindfulness",
    "yoga",
    "brunch",
)
AGE_RANGE_RE = re.compile(r"\b(\d{1,2})\s*(?:to|-|–)\s*(\d{1,2})\s+year\s+old", re.IGNORECASE)
AGE_AND_RE = re.compile(r"\b(\d{1,2})\s+and\s+(\d{1,2})\s+year\s+olds?\b", re.IGNORECASE)
AGES_LABEL_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:to|-|–)\s*(\d{1,2})\b", re.IGNORECASE)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": DMA_EVENTS_URL,
}


async def fetch_dma_json(
    url: str,
    *,
    params: dict[str, object] | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> dict[str, object]:
    print(f"[dma-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[dma-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url, params=params)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[dma-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[dma-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[dma-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                return response.json()

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[dma-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Dallas Museum of Art JSON") from last_exception
    raise RuntimeError("Unable to fetch Dallas Museum of Art JSON after retries")


def _default_date_window(now: datetime | None = None) -> tuple[int, int]:
    reference = now or datetime.now(UTC)
    start = datetime(reference.year, reference.month, reference.day, tzinfo=UTC)
    end = start + timedelta(days=180)
    return int(start.timestamp()), int(end.timestamp())


def build_dma_detail_json_url(path: str) -> str:
    normalized = path if path.startswith("/") else f"/{path}"
    return f"{DMA_CONTENT_BASE_URL}{normalized}.json"


async def load_dma_payload(
    base_url: str = DMA_EVENTS_URL,
    *,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
) -> dict[str, object]:
    del base_url  # The page URL is fixed; the API endpoints below hold the data we need.
    if start_timestamp is None or end_timestamp is None:
        start_timestamp, end_timestamp = _default_date_window()

    search_pages: list[dict[str, object]] = []
    all_results: list[dict[str, object]] = []
    unique_paths: list[str] = []
    seen_paths: set[str] = set()
    from_offset = 0

    while True:
        params = {
            "size": DMA_PAGE_SIZE,
            "from": from_offset,
            "dateFrom": start_timestamp,
            "dateTo": end_timestamp,
        }
        page = await fetch_dma_json(DMA_SEARCH_URL, params=params)
        search_pages.append(page)
        results = page.get("results") or []
        if not isinstance(results, list):
            break

        for result in results:
            if not isinstance(result, dict):
                continue
            all_results.append(result)
            path = str(result.get("url") or "").strip()
            if not path or path in seen_paths:
                continue
            seen_paths.add(path)
            unique_paths.append(path)

        pagination = page.get("pagination") or {}
        if not isinstance(pagination, dict) or not pagination.get("hasNext"):
            break
        from_offset += DMA_PAGE_SIZE

    detail_jsons_raw = await asyncio.gather(
        *(fetch_dma_json(build_dma_detail_json_url(path)) for path in unique_paths)
    )
    detail_jsons = dict(zip(unique_paths, detail_jsons_raw))
    return {
        "search_pages": search_pages,
        "results": all_results,
        "detail_jsons": detail_jsons,
    }


class DmaEventsAdapter(BaseSourceAdapter):
    source_name = "dma_events"

    def __init__(self, url: str = DMA_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_dma_payload(self.url)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_dma_payload(data)


def parse_dma_payload(payload: dict[str, object]) -> list[ExtractedActivity]:
    results = payload.get("results") or []
    detail_jsons = payload.get("detail_jsons") or {}
    if not isinstance(results, list) or not isinstance(detail_jsons, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for result in results:
        if not isinstance(result, dict):
            continue
        path = str(result.get("url") or "").strip()
        if not path:
            continue
        detail = detail_jsons.get(path)
        if not isinstance(detail, dict):
            continue

        title = _normalize_space(result.get("field_caption_text") or _detail_title(detail))
        if not title:
            continue

        description = _extract_detail_description(detail)
        category = _detail_category(detail)
        audiences = _detail_audiences(detail)
        title_category_blob = " ".join([title, category]).lower()
        text_blob = " ".join([title, description or "", category, " ".join(audiences)]).lower()
        if not _should_keep_dma_event(title_category_blob=title_category_blob, text_blob=text_blob):
            continue

        start_at = _timestamp_to_naive_datetime(result.get("date_timestamp"))
        if start_at is None:
            continue
        end_at = _timestamp_to_naive_datetime(result.get("date_end_timestamp"))

        detail_attributes = detail.get("data", {}).get("attributes", {})
        regular_cost = _to_float(detail_attributes.get("field_regular_cost"))
        member_cost = _to_float(detail_attributes.get("field_members_cost"))
        price_amount = regular_cost if regular_cost is not None else member_cost
        is_free, free_status = infer_price_classification_from_amount(
            price_amount,
            text=description,
        )

        location_name = _detail_location_name(detail)
        if location_name:
            location_text = f"{location_name}, {DMA_LOCATION}"
        else:
            location_text = DMA_LOCATION

        age_min, age_max = _extract_age_range(title=title, description=description)
        registration_required = bool(
            detail_attributes.get("field_purchase_ticket_url")
            or detail_attributes.get("field_member_purchase_ticket_url")
            or "registration" in text_blob
            or "ticket" in text_blob
            or "waiting list" in text_blob
        )

        item = ExtractedActivity(
            source_url=urljoin(DMA_EVENTS_URL, path),
            title=title,
            description=_combine_description(description, category, audiences, detail_attributes),
            venue_name=DMA_VENUE_NAME,
            location_text=location_text,
            city=DMA_CITY,
            state=DMA_STATE,
            activity_type=_infer_dma_activity_type(title=title, description=description, category=category),
            age_min=age_min,
            age_max=age_max,
            drop_in=("drop-in" in text_blob or "drop in" in text_blob),
            registration_required=registration_required,
            start_at=start_at,
            end_at=end_at,
            timezone=DMA_TIMEZONE,
            is_free=is_free,
            free_verification_status=free_status,
        )
        key = (item.source_url, item.title, item.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(item)

    return rows


def _should_keep_dma_event(*, title_category_blob: str, text_blob: str) -> bool:
    if any(keyword in text_blob for keyword in DMA_EXCLUDE_KEYWORDS):
        return False
    return any(keyword in title_category_blob for keyword in DMA_INCLUDE_KEYWORDS)


def _detail_title(detail: dict[str, object]) -> str:
    data = detail.get("data") or {}
    attributes = data.get("attributes") or {}
    return _normalize_space(attributes.get("title"))


def _detail_category(detail: dict[str, object]) -> str:
    relationship = (((detail.get("data") or {}).get("relationships") or {}).get("field_event_category") or {})
    data = relationship.get("data")
    if not isinstance(data, dict):
        return ""
    return _normalize_space((data.get("attributes") or {}).get("name"))


def _detail_audiences(detail: dict[str, object]) -> list[str]:
    audiences = (
        (((detail.get("data") or {}).get("relationships") or {}).get("field_audiences") or {})
        .get("data", [])
    )
    values: list[str] = []
    if not isinstance(audiences, list):
        return values
    for audience in audiences:
        if not isinstance(audience, dict):
            continue
        name = _normalize_space((audience.get("attributes") or {}).get("name"))
        if name:
            values.append(name)
    return values


def _detail_location_name(detail: dict[str, object]) -> str | None:
    relationship = (((detail.get("data") or {}).get("relationships") or {}).get("field_location") or {})
    data = relationship.get("data")
    if not isinstance(data, dict):
        return None
    name = _normalize_space((data.get("attributes") or {}).get("name"))
    return name or None


def _extract_detail_description(detail: dict[str, object]) -> str | None:
    paragraphs = (
        (((detail.get("data") or {}).get("relationships") or {}).get("field_paragraph") or {})
        .get("data", [])
    )
    if not isinstance(paragraphs, list):
        return None

    description_parts: list[str] = []
    for paragraph in paragraphs:
        if not isinstance(paragraph, dict):
            continue
        attrs = paragraph.get("attributes") or {}
        field_text = attrs.get("field_text")
        if not isinstance(field_text, dict):
            continue
        processed = field_text.get("processed")
        if not processed:
            continue
        text = _html_to_text(str(processed))
        if text:
            description_parts.append(text)

    if not description_parts:
        return None
    return " ".join(description_parts)


def _combine_description(
    description: str | None,
    category: str,
    audiences: list[str],
    detail_attributes: dict[str, object],
) -> str | None:
    parts: list[str] = []
    if description:
        parts.append(description)
    if category:
        parts.append(f"Category: {category}")
    if audiences:
        parts.append(f"Audiences: {', '.join(audiences)}")

    regular_cost = _to_float(detail_attributes.get("field_regular_cost"))
    member_cost = _to_float(detail_attributes.get("field_members_cost"))
    if regular_cost is not None or member_cost is not None:
        cost_parts: list[str] = []
        if member_cost is not None:
            cost_parts.append(f"Members ${member_cost:g}")
        if regular_cost is not None:
            cost_parts.append(f"Public ${regular_cost:g}")
        parts.append("Pricing: " + ", ".join(cost_parts))

    return " | ".join(parts) if parts else None


def _infer_dma_activity_type(*, title: str, description: str | None, category: str) -> str:
    title_text = title.lower()
    text = f"{title} {description or ''} {category}".lower()
    if "workshop" in title_text or "class" in title_text or "classes & workshops" in text:
        return "workshop"
    if "lecture" in title_text or "talk" in title_text or "conversation" in title_text:
        return "talk"
    if "lecture" in text or "talk" in text or "conversation" in text:
        return "talk"
    if "activity" in text or "lab" in text or "studio" in text:
        return "activity"
    return "activity"


def _extract_age_range(*, title: str, description: str | None) -> tuple[int | None, int | None]:
    text = f"{title} {description or ''}"
    match = AGE_RANGE_RE.search(text)
    if match is not None:
        low = int(match.group(1))
        high = int(match.group(2))
        return min(low, high), max(low, high)

    match = AGE_AND_RE.search(text)
    if match is not None:
        low = int(match.group(1))
        high = int(match.group(2))
        return min(low, high), max(low, high)

    match = AGES_LABEL_RE.search(text)
    if match is not None:
        low = int(match.group(1))
        high = int(match.group(2))
        return min(low, high), max(low, high)

    return None, None


def _timestamp_to_naive_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), ZoneInfo(DMA_TIMEZONE)).replace(tzinfo=None)
    except (TypeError, ValueError, OSError):
        return None


def _to_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _html_to_text(value: str) -> str:
    soup = BeautifulSoup(value, "html.parser")
    return _normalize_space(soup.get_text(" ", strip=True))


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())
