import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

CROCKER_EVENTS_URL = "https://www.crockerart.org/events"

LA_TIMEZONE = "America/Los_Angeles"
CROCKER_VENUE_NAME = "Crocker Art Museum"
CROCKER_CITY = "Sacramento"
CROCKER_STATE = "CA"
CROCKER_DEFAULT_LOCATION = "Sacramento, CA"

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(?P<payload>.*?)</script>'
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PAREN_RANGE_RE = re.compile(r"\((?:ages?\s*)?(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\)", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.crockerart.org/",
}

EXCLUDED_CATEGORY_KEYWORDS = (
    "guided tours",
    "tour",
    "films",
    "film",
    "concert",
    "artmix",
    "camp",
    "member event",
    "member",
    "exhibition-related",
    "performance",
    "music",
)
EXCLUDED_KEYWORDS = (
    "ticket",
    "tickets",
    "tour",
    "registration",
    "camp",
    "free night",
    "fundraising",
    "admission",
    "exhibition",
    "film",
    "tv",
    "reading",
    "writing",
    "open house",
    "performance",
    "music",
    "concert",
    "festival",
    "auction",
    "gala",
)
INCLUDED_KEYWORDS = (
    "talk",
    "class",
    "lecture",
    "activity",
    "workshop",
    "lab",
    "conversation",
    "studio",
    "artmaking",
    "hands-on",
    "painting",
    "paint",
    "drawing",
    "draw",
    "printmaking",
    "collage",
    "clay",
    "sculpt",
)
ALLOWED_CATEGORIES = {
    "adult studio classes",
    "kids studio classes",
    "studio art classes",
    "talks + conversations",
    "school + educator programs",
    "art + wellness",
}
FAMILY_ACTIVITY_CATEGORIES = {
    "family programs",
    "series",
}
FAMILY_ACTIVITY_KEYWORDS = (
    "artmaking",
    "clay",
    "sculpt",
    "paint",
    "painting",
    "drawing",
    "draw",
    "collage",
    "printmaking",
    "visual literacy",
    "workshop",
)


async def fetch_crocker_events_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[crocker-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[crocker-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[crocker-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[crocker-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[crocker-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[crocker-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[crocker-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Crocker events page") from last_exception
    raise RuntimeError("Unable to fetch Crocker events page after retries")


class CrockerEventsAdapter(BaseSourceAdapter):
    source_name = "crocker_events"

    def __init__(self, url: str = CROCKER_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_crocker_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_crocker_events_html(payload, list_url=self.url)


def parse_crocker_events_html(
    html: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    rows = _parse_from_next_data(html=html, list_url=list_url, now=now)
    if rows:
        return rows
    return _parse_from_dom_fallback(html=html, list_url=list_url, now=now)


def _parse_from_next_data(
    *,
    html: str,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    match = NEXT_DATA_RE.search(html)
    if match is None:
        return []

    try:
        payload = json.loads(match.group("payload"))
    except json.JSONDecodeError:
        return []

    days = payload.get("props", {}).get("pageProps", {}).get("days", [])
    if not isinstance(days, list):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = (now or datetime.now()).date()

    for day in days:
        if not isinstance(day, dict):
            continue
        for item in day.get("items", []):
            if not isinstance(item, dict):
                continue
            row = _build_row_from_item(item=item, list_url=list_url, current_date=current_date)
            if row is None:
                continue
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    return rows


def _build_row_from_item(
    *,
    item: dict,
    list_url: str,
    current_date,
) -> ExtractedActivity | None:
    title = _normalize_text(item.get("title"))
    if not title or is_irrelevant_item_text(title):
        return None

    category = _normalize_text((item.get("PerformanceType") or {}).get("Description"))
    performance_description = _normalize_text(item.get("PerformanceDescription"))
    description = _normalize_text(item.get("description"))
    slug = _normalize_text(item.get("slug"))
    if not slug:
        return None

    if _should_exclude_event(
        title=title,
        description=description,
        category=category,
        performance_description=performance_description,
    ):
        return None
    if not _should_include_event(
        title=title,
        description=description,
        category=category,
        performance_description=performance_description,
    ):
        return None

    start_at = _parse_datetime(item.get("PerformanceDate"))
    if start_at is None or start_at.date() < current_date:
        return None

    source_url = _build_event_source_url(item=item, list_url=list_url, slug=slug)
    full_description = _build_description(
        description=description,
        category=category,
        performance_description=performance_description,
    )
    age_min, age_max = _parse_age_range(
        title=title,
        description=description,
        performance_description=performance_description,
    )
    text_blob = " ".join(
        [
            title,
            description or "",
            category or "",
            performance_description or "",
        ]
    ).lower()

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=CROCKER_VENUE_NAME,
        location_text=CROCKER_DEFAULT_LOCATION,
        city=CROCKER_CITY,
        state=CROCKER_STATE,
        activity_type=_infer_activity_type(
            title=title,
            description=description,
            category=category,
            performance_description=performance_description,
        ),
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=("registration" in text_blob and "no registration" not in text_blob),
        start_at=start_at,
        end_at=None,
        timezone=LA_TIMEZONE,
        free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
    )


def _build_event_source_url(*, item: dict, list_url: str, slug: str) -> str:
    performance_id = item.get("PerformanceId")
    if isinstance(performance_id, int) and performance_id > 0:
        return urljoin(list_url, f"/events/{performance_id}/{slug}")

    if isinstance(performance_id, str):
        performance_id = performance_id.strip()
        if performance_id.isdigit():
            return urljoin(list_url, f"/events/{performance_id}/{slug}")

    return urljoin(list_url, f"/events/{slug}")


def _parse_from_dom_fallback(
    *,
    html: str,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = (now or datetime.now()).date()

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if "/events/" not in href:
            continue
        title = _normalize_text(anchor.get_text(" ", strip=True))
        if not title or is_irrelevant_item_text(title):
            continue
        container = anchor.find_parent(["article", "li", "section", "div"]) or anchor
        blob = _normalize_space(container.get_text(" ", strip=True))
        if _should_exclude_event(
            title=title,
            description=blob,
            category=None,
            performance_description=None,
        ):
            continue
        if not _should_include_event(
            title=title,
            description=blob,
            category=None,
            performance_description=None,
        ):
            continue
        start_at = _parse_datetime(blob)
        if start_at is None or start_at.date() < current_date:
            continue
        source_url = urljoin(list_url, href)
        age_min, age_max = _parse_age_range(
            title=title,
            description=blob,
            performance_description=None,
        )
        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=blob,
                venue_name=CROCKER_VENUE_NAME,
                location_text=CROCKER_DEFAULT_LOCATION,
                city=CROCKER_CITY,
                state=CROCKER_STATE,
                activity_type=_infer_activity_type(
                    title=title,
                    description=blob,
                    category=None,
                    performance_description=None,
                ),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in blob.lower() or "drop in" in blob.lower()),
                registration_required=("registration" in blob.lower() and "no registration" not in blob.lower()),
                start_at=start_at,
                end_at=None,
                timezone=LA_TIMEZONE,
                free_verification_status=("confirmed" if "free" in blob.lower() else "inferred"),
            )
        )

    return rows


def _should_exclude_event(
    *,
    title: str,
    description: str | None,
    category: str | None,
    performance_description: str | None,
) -> bool:
    blob = " ".join([title, description or "", category or "", performance_description or ""]).lower()
    has_include_signal = _has_include_signal(
        title=title,
        description=description,
        category=category,
        performance_description=performance_description,
    )
    strong_include_signal = _has_strong_include_signal(
        title=title,
        category=category,
        performance_description=performance_description,
    )
    title_blob = " ".join([title, performance_description or ""]).lower()

    if category and category.lower() in ALLOWED_CATEGORIES:
        return False

    if "tour" in title_blob and not any(keyword in title_blob for keyword in ("talk", "lecture", "conversation")):
        return True

    if category and any(keyword in category.lower() for keyword in EXCLUDED_CATEGORY_KEYWORDS):
        if not strong_include_signal or "tours + talks" not in category.lower():
            return True

    if "tour" in blob and not strong_include_signal:
        return True

    if any(keyword in blob for keyword in EXCLUDED_KEYWORDS) and not has_include_signal:
        return True

    return False


def _should_include_event(
    *,
    title: str,
    description: str | None,
    category: str | None,
    performance_description: str | None,
) -> bool:
    if category and category.lower() in ALLOWED_CATEGORIES:
        return True

    if _has_include_signal(
        title=title,
        description=description,
        category=category,
        performance_description=performance_description,
    ):
        return True

    blob = " ".join([title, description or "", performance_description or ""]).lower()
    if category and category.lower() in FAMILY_ACTIVITY_CATEGORIES:
        return any(keyword in blob for keyword in FAMILY_ACTIVITY_KEYWORDS)

    return False


def _has_include_signal(
    *,
    title: str,
    description: str | None,
    category: str | None,
    performance_description: str | None,
) -> bool:
    blob = " ".join([title, description or "", category or "", performance_description or ""]).lower()
    return any(keyword in blob for keyword in INCLUDED_KEYWORDS)


def _has_strong_include_signal(
    *,
    title: str,
    category: str | None,
    performance_description: str | None,
) -> bool:
    blob = " ".join([title, category or "", performance_description or ""]).lower()
    return any(keyword in blob for keyword in ("talk", "lecture", "conversation", "workshop", "class", "studio", "activity", "lab"))


def _infer_activity_type(
    *,
    title: str,
    description: str | None,
    category: str | None,
    performance_description: str | None,
) -> str:
    blob = " ".join([title, description or "", category or "", performance_description or ""]).lower()
    if any(keyword in blob for keyword in ("talk", "lecture", "conversation")):
        return "talk"
    if any(keyword in blob for keyword in ("workshop", "studio", "class", "painting", "drawing", "clay", "sculpt")):
        return "workshop"
    return "activity"


def _build_description(
    *,
    description: str | None,
    category: str | None,
    performance_description: str | None,
) -> str | None:
    parts: list[str] = []
    if description:
        parts.append(description)
    if performance_description and performance_description != description:
        parts.append(f"Session: {performance_description}")
    if category:
        parts.append(f"Category: {category}")
    return " | ".join(parts) if parts else None


def _parse_age_range(
    *,
    title: str,
    description: str | None,
    performance_description: str | None,
) -> tuple[int | None, int | None]:
    blob = " ".join([title, description or "", performance_description or ""])
    for pattern in (AGE_RANGE_RE, AGE_PAREN_RANGE_RE):
        match = pattern.search(blob)
        if match:
            return int(match.group(1)), int(match.group(2))

    plus_match = AGE_PLUS_RE.search(blob)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text).replace(second=0, microsecond=0)
        if parsed.tzinfo is not None:
            # Keep the local wall-clock time in start_at and store the timezone separately.
            parsed = parsed.replace(tzinfo=None)
        return parsed
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d",
        "%B %d, %Y",
        "%b %d, %Y",
        "%A, %B %d, %Y",
    ):
        try:
            return datetime.strptime(text, fmt).replace(second=0, microsecond=0)
        except ValueError:
            continue

    return None


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    text = _normalize_space(str(value))
    return text or None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())
