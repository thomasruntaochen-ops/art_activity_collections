import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

PSMUSEUM_PROGRAMS_URL = "https://www.psmuseum.org/events/programs-events"
PSMUSEUM_FPLUS_URL = "https://www.psmuseum.org/events/fplus"
PSMUSEUM_ARTFUL_URL = "https://www.psmuseum.org/events/artful-events"

LA_TIMEZONE = "America/Los_Angeles"
PSMUSEUM_VENUE_NAME = "Palm Springs Art Museum"
PSMUSEUM_CITY = "Palm Springs"
PSMUSEUM_STATE = "CA"
PSMUSEUM_DEFAULT_LOCATION = "Palm Springs, CA"

EXCLUDED_PRIMARY_KEYWORDS = (
    "ticket",
    "tickets",
    "tour",
    "registration",
    "camp",
    "free night",
    "free thursday night",
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
    "comedy",
    "member event",
    "member preview",
)
INCLUDED_KEYWORDS = (
    "talk",
    "lecture",
    "conversation",
    "class",
    "activity",
    "workshop",
    "lab",
    "family",
    "family+",
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>am|pm)",
    re.IGNORECASE,
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.psmuseum.org/events/programs-events",
}


async def fetch_psmuseum_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[psmuseum-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[psmuseum-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[psmuseum-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[psmuseum-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[psmuseum-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[psmuseum-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[psmuseum-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Palm Springs Art Museum page") from last_exception
    raise RuntimeError("Unable to fetch Palm Springs Art Museum page after retries")


class PalmSpringsArtMuseumAdapter(BaseSourceAdapter):
    source_name = "psmuseum_events"

    def __init__(self, url: str = PSMUSEUM_PROGRAMS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_psmuseum_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_psmuseum_events_html(payload, list_url=self.url)


def parse_psmuseum_events_html(
    html: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    current_date = (now or datetime.now()).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for anchor in soup.select("a.m-slider__child"):
        row = _build_row_from_slider_card(anchor=anchor, list_url=list_url, current_date=current_date)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    return rows


def _build_row_from_slider_card(
    *,
    anchor,
    list_url: str,
    current_date,
) -> ExtractedActivity | None:
    title = _normalize_text(
        anchor.select_one(".m-overlay-card__title")
        or anchor.select_one(".m-card__small-title")
    )
    if not title or is_irrelevant_item_text(title):
        return None

    tag = _normalize_text(anchor.select_one(".m-overlay-card__tag") or anchor.select_one(".m-card__tag"))
    desc = _normalize_text(anchor.select_one(".m-overlay-card__desc"))
    date_text = _extract_date_text(anchor)

    text_blob = " ".join(part for part in [title, tag or "", desc or "", date_text or ""] if part).lower()
    if any(keyword in text_blob for keyword in EXCLUDED_PRIMARY_KEYWORDS):
        return None
    if not any(keyword in text_blob for keyword in INCLUDED_KEYWORDS):
        return None

    start_at, end_at = _parse_schedule(desc or date_text or "")
    if start_at is None or start_at.date() < current_date:
        return None

    source_url = urljoin(list_url, anchor.get("href", "").strip())
    description = _join_non_empty(
        [
            f"Tag: {tag}" if tag else None,
            f"Schedule: {desc}" if desc else None,
            f"Date: {date_text}" if date_text and date_text != desc else None,
        ]
    )

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=PSMUSEUM_VENUE_NAME,
        location_text=PSMUSEUM_DEFAULT_LOCATION,
        city=PSMUSEUM_CITY,
        state=PSMUSEUM_STATE,
        activity_type=_infer_activity_type(title=title, tag=tag, description=description),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=("registration" in text_blob and "not required" not in text_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=LA_TIMEZONE,
        free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
    )


def _extract_date_text(anchor) -> str | None:
    desc = _normalize_text(anchor.select_one(".m-overlay-card__desc"))
    if desc:
        return desc

    date_node = anchor.select_one(".m-content-rte p span")
    return _normalize_text(date_node)


def _parse_schedule(value: str) -> tuple[datetime | None, datetime | None]:
    normalized = _normalize_space(value)
    if not normalized:
        return None, None

    if "," not in normalized:
        return None, None
    date_text, _, trailing = normalized.partition(",")
    date_text = f"{date_text},{trailing.split(',')[0]}" if trailing and re.search(r"\b\d{4}\b", trailing) else normalized

    parts = [part.strip() for part in normalized.split(",", 1)]
    try:
        day = datetime.strptime(parts[0], "%B %d")
        day = day.replace(year=datetime.now().year)
    except ValueError:
        try:
            day = datetime.strptime(parts[0], "%B %d %Y")
        except ValueError:
            try:
                day = datetime.strptime(parts[0], "%B %d, %Y")
            except ValueError:
                return None, None

    remaining = parts[1].strip() if len(parts) > 1 else ""
    if remaining and re.fullmatch(r"\d{4}", remaining):
        return day.replace(year=int(remaining), hour=0, minute=0, second=0, microsecond=0), None

    if remaining.startswith(tuple(str(year) for year in range(2000, 2101))):
        year_text, _, time_text = remaining.partition(",")
        day = day.replace(year=int(year_text.strip()))
        remaining = time_text.strip()

    if not remaining:
        return day.replace(hour=0, minute=0, second=0, microsecond=0), None

    range_match = TIME_RANGE_RE.search(remaining)
    if range_match:
        start_hour, start_minute = _to_24h(
            int(range_match.group("start_hour")),
            int(range_match.group("start_minute") or 0),
            range_match.group("start_meridiem") or range_match.group("end_meridiem"),
        )
        end_hour, end_minute = _to_24h(
            int(range_match.group("end_hour")),
            int(range_match.group("end_minute") or 0),
            range_match.group("end_meridiem"),
        )
        return (
            day.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0),
            day.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0),
        )

    single_match = TIME_SINGLE_RE.search(remaining)
    if single_match:
        hour, minute = _to_24h(
            int(single_match.group("hour")),
            int(single_match.group("minute") or 0),
            single_match.group("meridiem"),
        )
        return day.replace(hour=hour, minute=minute, second=0, microsecond=0), None

    return day.replace(hour=0, minute=0, second=0, microsecond=0), None


def _to_24h(hour: int, minute: int, meridiem: str | None) -> tuple[int, int]:
    suffix = (meridiem or "").lower()
    if suffix == "pm" and hour != 12:
        hour += 12
    if suffix == "am" and hour == 12:
        hour = 0
    return hour, minute


def _infer_activity_type(*, title: str, tag: str | None, description: str | None) -> str:
    blob = " ".join(part for part in [title, tag or "", description or ""] if part).lower()
    if any(keyword in blob for keyword in ("talk", "lecture", "conversation")):
        return "talk"
    if any(keyword in blob for keyword in ("family", "workshop", "class", "activity", "lab")):
        return "workshop"
    return "activity"


def _normalize_text(node) -> str | None:
    if node is None:
        return None
    if hasattr(node, "get_text"):
        value = node.get_text(" ", strip=True)
    else:
        value = str(node)
    text = _normalize_space(value)
    return text or None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    return " | ".join(values) if values else None
