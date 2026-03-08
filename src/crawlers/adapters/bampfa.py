import asyncio
import re
import subprocess
from datetime import datetime
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

BAMPFA_CALENDAR_URL = "https://bampfa.org/visit/calendar"

LA_TIMEZONE = "America/Los_Angeles"
BAMPFA_VENUE_NAME = "Berkeley Art Museum and Pacific Film Archive"
BAMPFA_CITY = "Berkeley"
BAMPFA_STATE = "CA"
BAMPFA_DEFAULT_LOCATION = "Berkeley, CA"

DATE_RE = re.compile(r"^[A-Za-z]+,\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})$")
AGE_RANGE_RE = re.compile(r"\b(?:for\s+)?ages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://bampfa.org/",
}

EXCLUDED_KEYWORDS = (
    "ticket",
    "tickets",
    "tour",
    "registration",
    "camp",
    "free night",
    "free first thursdays",
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
    "members",
)
INCLUDED_KEYWORDS = (
    "talk",
    "class",
    "lecture",
    "activity",
    "workshop",
    "lab",
    "conversation",
)


async def fetch_bampfa_calendar_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[bampfa-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[bampfa-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[bampfa-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[bampfa-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[bampfa-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[bampfa-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text
            if response.status_code == 403:
                print("[bampfa-fetch] received 403, retrying with curl fallback")
                return await asyncio.to_thread(_fetch_with_curl, url)

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[bampfa-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch BAMPFA calendar page") from last_exception
    raise RuntimeError("Unable to fetch BAMPFA calendar page after retries")


class BampfaCalendarAdapter(BaseSourceAdapter):
    source_name = "bampfa_calendar"

    def __init__(self, url: str = BAMPFA_CALENDAR_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_bampfa_calendar_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_bampfa_events_html(payload, list_url=self.url)


def parse_bampfa_events_html(
    html: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    current_date = (now or datetime.now()).date()

    for popup in soup.select(".popupboxthing[data-popup]"):
        detail_link = popup.select_one(".event-content .title a[href]")
        title = _normalize_text(detail_link.get_text(" ", strip=True) if detail_link else None)
        if not title or not detail_link:
            continue
        if is_irrelevant_item_text(title):
            continue

        summary = _normalize_text(_text_from_node(popup.select_one(".event-summary")))
        event_information = _normalize_text(_text_from_node(popup.select_one(".event-information")))
        admission_information = _normalize_text(_text_from_node(popup.select_one(".admission_information")))
        series_name = _normalize_text(_text_from_node(popup.select_one(".parent-series a")))
        date_text = _normalize_text(_text_from_node(popup.select_one(".popupboxthing-date")))
        time_text = _normalize_text(_text_from_node(popup.select_one(".popupboxthing-time strong")))

        metadata_block = popup.find_previous_sibling("div", class_="views-field")
        tags = [
            _normalize_space(item.get_text(" ", strip=True))
            for item in metadata_block.select(".calendar_filter li")
        ] if metadata_block else []

        if _should_exclude_event(title=title, summary=summary, tags=tags):
            continue
        if not _should_include_event(title=title, summary=summary, tags=tags, series_name=series_name):
            continue

        source_url = urljoin(list_url, detail_link["href"])
        start_at, end_at = _parse_calendar_link_datetimes(popup)
        if (
            start_at is not None
            and time_text
            and time_text.lower() != "all day"
            and start_at.hour == 0
            and start_at.minute == 0
        ):
            popup_start_at, popup_end_at = _parse_popup_datetimes(date_text=date_text, time_text=time_text)
            if popup_start_at is not None:
                start_at = popup_start_at
                end_at = popup_end_at
        if start_at is None:
            start_at, end_at = _parse_popup_datetimes(date_text=date_text, time_text=time_text)
        if start_at is None:
            continue
        if start_at.date() < current_date:
            continue

        description_parts = []
        if summary:
            description_parts.append(summary)
        if event_information:
            description_parts.append(f"Audience: {event_information}")
        if admission_information:
            description_parts.append(f"Admission: {admission_information}")
        if tags:
            description_parts.append(f"Tags: {', '.join(tags)}")
        if series_name:
            description_parts.append(f"Series: {series_name}")
        description = " | ".join(description_parts) if description_parts else None

        age_min, age_max = _parse_age_range(
            title=title,
            event_information=event_information,
            summary=summary,
            admission_information=admission_information,
        )
        text_blob = " ".join(
            [
                title,
                summary or "",
                event_information or "",
                admission_information or "",
                " ".join(tags),
                series_name or "",
            ]
        ).lower()

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=BAMPFA_VENUE_NAME,
                location_text=BAMPFA_DEFAULT_LOCATION,
                city=BAMPFA_CITY,
                state=BAMPFA_STATE,
                activity_type=_infer_activity_type(title=title, tags=tags, series_name=series_name),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=("registration" in text_blob and "not required" not in text_blob),
                start_at=start_at,
                end_at=end_at,
                timezone=LA_TIMEZONE,
                free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
            )
        )

    return rows


def _should_exclude_event(*, title: str, summary: str | None, tags: list[str]) -> bool:
    blob = " ".join([title, summary or "", " ".join(tags)]).lower()
    return any(keyword in blob for keyword in EXCLUDED_KEYWORDS)


def _should_include_event(
    *,
    title: str,
    summary: str | None,
    tags: list[str],
    series_name: str | None,
) -> bool:
    blob = " ".join([title, summary or "", " ".join(tags), series_name or ""]).lower()
    tag_blob = " ".join(tag.lower() for tag in tags)
    if any(tag in tag_blob for tag in ("workshop", "activity", "families")):
        return True
    return any(keyword in blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(*, title: str, tags: list[str], series_name: str | None) -> str:
    blob = " ".join([title, " ".join(tags), series_name or ""]).lower()
    if any(keyword in blob for keyword in ("talk", "lecture", "conversation")):
        return "talk"
    if any(keyword in blob for keyword in ("workshop", "lab", "class")):
        return "workshop"
    return "activity"


def _parse_calendar_link_datetimes(popup: BeautifulSoup) -> tuple[datetime | None, datetime | None]:
    add_to_calendar_link = popup.select_one("a.add-to-cal-link[href]")
    if not add_to_calendar_link:
        return None, None

    href = unquote(add_to_calendar_link["href"].strip())
    parsed = urlparse(href)
    dates_value = parse_qs(parsed.query).get("dates", [])
    if not dates_value:
        return None, None

    parts = dates_value[0].split("/")
    if not parts:
        return None, None

    start_at = _parse_compact_datetime(parts[0])
    end_at = _parse_compact_datetime(parts[1]) if len(parts) > 1 else None
    if start_at and end_at and end_at == start_at:
        end_at = None
    return start_at, end_at


def _parse_compact_datetime(value: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None

    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _parse_popup_datetimes(
    *,
    date_text: str | None,
    time_text: str | None,
) -> tuple[datetime | None, datetime | None]:
    if not date_text:
        return None, None

    match = DATE_RE.match(_normalize_space(date_text))
    if not match:
        return None, None

    try:
        day = datetime.strptime(match.group(1), "%B %d, %Y")
    except ValueError:
        return None, None

    if not time_text or time_text.lower() == "all day":
        return day, None

    compact = time_text.replace("–", "-").replace("—", "-")
    if "-" not in compact:
        return _parse_single_time(day, compact), None

    start_text, end_text = [part.strip() for part in compact.split("-", 1)]
    start_at = _parse_single_time(day, start_text, fallback_meridiem=_extract_meridiem(end_text))
    end_at = _parse_single_time(day, end_text)
    return start_at, end_at


def _parse_single_time(
    day: datetime,
    time_text: str,
    *,
    fallback_meridiem: str | None = None,
) -> datetime | None:
    normalized = _normalize_space(time_text).upper()
    if fallback_meridiem and fallback_meridiem not in normalized:
        normalized = f"{normalized} {fallback_meridiem}"

    for fmt in ("%I:%M %p", "%I %p"):
        try:
            parsed = datetime.strptime(normalized, fmt)
            return day.replace(hour=parsed.hour, minute=parsed.minute, second=0, microsecond=0)
        except ValueError:
            continue
    return None


def _extract_meridiem(value: str) -> str | None:
    upper = value.upper()
    if upper.endswith("AM"):
        return "AM"
    if upper.endswith("PM"):
        return "PM"
    return None


def _parse_age_range(
    *,
    title: str,
    event_information: str | None,
    summary: str | None,
    admission_information: str | None,
) -> tuple[int | None, int | None]:
    blob = " ".join([title, event_information or "", summary or "", admission_information or ""])

    range_match = AGE_RANGE_RE.search(blob)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))

    plus_match = AGE_PLUS_RE.search(blob)
    if plus_match:
        return int(plus_match.group(1)), None

    return None, None


def _text_from_node(node) -> str | None:
    if node is None:
        return None
    return node.get_text(" ", strip=True)


def _fetch_with_curl(url: str) -> str:
    result = subprocess.run(
        ["curl", "-L", "--max-time", "30", url],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise RuntimeError(f"curl fallback failed: {stderr or f'exit code {result.returncode}'}")
    return result.stdout


def _normalize_space(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    text = _normalize_space(str(value))
    return text or None
