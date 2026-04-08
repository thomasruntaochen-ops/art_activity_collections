import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

PLAINS_ART_MUSEUM_LIST_URL = "https://plainsart.org/events/"
PLAINS_ART_MUSEUM_TIMEZONE = "America/Chicago"
PLAINS_ART_MUSEUM_VENUE_NAME = "Plains Art Museum"
PLAINS_ART_MUSEUM_CITY = "Fargo"
PLAINS_ART_MUSEUM_STATE = "ND"
PLAINS_ART_MUSEUM_DEFAULT_LOCATION = "Plains Art Museum, Fargo, ND"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://plainsart.org/",
}

STRONG_INCLUDE_MARKERS = (
    " workshop ",
    " workshops ",
    " class ",
    " classes ",
    " lecture ",
    " lectures ",
    " talk ",
    " talks ",
    " conversation ",
    " conversations ",
    " discussion ",
    " panel ",
    " lab ",
    " labs ",
    " activity ",
    " activities ",
    " artmaking ",
    " art making ",
    " studio ",
    " create ",
    " creativity ",
    " hands-on ",
    " hands on ",
    " kid quest ",
    " family program ",
    " sensory friendly ",
)
AUDIENCE_MARKERS = (
    " family ",
    " kid ",
    " kids ",
    " youth ",
    " teen ",
    " teens ",
)
ART_CONTEXT_MARKERS = (
    " art ",
    " artist ",
    " artists ",
    " gallery ",
    " galleries ",
    " museum ",
)
EXCLUDE_MARKERS = (
    " tour ",
    " tours ",
    " exhibition ",
    " exhibitions ",
    " camp ",
    " camps ",
    " yoga ",
    " fundraising ",
    " fundraiser ",
    " gala ",
    " auction ",
    " benefit ",
    " film ",
    " tv ",
    " reading ",
    " writing ",
    " performance ",
    " music ",
    " concert ",
    " story ",
    " poem ",
    " poetry ",
    " meditation ",
    " mindfulness ",
    " storytime ",
    " dinner ",
    " reception ",
    " jazz ",
    " orchestra ",
    " band ",
    " tai chi ",
    " fair ",
    " market ",
)
DATE_RE = re.compile(
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})(?:\s*[-,]\s*(?P<time_blob>.*))?$",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>a\.?m\.?|p\.?m\.?|am|pm)"
    r"\s*(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>a\.?m\.?|p\.?m\.?|am|pm)\b",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|to|–)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)


async def fetch_html(
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
        raise RuntimeError("Unable to fetch Plains Art Museum HTML") from last_exception
    raise RuntimeError("Unable to fetch Plains Art Museum HTML after retries")


async def load_plains_art_museum_payload() -> dict:
    list_html = await fetch_html(PLAINS_ART_MUSEUM_LIST_URL)
    entries = _extract_entries_from_list_html(list_html)
    detail_html_by_url: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for entry in entries:
            source_url = entry["source_url"]
            if not isinstance(source_url, str) or not source_url:
                continue
            detail_html_by_url[source_url] = await fetch_html(source_url, client=client)

    return {
        "list_html": list_html,
        "entries": entries,
        "detail_html_by_url": detail_html_by_url,
    }


def parse_plains_art_museum_payload(payload: dict) -> list[ExtractedActivity]:
    entries = payload.get("entries") or _extract_entries_from_list_html(payload.get("list_html") or "")
    detail_html_by_url = payload.get("detail_html_by_url") or {}
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for entry in entries:
        source_url = entry.get("source_url")
        detail_html = detail_html_by_url.get(source_url) if isinstance(source_url, str) else None
        row = _build_row(entry=entry, detail_html=detail_html)
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class PlainsArtMuseumAdapter(BaseSourceAdapter):
    source_name = "plains_art_museum_events"

    async def fetch(self) -> list[str]:
        payload = await load_plains_art_museum_payload()
        return [json.dumps(payload, ensure_ascii=False)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_plains_art_museum_payload(json.loads(payload))


def _extract_entries_from_list_html(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    entries: list[dict[str, str]] = []

    for card in soup.select("div.current-events-grid div.event"):
        link = card.select_one("a.view-event[href]")
        if link is None:
            continue

        source_url = urljoin(PLAINS_ART_MUSEUM_LIST_URL, link.get("href") or "")
        title = _normalize_space(_text_or_none(card.select_one("div.event-title")))
        if not title:
            title = _normalize_space(_text_or_none(card.select_one("div.event-hero h2")))
        if not title or not source_url:
            continue

        entries.append(
            {
                "source_url": source_url,
                "title": title,
                "date_text": _normalize_space(_text_or_none(card.select_one("div.event-date"))),
                "summary": _normalize_space(_text_or_none(card.select_one("div.event-summary"))),
            }
        )

    return entries


def _build_row(*, entry: dict[str, str], detail_html: str | None) -> ExtractedActivity | None:
    title = _normalize_space(entry.get("title"))
    source_url = _normalize_space(entry.get("source_url"))
    if not title or not source_url:
        return None

    detail = _parse_detail_html(detail_html or "", fallback_summary=entry.get("summary"))
    description = detail["description"] or _normalize_space(entry.get("summary"))
    text_blob = _search_blob(" ".join(part for part in [title, description] if part))
    if not _is_qualifying(text_blob):
        return None

    start_at, end_at = _parse_datetimes(detail["date_text"] or _normalize_space(entry.get("date_text")))
    if start_at is None:
        return None

    age_min, age_max = _parse_age_range(title=title, description=description)
    pricing_text = _join_non_empty([description, " | ".join(detail["cta_texts"]) if detail["cta_texts"] else None])
    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=PLAINS_ART_MUSEUM_VENUE_NAME,
        location_text=PLAINS_ART_MUSEUM_DEFAULT_LOCATION,
        city=PLAINS_ART_MUSEUM_CITY,
        state=PLAINS_ART_MUSEUM_STATE,
        activity_type=_infer_activity_type(text_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=(" drop in " in text_blob or " drop-in " in text_blob or " come and go " in text_blob),
        registration_required=_registration_required(text_blob=text_blob, cta_texts=detail["cta_texts"]),
        start_at=start_at,
        end_at=end_at,
        timezone=PLAINS_ART_MUSEUM_TIMEZONE,
        **price_classification_kwargs(pricing_text, default_is_free=None),
    )


def _parse_detail_html(detail_html: str, *, fallback_summary: str | None) -> dict[str, object]:
    if not detail_html:
        return {
            "date_text": None,
            "description": _normalize_space(fallback_summary),
            "cta_texts": [],
        }

    soup = BeautifulSoup(detail_html, "html.parser")
    summary = soup.select_one("div.event-full div.event-summary")
    description = _normalize_space(_text_or_none(summary)) or _normalize_space(fallback_summary)
    cta_texts = [
        text
        for text in (
            _normalize_space(anchor.get_text(" ", strip=True))
            for anchor in soup.select("div.event-full a[href]")
        )
        if text
    ]
    return {
        "date_text": _normalize_space(_text_or_none(soup.select_one("div.event-full div.event-date"))),
        "description": description,
        "cta_texts": cta_texts,
    }


def _is_qualifying(text_blob: str) -> bool:
    has_strong_include = any(marker in text_blob for marker in STRONG_INCLUDE_MARKERS)
    has_audience_include = any(marker in text_blob for marker in AUDIENCE_MARKERS)
    has_art_context = any(marker in text_blob for marker in ART_CONTEXT_MARKERS)
    has_exclusion = any(marker in text_blob for marker in EXCLUDE_MARKERS)

    if has_exclusion and not has_strong_include:
        return False
    if has_strong_include:
        return True
    return has_audience_include and has_art_context


def _infer_activity_type(text_blob: str) -> str:
    if any(marker in text_blob for marker in (" lecture ", " lectures ", " talk ", " talks ", " conversation ", " conversations ", " discussion ", " panel ")):
        return "lecture"
    if any(marker in text_blob for marker in (" workshop ", " workshops ", " class ", " classes ", " lab ", " labs ", " studio ", " artmaking ", " art making ")):
        return "workshop"
    return "activity"


def _registration_required(*, text_blob: str, cta_texts: list[str]) -> bool | None:
    cta_blob = _search_blob(" ".join(cta_texts))
    if any(marker in text_blob or marker in cta_blob for marker in (" register ", " registration ", " rsvp ", " sign up ", " reserve ")):
        return True
    if any(marker in cta_blob for marker in (" tickets ", " purchase tickets ", " buy tickets ")):
        return True
    return None


def _parse_datetimes(date_text: str | None) -> tuple[datetime | None, datetime | None]:
    normalized = _normalize_space(date_text)
    if not normalized:
        return None, None

    match = DATE_RE.search(normalized)
    if match is None:
        return None, None

    try:
        base_date = datetime.strptime(
            f"{match.group('month')} {int(match.group('day'))} {match.group('year')}",
            "%B %d %Y",
        )
    except ValueError:
        return None, None

    time_blob = _normalize_space(match.group("time_blob"))
    if not time_blob:
        start_at = datetime(base_date.year, base_date.month, base_date.day, 0, 0)
        return start_at, None

    range_match = TIME_RANGE_RE.search(time_blob)
    if range_match is not None:
        start_hour, start_minute = _to_24h(
            int(range_match.group("start_hour")),
            int(range_match.group("start_minute") or 0),
            range_match.group("start_meridiem"),
        )
        end_hour, end_minute = _to_24h(
            int(range_match.group("end_hour")),
            int(range_match.group("end_minute") or 0),
            range_match.group("end_meridiem"),
        )
        return (
            datetime(base_date.year, base_date.month, base_date.day, start_hour, start_minute),
            datetime(base_date.year, base_date.month, base_date.day, end_hour, end_minute),
        )

    single_match = TIME_SINGLE_RE.search(time_blob)
    if single_match is not None:
        hour, minute = _to_24h(
            int(single_match.group("hour")),
            int(single_match.group("minute") or 0),
            single_match.group("meridiem"),
        )
        return datetime(base_date.year, base_date.month, base_date.day, hour, minute), None

    start_at = datetime(base_date.year, base_date.month, base_date.day, 0, 0)
    return start_at, None


def _parse_age_range(*, title: str, description: str | None) -> tuple[int | None, int | None]:
    blob = " ".join(part for part in [title, description or ""] if part)
    range_match = AGE_RANGE_RE.search(blob)
    if range_match is not None:
        return int(range_match.group(1)), int(range_match.group(2))

    plus_match = AGE_PLUS_RE.search(blob)
    if plus_match is not None:
        return int(plus_match.group(1)), None

    return None, None


def _to_24h(hour: int, minute: int, meridiem: str | None) -> tuple[int, int]:
    normalized = (meridiem or "").strip().lower().replace(".", "")
    if normalized == "pm" and hour != 12:
        hour += 12
    if normalized == "am" and hour == 12:
        hour = 0
    return hour, minute


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def _text_or_none(node: object) -> str | None:
    if node is None:
        return None
    if hasattr(node, "get_text"):
        return node.get_text(" ", strip=True)
    return str(node)


def _search_blob(value: str | None) -> str:
    return f" {_normalize_space(value).lower()} "


def _join_non_empty(parts: list[str | None]) -> str | None:
    normalized = [_normalize_space(part) for part in parts if _normalize_space(part)]
    if not normalized:
        return None
    return " | ".join(normalized)
