"""Vancouver Art Gallery (Vancouver, BC, Canada) public programs.

The gallery runs WordPress with an ``events`` post type exposed on the public
WP REST API. Each event carries ACF fields with the local start/end time
(``acf.group.event_date_and_time`` / ``event_end``), a program type
(``acf.event_type``: Families, Lectures and Talks, Special Events, Tours,
Membership, Seniors, ...) and a subcategory (Artist Talk, Lecture, Performance,
Film Screening, ...). The REST ``content`` body is empty, so descriptions,
room names and pricing come from each event's detail page.
"""

import asyncio
import html as html_lib
import json
import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.candidates import record_candidate_count
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity
from src.crawlers.user_agent import BROWSER_USER_AGENT

VAG_EVENTS_URL = "https://www.vanartgallery.bc.ca/events/"
VAG_API_URL = "https://www.vanartgallery.bc.ca/wp-json/wp/v2/events"
VAG_TIMEZONE = "America/Vancouver"
VAG_VENUE_NAME = "Vancouver Art Gallery"
VAG_CITY = "Vancouver"
VAG_STATE = "BC"
VAG_LOCATION = "750 Hornby Street, Vancouver, BC V6Z 2H7"

VAG_API_FIELDS = ",".join(
    (
        "id",
        "date",
        "modified",
        "slug",
        "link",
        "title",
        "acf.group",
        "acf.event_type",
        "acf.event_subcateogry",  # sic: the site's ACF key is misspelled
        "acf.multi_line_event_title",
    )
)
VAG_API_PAGE_SIZE = 100
# Events are paged newest-published first; upcoming events sit on page 1-2.
VAG_API_MAX_PAGES = 4

# Program types worth reading. Special Events is mostly parties, free nights and
# screenings, so only its talk-like subcategories are kept (see below).
VAG_KEEP_EVENT_TYPES = {"families", "lectures and talks", "teens", "youth", "workshops"}
VAG_TALK_SUBCATEGORIES = {
    "artist talk",
    "lecture",
    "curator's talk",
    "curators in conversation",
    "in conversation",
    "panel conversation",
    "roundtable",
    "speaker series",
    "heller lecture",
}
VAG_EXCLUDED_SUBCATEGORIES = {
    "performance",
    "film screening",
    "poetry reading",
    "book launch",
    "catalogue launch",
    "reading group",
}
# Event-type words: match on the title and labels only, never the description
# (descriptions say "included with Gallery admission" or "join a Family Tour").
VAG_TITLE_EXCLUDE_PATTERNS = (
    r"\btours?\b",
    r"\bfilms?\b",
    r"\bscreenings?\b",
    r"\bperformances?\b",
    r"\bconcerts?\b",
    r"\bmusic\b",
    r"\bjazz\b",
    r"\bdj\b",
    r"\bdance\b",
    r"\breception\b",
    r"\bgala\b",
    r"\bfundrais",
    r"\bfree (?:first )?friday",
    r"\bfree night",
    r"\bopen house\b",
    r"\bbook (?:launch|signing)\b",
    r"\bpoetry\b",
    r"\breadings?\b",
    r"\byoga\b",
    r"\bmeditation\b",
    r"\bmindful",
    r"\bcamps?\b",
    r"\bmember(?:s|ship)?\b",
    r"\bclosed\b",
    r"\bcancell?ed\b",
)
# Activities that are never visual art, so they may match anywhere.
VAG_FULL_TEXT_EXCLUDE_PATTERNS = (
    r"\byoga\b",
    r"\btai chi\b",
    r"\bmeditation\b",
)

FREE_FOR_YOUTH_RE = re.compile(
    r"free for[^.]*\b(?:children|youth|kids)\b|"
    r"\b(?:children|youth|kids)\b[^.]*\b(?:18|17|12) and under\b[^.]*free|"
    r"always free[^.]*\b(?:children|youth)\b",
    re.IGNORECASE,
)
WITH_ADMISSION_RE = re.compile(r"\bincluded with (?:gallery )?admission\b|\bwith (?:gallery )?admission\b", re.IGNORECASE)
REGISTRATION_RE = re.compile(r"\bregistration (?:is )?required\b|\bregister now\b|\bpre-?register\b|\brsvp\b", re.IGNORECASE)
DROP_IN_RE = re.compile(r"\bdrop[- ]in\b", re.IGNORECASE)
WHITESPACE_RE = re.compile(r"\s+")
SPACE_BEFORE_PUNCT_RE = re.compile(r"\s+([,.;:!?)])")

DEFAULT_HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
    "Accept-Language": "en-CA,en;q=0.9",
    "Referer": VAG_EVENTS_URL,
}


async def _fetch_with_retries(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    max_attempts: int = 4,
    base_backoff_seconds: float = 2.0,
) -> httpx.Response:
    last_exception: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = await client.get(url, params=params)
        except httpx.HTTPError as exc:
            last_exception = exc
            print(f"[vag-fetch] attempt {attempt}/{max_attempts} url={url}: transport error={exc}")
            if attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue
            break

        if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            retry_after = response.headers.get("Retry-After")
            wait_seconds = (
                float(retry_after)
                if retry_after and retry_after.isdigit()
                else base_backoff_seconds * (2 ** (attempt - 1))
            )
            print(f"[vag-fetch] status={response.status_code} url={url}, retrying after {wait_seconds:.1f}s")
            await asyncio.sleep(wait_seconds)
            continue
        return response

    if last_exception is not None:
        raise RuntimeError(f"Unable to fetch Vancouver Art Gallery URL: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch Vancouver Art Gallery URL after retries: {url}")


async def load_vancouver_art_gallery_payload(
    *,
    api_url: str = VAG_API_URL,
    now: datetime | None = None,
) -> dict[str, object]:
    """Fetch upcoming events from the REST API, then detail pages for candidates."""
    today = _local_today(now)
    events: list[dict[str, Any]] = []
    detail_pages: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        total_pages: int | None = None
        for page in range(1, VAG_API_MAX_PAGES + 1):
            if total_pages is not None and page > total_pages:
                break
            response = await _fetch_with_retries(
                client,
                api_url,
                params={
                    "per_page": VAG_API_PAGE_SIZE,
                    "page": page,
                    "orderby": "date",
                    "order": "desc",
                    "_fields": VAG_API_FIELDS,
                },
            )
            if response.status_code == 400 and page > 1:
                break  # WordPress answers 400 for a page past the end.
            response.raise_for_status()
            header_total = response.headers.get("X-WP-TotalPages")
            if header_total and header_total.isdigit():
                total_pages = int(header_total)

            page_events = response.json()
            if not isinstance(page_events, list) or not page_events:
                break
            events.extend(item for item in page_events if isinstance(item, dict))
            print(f"[vag-fetch] api page={page} events={len(page_events)}")

            # Stop once a whole page is in the past: older posts are older events.
            if not any(_event_is_upcoming(item, today=today) for item in page_events if isinstance(item, dict)):
                break

        for item in events:
            if not _event_is_upcoming(item, today=today) or not _is_candidate_event(item):
                continue
            url = str(item.get("link") or "").strip()
            if not url or url in detail_pages:
                continue
            response = await _fetch_with_retries(client, url)
            if response.status_code >= 400:
                print(f"[vag-fetch] detail status={response.status_code} url={url}; using API fields only")
                continue
            detail_pages[url] = response.text
            await asyncio.sleep(0.5)

    return {"events": events, "detail_pages": detail_pages}


class VancouverArtGalleryEventsAdapter(BaseSourceAdapter):
    source_name = "vancouver_art_gallery_events"

    def __init__(self, api_url: str = VAG_API_URL):
        self.api_url = api_url

    async def fetch(self) -> list[str]:
        payload = await load_vancouver_art_gallery_payload(api_url=self.api_url)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_vancouver_art_gallery_payload(json.loads(payload))


def parse_vancouver_art_gallery_payload(
    payload: dict[str, object],
    *,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    events = payload.get("events")
    if not isinstance(events, list):
        return []
    detail_pages = payload.get("detail_pages")
    if not isinstance(detail_pages, dict):
        detail_pages = {}

    today = _local_today(now)
    upcoming = [item for item in events if isinstance(item, dict) and _event_is_upcoming(item, today=today)]
    # Weeks with only free nights, tours and screenings are normal here; report
    # how many upcoming events were seen so an empty result reads as quiet.
    record_candidate_count(len(upcoming))

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for item in upcoming:
        if not _is_candidate_event(item):
            continue

        source_url = str(item.get("link") or "").strip()
        title = _event_title(item)
        start_at, end_at = _event_datetimes(item)
        if not source_url or not title or start_at is None:
            continue

        detail = _parse_detail_page(detail_pages.get(source_url))
        description = detail["description"] or None
        full_text = f"{title} {description or ''}"
        if any(re.search(pattern, full_text, re.IGNORECASE) for pattern in VAG_FULL_TEXT_EXCLUDE_PATTERNS):
            continue

        event_type = _acf_text(item, "event_type")
        subcategory = _subcategory(item)
        is_free, free_status = _infer_price(event_type=event_type, description=description)
        # The runner copies location_text into the venue's address (longest
        # wins), so keep it the building address and put the room in the text.
        if detail["room"]:
            description = f"{description} Location: {detail['room']}." if description else f"Location: {detail['room']}."

        row = ExtractedActivity(
            source_url=source_url,
            title=title,
            description=description,
            venue_name=VAG_VENUE_NAME,
            location_text=VAG_LOCATION,
            city=VAG_CITY,
            state=VAG_STATE,
            activity_type=_infer_activity_type(title=title, event_type=event_type, subcategory=subcategory),
            age_min=None,
            age_max=None,
            drop_in=bool(DROP_IN_RE.search(full_text)),
            registration_required=bool(REGISTRATION_RE.search(full_text)),
            start_at=start_at,
            end_at=end_at,
            timezone=VAG_TIMEZONE,
            is_free=is_free,
            free_verification_status=free_status,
            audience_segment=_infer_audience(
                title=title,
                description=description,
                event_type=event_type,
            ),
        )
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


def _is_candidate_event(item: dict[str, Any]) -> bool:
    event_type = _acf_text(item, "event_type").lower()
    subcategory = _subcategory(item).lower()
    title = _event_title(item)

    if subcategory in VAG_EXCLUDED_SUBCATEGORIES:
        return False
    if event_type not in VAG_KEEP_EVENT_TYPES and subcategory not in VAG_TALK_SUBCATEGORIES:
        return False
    if event_type in {"membership", "seniors", "tours"}:
        return False
    label_blob = f"{title} {subcategory}"
    if any(re.search(pattern, label_blob, re.IGNORECASE) for pattern in VAG_TITLE_EXCLUDE_PATTERNS):
        return False
    return True


def _event_is_upcoming(item: dict[str, Any], *, today: datetime) -> bool:
    start_at, _ = _event_datetimes(item)
    return start_at is not None and start_at >= today


def _event_title(item: dict[str, Any]) -> str:
    # The listing shows the multi-line title ("The Making Place | Collage and
    # Sound Sculptures"); the post title is often just a dated slug label.
    multi_line = _acf_text(item, "multi_line_event_title")
    if multi_line:
        return _normalize_space(html_lib.unescape(re.sub(r"<[^>]+>", " ", multi_line)))
    title = item.get("title")
    rendered = title.get("rendered") if isinstance(title, dict) else title
    return _normalize_space(html_lib.unescape(re.sub(r"<[^>]+>", " ", str(rendered or ""))))


def _event_datetimes(item: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    acf = item.get("acf")
    group = acf.get("group") if isinstance(acf, dict) else None
    if not isinstance(group, dict):
        return None, None

    start_at = _parse_local_datetime(group.get("event_date_and_time"))
    if start_at is None:
        return None, None
    end_at = _parse_local_datetime(group.get("event_end"))

    if group.get("event_show_time") is False:
        # The gallery hides the time on the page; don't present one.
        start_at = start_at.replace(hour=0, minute=0, second=0)
        end_at = None
    if end_at is not None and (end_at <= start_at or end_at.date() != start_at.date()):
        # Multi-day spans describe a run of sessions, not one event's end time.
        end_at = None
    return start_at, end_at


def _parse_local_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _parse_detail_page(html: object) -> dict[str, str]:
    if not isinstance(html, str) or not html.strip():
        return {"description": "", "room": ""}

    soup = BeautifulSoup(html, "html.parser")
    room = ""
    dates_block = soup.select_one(".wp-block-acf-event-dates")
    if dates_block is not None:
        sibling = dates_block.find_next_sibling(class_="wp-block-mfb-meta-field-block")
        if sibling is not None:
            room = _normalize_space(sibling.get_text(" ", strip=True))

    paragraphs: list[str] = []
    contents = soup.select_one(".written-contents")
    if contents is not None:
        for node in contents.find_all(["h2", "h3", "h4", "h5", "p", "li"]):
            # Skip nested matches (a <p> inside an <li>) to avoid duplicates.
            if node.find_parent(["p", "li"]) is not None:
                continue
            text = _normalize_space(node.get_text(" ", strip=True))
            # Joining inline links with spaces leaves "Nature , and"; tidy it.
            text = SPACE_BEFORE_PUNCT_RE.sub(r"\1", text.replace("[email protected]", "").strip())
            if text and text.upper() not in {"REGISTER NOW", "BUY TICKETS"}:
                paragraphs.append(text)
    description = " ".join(paragraphs)
    if not description:
        meta = soup.find("meta", attrs={"name": "description"}) or soup.find(
            "meta", attrs={"property": "og:description"}
        )
        if meta is not None:
            description = _normalize_space(str(meta.get("content") or ""))

    return {"description": description, "room": room}


def _infer_price(*, event_type: str, description: str | None) -> tuple[bool | None, str]:
    text = description or ""
    if event_type.lower() in {"families", "teens", "youth"} and FREE_FOR_YOUTH_RE.search(text):
        # Gallery admission is free for visitors aged 18 and under, which is
        # who these family programs are for.
        return True, "confirmed"
    is_free, status = infer_price_classification(text)
    if is_free is True and WITH_ADMISSION_RE.search(text) and not re.search(r"\bfree (?:event|program|admission)\b|\bfree and open\b", text, re.IGNORECASE):
        # Adult programs "included with Gallery admission" at a paid-admission
        # gallery are not free for the people attending them.
        return False, "inferred"
    return is_free, status


def _infer_activity_type(*, title: str, event_type: str, subcategory: str) -> str:
    text = f"{title} {subcategory}".lower()
    if any(word in text for word in ("workshop", "studio", "making place", "lab", "art-making", "art making")):
        return "workshop"
    if "class" in text:
        return "class"
    if any(word in text for word in ("talk", "lecture", "conversation", "panel", "roundtable", "speaker")):
        return "talk"
    if event_type.lower() == "lectures and talks":
        return "talk"
    return "activity"


def _infer_audience(*, title: str, description: str | None, event_type: str) -> str:
    event_type_lower = event_type.lower()
    if event_type_lower in {"teens", "youth"}:
        return "teens"
    if event_type_lower == "families":
        if re.search(r"\b(?:open to|for) all ages\b|\ball-ages\b", description or "", re.IGNORECASE):
            return "all_ages"
        return "kids"
    # Base adult-program audience on the title; speaker bios trip keyword hits.
    return infer_audience_segment(title=title, category=event_type, default=None) or "unknown"


def _acf_text(item: dict[str, Any], key: str) -> str:
    acf = item.get("acf")
    if not isinstance(acf, dict):
        return ""
    value = acf.get(key)
    return _normalize_space(value) if isinstance(value, str) else ""


def _subcategory(item: dict[str, Any]) -> str:
    # ACF returns False when no subcategory is set. Normalize curly quotes.
    return _acf_text(item, "event_subcateogry").replace("’", "'")


def _local_today(now: datetime | None) -> datetime:
    if now is None:
        now = datetime.now(ZoneInfo(VAG_TIMEZONE)).replace(tzinfo=None)
    elif now.tzinfo is not None:
        now = now.astimezone(ZoneInfo(VAG_TIMEZONE)).replace(tzinfo=None)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return WHITESPACE_RE.sub(" ", value).strip()


__all__ = [
    "VAG_API_URL",
    "VAG_EVENTS_URL",
    "VAG_VENUE_NAME",
    "VAG_CITY",
    "VAG_STATE",
    "VancouverArtGalleryEventsAdapter",
    "load_vancouver_art_gallery_payload",
    "parse_vancouver_art_gallery_payload",
]
