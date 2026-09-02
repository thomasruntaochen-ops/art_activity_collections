"""The Branch Museum of Design (Richmond, VA).

Formerly a sub-venue of the VA Tribe bundle. The museum left WordPress in
mid-2026 -- /wp-json/ stopped serving JSON and both /makers-studio/ and /events/
now 404 -- so the Tribe REST fetch failed on every run and the sub-venue was
dropped from that bundle on 2026-09-01.

The rebuilt site is Drupal. Two public listings carry events:

  /calendar/        one month at a time, no start times, includes standing
                    exhibitions repeated across the month.
  /events_programs  every upcoming event with its start time, no exhibitions.

The second is what this adapter reads. Its rows live in a set of quicktabs panes
(All / Today / This Week / Past) that repeat the same events, so rows are
deduplicated by URL and start time and past dates are dropped, rather than
depending on a pane id that is one theme change away from moving.
"""

import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urljoin
from urllib.parse import urlsplit
from urllib.parse import urlunsplit
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.candidates import record_candidate_count
from src.crawlers.pipeline.candidates import record_listing_recognized
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

BRANCH_EVENTS_URL = "https://branchmuseum.org/events_programs"
BRANCH_CALENDAR_URL = "https://branchmuseum.org/calendar/"

NY_TIMEZONE = "America/New_York"
BRANCH_VENUE_NAME = "The Branch Museum of Design"
BRANCH_CITY = "Richmond"
BRANCH_STATE = "VA"
BRANCH_DEFAULT_LOCATION = "2501 Monument Avenue, Richmond, VA"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# "September 16, 2026 | 6:00 PM" -- the time half is not always present.
LISTING_DATE_RE = re.compile(
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})"
    r"(?:\s*\|\s*(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*(?P<meridiem>[AP])\.?M\.?)?",
    re.IGNORECASE,
)

INCLUDED_KEYWORDS = (
    "artist talk",
    "art class",
    "class",
    "conversation",
    "demonstration",
    "discussion",
    "drawing",
    "hands-on",
    "hands on",
    "lecture",
    "maker",
    "open studio",
    "painting",
    "panel",
    "printmaking",
    "sketch",
    "studio",
    "talk",
    "workshop",
    # Audience words: a family or youth programme qualifies on its own, even when
    # the blurb never names a format.
    "all ages",
    "children",
    "families",
    "family",
    "kids",
    "teen",
    "teens",
    "youth",
)
# Checked against title *and* description. The museum files its closures under a
# "Free Event" category with a ribbon to match, so "Closed for a Private Event"
# reads as a free public programme unless it is rejected outright -- that is the
# single most important rule here.
ALWAYS_REJECT_KEYWORDS = (
    "closed for",
    "is closed",
    "private event",
    "private rental",
    "sold out",
    "members-only",
    "members only",
    "member preview",
    "auction",
    "concert",
    "gala",
    "film",
    "movies",
    "screening",
    "camp",
    "reception",
    # The museum's music programming ("Live at The Branch", festival nights) is
    # not an art activity, and its blurbs reliably name performers rather than a
    # format -- "Southern Gothic" otherwise slips through on a passing mention of
    # an artist panel.
    "performers",
    "musicians",
    "festival",
)
AGE_RANGE_MARKERS = (
    "ages ",
    "age ",
)


async def fetch_branch_html(
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
        raise RuntimeError(f"Unable to fetch Branch Museum page: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch Branch Museum page after retries: {url}")


async def load_branch_museum_payload(*, detail_limit: int | None = None) -> dict:
    """The events listing plus a detail page per upcoming event."""
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        listing_html = await fetch_branch_html(BRANCH_EVENTS_URL, client=client)

        detail_urls: list[str] = []
        for entry in _extract_listing_entries(listing_html):
            if entry["source_url"] not in detail_urls:
                detail_urls.append(entry["source_url"])
        if detail_limit is not None:
            detail_urls = detail_urls[: max(detail_limit, 0)]

        detail_pages: dict[str, str] = {}
        for detail_url in detail_urls:
            try:
                detail_pages[detail_url] = await fetch_branch_html(detail_url, client=client)
            except Exception as exc:  # one dead detail page must not sink the venue
                print(f"[branch-fetch] detail failed url={detail_url}: {exc}")

    return {"listing_html": listing_html, "detail_pages": detail_pages}


def parse_branch_museum_payload(payload: dict) -> list[ExtractedActivity]:
    listing_html = payload.get("listing_html") or ""
    detail_pages = payload.get("detail_pages") or {}
    today = datetime.now(ZoneInfo(NY_TIMEZONE)).date()

    entries = _extract_listing_entries(listing_html)
    upcoming = [entry for entry in entries if entry["start_at"].date() >= today]
    # Report before the audience filters run: "0 kept out of N candidates" is a
    # museum with nothing for our audience this month, not a broken parser.
    record_candidate_count(len(upcoming))

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    for entry in upcoming:
        row = _build_row(entry, detail_html=detail_pages.get(entry["source_url"]) or "")
        if row is None:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class BranchMuseumAdapter(BaseSourceAdapter):
    source_name = "branch_events"

    async def fetch(self) -> list[str]:
        payload = await load_branch_museum_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_branch_museum_payload(json.loads(payload))


def _extract_listing_entries(listing_html: str) -> list[dict]:
    """Upcoming events from the listing, deduplicated across the quicktabs panes."""
    if not listing_html:
        return []

    soup = BeautifulSoup(listing_html, "html.parser")

    # The tab wrapper is the listing container; the rows are the items. Reporting
    # the wrapper lets the empty-parse guard tell "no events published" apart from
    # "the theme moved and our selectors are dead".
    if soup.select_one(".quicktabs-wrapper, #events_page_div") is not None:
        record_listing_recognized()

    entries: list[dict] = []
    seen: set[tuple[str, datetime]] = set()
    for row in soup.select(".events-list-row"):
        date_el = row.select_one(".date")
        title_el = row.select_one("h2")
        link = row.select_one("a.tckt-btn[href]") or row.select_one("a[href]")
        if date_el is None or title_el is None or link is None:
            continue

        start_at = _parse_listing_datetime(_normalize_space(date_el.get_text(" ", strip=True)))
        title = _strip_ellipsis(_normalize_space(title_el.get_text(" ", strip=True)))
        if start_at is None or not title:
            continue

        source_url = _canonical_url(str(link.get("href") or ""))
        if not source_url:
            continue

        key = (source_url, start_at)
        if key in seen:
            continue
        seen.add(key)

        location_el = row.select_one(".loc")
        entries.append(
            {
                "title": title,
                "source_url": source_url,
                "start_at": start_at,
                "location_text": _normalize_space(location_el.get_text(" ", strip=True)) if location_el else "",
            }
        )

    return entries


def _canonical_url(href: str) -> str:
    """Drop the ?v=NNNN cache-buster.

    It changes between crawls, and the upsert identity is (source_url, title,
    start_at) -- keeping it would insert a duplicate row for the same event every
    time the site bumped the number.
    """
    absolute = urljoin(BRANCH_EVENTS_URL, href.strip())
    if not absolute:
        return ""
    parts = urlsplit(absolute)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _parse_listing_datetime(date_text: str) -> datetime | None:
    match = LISTING_DATE_RE.search(date_text or "")
    if match is None:
        return None

    try:
        day = datetime.strptime(
            f"{match.group('month')} {match.group('day')} {match.group('year')}",
            "%B %d %Y",
        )
    except ValueError:
        return None

    if match.group("hour") is None:
        return day

    hour = int(match.group("hour")) % 12
    if match.group("meridiem").upper().startswith("P"):
        hour += 12
    return day.replace(hour=hour, minute=int(match.group("minute")))


def _build_row(entry: dict, *, detail_html: str) -> ExtractedActivity | None:
    # The listing truncates long titles with an ellipsis, so the detail page's h1
    # is the canonical form; the listing title is the fallback when that fetch failed.
    title = _extract_detail_title(detail_html) or str(entry["title"])
    source_url = str(entry["source_url"])
    description = _extract_detail_description(detail_html)

    if not _should_include_event(title=title, description=description):
        return None

    text_blob = " ".join(part for part in [title, description] if part).lower()
    age_min, age_max = _parse_age_range(description)
    is_free, free_status = infer_price_classification(text_blob)
    activity_type = _infer_activity_type(text_blob)

    location_text = BRANCH_DEFAULT_LOCATION
    if entry.get("location_text"):
        location_text = f"{entry['location_text']}, {BRANCH_DEFAULT_LOCATION}"

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description or None,
        venue_name=BRANCH_VENUE_NAME,
        location_text=location_text,
        city=BRANCH_CITY,
        state=BRANCH_STATE,
        activity_type=activity_type,
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=any(
            keyword in text_blob for keyword in ("register", "registration", "ticket", "rsvp", "book now")
        ),
        start_at=entry["start_at"],
        end_at=None,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
        audience_segment=infer_audience_segment(
            title=title,
            description=description,
            source_url=source_url,
            age_min=age_min,
            age_max=age_max,
        ),
    )


def _extract_detail_title(detail_html: str) -> str:
    if not detail_html:
        return ""
    soup = BeautifulSoup(detail_html, "html.parser")
    heading = soup.select_one("h1")
    return _strip_ellipsis(_normalize_space(heading.get_text(" ", strip=True))) if heading else ""


def _strip_ellipsis(value: str) -> str:
    return re.sub(r"\s*(?:\u2026|\.\.\.)\s*$", "", value or "").strip()


def _extract_detail_description(detail_html: str) -> str:
    if not detail_html:
        return ""
    soup = BeautifulSoup(detail_html, "html.parser")
    body = soup.select_one(".eventdetail-right")
    if body is None:
        return ""
    return _normalize_space(body.get_text(" ", strip=True))


def _should_include_event(*, title: str, description: str) -> bool:
    blob = _normalize_space(f"{title} {description}").lower()
    if not blob:
        return False

    if any(keyword in blob for keyword in ALWAYS_REJECT_KEYWORDS):
        return False

    return any(keyword in blob for keyword in INCLUDED_KEYWORDS)


def _infer_activity_type(text_blob: str) -> str:
    if any(keyword in text_blob for keyword in ("talk", "lecture", "conversation", "discussion", "panel")):
        return "talk"
    if any(keyword in text_blob for keyword in ("workshop", "class", "studio", "hands-on", "hands on")):
        return "workshop"
    return "activity"


def _parse_age_range(description: str) -> tuple[int | None, int | None]:
    lowered = (description or "").lower()
    for marker in AGE_RANGE_MARKERS:
        start = lowered.find(marker)
        if start == -1:
            continue
        fragment = lowered[start : start + 24]
        digits = [part for part in fragment.replace("+", " + ").replace("-", " - ").split() if part.isdigit()]
        if "+" in fragment and digits:
            return int(digits[0]), None
        if len(digits) >= 2:
            return int(digits[0]), int(digits[1])
    return None, None


def _normalize_space(value: str) -> str:
    return " ".join((value or "").split())
