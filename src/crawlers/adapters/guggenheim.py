"""Solomon R. Guggenheim Museum calendar adapter.

Two things about guggenheim.org shape this adapter:

1. The site is entirely client-rendered. Fetching any URL over plain HTTP
   returns the same generic homepage shell with no event data, so the calendar
   has to be rendered in a browser before it can be parsed.
2. ``/calendar`` answers with HTTP 404 while serving the real, working Calendar
   page. The status code is simply wrong, so it is deliberately not checked.

Together those two made the venue look unparseable, and it was marked
``fail_implement`` in data/resoures/artmuseums/NY.json on that basis.

The rendered calendar also has one trap worth naming: every card carries a
``<time datetime>`` attribute whose clock is written in 12-hour form with no
meridiem applied, so a 2 pm event reads as ``T02:00:00``. Only the visible
label ("2 pm EDT") states the time unambiguously, so that is what is parsed,
with the date taken from the occurrence URL.
"""

from __future__ import annotations

import re
from datetime import date
from datetime import datetime
from datetime import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None

from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

GUGGENHEIM_CALENDAR_URL = "https://www.guggenheim.org/calendar"
GUGGENHEIM_BASE_URL = "https://www.guggenheim.org"
GUGGENHEIM_VENUE_NAME = "Solomon R. Guggenheim Museum"
GUGGENHEIM_CITY = "New York"
GUGGENHEIM_STATE = "NY"
GUGGENHEIM_DEFAULT_LOCATION = "Solomon R. Guggenheim Museum, New York, NY"
NY_TIMEZONE = "America/New_York"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# /event/<slug>/<YYYY-MM-DD> — the trailing date is the occurrence's own date.
OCCURRENCE_URL_RE = re.compile(r"/event/(?P<slug>[^/]+)/(?P<date>\d{4}-\d{2}-\d{2})/?$")
TIME_LABEL_RE = re.compile(
    r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>am|pm)\b",
    re.IGNORECASE,
)
DETAIL_TIME_RANGE_RE = re.compile(
    r"\b(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>am|pm)?\s*"
    r"[–—-]\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>am|pm)\b",
    re.IGNORECASE,
)
AGE_RANGE_PATTERNS = (
    re.compile(r"\b(\d{1,2})\s*[–—-]\s*(\d{1,2})\s*-?\s*year\s*-?\s*olds?\b", re.IGNORECASE),
    re.compile(r"\bages?\s*(\d{1,2})\s*(?:[–—-]|to)\s*(\d{1,2})\b", re.IGNORECASE),
)
AGE_AND_UNDER_RE = re.compile(r"\bages?\s*(\d{1,2})\s*and\s*under\b", re.IGNORECASE)

KIDS_TAG = "for kids, teens, and family"
FREE_WITH_ADMISSION_TAG = "free with admission"
# Wording that means "no extra charge on top of a ticket you still have to buy".
ADMISSION_ONLY_MARKERS = (
    " free with admission ",
    " free with museum admission ",
    " included with admission ",
    " included with museum admission ",
    " free for members ",
)
# Tags that mean the programme is not a publicly attendable in-person activity.
REJECT_TAGS = (
    "for members",
    "online event",
)
# "Tours" alone is an adult docent offering; kept only when it is also tagged
# for kids/teens/family (Teen Tuesdays carries both).
TOUR_TAG = "tours"

ACTIVITY_MARKERS = (
    " art ",
    " artmaking ",
    " art making ",
    " camp ",
    " class ",
    " classes ",
    " craft ",
    " create ",
    " creative ",
    " draw ",
    " drawing ",
    " family ",
    " kids ",
    " make ",
    " making ",
    " sketch ",
    " studio ",
    " teen ",
    " teens ",
    " workshop ",
    " workshops ",
    " youth ",
)


async def fetch_guggenheim_calendar_html(*, timeout_ms: int = 90000) -> str:
    """Render the calendar in Chromium and return its HTML.

    The response status is ignored on purpose: the page reports 404 while
    serving correct content.
    """
    if async_playwright is None:  # pragma: no cover
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent=USER_AGENT,
            locale="en-US",
            timezone_id=NY_TIMEZONE,
        )
        try:
            await page.goto(GUGGENHEIM_CALENDAR_URL, wait_until="networkidle", timeout=timeout_ms)
            await page.wait_for_timeout(2000)
            return await page.content()
        finally:
            await browser.close()


async def load_guggenheim_payload(*, timeout_ms: int = 90000) -> dict:
    """Render the calendar, then one detail page per distinct programme.

    Calendar cards give a start time but no end time and no age range; the
    detail page states both ("9 am-3 pm EDT", "For 6-8-year-olds"). Recurring
    programmes repeat one slug across many days, so fetching per slug rather
    than per occurrence keeps this to a handful of extra renders.
    """
    if async_playwright is None:  # pragma: no cover
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent=USER_AGENT,
            locale="en-US",
            timezone_id=NY_TIMEZONE,
        )
        try:
            await page.goto(GUGGENHEIM_CALENDAR_URL, wait_until="networkidle", timeout=timeout_ms)
            await page.wait_for_timeout(2000)
            calendar_html = await page.content()

            detail_text_by_slug: dict[str, str] = {}
            for slug, detail_url in _detail_urls_by_slug(calendar_html).items():
                try:
                    await page.goto(detail_url, wait_until="networkidle", timeout=timeout_ms)
                    detail_text_by_slug[slug] = _normalize_space(await page.inner_text("body"))
                except Exception as exc:  # noqa: BLE001 - one bad page must not sink the run
                    print(f"[guggenheim-fetch] detail failed slug={slug}: {exc}")
        finally:
            await browser.close()

    return {"calendar_html": calendar_html, "detail_text_by_slug": detail_text_by_slug}


def parse_guggenheim_payload(payload: dict | str) -> list[ExtractedActivity]:
    if isinstance(payload, str):
        return parse_guggenheim_calendar_html(payload, list_url=GUGGENHEIM_CALENDAR_URL)
    return parse_guggenheim_calendar_html(
        payload.get("calendar_html") or "",
        list_url=GUGGENHEIM_CALENDAR_URL,
        detail_text_by_slug=payload.get("detail_text_by_slug") or {},
    )


def _detail_urls_by_slug(calendar_html: str) -> dict[str, str]:
    soup = BeautifulSoup(calendar_html, "html.parser")
    urls: dict[str, str] = {}
    for anchor in soup.select("a[href^='/event/']"):
        match = OCCURRENCE_URL_RE.search(_normalize_space(anchor.get("href")))
        if match is None:
            continue
        urls.setdefault(match.group("slug"), urljoin(GUGGENHEIM_BASE_URL, anchor.get("href")))
    return urls


def parse_guggenheim_calendar_html(
    html: str,
    *,
    list_url: str,
    detail_text_by_slug: dict[str, str] | None = None,
) -> list[ExtractedActivity]:
    detail_text_by_slug = detail_text_by_slug or {}
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for article in soup.select("article[class*='_event_']"):
        link = article.select_one("a[href^='/event/'][class*='_title_']") or article.select_one(
            "a[href^='/event/']"
        )
        if link is None:
            continue

        href = _normalize_space(link.get("href"))
        match = OCCURRENCE_URL_RE.search(href or "")
        if match is None:
            # Series landing pages ("/event/event_series/youth-programs") and the
            # "no events scheduled" placeholder carry no occurrence date.
            continue

        occurrence_date = _parse_iso_date(match.group("date"))
        if occurrence_date is None:
            continue

        title = _normalize_space(link.get_text(" ", strip=True))
        if not title:
            continue

        section = article.select_one("section[class*='_text_']") or article
        tags = [
            _normalize_space(anchor.get_text(" ", strip=True)).lower()
            for anchor in section.select("a")
            if anchor is not link
        ]
        tags = [tag for tag in tags if tag]

        description = None
        paragraph = section.select_one("p")
        if paragraph is not None:
            description = _normalize_space(paragraph.get_text(" ", strip=True)) or None

        start_time = _parse_time_label(article.select_one("time.subtitle-2"))
        if start_time is None:
            continue
        start_at = datetime.combine(occurrence_date, start_time)

        detail_text = detail_text_by_slug.get(match.group("slug")) or ""
        # The detail page shows one occurrence's schedule, so its end time only
        # transfers to occurrences that start at the same clock time.
        end_at = None
        detail_range = _parse_detail_time_range(detail_text)
        if detail_range is not None and detail_range[0] == start_time:
            end_at = datetime.combine(occurrence_date, detail_range[1])
            if end_at <= start_at:
                end_at = None
        age_min, age_max = _parse_age_range(detail_text)

        blob = " ".join([title, description or "", " ".join(tags), detail_text])
        if not _should_keep_event(title=title, blob=blob, tags=tags):
            continue

        key = (urljoin(GUGGENHEIM_BASE_URL, href), title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=key[0],
                title=title,
                description=description,
                venue_name=GUGGENHEIM_VENUE_NAME,
                location_text=GUGGENHEIM_DEFAULT_LOCATION,
                city=GUGGENHEIM_CITY,
                state=GUGGENHEIM_STATE,
                activity_type=_infer_activity_type(blob=blob, tags=tags),
                age_min=age_min,
                age_max=age_max,
                audience_segment=_infer_guggenheim_audience(
                    title=title,
                    description=description,
                    tags=tags,
                    age_min=age_min,
                    age_max=age_max,
                ),
                drop_in=" drop in " in _searchable(blob),
                registration_required=_registration_required(blob),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                **_price_kwargs(text=" ".join([title, description or ""]), tags=tags),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _should_keep_event(*, title: str, blob: str, tags: list[str]) -> bool:
    if any(tag in REJECT_TAGS for tag in tags):
        return False
    if TOUR_TAG in tags and KIDS_TAG not in tags:
        return False
    if KIDS_TAG in tags:
        return True
    searchable = _searchable(blob)
    return any(marker in searchable for marker in ACTIVITY_MARKERS)


def _infer_guggenheim_audience(
    *,
    title: str,
    description: str | None,
    tags: list[str],
    age_min: int | None = None,
    age_max: int | None = None,
):
    """Prefer what the event says over the broad kids/teens/family tag.

    That tag covers everything from Stroller Hour to Teen Tuesdays, so passing
    it as the default would flatten every teen programme into "kids" — the
    shared helper treats an explicit default as final.
    """
    segment = infer_audience_segment(
        title=title, description=description, tags=tags, age_min=age_min, age_max=age_max
    )
    if segment != "unknown":
        return segment
    if KIDS_TAG in tags:
        return infer_audience_segment(
            title=title,
            description=description,
            tags=tags,
            age_min=age_min,
            age_max=age_max,
            default="kids",
        )
    return segment


def _price_kwargs(*, text: str, tags: list[str]) -> dict[str, bool | None | str]:
    """The Guggenheim charges admission, so "free with admission" is not free.

    Mirrors the paid-admission handling used for the Illinois venues: a
    programme only counts as free when its own wording says so, not when it is
    merely included in a ticket the visitor still has to buy. Only the title and
    description are classified — feeding the "Free with Admission" tag itself to
    the classifier would make it read as free and defeat the check below.
    """
    searchable = _searchable(text)
    # Checked before the classifier: it reads the "free" in "free with museum
    # admission" as free to attend, which at a ticketed museum it is not.
    if any(marker in searchable for marker in ADMISSION_ONLY_MARKERS):
        return {"is_free": False, "free_verification_status": "confirmed"}

    kwargs = price_classification_kwargs(text, default_is_free=None)
    if kwargs["is_free"] is True:
        return kwargs
    if FREE_WITH_ADMISSION_TAG in tags:
        return {"is_free": False, "free_verification_status": "confirmed"}
    if kwargs["is_free"] is None:
        return {"is_free": False, "free_verification_status": "inferred"}
    return kwargs


def _infer_activity_type(*, blob: str, tags: list[str]) -> str:
    searchable = _searchable(blob)
    if "courses and workshops" in tags:
        return "workshop"
    if "conversations and talks" in tags or " lecture " in searchable or " talk " in searchable:
        return "talk"
    if TOUR_TAG in tags:
        return "tour"
    return "workshop"


def _registration_required(blob: str) -> bool | None:
    searchable = _searchable(blob)
    if " drop in " in searchable or " drop-in " in searchable:
        return False
    if any(marker in searchable for marker in (" register ", " registration ", " rsvp ", " tickets ")):
        return True
    return None


def _parse_time_label(node: object) -> time | None:
    """Read "9 am EDT" / "6:30 pm EDT" into a clock time.

    The sibling ``datetime`` attribute is not used: it renders 2 pm as
    ``T02:00:00``, which would silently shift afternoon events into the morning.
    """
    if node is None or not hasattr(node, "get_text"):
        return None
    match = TIME_LABEL_RE.search(_normalize_space(node.get_text(" ", strip=True)))
    if match is None:
        return None

    hour = int(match.group("hour"))
    minute = int(match.group("minute") or 0)
    if not 1 <= hour <= 12 or not 0 <= minute <= 59:
        return None
    if match.group("meridiem").lower() == "pm":
        hour = hour if hour == 12 else hour + 12
    elif hour == 12:
        hour = 0
    return time(hour=hour, minute=minute)


def _parse_detail_time_range(detail_text: str) -> tuple[time, time] | None:
    """Read the detail page's "9 am-3 pm EDT" / "3-5 pm EDT" schedule line."""
    match = DETAIL_TIME_RANGE_RE.search(detail_text or "")
    if match is None:
        return None

    end_meridiem = match.group("end_meridiem").lower()
    # "3-5 pm" leaves the opening time's meridiem implicit; it carries over
    # unless doing so would put the end before the start (e.g. "11 am-1 pm").
    start_meridiem = (match.group("start_meridiem") or "").lower() or end_meridiem
    start = _to_time(match.group("start_hour"), match.group("start_minute"), start_meridiem)
    end = _to_time(match.group("end_hour"), match.group("end_minute"), end_meridiem)
    if start is None or end is None or end <= start:
        return None
    return start, end


def _parse_age_range(detail_text: str) -> tuple[int | None, int | None]:
    for pattern in AGE_RANGE_PATTERNS:
        match = pattern.search(detail_text or "")
        if match is None:
            continue
        low, high = int(match.group(1)), int(match.group(2))
        if 0 <= low <= high <= 18:
            return low, high
    match = AGE_AND_UNDER_RE.search(detail_text or "")
    if match is not None:
        high = int(match.group(1))
        if 0 < high <= 18:
            return None, high
    return None, None


def _to_time(hour: str, minute: str | None, meridiem: str) -> time | None:
    value = int(hour)
    minutes = int(minute or 0)
    if not 1 <= value <= 12 or not 0 <= minutes <= 59:
        return None
    if meridiem == "pm":
        value = value if value == 12 else value + 12
    elif value == 12:
        value = 0
    return time(hour=value, minute=minutes)


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _searchable(text: str | None) -> str:
    return f" {re.sub(r'[^a-z0-9]+', ' ', (text or '').lower()).strip()} "


def _normalize_space(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(str(text).split())
