import re
import asyncio
import json
import html as html_lib
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity

MET_TEENS_FREE_WORKSHOPS_URL = (
    "https://www.metmuseum.org/events?audience=teens&price=free&type=workshopsClasses"
)
NY_TIMEZONE = "America/New_York"
MET_VENUE_NAME = "The Metropolitan Museum of Art"
MET_CITY = "New York"
MET_STATE = "NY"
MET_DEFAULT_LOCATION = "New York, NY"
DATE_HEADING_RE = re.compile(
    r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+([A-Za-z]+\s+\d{1,2})$"
)
TIME_LOCATION_RE = re.compile(r"^(\d{1,2}:\d{2}\s*[AP]M)\s*(.*)$", re.IGNORECASE)
AGE_RE = re.compile(r"Ages?\s*(\d{1,2})\s*[\-\u2013]\s*(\d{1,2})", re.IGNORECASE)
EMBEDDED_SOURCE_RE = re.compile(r'\\"_source\\":(\{.*?\})\\,\\"highlight\\"', re.DOTALL)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.metmuseum.org/events",
}


async def fetch_met_events_page(
    url: str = MET_TEENS_FREE_WORKSHOPS_URL,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
    use_playwright_fallback: bool = True,
) -> str:
    print(
        f"[met-fetch] start url={url} max_attempts={max_attempts} "
        f"playwright_fallback={use_playwright_fallback}"
    )
    last_exception: Exception | None = None
    last_response: httpx.Response | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[met-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[met-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[met-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            last_response = response
            print(f"[met-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[met-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            # Retry on transient status codes, especially 429 rate limiting.
            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[met-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            print(
                f"[met-fetch] non-retriable failure status={response.status_code} "
                f"on attempt {attempt}"
            )
            break

    if use_playwright_fallback:
        print("[met-fetch] switching to Playwright fallback")
        try:
            html = await fetch_met_events_page_playwright(url)
            print(f"[met-fetch] Playwright success, bytes={len(html)}")
            return html
        except Exception as exc:
            print(f"[met-fetch] Playwright fallback failed: {exc}")
            last_exception = exc

    if last_response is not None:
        last_response.raise_for_status()
    if last_exception is not None:
        raise RuntimeError("Unable to fetch MET events page due to transport/fallback failure") from last_exception

    raise RuntimeError("Unable to fetch MET events page after retries")


async def fetch_met_events_page_playwright(url: str, *, timeout_ms: int = 45000) -> str:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install extras and browsers: "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            locale="en-US",
        )
        page = await context.new_page()
        print("[met-fetch] Playwright: opening page")
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(3000)
        html = await page.content()
        await context.close()
        await browser.close()
        return html


class MetEventsAdapter(BaseSourceAdapter):
    source_name = "met_teens_free_workshops"

    def __init__(self, url: str = MET_TEENS_FREE_WORKSHOPS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_met_events_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_met_events_html(payload, list_url=self.url)


def parse_met_events_html(
    html: str,
    *,
    list_url: str = MET_TEENS_FREE_WORKSHOPS_URL,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    # Primary parsing path: Met's page includes embedded event JSON in script payloads.
    embedded_rows = _parse_embedded_event_sources(html)
    if embedded_rows:
        return embedded_rows

    # Fallback path for legacy/static snapshots where script payload is absent.
    soup = BeautifulSoup(html, "html.parser")

    # Keep event-detail links in document order so repeated titles remain stable.
    title_to_links: dict[str, list[str]] = {}
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        text = anchor.get_text(" ", strip=True)
        if not text or "engage.metmuseum.org" not in href:
            continue
        title_to_links.setdefault(text, []).append(href)

    lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    cursor_date: datetime | None = None
    rows: list[ExtractedActivity] = []

    i = 0
    while i < len(lines):
        line = lines[i]

        date_match = DATE_HEADING_RE.match(line)
        if date_match:
            cursor_date = _parse_date_heading(date_match.group(2), now=now)
            i += 1
            continue

        # We treat any known event-link title as the start of one event block.
        if line not in title_to_links:
            i += 1
            continue

        title = line
        if is_irrelevant_item_text(title):
            i += 1
            continue
        source_url = title_to_links[title][0]

        description = None
        time_line = None
        price_line = None

        j = i + 1
        while j < len(lines):
            nxt = lines[j]
            if DATE_HEADING_RE.match(nxt) or nxt in title_to_links:
                break
            if description is None and not TIME_LOCATION_RE.match(nxt) and not _looks_like_price(nxt):
                description = nxt
            if time_line is None and TIME_LOCATION_RE.match(nxt):
                time_line = nxt
            if price_line is None and _looks_like_price(nxt):
                price_line = nxt
            j += 1

        # Free-only rule: if the page includes non-free noise, keep free rows only.
        if price_line and "free" not in price_line.lower():
            i = j
            continue

        start_at = _parse_start_datetime(cursor_date, time_line, now=now)
        age_min, age_max = _parse_age_range(title, description)

        normalized_description = description
        if time_line and normalized_description:
            normalized_description = f"{normalized_description} | {time_line}"
        elif time_line:
            normalized_description = time_line
        if price_line and normalized_description:
            normalized_description = f"{normalized_description} | {price_line}"
        elif price_line:
            normalized_description = price_line

        lower_blob = f"{title} {description or ''}".lower()
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=normalized_description,
                venue_name=MET_VENUE_NAME,
                location_text=MET_DEFAULT_LOCATION,
                city=MET_CITY,
                state=MET_STATE,
                activity_type="workshop",
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in lower_blob or "drop in" in lower_blob),
                registration_required=("registration required" in lower_blob),
                start_at=start_at,
                end_at=None,
                timezone=NY_TIMEZONE,
                free_verification_status="confirmed" if price_line else "inferred",
            )
        )

        i = j

    return rows


def _parse_embedded_event_sources(html: str) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen_keys: set[tuple[str, str, datetime]] = set()

    for match in EMBEDDED_SOURCE_RE.finditer(html):
        source_obj_escaped = match.group(1)
        source_obj_json = source_obj_escaped.replace('\\"', '"').replace("\\/", "/")
        try:
            source_obj = json.loads(source_obj_json)
        except json.JSONDecodeError:
            continue

        # Keep free-only records and ensure audience includes teens.
        paid = str(source_obj.get("paid", "")).lower()
        is_paid = bool(source_obj.get("isPaid"))
        audiences = [str(v).lower() for v in source_obj.get("audiences", [])]
        if is_paid or (paid and paid != "free"):
            continue
        if not any("teen" in audience for audience in audiences):
            continue

        url = source_obj.get("url")
        title = source_obj.get("title")
        start_date = source_obj.get("startDate")
        if not url or not title or not start_date:
            continue
        if is_irrelevant_item_text(title):
            continue

        try:
            start_at = datetime.fromisoformat(start_date)
        except ValueError:
            continue

        end_raw = source_obj.get("endDate")
        try:
            end_at = datetime.fromisoformat(end_raw) if end_raw else None
        except ValueError:
            end_at = None

        description_parts = []
        teaser_text = _strip_html_fragment(source_obj.get("teaserText", ""))
        if teaser_text:
            description_parts.append(teaser_text)
        location = _strip_html_fragment(source_obj.get("location", ""))
        if location:
            description_parts.append(f"Location: {location}")
        location_text = MET_DEFAULT_LOCATION
        programs = source_obj.get("programs") or []
        if programs:
            description_parts.append(f"Programs: {', '.join(str(v) for v in programs)}")
        description = " | ".join(description_parts) if description_parts else None

        age_min, age_max = _parse_age_range(title, description)
        category_blob = " ".join(str(v) for v in source_obj.get("searchCategories", []))
        text_blob = f"{title} {description or ''} {category_blob}".lower()
        key = (url, title, start_at)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        rows.append(
            ExtractedActivity(
                source_url=url,
                title=title,
                description=description,
                venue_name=MET_VENUE_NAME,
                location_text=location_text,
                city=MET_CITY,
                state=MET_STATE,
                activity_type="workshop",
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=bool(source_obj.get("ticketRequired")),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                free_verification_status="confirmed",
            )
        )

    return rows


def _looks_like_price(text: str) -> bool:
    low = text.lower()
    return "free" in low or "$" in text or "member" in low or "ticket" in low


def _parse_date_heading(label: str, *, now: datetime | None = None) -> datetime:
    now = now or datetime.now()
    base = datetime.strptime(f"{label} {now.year}", "%B %d %Y")
    # Handle year rollover around Jan/Dec listing windows.
    if (base - now).days < -300:
        base = base.replace(year=base.year + 1)
    return base


def _parse_start_datetime(
    day: datetime | None,
    time_line: str | None,
    *,
    now: datetime | None = None,
) -> datetime:
    now = now or datetime.now()
    if day is None:
        day = now

    if not time_line:
        return day.replace(hour=0, minute=0, second=0, microsecond=0)

    match = TIME_LOCATION_RE.match(time_line)
    if not match:
        return day.replace(hour=0, minute=0, second=0, microsecond=0)

    parsed_time = datetime.strptime(match.group(1).upper(), "%I:%M %p")
    return day.replace(hour=parsed_time.hour, minute=parsed_time.minute, second=0, microsecond=0)


def _parse_age_range(title: str, description: str | None) -> tuple[int | None, int | None]:
    blob = title if not description else f"{title} {description}"
    match = AGE_RE.search(blob)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _strip_html_fragment(value: str) -> str:
    if not value:
        return ""
    unescaped = html_lib.unescape(value)
    return BeautifulSoup(unescaped, "html.parser").get_text(" ", strip=True)
