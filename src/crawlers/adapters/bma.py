import re
from datetime import date as date_cls
from datetime import datetime, timedelta

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

BMA_EVENTS_URL = "https://artbma.org/events"
BMA_TIMEZONE = "America/New_York"
BMA_VENUE_NAME = "Baltimore Museum of Art"
BMA_CITY = "Baltimore"
BMA_STATE = "MD"
BMA_DEFAULT_LOCATION = "Baltimore, MD"

# artbma.org sits behind a Cloudflare "Just a moment..." JS challenge and was
# rebuilt from a Nuxt/Apollo SPA into a server-rendered WordPress (Flynt theme).
# We clear the challenge with a realistic browser fingerprint, then scrape the
# `article.event-card` markup the listing now renders.
BMA_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
BMA_LAUNCH_ARGS = (
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
)

MONTH_ABBR_TO_NUM = {
    abbr: index
    for index, abbr in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        start=1,
    )
}

# Walk the rendered listing in document order, tracking the most recent
# "Month YYYY" group header so each card inherits the correct year, then read the
# card's labelled child elements.
BMA_EXTRACT_JS = r"""
() => {
    const MONTHS = {January:1,February:2,March:3,April:4,May:5,June:6,
        July:7,August:8,September:9,October:10,November:11,December:12};
    const cards = [];
    let curYear = null;
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
    let node = walker.currentNode;
    while (node) {
        const cls = typeof node.className === 'string' ? node.className : '';
        const text = (node.innerText || '').trim();
        const header = text.match(/^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d\d)$/);
        if (header) {
            curYear = parseInt(header[2], 10);
        }
        if (node.tagName === 'ARTICLE' && /event-card/.test(cls)) {
            const pick = (sel) => {
                const el = node.querySelector(sel);
                return el ? (el.innerText || '').trim() : null;
            };
            const anchor = node.querySelector('a[href*="/event/"]');
            cards.push({
                url: anchor ? anchor.href.split('#')[0].split('?')[0] : null,
                date: pick('.event-card-date'),
                tags: pick('.event-card-tags'),
                title: pick('.event-card-title'),
                time: pick('.event-card-time'),
                cost: pick('.event-card-cost'),
                location: pick('.event-card-location'),
                year: curYear,
            });
        }
        node = walker.nextNode();
    }
    return { events: cards };
}
"""

INCLUDE_PATTERNS = (
    " discussion ",
    " discussions ",
    " family ",
    " families ",
    " for families ",
    " lecture ",
    " lectures ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
    " making with mica ",
    " baby art date ",
    " family art adventures ",
    " sunday shorts ",
    " teen lab ",
    " hearts for art ",
)
EXCLUDE_PATTERNS = (
    " admission day ",
    " after hours ",
    " concert ",
    " financial education ",
    " free admission ",
    " members only ",
    " performance ",
    " rental event ",
    " shopping days ",
    " sound bath ",
    " tax prep ",
    " therapist ",
    " therapy ",
    " tour ",
    " tours ",
)


async def _launch_browser(p):
    """Prefer real Chrome (best at clearing Cloudflare), fall back to bundled Chromium."""
    last_error: Exception | None = None
    for channel in ("chrome", None):
        launch_kwargs: dict = {"headless": True, "args": list(BMA_LAUNCH_ARGS)}
        if channel:
            launch_kwargs["channel"] = channel
        try:
            return await p.chromium.launch(**launch_kwargs)
        except Exception as exc:  # channel may be unavailable in some environments
            last_error = exc
            continue
    raise RuntimeError(f"Could not launch Chrome/Chromium for the BMA crawl: {last_error}")


async def _wait_for_events(page, *, timeout_ms: int) -> bool:
    """Poll until the Cloudflare interstitial clears and event cards are present."""
    for _ in range(max(1, timeout_ms // 1000)):
        await page.wait_for_timeout(1000)
        state = await page.evaluate(
            "() => ({ title: document.title || '', "
            "cards: document.querySelectorAll('article.event-card').length })"
        )
        if "just a moment" not in state["title"].lower() and state["cards"] > 0:
            return True
    return False


async def load_bma_events_payload(*, timeout_ms: int = 90000) -> dict[str, list[dict]]:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install extras and browsers: "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as p:
        browser = await _launch_browser(p)
        context = await browser.new_context(
            locale="en-US",
            timezone_id=BMA_TIMEZONE,
            user_agent=BMA_USER_AGENT,
            viewport={"width": 1366, "height": 900},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        # Hide the headless automation fingerprint Cloudflare looks for.
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        page = await context.new_page()
        try:
            await page.goto(BMA_EVENTS_URL, wait_until="domcontentloaded", timeout=timeout_ms)
            if not await _wait_for_events(page, timeout_ms=30000):
                raise RuntimeError(
                    "BMA events did not load (Cloudflare challenge was not cleared "
                    "or the page markup changed). No event cards were found."
                )
            # Best-effort: load any additional pages if a "load more" control appears.
            for _ in range(20):
                clicked = await page.evaluate(
                    """() => {
                        const btn = [...document.querySelectorAll('button, a')]
                            .find(el => /load more|show more|more events/i.test(el.innerText || '')
                                && el.offsetParent !== null);
                        if (btn) { btn.click(); return true; }
                        return false;
                    }"""
                )
                if not clicked:
                    break
                await page.wait_for_timeout(1500)
            payload = await page.evaluate(BMA_EXTRACT_JS)
        finally:
            await page.close()
            await browser.close()

    return payload


class BmaEventsAdapter(BaseSourceAdapter):
    source_name = "bma_events"

    async def fetch(self) -> list[str]:
        payload = await load_bma_events_payload()
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_bma_events_payload/parse_bma_events_payload from script runner.")


def parse_bma_events_payload(payload: dict) -> list[ExtractedActivity]:
    current_day = datetime.now().date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        title = _normalize_space(event_obj.get("title"))
        source_url = _normalize_space(event_obj.get("url"))
        if not title or not source_url:
            continue

        tags_text = _normalize_space(event_obj.get("tags"))
        category_names = [tags_text] if tags_text else []
        cost_text = _normalize_space(event_obj.get("cost"))
        location_text = _normalize_space(event_obj.get("location")) or BMA_DEFAULT_LOCATION

        if not _should_include_event(
            title=title,
            category_names=category_names,
            category_slugs=[],
            cost_text=cost_text,
        ):
            continue

        event_date = _parse_card_date(event_obj.get("date"), event_obj.get("year"))
        if event_date is None or event_date < current_day:
            continue

        start_at, end_at = _build_start_end(event_date, event_obj.get("time"))

        description_parts = []
        if category_names:
            description_parts.append(f"Categories: {', '.join(category_names)}")
        if location_text:
            description_parts.append(f"Location: {location_text}")
        if cost_text:
            description_parts.append(f"Price: {cost_text}")
        description = " | ".join(description_parts) if description_parts else None

        text_blob = _searchable_blob(" ".join([title, description or "", tags_text, cost_text]))

        key = (source_url, title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=BMA_VENUE_NAME,
                location_text=location_text,
                city=BMA_CITY,
                state=BMA_STATE,
                activity_type=_infer_activity_type(text_blob),
                age_min=None,
                age_max=None,
                audience_segment=_infer_bma_audience(
                    title=title,
                    description=description,
                    category_text=tags_text,
                ),
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=(
                    ("registration required" in text_blob)
                    or ("registration encouraged" in text_blob)
                    or ("tickets required" in text_blob)
                    or ("sold out" in text_blob)
                ),
                start_at=start_at,
                end_at=end_at,
                timezone=BMA_TIMEZONE,
                **price_classification_kwargs(
                    " ".join([cost_text, title, tags_text]),
                    default_is_free=True if "free" in text_blob else None,
                ),
            )
        )

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


def _should_include_event(
    *,
    title: str,
    category_names: list[str],
    category_slugs: list[str],
    cost_text: str,
) -> bool:
    blob = _searchable_blob(" ".join([title, " ".join(category_names), " ".join(category_slugs), cost_text]))
    if " sold out " in blob or " waitlist " in blob or " wait list " in blob:
        return False
    if any(pattern in blob for pattern in EXCLUDE_PATTERNS):
        return False
    if " members only " in blob or " contributor " in blob:
        return False
    if " for families " in blob:
        return True
    if " workshops " in blob or " discussions " in blob:
        if " financial education " in blob or " tax prep " in blob:
            return False
        return True
    return any(pattern in blob for pattern in INCLUDE_PATTERNS)


def _infer_activity_type(blob: str) -> str:
    if " discussion " in blob or " discussions " in blob or " lecture " in blob or " talk " in blob:
        return "lecture"
    if " family " in blob or " teen lab " in blob:
        return "activity"
    return "workshop"


def _infer_bma_audience(*, title: str, description: str | None, category_text: str | None) -> str:
    blob = _searchable_blob(" ".join(part for part in (title, description or "", category_text or "") if part))
    if " teen lab " in blob:
        return "teens"
    if any(
        marker in blob
        for marker in (
            " baby art date ",
            " family ",
            " families ",
            " family art adventures ",
            " free family sundays ",
            " for families ",
        )
    ):
        return "kids"
    if any(marker in blob for marker in (" curator talk ", " discussion ", " lecture ", " talk ", " workshop ")):
        return "adults"
    return infer_audience_segment(
        title=title,
        description=description,
        category=category_text,
    )


def _parse_card_date(date_text: str | None, year: int | None) -> date_cls | None:
    """Parse a card date like 'Jun 14'; resolve the year from the group header."""
    match = re.match(r"([A-Za-z]{3,})\.?\s+(\d{1,2})", _normalize_space(date_text))
    if not match:
        return None
    month = MONTH_ABBR_TO_NUM.get(match.group(1)[:3].title())
    if not month:
        return None
    day = int(match.group(2))

    resolved_year = year
    if not resolved_year:
        # No group header: assume the next occurrence of this month/day.
        today = datetime.now().date()
        resolved_year = today.year
        if month < today.month:
            resolved_year += 1
    try:
        return date_cls(resolved_year, month, day)
    except ValueError:
        return None


def _parse_clock(value: str) -> tuple[int, int] | None:
    """Parse a clock time like '2:00 pm' or '11 am' into (hour, minute)."""
    match = re.match(r"(\d{1,2})(?::(\d{2}))?\s*([ap]m)", _normalize_space(value).lower().replace(".", ""))
    if not match:
        return None
    hour = int(match.group(1)) % 12
    if match.group(3) == "pm":
        hour += 12
    minute = int(match.group(2) or 0)
    return hour, minute


def _build_start_end(event_date: date_cls, time_text: str | None) -> tuple[datetime, datetime | None]:
    normalized = _normalize_space(time_text).replace("–", "-").replace("—", "-")
    parts = [part.strip() for part in normalized.split("-")] if normalized else []
    start_clock = _parse_clock(parts[0]) if parts and parts[0] else None
    end_clock = _parse_clock(parts[1]) if len(parts) > 1 and parts[1] else None

    if start_clock:
        start_at = datetime(event_date.year, event_date.month, event_date.day, start_clock[0], start_clock[1])
    else:
        start_at = datetime(event_date.year, event_date.month, event_date.day, 0, 0)

    end_at: datetime | None = None
    if end_clock:
        end_at = datetime(event_date.year, event_date.month, event_date.day, end_clock[0], end_clock[1])
        if end_at <= start_at:
            # Time range crosses midnight (e.g. 5:00 pm - 12:00 am).
            end_at += timedelta(days=1)
    return start_at, end_at


def _normalize_space(value: str | None) -> str:
    return " ".join((value or "").split())


def _searchable_blob(value: str) -> str:
    return f" {_normalize_space(value).lower()} "
