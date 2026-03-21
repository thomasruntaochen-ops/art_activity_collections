from datetime import datetime
from urllib.parse import urljoin

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

BMA_EVENTS_URL = "https://artbma.org/events"
BMA_TIMEZONE = "America/New_York"
BMA_VENUE_NAME = "Baltimore Museum of Art"
BMA_CITY = "Baltimore"
BMA_STATE = "MD"
BMA_DEFAULT_LOCATION = "Baltimore, MD"
BMA_ROOT_QUERY_PREFIX = "$ROOT_QUERY.events("

VENUE_ID_TO_LOCATION = {
    "bma_main": "Baltimore Museum of Art",
    "bma_lexington": "BMA Lexington Market",
}

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


async def load_bma_events_payload(*, timeout_ms: int = 90000) -> dict[str, list[dict]]:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install extras and browsers: "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(locale="en-US")
        try:
            await page.goto(BMA_EVENTS_URL, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(3000)
            payload = await page.evaluate(
                """() => {
                    const nuxt = window.__NUXT__ || null;
                    const client = (nuxt && nuxt.apollo && nuxt.apollo.defaultClient) || {};
                    const rootKey = Object.keys(client).find((key) => key.startsWith('$ROOT_QUERY.events('));
                    const resolveCategories = (id) => {
                        const connection = client[`$${id}.categories`] || { edges: [] };
                        return (connection.edges || []).map((edgeRef) => {
                            const edge = client[edgeRef.id] || {};
                            const node = edge.node ? client[edge.node.id] : null;
                            return node ? { name: node.name || null, slug: node.slug || null } : null;
                        }).filter(Boolean);
                    };
                    const resolveDates = (id) => {
                        const acf = client[`$${id}.acfEvent`] || {};
                        return (acf.eventDatesTimes || []).map((dateRef) => client[dateRef.id]).filter(Boolean);
                    };
                    const resolveVenue = (id) => {
                        const acf = client[`$${id}.acfEvent`] || {};
                        const location = acf.eventLocation ? client[acf.eventLocation.id] : null;
                        const venue = location && location.venue ? client[location.venue.id] : null;
                        return {
                            venueId: venue ? venue.venueId || null : null,
                            venueText: venue ? venue.venueText || null : null,
                        };
                    };
                    const resolveCost = (id) => {
                        const acf = client[`$${id}.acfEvent`] || {};
                        const additional = acf.eventAdditional ? client[acf.eventAdditional.id] : null;
                        return additional ? additional.cost || null : null;
                    };

                    const refs = rootKey ? ((client[rootKey] || {}).nodes || []) : [];
                    return {
                        events: refs.map((ref) => {
                            const event = client[ref.id] || {};
                            return {
                                id: ref.id,
                                title: event.title || null,
                                slug: event.slug || null,
                                uri: event.uri || null,
                                categories: resolveCategories(ref.id),
                                dates: resolveDates(ref.id),
                                venue: resolveVenue(ref.id),
                                cost: resolveCost(ref.id),
                            };
                        }),
                    };
                }"""
            )
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
        source_url = urljoin(BMA_EVENTS_URL, _normalize_space(event_obj.get("uri")) or "")
        if not title or not source_url:
            continue

        category_names = [_normalize_space(item.get("name")) for item in (event_obj.get("categories") or [])]
        category_names = [value for value in category_names if value]
        category_slugs = [_normalize_space(item.get("slug")) for item in (event_obj.get("categories") or [])]
        category_slugs = [value for value in category_slugs if value]
        venue = event_obj.get("venue") or {}
        cost_text = _normalize_space(event_obj.get("cost"))

        if not _should_include_event(
            title=title,
            category_names=category_names,
            category_slugs=category_slugs,
            cost_text=cost_text,
        ):
            continue

        location_text = _resolve_location_text(venue)
        description_parts = []
        if category_names:
            description_parts.append(f"Categories: {', '.join(category_names)}")
        if venue.get("venueId"):
            description_parts.append(f"Location: {location_text}")
        if cost_text:
            description_parts.append(f"Price: {cost_text}")
        description = " | ".join(description_parts) if description_parts else None

        text_blob = _searchable_blob(" ".join([title, description or "", " ".join(category_names), cost_text]))
        for date_info in event_obj.get("dates") or []:
            start_at = _parse_date_time(
                date_info.get("date"),
                date_info.get("timeBegin"),
            )
            if start_at is None or start_at.date() < current_day:
                continue
            end_at = _parse_date_time(
                date_info.get("date"),
                date_info.get("timeEnd"),
            )

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
                    drop_in=(" drop-in " in text_blob or " drop in " in text_blob),
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
                        " ".join([cost_text, title, " ".join(category_names)]),
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


def _resolve_location_text(venue: dict) -> str:
    venue_id = _normalize_space(venue.get("venueId"))
    venue_text = _normalize_space(venue.get("venueText"))
    if venue_text:
        return venue_text
    if venue_id in VENUE_ID_TO_LOCATION:
        return VENUE_ID_TO_LOCATION[venue_id]
    return BMA_DEFAULT_LOCATION


def _parse_date_time(date_value: str | None, time_value: str | None) -> datetime | None:
    if not date_value:
        return None
    normalized_date = _normalize_space(date_value)
    normalized_time = _normalize_space(time_value)
    if not normalized_time:
        normalized_time = "12:00 am"
    try:
        return datetime.strptime(f"{normalized_date} {normalized_time}", "%Y-%m-%d %I:%M %p")
    except ValueError:
        return None


def _normalize_space(value: str | None) -> str:
    return " ".join((value or "").split())


def _searchable_blob(value: str) -> str:
    return f" {_normalize_space(value).lower()} "
