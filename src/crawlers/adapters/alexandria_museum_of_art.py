import asyncio
import json
import re
from datetime import datetime
from urllib.parse import parse_qs
from urllib.parse import unquote
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - environment-specific dependency
    async_playwright = None


ALEXANDRIA_EVENTS_URL = "https://themuseum.org/all-events/"
CT_TIMEZONE = "America/Chicago"
ALEXANDRIA_VENUE_NAME = "Alexandria Museum of Art"
ALEXANDRIA_CITY = "Alexandria"
ALEXANDRIA_STATE = "LA"
ALEXANDRIA_LOCATION = "Alexandria Museum of Art, Alexandria, LA"

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " conversation ",
    " creative roundtable ",
    " roundtable ",
    " sketch ",
    " sketching ",
    " stitch ",
    " stitching ",
    " workshop ",
    " workshops ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " competition ",
    " exhibition ",
    " opening reception ",
    " showcase ",
    " yoga ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
)
REGISTER_PATTERNS = (
    " preregister ",
    " pre-register ",
    " register ",
    " registration ",
)
NEGATIVE_REGISTER_PATTERNS = (
    " no need to register ",
    " no pre registration required ",
    " no pre-registration required ",
    " no preregistration required ",
    " no registration required ",
    " registration is not required ",
)


async def load_alexandria_museum_of_art_payload(
    *,
    url: str = ALEXANDRIA_EVENTS_URL,
) -> dict[str, object]:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is required for Alexandria Museum of Art parsing. "
            "Install crawler extras and chromium first."
        )

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle", timeout=120000)

        cards = await page.eval_on_selector_all(
            ".ajde_evcal_calendar .eventon_list_event",
            """events => events.map((ev) => {
                const script = ev.querySelector("script[type='application/ld+json']");
                const rows = Array.from(ev.querySelectorAll(".evcal_evdata_row")).map((row) =>
                    (row.innerText || "").replace(/\\s+/g, " ").trim()
                );
                const hrefs = Array.from(ev.querySelectorAll("a[href]")).map((a) => a.getAttribute("href"));
                return {
                    event_id: ev.getAttribute("data-event_id"),
                    classes: ev.className,
                    card_text: (ev.innerText || "").replace(/\\s+/g, " ").trim(),
                    script_json: script ? script.textContent : null,
                    detail_rows: rows,
                    hrefs,
                };
            })""",
        )

        await browser.close()
        return {"cards": cards}


def parse_alexandria_museum_of_art_payload(payload: dict) -> list[ExtractedActivity]:
    today = datetime.now(ZoneInfo(CT_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in payload.get("cards") or []:
        row = _build_row(card)
        if row is None:
            continue
        if row.start_at.date() < today:
            continue
        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class AlexandriaMuseumOfArtAdapter(BaseSourceAdapter):
    source_name = "alexandria_museum_of_art_events"

    async def fetch(self) -> list[str]:
        payload = await load_alexandria_museum_of_art_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_alexandria_museum_of_art_payload(json.loads(payload))


def _build_row(card: dict) -> ExtractedActivity | None:
    event_obj = _parse_event_json(card.get("script_json"))
    if event_obj is None:
        return None

    title = _normalize_space(event_obj.get("name"))
    if not title:
        return None

    source_url = _normalize_space(event_obj.get("url")) or ALEXANDRIA_EVENTS_URL
    google_href = next((href for href in card.get("hrefs") or [] if "google.com/calendar/event" in (href or "")), None)
    start_at, end_at = _parse_google_calendar_dates(google_href)
    if start_at is None:
        start_at = _parse_event_datetime(event_obj.get("startDate"))
        end_at = _parse_event_datetime(event_obj.get("endDate"))
    if start_at is None:
        return None

    detail_rows = [row for row in (card.get("detail_rows") or []) if _normalize_space(row)]
    detail_text = _extract_detail_row(detail_rows, prefix="Event Details")
    time_text = _extract_detail_row(detail_rows, prefix="Time")
    location_text = _clean_location_text(_extract_detail_row(detail_rows, prefix="Location")) or ALEXANDRIA_LOCATION
    description = _join_non_empty(
        [
            detail_text,
            time_text and f"Time: {time_text}",
            location_text and f"Location: {location_text}",
        ]
    )

    token_blob = _searchable_blob(" ".join(part for part in [title, description or ""] if part))
    title_blob = _searchable_blob(title)
    if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
        return None

    age_min, age_max = parse_age_range(" ".join(part for part in [title, detail_text] if part))
    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=ALEXANDRIA_VENUE_NAME,
        location_text=location_text,
        city=ALEXANDRIA_CITY,
        state=ALEXANDRIA_STATE,
        activity_type=_infer_activity_type(token_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=_requires_registration(token_blob),
        start_at=start_at,
        end_at=end_at,
        timezone=CT_TIMEZONE,
        **price_classification_kwargs(detail_text),
    )


def _parse_event_json(raw: object) -> dict | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _extract_detail_row(rows: list[str], *, prefix: str) -> str | None:
    normalized_prefix = prefix.lower()
    for row in rows:
        if not row.lower().startswith(normalized_prefix):
            continue
        cleaned = row[len(prefix) :].strip()
        cleaned = re.sub(rf"^{re.escape(prefix)}\s*", "", cleaned, flags=re.IGNORECASE)
        if cleaned.lower().startswith(prefix.lower()):
            cleaned = cleaned[len(prefix) :].strip()
        return cleaned or None
    return None


def _clean_location_text(value: str | None) -> str | None:
    text = _normalize_space(value)
    if not text:
        return None
    text = re.sub(r"(Alexandria Museum of Art)\s*(\d)", r"\1, \2", text)
    return text


def _parse_google_calendar_dates(url: str | None) -> tuple[datetime | None, datetime | None]:
    if not url:
        return None, None
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    value = (query.get("dates") or [None])[0]
    if not value or "/" not in value:
        return None, None
    start_raw, end_raw = value.split("/", 1)
    return _parse_calendar_utc_datetime(start_raw), _parse_calendar_utc_datetime(end_raw)


def _parse_calendar_utc_datetime(value: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt.endswith("Z"):
                return parsed.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(CT_TIMEZONE)).replace(tzinfo=None)
            return parsed
        except ValueError:
            continue
    return None


def _parse_event_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None

    offset_match = re.search(r"([+-])(\d):(\d{2})$", text)
    if offset_match:
        text = re.sub(r"([+-])(\d):(\d{2})$", r"\g<1>0\2:\3", text)

    for fmt in (
        "%Y-%m-%dT%H:%M%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
    ):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is not None:
                return parsed.astimezone(ZoneInfo(CT_TIMEZONE)).replace(tzinfo=None)
            return parsed
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(ZoneInfo(CT_TIMEZONE)).replace(tzinfo=None)
    return parsed


def _should_keep_event(*, title_blob: str, token_blob: str) -> bool:
    if not any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS):
        return False
    if any(pattern in title_blob for pattern in CONTEXTUAL_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS):
        return False
    return True


def _infer_activity_type(token_blob: str) -> str:
    if any(pattern in token_blob for pattern in (" conversation ", " roundtable ", " talk ", " lecture ")):
        return "lecture"
    return "workshop"


def _requires_registration(token_blob: str) -> bool:
    if any(pattern in token_blob for pattern in NEGATIVE_REGISTER_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in DROP_IN_PATTERNS):
        return False
    return any(pattern in token_blob for pattern in REGISTER_PATTERNS)


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _join_non_empty(parts: list[str | None]) -> str | None:
    cleaned = [_normalize_space(part) for part in parts if _normalize_space(part)]
    if not cleaned:
        return None
    return " | ".join(cleaned)


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(unquote(value).split())
