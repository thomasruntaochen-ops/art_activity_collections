import asyncio
from datetime import datetime
from decimal import Decimal
from html import unescape
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

MASSMOCA_EVENTS_URL = "https://massmoca.org/calendar/"
MASSMOCA_EVENTS_API_URL = "https://massmoca.org/wp-json/tribe/events/v1/events"

NY_TIMEZONE = "America/New_York"
MASSMOCA_VENUE_NAME = "MASS MoCA"
MASSMOCA_CITY = "North Adams"
MASSMOCA_STATE = "MA"
MASSMOCA_DEFAULT_LOCATION = "MASS MoCA, 1040 MASS MoCA Way, North Adams, MA 01247"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": MASSMOCA_EVENTS_URL,
}

INCLUDE_PATTERNS = (
    " workshop ",
    " workshops ",
    " conversation ",
    " discussion ",
    " seminar ",
    " seminars ",
    " class ",
    " classes ",
    " art medium ",
    " process ",
)
HARD_EXCLUDE_PATTERNS = (
    " camp ",
    " camps ",
    " concert ",
    " festival ",
    " film ",
    " fundraiser ",
    " fundraising ",
    " gala ",
    " membership ",
    " member appreciation ",
    " members event ",
    " performance ",
    " pep rally ",
    " storytime ",
    " story time ",
    " tour ",
    " tours ",
)
TITLE_EXCLUDE_PATTERNS = (
    "admission",
    "book club",
    "book talk",
    "camp mass",
    "homecoming",
    "museum admission",
    "member appreciation",
    "reception",
    "solid sound",
    "lucy dacus",
    "yaya bey",
    "big thief",
    "family storytime",
    "teen invitational reception",
    "pep rally",
)
POSITIVE_CATEGORY_NAMES = {
    "public program",
    "kidspace",
    "exhibition program",
    "workshop",
    "research & development",
}


async def fetch_massmoca_events_page(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> dict:
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
                return response.json()

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch MASS MoCA events API") from last_exception
    raise RuntimeError("Unable to fetch MASS MoCA events API after retries")


def _default_start_date() -> str:
    return datetime.now(ZoneInfo(NY_TIMEZONE)).date().isoformat()


async def load_massmoca_payload(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    page_limit: int | None = None,
    per_page: int = 50,
) -> dict:
    resolved_start_date = start_date or _default_start_date()
    resolved_end_date = end_date or "2027-03-31"
    next_url = None
    next_params: dict[str, str | int] | None = {
        "per_page": per_page,
        "start_date": f"{resolved_start_date} 00:00:00",
        "end_date": f"{resolved_end_date} 23:59:59",
        "status": "publish",
    }
    pages_seen = 0
    events: list[dict] = []

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        while next_url or next_params:
            if page_limit is not None and pages_seen >= max(page_limit, 1):
                break
            if next_url:
                payload = await fetch_massmoca_events_page(next_url, client=client)
            else:
                response = await client.get(MASSMOCA_EVENTS_API_URL, params=next_params)
                response.raise_for_status()
                payload = response.json()
            pages_seen += 1
            events.extend(payload.get("events") or [])
            next_url = payload.get("next_rest_url")
            next_params = None

    return {"events": events}


def parse_massmoca_payload(payload: dict) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for event_obj in payload.get("events") or []:
        row = _build_row(event_obj)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue

        key = (row.source_url, row.title, row.start_at)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class MassMoCAEventsAdapter(BaseSourceAdapter):
    source_name = "massmoca_events"

    async def fetch(self) -> list[str]:
        payload = await load_massmoca_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_massmoca_payload/parse_massmoca_payload from script runner.")


def _build_row(event_obj: dict) -> ExtractedActivity | None:
    title = _html_to_text(event_obj.get("title"))
    source_url = _normalize_space(event_obj.get("url"))
    start_at = _parse_datetime(event_obj.get("start_date"))
    if not title or not source_url or start_at is None:
        return None

    end_at = _parse_datetime(event_obj.get("end_date"))
    description = _join_non_empty(
        [
            _html_to_text(event_obj.get("excerpt")),
            _html_to_text(event_obj.get("description")),
        ]
    )

    category_names = [_html_to_text(item.get("name")) for item in (event_obj.get("categories") or [])]
    category_names = [value for value in category_names if value]
    category_blob = " ".join(category_names).lower()
    token_blob = _token_blob(" ".join([title, description or "", " ".join(category_names)]))

    if not _should_include_event(title=title, description=description or "", category_names=category_names, token_blob=token_blob):
        return None

    if category_names:
        description = _join_non_empty([description, f"Categories: {', '.join(category_names)}"])

    venue_obj = event_obj.get("venue") or {}
    cost_text = _html_to_text(event_obj.get("cost"))
    amount = _extract_amount(event_obj.get("cost_details"))
    is_free, free_status = _price_classification(cost_text=cost_text, amount=amount)

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=MASSMOCA_VENUE_NAME,
        location_text=_build_location_text(venue_obj),
        city=MASSMOCA_CITY,
        state=MASSMOCA_STATE,
        activity_type=_infer_activity_type(token_blob=token_blob, category_blob=category_blob),
        age_min=None,
        age_max=None,
        drop_in=("drop in" in token_blob),
        registration_required=_has_registration_signal(token_blob=token_blob, cost_text=cost_text),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _should_include_event(
    *,
    title: str,
    description: str,
    category_names: list[str],
    token_blob: str,
) -> bool:
    normalized_title = title.lower()
    if any(pattern in normalized_title for pattern in TITLE_EXCLUDE_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in HARD_EXCLUDE_PATTERNS):
        return False

    if not any(name.lower() in POSITIVE_CATEGORY_NAMES for name in category_names):
        return False

    if any(pattern in token_blob for pattern in INCLUDE_PATTERNS):
        return True

    # Keep this seminar-style gallery series even though the title says "visits".
    has_multi_session = " multi session " in token_blob or " multi-session " in token_blob
    has_seminar_style = " seminar style " in token_blob or " seminar-style " in token_blob
    if has_multi_session and has_seminar_style and " discussion" in token_blob:
        return True

    return False


def _price_classification(*, cost_text: str, amount: Decimal | None) -> tuple[bool | None, str]:
    normalized = _token_blob(cost_text)
    if " with museum admission " in normalized:
        return None, "uncertain"
    return infer_price_classification_from_amount(amount, text=cost_text)


def _has_registration_signal(*, token_blob: str, cost_text: str) -> bool | None:
    if " rsvp " in token_blob or " reserve " in token_blob or " register " in token_blob:
        return True
    if cost_text:
        return True
    return None


def _infer_activity_type(*, token_blob: str, category_blob: str) -> str:
    if any(keyword in token_blob for keyword in (" conversation ", " discussion ", " author talk ", " talk ")):
        return "lecture"
    if "workshop" in category_blob:
        return "workshop"
    if " seminar " in token_blob or " workshops " in token_blob or " workshop " in token_blob:
        return "workshop"
    return "workshop"


def _extract_amount(cost_details: object) -> Decimal | None:
    if not isinstance(cost_details, dict):
        return None
    values = cost_details.get("values")
    if isinstance(values, list):
        for value in values:
            try:
                return Decimal(str(value))
            except Exception:
                continue
    return None


def _build_location_text(venue_obj: object) -> str:
    if not isinstance(venue_obj, dict):
        return MASSMOCA_DEFAULT_LOCATION
    parts = [
        _html_to_text(venue_obj.get("venue")),
        _html_to_text(venue_obj.get("address")),
        _html_to_text(venue_obj.get("city")) or MASSMOCA_CITY,
        _html_to_text(venue_obj.get("state")) or _html_to_text(venue_obj.get("stateprovince")) or MASSMOCA_STATE,
    ]
    cleaned = [part for part in parts if part]
    return ", ".join(cleaned) if cleaned else MASSMOCA_DEFAULT_LOCATION


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y-%m-%d":
                return datetime.combine(parsed.date(), datetime.min.time())
            return parsed
        except ValueError:
            continue
    return None


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    if not values:
        return None
    return " | ".join(values)


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    soup = BeautifulSoup(unescape(value), "html.parser")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    text = soup.get_text(" ", strip=True)
    return _normalize_space(text)


def _token_blob(value: str) -> str:
    return f" {' '.join((value or '').lower().split())} "


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(unescape(value).split())
