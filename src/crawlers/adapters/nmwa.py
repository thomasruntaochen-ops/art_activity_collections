from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from html import unescape
from typing import Any

from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

NMWA_CALENDAR_URL = "https://nmwa.org/whats-on/calendar/"
NMWA_MONTHS_URL = "https://nmwa.org/wp-json/nmwa/v1/events/months"
NMWA_EVENT_API_URL = "https://nmwa.org/wp-json/wp/v2/event"
NMWA_CATALOGUE_URL = "https://nmwa.org/wp-json/nmwa/v1/catalogue?contentId=nmwa-calendar"
NMWA_TIMEZONE = "America/New_York"
NMWA_VENUE_NAME = "National Museum of Women in the Arts"
NMWA_CITY = "Washington"
NMWA_STATE = "DC"
NMWA_DEFAULT_LOCATION = "Washington, DC"
NMWA_RESULTS_PER_PAGE = 15
NMWA_MAX_PAGES = 3

PLAYWRIGHT_LAUNCH_ARGS = (
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
)
PLAYWRIGHT_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'platform', {get: () => 'MacIntel'});
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
"""

EXCLUDED_MARKERS = (
    " admission ",
    " free admission ",
    " tour ",
    " tours ",
    " film ",
    " films ",
    " lecture ",
    " lectures ",
    " talk ",
    " talks ",
    " reading ",
    " writing ",
    " poetry ",
    " poem ",
    " performance ",
    " performances ",
    " music ",
    " story ",
    " storytime ",
    " yoga ",
    " meditation ",
    " mindfulness ",
    " dinner ",
    " reception ",
    " jazz ",
    " orchestra ",
    " band ",
    " tai chi ",
)


class NMWAEventsAdapter(BaseSourceAdapter):
    source_name = "nmwa_calendar"

    async def fetch(self) -> list[str]:
        payload = await load_nmwa_events_payload()
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_nmwa_events_payload(json.loads(payload))


async def load_nmwa_events_payload(*, timeout_ms: int = 90000) -> dict[str, Any]:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not installed. Install crawler extras and Chromium with "
            "`pip install -e .[crawler]` then `playwright install chromium`."
        )

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, args=list(PLAYWRIGHT_LAUNCH_ARGS))
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id=NMWA_TIMEZONE,
            viewport={"width": 1365, "height": 900},
            screen={"width": 1365, "height": 900},
            color_scheme="light",
        )
        await context.add_init_script(PLAYWRIGHT_INIT_SCRIPT)
        page = await context.new_page()
        try:
            await page.goto(NMWA_CALENDAR_URL, wait_until="networkidle", timeout=timeout_ms)
            calendar_title = await page.title()
            listing_html = await page.content()

            first_page = await _fetch_json_via_page(
                page,
                f"{NMWA_EVENT_API_URL}?per_page={NMWA_RESULTS_PER_PAGE}&page=1&_embed=1",
                timeout_ms=timeout_ms,
            )
            total_pages = int(first_page["headers"].get("x-wp-totalpages", NMWA_MAX_PAGES))
            total_pages = max(1, min(NMWA_MAX_PAGES, total_pages))

            pages: list[dict[str, Any]] = [first_page["json"]]
            for page_number in range(2, total_pages + 1):
                page_payload = await _fetch_json_via_page(
                    page,
                    f"{NMWA_EVENT_API_URL}?per_page={NMWA_RESULTS_PER_PAGE}&page={page_number}&_embed=1",
                    timeout_ms=timeout_ms,
                )
                pages.append(page_payload["json"])

            catalogue = await _fetch_json_via_page(
                page,
                NMWA_CATALOGUE_URL,
                timeout_ms=timeout_ms,
            )
            months = await _fetch_json_via_page(page, NMWA_MONTHS_URL, timeout_ms=timeout_ms)
        finally:
            await page.close()
            await context.close()
            await browser.close()

    return {
        "calendar_url": NMWA_CALENDAR_URL,
        "calendar_title": calendar_title,
        "listing_html": listing_html,
        "months": months["json"],
        "catalogue": catalogue["json"],
        "pages": pages,
    }


def parse_nmwa_events_payload(payload: dict[str, Any]) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for page in payload.get("pages") or []:
        if not isinstance(page, list):
            continue
        for item in page:
            if not isinstance(item, dict):
                continue
            extracted = _build_activity_from_event(item)
            if extracted is None:
                continue
            key = (extracted.source_url, extracted.title, extracted.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(extracted)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


async def _fetch_json_via_page(page, url: str, *, timeout_ms: int) -> dict[str, Any]:
    payload = await page.evaluate(
        """
        async ({ url }) => {
          const response = await fetch(url, { credentials: 'include' });
          const text = await response.text();
          return {
            status: response.status,
            headers: Object.fromEntries(response.headers.entries()),
            text,
          };
        }
        """,
        {"url": url},
    )
    if payload["status"] >= 400:
        raise RuntimeError(f"Fetch failed for {url}: HTTP {payload['status']}")
    try:
        data = json.loads(payload["text"])
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Unable to decode JSON from {url}") from exc
    return {"json": data, "headers": payload["headers"]}


def _build_activity_from_event(item: dict[str, Any]) -> ExtractedActivity | None:
    title = _normalize_space(_text_or_empty(item.get("title", {}).get("rendered")))
    if not title:
        return None

    class_list = item.get("class_list") or []
    acf = item.get("acf") or {}
    source_url = _normalize_space(str(item.get("link") or ""))
    if not source_url:
        return None

    if not _should_include_event(title=title, class_list=class_list, acf=acf):
        return None

    start_at = _parse_event_datetime(acf.get("event_start_date"), acf.get("start_time"))
    if start_at is None:
        return None
    end_at = _parse_event_datetime(acf.get("event_end_date") or acf.get("event_start_date"), acf.get("end_time"))
    if end_at is None and acf.get("event_end_date"):
        end_at = _parse_event_datetime(acf.get("event_end_date"), acf.get("start_time"))

    location_text = _html_to_text(acf.get("location"))
    subheading = _html_to_text(acf.get("subheading"))
    tickets_text = _normalize_space(str(acf.get("tickets_reservation_info") or ""))

    description_parts: list[str] = []
    if subheading:
        description_parts.append(subheading)
    if location_text:
        description_parts.append(f"Location: {location_text}")
    if tickets_text:
        description_parts.append(f"Tickets: {tickets_text}")

    description = " | ".join(description_parts) if description_parts else None
    combined_text = _normalized_blob(title, description, tickets_text, location_text, " ".join(class_list))

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=NMWA_VENUE_NAME,
        location_text=location_text or NMWA_DEFAULT_LOCATION,
        city=NMWA_CITY,
        state=NMWA_STATE,
        activity_type=_infer_activity_type(class_list=class_list, title=title, description=description),
        age_min=None,
        age_max=None,
        drop_in=("drop-in" in combined_text or "drop in" in combined_text),
        registration_required=_infer_registration_required(acf=acf, combined_text=combined_text),
        start_at=start_at,
        end_at=end_at,
        timezone=NMWA_TIMEZONE,
        **price_classification_kwargs(
            " | ".join(
                filter(
                    None,
                    [
                        tickets_text,
                        "Free" if acf.get("free_event") else "",
                        "Free with admission" if acf.get("free_w_admission") else "",
                    ],
                )
            ),
            default_is_free=None,
        ),
    )


def _should_include_event(*, title: str, class_list: list[str], acf: dict[str, Any]) -> bool:
    class_blob = " ".join(class_list).lower()
    if "event_types-family" not in class_blob:
        return False

    combined = _normalized_blob(
        title,
        _html_to_text(acf.get("subheading")),
        _html_to_text(acf.get("location")),
        str(acf.get("tickets_reservation_info") or ""),
        " ".join(class_list),
    )

    if any(marker in combined for marker in EXCLUDED_MARKERS):
        return False

    if "community day" in combined:
        return False

    if acf.get("free_w_admission"):
        return False

    return True


def _infer_activity_type(*, class_list: list[str], title: str, description: str | None) -> str:
    class_blob = " ".join(class_list).lower()
    combined = _normalized_blob(title, description, class_blob)
    if "workshops" in class_blob or "art making" in class_blob:
        return "workshop"
    if "discussion" in class_blob or "gallery-talk" in class_blob:
        return "talk"
    return "activity"


def _infer_registration_required(*, acf: dict[str, Any], combined_text: str) -> bool | None:
    tickets_text = str(acf.get("tickets_reservation_info") or "").lower()
    if "registration required" in tickets_text or "tickets required" in tickets_text:
        return True
    if "registration recommended" in tickets_text or "registration encouraged" in tickets_text:
        return False
    if "reservations recommended" in tickets_text:
        return False
    if acf.get("event_sold_out"):
        return True
    if "registration" in combined_text:
        return True
    return None


def _parse_event_datetime(date_value: Any, time_value: Any) -> datetime | None:
    if date_value in (None, "", 0):
        return None
    date_str = str(date_value).strip()
    if len(date_str) != 8 or not date_str.isdigit():
        return None

    base = datetime.strptime(date_str, "%Y%m%d")
    if time_value in (None, "", False):
        return base.replace(hour=0, minute=0, second=0, microsecond=0)

    time_str = str(time_value).strip()
    if not time_str.isdigit():
        return base.replace(hour=0, minute=0, second=0, microsecond=0)

    time_str = time_str.zfill(6)
    try:
        parsed_time = datetime.strptime(time_str, "%H%M%S")
    except ValueError:
        return base.replace(hour=0, minute=0, second=0, microsecond=0)

    return base.replace(
        hour=parsed_time.hour,
        minute=parsed_time.minute,
        second=parsed_time.second,
        microsecond=0,
    )


def _html_to_text(value: Any) -> str | None:
    if value in (None, "", False):
        return None
    if isinstance(value, str):
        soup = BeautifulSoup(value, "html.parser")
        text = soup.get_text(" ", strip=True)
        return _normalize_space(unescape(text)) or None
    return _normalize_space(str(value)) or None


def _text_or_empty(value: Any) -> str:
    if value in (None, "", False):
        return ""
    if isinstance(value, str):
        return _normalize_space(unescape(value))
    return _normalize_space(str(value))


def _normalized_blob(*parts: str | None) -> str:
    return f" {' '.join(_normalize_space(part or '') for part in parts if part).lower()} "


def _normalize_space(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())
