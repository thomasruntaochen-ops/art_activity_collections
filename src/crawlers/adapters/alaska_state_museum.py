import json
import re
import asyncio
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.types import ExtractedActivity

ASM_EVENTS_URL = "https://lam.alaska.gov/asm-events"
ASM_YOUTH_URL = "https://lam.alaska.gov/youthart"
ASM_TIMEZONE = "America/Anchorage"
ASM_VENUE_NAME = "Alaska State Museum"
ASM_CITY = "Juneau"
ASM_STATE = "AK"
ASM_LOCATION = "Juneau, AK"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

MONTH_DAY_RE = re.compile(
    r"\b(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)\s+"
    r"(?P<day>\d{1,2})\b",
    re.IGNORECASE,
)
DATE_TIME_RE = re.compile(
    r"^(?:(?P<weekday>[A-Za-z]+),\s+)?"
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+"
    r"(?P<time>[^|]+)$",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*"
    r"(?P<start_meridiem>a\.?m\.?|p\.?m\.?|am|pm)?\s*"
    r"(?:-|–|—|to)\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*"
    r"(?P<end_meridiem>a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+", re.IGNORECASE)
LAST_UPDATED_RE = re.compile(r"\b[A-Z][a-z]{2}\s+\d{1,2},\s+(?P<year>\d{4})\b")
WEEKDAY_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


async def fetch_alaska_state_museum_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[alaska-state-museum-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[alaska-state-museum-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[alaska-state-museum-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            print(
                "[alaska-state-museum-fetch] "
                f"attempt {attempt}/{max_attempts}: status={response.status_code}"
            )
            if response.status_code < 400:
                print(
                    "[alaska-state-museum-fetch] "
                    f"success on attempt {attempt}, bytes={len(response.text)}"
                )
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Alaska State Museum page") from last_exception
    raise RuntimeError("Unable to fetch Alaska State Museum page after retries")


async def load_alaska_state_museum_payload() -> dict[str, object]:
    urls = [ASM_EVENTS_URL, ASM_YOUTH_URL]
    page_html = await _gather_pages(urls)
    return {"pages": page_html}


async def _gather_pages(urls: list[str]) -> dict[str, str]:
    responses = await _fetch_many(urls)
    return dict(zip(urls, responses))


async def _fetch_many(urls: list[str]) -> list[str]:
    results: list[str] = []
    for url in urls:
        results.append(await fetch_alaska_state_museum_page(url))
    return results


class AlaskaStateMuseumAdapter(BaseSourceAdapter):
    source_name = "alaska_state_museum_events"

    async def fetch(self) -> list[str]:
        payload = await load_alaska_state_museum_payload()
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_alaska_state_museum_payload(data)


def parse_alaska_state_museum_payload(payload: dict[str, object]) -> list[ExtractedActivity]:
    pages = payload.get("pages") or {}
    if not isinstance(pages, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    asm_html = pages.get(ASM_EVENTS_URL)
    if isinstance(asm_html, str):
        item = _parse_asm_events_page(asm_html, source_url=ASM_EVENTS_URL)
        if item is not None:
            key = (item.source_url, item.title, item.start_at)
            if key not in seen:
                seen.add(key)
                rows.append(item)

    youth_html = pages.get(ASM_YOUTH_URL)
    if isinstance(youth_html, str):
        item = _parse_youthart_page(youth_html, source_url=ASM_YOUTH_URL)
        if item is not None:
            key = (item.source_url, item.title, item.start_at)
            if key not in seen:
                seen.add(key)
                rows.append(item)

    return rows


def _parse_asm_events_page(html: str, *, source_url: str) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    year = _extract_reference_year(soup)
    for heading in soup.find_all("h2"):
        title = normalize_space(heading.get_text(" ", strip=True))
        if "sketching at the museum" not in title.lower():
            continue

        info_block = heading.find_next_sibling("div")
        if info_block is None:
            return None

        lines = _extract_unique_lines(info_block)
        if not lines:
            return None

        meta_line = lines[0]
        description = " | ".join(lines[1:]) if len(lines) > 1 else None
        date_match = MONTH_DAY_RE.search(meta_line)
        if date_match is None:
            return None

        start_at, end_at = _parse_month_day_time(
            month_name=date_match.group("month"),
            day_text=date_match.group("day"),
            year=year,
            time_text=meta_line,
        )
        if start_at is None:
            return None

        age_min, age_max = _parse_age_range(" ".join(lines))
        is_free, free_status = _classify_asm_price(" | ".join(lines))
        text_blob = " ".join([title, meta_line, description or ""]).lower()

        return ExtractedActivity(
            source_url=source_url,
            title=title,
            description=description,
            venue_name=ASM_VENUE_NAME,
            location_text=ASM_LOCATION,
            city=ASM_CITY,
            state=ASM_STATE,
            activity_type="workshop",
            age_min=age_min,
            age_max=age_max,
            drop_in=("drop-in" in text_blob or "drop in" in text_blob),
            registration_required=(
                "registration required" in text_blob or "registration is required" in text_blob
            ),
            start_at=start_at,
            end_at=end_at,
            timezone=ASM_TIMEZONE,
            is_free=is_free,
            free_verification_status=free_status,
        )
    return None


def _parse_youthart_page(html: str, *, source_url: str) -> ExtractedActivity | None:
    soup = BeautifulSoup(html, "html.parser")
    year = _extract_reference_year(soup)
    heading = None
    for candidate in soup.find_all("h3"):
        title = normalize_space(candidate.get_text(" ", strip=True))
        if "workshop" in title.lower():
            heading = candidate
            break
    if heading is None:
        return None

    title = normalize_space(heading.get_text(" ", strip=True))
    lines: list[str] = []
    sibling = heading.find_next_sibling()
    while sibling is not None and sibling.name != "h3":
        if sibling.name == "p":
            text = normalize_space(sibling.get_text(" | ", strip=True))
            if text:
                lines.append(text)
        sibling = sibling.find_next_sibling()

    if not lines:
        return None

    meta_line = lines[0]
    meta_parts = [normalize_space(part) for part in meta_line.split("|") if normalize_space(part)]
    date_line = meta_parts[0] if meta_parts else meta_line
    location_line = meta_parts[1] if len(meta_parts) > 1 else ASM_LOCATION
    location_line = re.sub(r",\s*free\s*$", "", location_line, flags=re.IGNORECASE).strip()
    date_match = DATE_TIME_RE.match(date_line)
    if date_match is None:
        return None

    start_at, end_at = _parse_month_day_time(
        month_name=date_match.group("month"),
        day_text=date_match.group("day"),
        year=year,
        time_text=date_match.group("time"),
        weekday=date_match.group("weekday"),
    )
    if start_at is None:
        return None

    description_parts = [
        line
        for line in lines[1:]
        if not re.match(r"^(sign up|questions\?|hosted through)", line, re.IGNORECASE)
    ]
    description = " | ".join(description_parts) if description_parts else None
    age_min, age_max = _parse_age_range(" ".join(lines))
    is_free, free_status = infer_price_classification(" | ".join([meta_line, description or ""]))
    text_blob = " ".join([title, meta_line, description or ""]).lower()

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=description,
        venue_name=ASM_VENUE_NAME,
        location_text=location_line or ASM_LOCATION,
        city=ASM_CITY,
        state=ASM_STATE,
        activity_type="workshop",
        age_min=age_min,
        age_max=age_max,
        drop_in=("drop-in" in text_blob or "drop in" in text_blob),
        registration_required=(
            "registration required" in text_blob or "registration is required" in text_blob
        ),
        start_at=start_at,
        end_at=end_at,
        timezone=ASM_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_reference_year(soup: BeautifulSoup) -> int:
    meta = soup.find("meta", attrs={"name": "DC.Date.Modified"})
    if meta is not None:
        content = normalize_space(meta.get("content"))
        if content:
            try:
                return datetime.strptime(content, "%b %d, %Y").year
            except ValueError:
                pass

    text = normalize_space(soup.get_text(" ", strip=True))
    match = LAST_UPDATED_RE.search(text)
    if match is not None:
        return int(match.group("year"))
    return datetime.now().year


def _parse_month_day_time(
    *,
    month_name: str,
    day_text: str,
    year: int,
    time_text: str,
    weekday: str | None = None,
) -> tuple[datetime | None, datetime | None]:
    try:
        base_date = datetime.strptime(f"{month_name} {day_text} {year}", "%B %d %Y")
    except ValueError:
        return None, None

    if weekday:
        weekday_index = WEEKDAY_INDEX.get(weekday.strip().lower())
        if weekday_index is not None and base_date.weekday() != weekday_index:
            return None, None

    start_at, end_at = _parse_time_range(base_date=base_date, time_text=time_text)
    return start_at, end_at


def _parse_time_range(*, base_date: datetime, time_text: str) -> tuple[datetime | None, datetime | None]:
    normalized = normalize_space(time_text).replace("–", "-").replace("—", "-")
    match = TIME_RANGE_RE.search(normalized)
    if match is None:
        return datetime.combine(base_date.date(), datetime.min.time()), None

    start_at = _build_time(
        base_date=base_date,
        hour_text=match.group("start_hour"),
        minute_text=match.group("start_minute"),
        meridiem=match.group("start_meridiem") or match.group("end_meridiem"),
    )
    end_at = _build_time(
        base_date=base_date,
        hour_text=match.group("end_hour"),
        minute_text=match.group("end_minute"),
        meridiem=match.group("end_meridiem"),
    )
    return start_at, end_at


def _build_time(
    *,
    base_date: datetime,
    hour_text: str | None,
    minute_text: str | None,
    meridiem: str | None,
) -> datetime | None:
    if hour_text is None or meridiem is None:
        return None

    hour = int(hour_text)
    minute = int(minute_text or "0")
    meridiem_text = meridiem.lower().replace(".", "")
    if meridiem_text == "pm" and hour != 12:
        hour += 12
    if meridiem_text == "am" and hour == 12:
        hour = 0
    return base_date.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _classify_asm_price(text: str) -> tuple[bool | None, str]:
    normalized = f" {normalize_space(text).lower()} "
    if "donations accepted" in normalized or "free for foslam members" in normalized:
        return None, "uncertain"
    return infer_price_classification(text)


def _extract_unique_lines(container) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()
    for node in container.find_all("p"):
        text = normalize_space(node.get_text(" ", strip=True))
        if not text or text in seen:
            continue
        seen.add(text)
        lines.append(text)
    return lines


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    normalized = normalize_space(text)
    range_match = AGE_RANGE_RE.search(normalized)
    if range_match is not None:
        return int(range_match.group(1)), int(range_match.group(2))
    plus_match = AGE_PLUS_RE.search(normalized)
    if plus_match is not None:
        return int(plus_match.group(1)), None
    return None, None
