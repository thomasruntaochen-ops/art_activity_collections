import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.types import ExtractedActivity

FRESNO_WORKSHOPS_URL = "https://fresnoartmuseum.org/workshop"

LA_TIMEZONE = "America/Los_Angeles"
FRESNO_VENUE_NAME = "Fresno Art Museum"
FRESNO_CITY = "Fresno"
FRESNO_STATE = "CA"
FRESNO_DEFAULT_LOCATION = "Fresno, CA"

DATE_RE = re.compile(r"^(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})(?:[.,])?\s+(?P<year>\d{4})$")
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://fresnoartmuseum.org/events",
}


async def fetch_fresno_workshops_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[fresno-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[fresno-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[fresno-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[fresno-fetch] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[fresno-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[fresno-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[fresno-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Fresno workshops page") from last_exception
    raise RuntimeError("Unable to fetch Fresno workshops page after retries")


class FresnoWorkshopsAdapter(BaseSourceAdapter):
    source_name = "fresno_workshops"

    def __init__(self, url: str = FRESNO_WORKSHOPS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_fresno_workshops_page(self.url)
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_fresno_workshops_html(payload, list_url=self.url)


def parse_fresno_workshops_html(
    html: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main")
    if main is None:
        return []

    title = _extract_workshop_title(main)
    if not title:
        return []

    description = _build_description(main)
    current_date = (now or datetime.now()).date()
    rows: list[ExtractedActivity] = []

    for date_value in _extract_workshop_dates(main):
        if date_value.date() < current_date:
            continue
        rows.append(
            ExtractedActivity(
                source_url=urljoin(list_url, "#registration-dates"),
                title=title,
                description=description,
                venue_name=FRESNO_VENUE_NAME,
                location_text=FRESNO_DEFAULT_LOCATION,
                city=FRESNO_CITY,
                state=FRESNO_STATE,
                activity_type="workshop",
                age_min=None,
                age_max=None,
                drop_in=False,
                registration_required=True,
                start_at=date_value,
                end_at=None,
                timezone=LA_TIMEZONE,
                free_verification_status="inferred",
            )
        )

    return rows


def _extract_workshop_title(main: BeautifulSoup) -> str | None:
    for heading in main.find_all("h3"):
        text = _normalize_space(heading.get_text(" ", strip=True))
        if not text or text.lower() == "details below:":
            continue
        return text
    return None


def _build_description(main: BeautifulSoup) -> str | None:
    paragraphs = [_normalize_space(node.get_text(" ", strip=True)) for node in main.find_all("p")]
    parts: list[str] = []
    for text in paragraphs:
        if not text:
            continue
        if DATE_RE.match(text):
            continue
        lowered = text.lower()
        if lowered.startswith("click here to"):
            continue
        if lowered == "registration is open for the following dates:":
            continue
        parts.append(text)
    return " | ".join(parts) if parts else None


def _extract_workshop_dates(main: BeautifulSoup) -> list[datetime]:
    dates: list[datetime] = []
    seen: set[datetime] = set()
    for node in main.find_all("p"):
        text = _normalize_space(node.get_text(" ", strip=True))
        parsed = _parse_date_text(text)
        if parsed is None or parsed in seen:
            continue
        seen.add(parsed)
        dates.append(parsed)
    return dates


def _parse_date_text(value: str) -> datetime | None:
    match = DATE_RE.match(value)
    if match is None:
        return None

    normalized = f"{match.group('month')} {match.group('day')}, {match.group('year')}"
    try:
        return datetime.strptime(normalized, "%B %d, %Y")
    except ValueError:
        return None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())
