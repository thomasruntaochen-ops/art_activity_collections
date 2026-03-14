import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.types import ExtractedActivity

THEMODERN_PROGRAMS_URL = "https://www.themodern.org/programs"
THEMODERN_TIMEZONE = "America/Chicago"
THEMODERN_VENUE_NAME = "The Modern Art Museum of Fort Worth"
THEMODERN_CITY = "Fort Worth"
THEMODERN_STATE = "TX"
THEMODERN_LOCATION = "Fort Worth, TX"

THEMODERN_INCLUDED_KEYWORDS = (
    "child program",
    "teen program",
    "family",
    "school-aged",
    "kids",
    "children",
    "art break",
    "gallery project",
    "drawing",
    "class",
    "activity",
    "studio",
)
THEMODERN_EXCLUDED_KEYWORDS = (
    "tour",
    "lecture",
    "registration",
    "camp",
    "adult program",
    "teachers",
    "educators",
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": THEMODERN_PROGRAMS_URL,
}
WHITESPACE_RE = re.compile(r"\s+")


async def fetch_themodern_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[themodern-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[themodern-fetch] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[themodern-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(
                        f"[themodern-fetch] transient transport error, retrying after {wait_seconds:.1f}s"
                    )
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[themodern-fetch] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[themodern-fetch] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[themodern-fetch] transient status={response.status_code}, "
                    f"retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch The Modern programs page") from last_exception
    raise RuntimeError("Unable to fetch The Modern programs page after retries")


async def load_themodern_payload(
    base_url: str = THEMODERN_PROGRAMS_URL,
    *,
    max_pages: int = 2,
) -> dict[str, object]:
    list_pages: dict[str, str] = {}
    next_url = base_url

    for _ in range(max(max_pages, 1)):
        if next_url in list_pages:
            break
        html = await fetch_themodern_page(next_url)
        list_pages[next_url] = html
        next_url = _extract_next_page_url(html, list_url=next_url)
        if not next_url:
            break

    detail_urls: list[str] = []
    seen_urls: set[str] = set()
    for page_url, html in list_pages.items():
        for card in _iter_themodern_cards(html, list_url=page_url):
            if card["source_url"] in seen_urls:
                continue
            seen_urls.add(card["source_url"])
            detail_urls.append(card["source_url"])

    detail_pages_raw = await asyncio.gather(*(fetch_themodern_page(url) for url in detail_urls))
    detail_pages = dict(zip(detail_urls, detail_pages_raw))
    return {
        "list_pages": list_pages,
        "detail_pages": detail_pages,
    }


class TheModernProgramsAdapter(BaseSourceAdapter):
    source_name = "themodern_programs"

    def __init__(self, url: str = THEMODERN_PROGRAMS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_themodern_payload(self.url)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_themodern_payload(data)


def parse_themodern_payload(payload: dict[str, object]) -> list[ExtractedActivity]:
    list_pages = payload.get("list_pages") or {}
    detail_pages = payload.get("detail_pages") or {}
    if not isinstance(list_pages, dict) or not isinstance(detail_pages, dict):
        return []

    cards_by_url: dict[str, list[dict[str, str]]] = {}
    for page_url, html in list_pages.items():
        if not isinstance(page_url, str) or not isinstance(html, str):
            continue
        for card in _iter_themodern_cards(html, list_url=page_url):
            cards_by_url.setdefault(card["source_url"], []).append(card)

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for source_url, detail_html in detail_pages.items():
        if not isinstance(source_url, str) or not isinstance(detail_html, str):
            continue
        listing_cards = cards_by_url.get(source_url, [])
        program_types = {card["program_type"] for card in listing_cards if card["program_type"]}
        detail = _parse_themodern_detail_page(detail_html)
        title = detail["title"] or (listing_cards[0]["title"] if listing_cards else "")
        description = detail["description"]

        if not _should_keep_themodern_event(
            title=title,
            program_types=program_types,
            description=description,
        ):
            continue

        occurrences = detail["occurrences"]
        if not occurrences:
            occurrences = []
            for card in listing_cards:
                start_at = _parse_listing_datetime(card["date_text"], card["time_text"])
                if start_at is not None:
                    occurrences.append(start_at)

        for start_at in occurrences:
            key = (source_url, title, start_at)
            if key in seen:
                continue
            seen.add(key)

            text_blob = " ".join([title, description or "", " ".join(program_types)]).lower()
            rows.append(
                ExtractedActivity(
                    source_url=source_url,
                    title=title,
                    description=description,
                    venue_name=THEMODERN_VENUE_NAME,
                    location_text=THEMODERN_LOCATION,
                    city=THEMODERN_CITY,
                    state=THEMODERN_STATE,
                    activity_type=_infer_themodern_activity_type(title, description),
                    age_min=None,
                    age_max=None,
                    drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                    registration_required=("registration" in text_blob and "free" not in text_blob),
                    start_at=start_at,
                    end_at=None,
                    timezone=THEMODERN_TIMEZONE,
                    free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
                )
            )

    return rows


def _iter_themodern_cards(html: str, *, list_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict[str, str]] = []
    for anchor in soup.select("a.program_calendar"):
        href = (anchor.get("href") or "").strip()
        title_node = anchor.select_one(".program_calendar-title")
        type_node = anchor.select_one(".program_calendar-type")
        date_node = anchor.select_one(".program_calendar-date .date")
        time_node = anchor.select_one(".program_calendar-date .time")
        title = _normalize_space(title_node.get_text(" ", strip=True) if title_node else "")
        if not href or not title:
            continue
        cards.append(
            {
                "source_url": urljoin(list_url, href),
                "title": title,
                "program_type": _normalize_space(type_node.get_text(" ", strip=True) if type_node else ""),
                "date_text": _normalize_space(date_node.get_text(" ", strip=True) if date_node else ""),
                "time_text": _normalize_space(time_node.get_text(" ", strip=True) if time_node else ""),
            }
        )
    return cards


def _extract_next_page_url(html: str, *, list_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    node = soup.select_one("ul.js-pager__items a[rel='next'], ul.js-pager__items a.button")
    href = (node.get("href") or "").strip() if node is not None else ""
    return urljoin(list_url, href) if href else None


def _parse_themodern_detail_page(html: str) -> dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    title_node = soup.select_one("h1.title")
    title = _normalize_space(title_node.get_text(" ", strip=True) if title_node else "")

    dates: list[datetime] = []
    for item in soup.select(".dates li"):
        parsed = _parse_detail_datetime(item.get_text(" ", strip=True))
        if parsed is not None:
            dates.append(parsed)

    body_node = soup.select_one(".view-programs-individual-body .col-md-12")
    description = _normalize_text_block(body_node.get_text("\n", strip=True) if body_node else "")
    return {
        "title": title,
        "occurrences": dates,
        "description": description or None,
    }


def _parse_listing_datetime(date_text: str, time_text: str) -> datetime | None:
    normalized = _normalize_space(f"{date_text} {time_text}")
    if not normalized:
        return None
    for fmt in ("%m/%d/%y %I:%M %p", "%m/%d/%Y %I:%M %p"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _parse_detail_datetime(value: str) -> datetime | None:
    normalized = _normalize_space(value)
    for fmt in ("%B %d, %Y %I:%M %p", "%b %d, %Y %I:%M %p"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _should_keep_themodern_event(
    *,
    title: str,
    program_types: set[str],
    description: str | None,
) -> bool:
    type_text = " ".join(sorted(program_types))
    text = " ".join([title, type_text, description or ""]).lower()
    included = any(keyword in text for keyword in THEMODERN_INCLUDED_KEYWORDS)

    if "tour" in title.lower() or "lecture" in title.lower():
        return False
    if "open to adults" in text or "adults at all skill levels" in text:
        return False
    if "click here to register" in text or "registration begins" in text or "art camp" in text:
        return False
    if "teachers" in text or "educators" in text:
        return False
    if any(keyword in text for keyword in THEMODERN_EXCLUDED_KEYWORDS) and not included:
        return False
    return included


def _infer_themodern_activity_type(title: str, description: str | None) -> str:
    text = " ".join([title, description or ""]).lower()
    if "class" in text:
        return "class"
    if "studio" in text or "workshop" in text:
        return "workshop"
    return "activity"


def _normalize_space(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def _normalize_text_block(value: str) -> str:
    lines = [_normalize_space(line) for line in value.splitlines()]
    return " ".join(line for line in lines if line)
