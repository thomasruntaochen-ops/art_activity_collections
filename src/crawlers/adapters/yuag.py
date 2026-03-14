import asyncio
import re
import subprocess
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.parse import urlparse
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.types import ExtractedActivity

YUAG_CALENDAR_URL = "https://artgallery.yale.edu/visit/calendar"

NY_TIMEZONE = "America/New_York"
YUAG_VENUE_NAME = "Yale University Art Gallery"
YUAG_CITY = "New Haven"
YUAG_STATE = "CT"
YUAG_DEFAULT_LOCATION = "New Haven, CT"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": YUAG_CALENDAR_URL,
}

YOUTH_CATEGORIES = {
    "teen events",
    "family programs",
}
EXCLUDED_KEYWORDS = (
    "tour",
    "tours",
    "virtual",
    "member event",
    "performance",
    "film",
    "reading",
    "concert",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
SCHEDULE_RE = re.compile(
    r"^[A-Za-z]+,\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}),\s+"
    r"(\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?|am|pm))\s*[–-]\s*"
    r"(\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?|am|pm))$",
    re.IGNORECASE,
)
PAGE_COUNT_RE = re.compile(r"Page\s+\d+\s+of\s+(\d+)", re.IGNORECASE)


async def fetch_yuag_page(
    url: str,
) -> str:
    return await asyncio.to_thread(_fetch_yuag_page_with_curl, url)


def build_yuag_calendar_url(*, start_date: str, page: int) -> str:
    query = urlencode({"start_date": start_date, "page": page})
    return f"{YUAG_CALENDAR_URL}?{query}"


async def load_yuag_calendar_payload(
    *,
    start_date: str,
    page_limit: int | None = None,
) -> dict:
    first_url = build_yuag_calendar_url(start_date=start_date, page=0)
    first_html = await fetch_yuag_page(first_url)
    page_total = _parse_total_pages(first_html)
    if page_limit is not None:
        page_total = min(page_total, max(page_limit, 1))

    page_payloads: list[tuple[str, str]] = [(first_url, first_html)]
    additional_urls = [
        build_yuag_calendar_url(start_date=start_date, page=page_index)
        for page_index in range(1, page_total)
    ]
    if additional_urls:
        fetched = await asyncio.gather(*(fetch_yuag_page(url) for url in additional_urls))
        page_payloads.extend(zip(additional_urls, fetched, strict=True))

    cards: list[dict] = []
    for list_url, html in page_payloads:
        cards.extend(_parse_listing_cards(html, list_url=list_url))

    detail_urls = sorted({card["source_url"] for card in cards})
    details_html = await asyncio.gather(*(fetch_yuag_page(url) for url in detail_urls))

    return {
        "cards": cards,
        "detail_pages": {url: html for url, html in zip(detail_urls, details_html, strict=True)},
    }


def download_yuag_calendar_cache(
    *,
    input_dir: str | Path,
    start_date: str,
    page_limit: int | None = None,
) -> dict[str, int]:
    directory = Path(input_dir)
    directory.mkdir(parents=True, exist_ok=True)

    first_url = build_yuag_calendar_url(start_date=start_date, page=0)
    first_path = directory / "calendar_page_0.html"
    _download_yuag_page_with_curl(first_url, first_path)
    first_html = first_path.read_text(encoding="utf-8")

    page_total = _parse_total_pages(first_html)
    if page_limit is not None:
        page_total = min(page_total, max(page_limit, 1))

    page_payloads: list[tuple[str, str]] = [(first_url, first_html)]
    for page_index in range(1, page_total):
        page_url = build_yuag_calendar_url(start_date=start_date, page=page_index)
        page_path = directory / f"calendar_page_{page_index}.html"
        _download_yuag_page_with_curl(page_url, page_path)
        page_payloads.append((page_url, page_path.read_text(encoding="utf-8")))

    cards: list[dict] = []
    for list_url, html in page_payloads:
        cards.extend(_parse_listing_cards(html, list_url=list_url))

    detail_urls = sorted({card["source_url"] for card in cards})
    for detail_url in detail_urls:
        detail_path = directory / f"detail_{_detail_cache_slug(detail_url)}.html"
        _download_yuag_page_with_curl(detail_url, detail_path)

    return {
        "pages": len(page_payloads),
        "details": len(detail_urls),
    }


def load_yuag_calendar_payload_from_dir(
    *,
    input_dir: str | Path,
    page_limit: int | None = None,
) -> dict:
    directory = Path(input_dir)
    if not directory.exists():
        raise FileNotFoundError(f"Input directory not found: {directory}")

    page_payloads: list[tuple[str, str]] = []
    page_index = 0
    while True:
        page_path = directory / f"calendar_page_{page_index}.html"
        if not page_path.exists():
            break
        page_payloads.append((YUAG_CALENDAR_URL, page_path.read_text(encoding="utf-8")))
        page_index += 1
        if page_limit is not None and page_index >= page_limit:
            break

    if not page_payloads:
        raise FileNotFoundError(f"No cached Yale calendar pages found in {directory}")

    cards: list[dict] = []
    for list_url, html in page_payloads:
        cards.extend(_parse_listing_cards(html, list_url=list_url))

    detail_pages: dict[str, str] = {}
    for detail_url in sorted({card["source_url"] for card in cards}):
        detail_path = directory / f"detail_{_detail_cache_slug(detail_url)}.html"
        if not detail_path.exists():
            raise FileNotFoundError(f"Missing cached detail page for {detail_url}: {detail_path}")
        detail_pages[detail_url] = detail_path.read_text(encoding="utf-8")

    return {"cards": cards, "detail_pages": detail_pages}


def parse_yuag_calendar_payload(payload: dict) -> list[ExtractedActivity]:
    detail_meta = {
        url: _parse_detail_page_meta(html, source_url=url)
        for url, html in payload["detail_pages"].items()
    }
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for card in payload["cards"]:
        detail = detail_meta.get(card["source_url"])
        if detail is None:
            continue

        title = card["title"]
        category = card["category"]
        description = detail["description"]
        if not _should_include_card(title=title, category=category, description=description):
            continue

        start_at, end_at = _parse_schedule_text(card["schedule_text"])
        if start_at is None:
            continue

        text_blob = " ".join([title, category, description or ""]).lower()
        age_min, age_max = _parse_age_range(description)
        key = (card["source_url"], title, start_at)
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            ExtractedActivity(
                source_url=card["source_url"],
                title=title,
                description=description,
                venue_name=YUAG_VENUE_NAME,
                location_text=YUAG_DEFAULT_LOCATION,
                city=YUAG_CITY,
                state=YUAG_STATE,
                activity_type=_infer_activity_type(title=title, category=category, description=description),
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=(
                    ("registration" in text_blob or "register" in text_blob)
                    and "no registration" not in text_blob
                ),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
            )
        )

    return rows


class YuagCalendarAdapter(BaseSourceAdapter):
    source_name = "yuag_calendar"

    def __init__(self, start_date: str):
        self.start_date = start_date

    async def fetch(self) -> list[str]:
        html = await fetch_yuag_page(build_yuag_calendar_url(start_date=self.start_date, page=0))
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        parsed_payload = {
            "cards": _parse_listing_cards(payload, list_url=build_yuag_calendar_url(start_date=self.start_date, page=0)),
            "detail_pages": {},
        }
        return parse_yuag_calendar_payload(parsed_payload)


def _parse_total_pages(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    info = soup.select_one("div.pagination__info")
    if info is None:
        return 1
    match = PAGE_COUNT_RE.search(info.get_text(" ", strip=True))
    if match is None:
        return 1
    return max(int(match.group(1)), 1)


def _fetch_yuag_page_with_curl(url: str) -> str:
    cmd = [
        "curl",
        "-L",
        "--max-time",
        "30",
        "--compressed",
        "-A",
        DEFAULT_HEADERS["User-Agent"],
        "-H",
        f"Accept: {DEFAULT_HEADERS['Accept']}",
        "-H",
        f"Accept-Language: {DEFAULT_HEADERS['Accept-Language']}",
        "-H",
        f"Referer: {DEFAULT_HEADERS['Referer']}",
        url,
    ]
    result = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"curl failed for Yale Art Gallery page ({url}): {result.stderr.strip() or result.returncode}"
        )
    return result.stdout


def _download_yuag_page_with_curl(url: str, output_path: str | Path) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "curl",
        "-L",
        "--max-time",
        "30",
        url,
        "-o",
        str(output),
    ]
    result = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"curl download failed for Yale Art Gallery page ({url}): {result.stderr.strip() or result.returncode}"
        )


def _parse_listing_cards(html: str, *, list_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict] = []
    for card in soup.select("div.teaser-card"):
        anchor = card.select_one("a.teaser-card__title-text[href]")
        if anchor is None:
            continue

        title = _normalize_space(anchor.get_text(" ", strip=True))
        category = _normalize_space(card.select_one("span.sub-label").get_text(" ", strip=True)) if card.select_one("span.sub-label") else ""
        info_field = card.select_one("div.teaser-card__info-field")
        schedule_text = _normalize_schedule_text(info_field.get_text(" ", strip=True) if info_field else "")
        if not title or not schedule_text:
            continue

        cards.append(
            {
                "title": title,
                "category": category,
                "schedule_text": schedule_text,
                "source_url": urljoin(list_url, anchor.get("href", "").strip()),
            }
        )

    return cards


def _parse_detail_page_meta(html: str, *, source_url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    body = soup.select_one("div.body-text") or soup.select_one("div.body-copy")
    description = None
    if body is not None:
        description_lines = [_normalize_space(node.get_text(" ", strip=True)) for node in body.select("p")]
        description_lines = [line for line in description_lines if line]
        if description_lines:
            description = " | ".join(description_lines)

    if description is None:
        meta_description = soup.find("meta", attrs={"name": "description"})
        if meta_description is not None:
            description = _normalize_space(meta_description.get("content"))

    category_button = soup.select_one("div.event-category span.button__text")
    category = _normalize_space(category_button.get_text(" ", strip=True)) if category_button else None

    return {
        "source_url": source_url,
        "category": category,
        "description": description,
    }


def _should_include_card(*, title: str, category: str, description: str | None) -> bool:
    normalized_category = category.strip().lower()
    if normalized_category not in YOUTH_CATEGORIES:
        return False

    text_blob = " ".join([title, category, description or ""]).lower()
    if any(keyword in text_blob for keyword in EXCLUDED_KEYWORDS):
        return False

    return True


def _parse_schedule_text(value: str) -> tuple[datetime | None, datetime | None]:
    match = SCHEDULE_RE.match(_normalize_schedule_text(value))
    if match is None:
        return None, None

    date_part = match.group(1)
    start_time = _normalize_meridiem(match.group(2))
    end_time = _normalize_meridiem(match.group(3))
    try:
        start_at = datetime.strptime(f"{date_part} {start_time}", "%B %d, %Y %I:%M %p")
        end_at = datetime.strptime(f"{date_part} {end_time}", "%B %d, %Y %I:%M %p")
    except ValueError:
        return None, None

    if end_at <= start_at:
        end_at = start_at + timedelta(minutes=90)
    return start_at, end_at


def _infer_activity_type(*, title: str, category: str, description: str | None) -> str:
    text_blob = " ".join([title, category, description or ""]).lower()
    if "lecture" in text_blob:
        return "lecture"
    if "conversation" in text_blob or "talk" in text_blob:
        return "talk"
    return "workshop"


def _parse_age_range(description: str | None) -> tuple[int | None, int | None]:
    if not description:
        return None, None

    if "month" in description.lower():
        return None, None

    match = AGE_RANGE_RE.search(description)
    if match is not None:
        first = int(match.group(1))
        second = int(match.group(2))
        return min(first, second), max(first, second)

    match = AGE_PLUS_RE.search(description)
    if match is not None:
        return int(match.group(1)), None

    return None, None


def _normalize_schedule_text(value: str) -> str:
    cleaned = value.replace("\xa0", " ").replace("\u2009", " ").replace("\u202f", " ")
    cleaned = cleaned.replace("–", " – ")
    return _normalize_space(cleaned)


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def _normalize_meridiem(value: str) -> str:
    normalized = value.strip().lower().replace(".", "")
    normalized = normalized.replace("am", " AM").replace("pm", " PM")
    return normalized.strip()


def _detail_cache_slug(url: str) -> str:
    parsed = urlparse(url)
    slug = parsed.path.rstrip("/").split("/")[-1]
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", slug)
