import asyncio
import json
import re
from datetime import datetime
from html import unescape
from urllib.parse import parse_qs
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import Request
from urllib.request import urlopen

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.types import ExtractedActivity

AMON_CARTER_EVENTS_URL = (
    "https://www.cartermuseum.org/events-calendar"
    "?e_type%5B74%5D=74&e_type%5B73%5D=73&e_type%5B39%5D=39&e_type%5B40%5D=40"
)
AMON_CARTER_TIMEZONE = "America/Chicago"
AMON_CARTER_VENUE_NAME = "Amon Carter Museum of American Art"
AMON_CARTER_CITY = "Fort Worth"
AMON_CARTER_STATE = "TX"
AMON_CARTER_LOCATION = "Fort Worth, TX"

CARD_RE = re.compile(
    r'<li class="events-list__card">.*?<a href="(?P<href>[^"]+)"[^>]*>.*?'
    r"<h4[^>]*>(?P<title>.*?)</h4>.*?"
    r'<div class="c-card-sm-horz__kicker[^"]*"[^>]*>(?P<kicker>.*?)</div>',
    re.DOTALL,
)
PAGER_RE = re.compile(r"[?&]page=(\d+)")
META_DESCRIPTION_RE = re.compile(
    r'<meta\s+name="description"\s+content="(?P<value>[^"]*)"',
    re.IGNORECASE,
)
DATA_LAYER_RE = re.compile(
    r"window\.dataLayer\.push\((\{.*?\})\);",
    re.DOTALL,
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\+\b", re.IGNORECASE)
MONTHS_YOUNGER_RE = re.compile(
    r"\b(\d{1,2})\s*months?\s+and\s+younger\b",
    re.IGNORECASE,
)
YEARS_OLDER_RE = re.compile(
    r"\b(\d{1,2})\s*(?:years?\s*)?(?:and|&)\s*older\b",
    re.IGNORECASE,
)
DATE_WITH_TIME_RE = re.compile(
    r"(?P<weekday>[A-Za-z]+),\s+"
    r"(?P<month>[A-Za-z]+)\s+"
    r"(?P<day>\d{1,2}),\s+"
    r"(?P<time>.+)"
)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?)\s*(?P<start_meridiem>a\.m\.|p\.m\.|am|pm)?"
    r"\s*[–-]\s*"
    r"(?P<end>\d{1,2}(?::\d{2})?)\s*(?P<end_meridiem>a\.m\.|p\.m\.|am|pm)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?)\s*(?P<meridiem>a\.m\.|p\.m\.|am|pm)",
    re.IGNORECASE,
)
STRIP_TAGS_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
FAMILY_KEYWORDS = (
    "family",
    "children",
    "child",
    "kids",
    "toddler",
    "teen",
    "young visitors",
    "young artists",
    "spring break",
    "playdate",
    "itty-bitty",
)
EXCLUDED_KEYWORDS = (
    "guided tour",
    "tea & tours",
    "tour",
    "bookish",
    "book signing",
    "film",
    "music",
    "concert",
    "performance",
    "member event",
    "member events",
    "exhibition talk",
    "paper forum",
    "admission",
    "tickets",
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.cartermuseum.org/events-calendar",
}


async def fetch_amon_carter_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[amon-carter-fetch] start url={url} max_attempts={max_attempts}")
    last_exception: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[amon-carter-fetch] attempt {attempt}/{max_attempts}: sending request")
            response = await asyncio.to_thread(_fetch_sync, url)
        except Exception as exc:
            last_exception = exc
            print(f"[amon-carter-fetch] attempt {attempt}/{max_attempts}: transport error={exc}")
            if attempt < max_attempts:
                wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(
                    f"[amon-carter-fetch] transient transport error, retrying after {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                continue
            break

        print(f"[amon-carter-fetch] attempt {attempt}/{max_attempts}: status={response['status']}")
        if response["status"] < 400:
            print(f"[amon-carter-fetch] success on attempt {attempt}, bytes={len(response['text'])}")
            return response["text"]

        if response["status"] in (429, 500, 502, 503, 504) and attempt < max_attempts:
            retry_after = response["headers"].get("Retry-After")
            if retry_after and retry_after.isdigit():
                wait_seconds = float(retry_after)
            else:
                wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
            print(
                f"[amon-carter-fetch] transient status={response['status']}, "
                f"retrying after {wait_seconds:.1f}s"
            )
            await asyncio.sleep(wait_seconds)
            continue

        raise RuntimeError(f"Unexpected response status: {response['status']}")

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Amon Carter events page") from last_exception
    raise RuntimeError("Unable to fetch Amon Carter events page after retries")


async def load_amon_carter_payload(base_url: str = AMON_CARTER_EVENTS_URL) -> dict[str, object]:
    first_page_html = await fetch_amon_carter_page(base_url)
    max_page = _extract_last_page(first_page_html)
    page_urls = [_build_page_url(base_url, page) for page in range(max_page + 1)]

    list_pages: list[str] = [first_page_html]
    if len(page_urls) > 1:
        extra_pages = await asyncio.gather(
            *(fetch_amon_carter_page(url) for url in page_urls[1:])
        )
        list_pages.extend(extra_pages)

    event_urls: list[str] = []
    seen_urls: set[str] = set()
    for html in list_pages:
        for card in _parse_listing_cards(html, list_url=base_url):
            if card["source_url"] in seen_urls:
                continue
            seen_urls.add(card["source_url"])
            event_urls.append(card["source_url"])

    detail_pages_raw = await asyncio.gather(
        *(fetch_amon_carter_page(url) for url in event_urls)
    )
    detail_pages = dict(zip(event_urls, detail_pages_raw))
    return {
        "list_pages": list_pages,
        "detail_pages": detail_pages,
    }


class AmonCarterEventsAdapter(BaseSourceAdapter):
    source_name = "amon_carter_events"

    def __init__(self, url: str = AMON_CARTER_EVENTS_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        payload = await load_amon_carter_payload(self.url)
        return [json.dumps(payload, ensure_ascii=True)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        data = json.loads(payload)
        return parse_amon_carter_payload(data, list_url=self.url)


def parse_amon_carter_payload(
    payload: dict[str, object],
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    list_pages = payload.get("list_pages") or []
    detail_pages = payload.get("detail_pages") or {}
    if not isinstance(list_pages, list) or not isinstance(detail_pages, dict):
        return []

    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    reference_now = now or datetime.now()

    for html in list_pages:
        if not isinstance(html, str):
            continue
        for card in _parse_listing_cards(html, list_url=list_url):
            detail_html = detail_pages.get(card["source_url"])
            detail_metadata = _parse_detail_metadata(detail_html if isinstance(detail_html, str) else None)
            combined_text = " ".join(
                [
                    card["title"],
                    card["kicker"],
                    detail_metadata["description"] or "",
                    " ".join(detail_metadata["categories"]),
                ]
            ).lower()
            if not _is_family_or_youth_activity(combined_text, detail_metadata["categories"]):
                continue
            if any(keyword in combined_text for keyword in EXCLUDED_KEYWORDS):
                continue

            start_at, end_at = _parse_listing_datetime(card["kicker"], now=reference_now)
            if start_at is None:
                continue

            age_min, age_max = _extract_age_range(
                title=card["title"],
                description=detail_metadata["description"],
            )
            description_parts = []
            if detail_metadata["description"]:
                description_parts.append(detail_metadata["description"])
            if detail_metadata["categories"]:
                description_parts.append(
                    "Categories: " + ", ".join(detail_metadata["categories"])
                )
            description = " | ".join(description_parts) if description_parts else None

            item = ExtractedActivity(
                source_url=card["source_url"],
                title=card["title"],
                description=description,
                venue_name=AMON_CARTER_VENUE_NAME,
                location_text=AMON_CARTER_LOCATION,
                city=AMON_CARTER_CITY,
                state=AMON_CARTER_STATE,
                activity_type="workshop",
                age_min=age_min,
                age_max=age_max,
                drop_in=("drop in" in combined_text or "drop-in" in combined_text),
                registration_required=(
                    ("registration" in combined_text or "register" in combined_text)
                    and "not required" not in combined_text
                ),
                start_at=start_at,
                end_at=end_at,
                timezone=AMON_CARTER_TIMEZONE,
                free_verification_status=(
                    "confirmed" if "free" in combined_text else "inferred"
                ),
            )
            key = (item.source_url, item.title, item.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(item)

    return rows


def _build_page_url(base_url: str, page: int) -> str:
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query["page"] = [str(page)]
    parts = []
    for key in sorted(query):
        for value in query[key]:
            parts.append(f"{key}={value}")
    query_string = "&".join(parts)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{query_string}"


def _extract_last_page(html: str) -> int:
    page_numbers = [int(match.group(1)) for match in PAGER_RE.finditer(html)]
    return max(page_numbers) if page_numbers else 0


def _parse_listing_cards(html: str, *, list_url: str) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []
    for match in CARD_RE.finditer(html):
        title = _normalize_html_text(match.group("title"))
        kicker = _normalize_html_text(match.group("kicker"))
        href = urljoin(list_url, unescape(match.group("href")))
        if not title or not kicker:
            continue
        cards.append(
            {
                "source_url": href,
                "title": title,
                "kicker": kicker,
            }
        )
    return cards


def _parse_detail_metadata(html: str | None) -> dict[str, object]:
    if not html:
        return {"description": None, "categories": []}

    description_match = META_DESCRIPTION_RE.search(html)
    description = (
        _normalize_html_text(description_match.group("value"))
        if description_match
        else None
    )

    categories: list[str] = []
    data_layer_match = DATA_LAYER_RE.search(html)
    if data_layer_match:
        try:
            data = json.loads(data_layer_match.group(1))
        except json.JSONDecodeError:
            data = None
        if isinstance(data, dict):
            entity_taxonomy = data.get("entityTaxonomy") or {}
            if isinstance(entity_taxonomy, dict):
                event_types = entity_taxonomy.get("event_types") or {}
                if isinstance(event_types, dict):
                    categories = [
                        _normalize_html_text(value)
                        for value in event_types.values()
                        if _normalize_html_text(value)
                    ]

    return {"description": description, "categories": categories}


def _normalize_html_text(value: str | None) -> str:
    if not value:
        return ""
    stripped = STRIP_TAGS_RE.sub(" ", value)
    return WHITESPACE_RE.sub(" ", unescape(stripped)).strip()


def _is_family_or_youth_activity(text: str, categories: list[str]) -> bool:
    category_blob = " ".join(categories).lower()
    if "for families" in category_blob:
        return True
    return any(keyword in text for keyword in FAMILY_KEYWORDS)


def _parse_listing_datetime(
    kicker: str,
    *,
    now: datetime,
) -> tuple[datetime | None, datetime | None]:
    match = DATE_WITH_TIME_RE.search(kicker)
    if not match:
        return None, None

    month_name = match.group("month")
    day = int(match.group("day"))
    time_blob = match.group("time")
    year = now.year
    try:
        month = datetime.strptime(month_name, "%B").month
    except ValueError:
        month = datetime.strptime(month_name, "%b").month

    start_at, end_at = _parse_time_blob(time_blob, year=year, month=month, day=day)
    if start_at is not None and start_at.date() < now.date():
        retry = _parse_time_blob(time_blob, year=year + 1, month=month, day=day)
        if retry[0] is not None:
            start_at, end_at = retry
    return start_at, end_at


def _parse_time_blob(
    time_blob: str,
    *,
    year: int,
    month: int,
    day: int,
) -> tuple[datetime | None, datetime | None]:
    range_match = TIME_RANGE_RE.search(time_blob)
    if range_match:
        start_meridiem = range_match.group("start_meridiem") or range_match.group("end_meridiem")
        start_at = _combine_date_and_time(
            year=year,
            month=month,
            day=day,
            time_text=range_match.group("start"),
            meridiem=start_meridiem,
        )
        end_at = _combine_date_and_time(
            year=year,
            month=month,
            day=day,
            time_text=range_match.group("end"),
            meridiem=range_match.group("end_meridiem"),
        )
        return start_at, end_at

    single_match = TIME_SINGLE_RE.search(time_blob)
    if single_match:
        start_at = _combine_date_and_time(
            year=year,
            month=month,
            day=day,
            time_text=single_match.group("start"),
            meridiem=single_match.group("meridiem"),
        )
        return start_at, None
    return None, None


def _combine_date_and_time(
    *,
    year: int,
    month: int,
    day: int,
    time_text: str,
    meridiem: str,
) -> datetime:
    cleaned = f"{time_text.strip()} {meridiem.strip().replace('.', '').upper()}"
    fmt = "%I:%M %p" if ":" in cleaned else "%I %p"
    parsed_time = datetime.strptime(cleaned, fmt)
    return datetime(
        year,
        month,
        day,
        parsed_time.hour,
        parsed_time.minute,
    )


def _extract_age_range(
    *,
    title: str,
    description: str | None,
) -> tuple[int | None, int | None]:
    blob = " ".join([title, description or ""])

    match = AGE_RANGE_RE.search(blob)
    if match:
        return int(match.group(1)), int(match.group(2))

    match = AGE_PLUS_RE.search(blob)
    if match:
        return int(match.group(1)), None

    match = MONTHS_YOUNGER_RE.search(blob)
    if match:
        months = int(match.group(1))
        years = max(0, months // 12)
        return 0, years

    match = YEARS_OLDER_RE.search(blob)
    if match:
        return int(match.group(1)), None

    return None, None


def _fetch_sync(url: str) -> dict[str, object]:
    request = Request(url, headers=DEFAULT_HEADERS)
    with urlopen(request, timeout=30) as response:
        return {
            "status": getattr(response, "status", response.getcode()),
            "headers": dict(response.headers.items()),
            "text": response.read().decode("utf-8", errors="replace"),
        }
