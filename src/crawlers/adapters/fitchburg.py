import asyncio
import re
import unicodedata
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from bs4 import NavigableString
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity

FITCHBURG_PROGRAMS_URL = "https://fitchburgartmuseum.org/upcoming-programs/"
FITCHBURG_PROGRAMS_API_URL = "https://fitchburgartmuseum.org/wp-json/wp/v2/pages"
FITCHBURG_TIMEZONE = "America/New_York"
FITCHBURG_VENUE_NAME = "Fitchburg Art Museum"
FITCHBURG_CITY = "Fitchburg"
FITCHBURG_STATE = "MA"
FITCHBURG_DEFAULT_LOCATION = "Fitchburg Art Museum, Fitchburg, MA"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": FITCHBURG_PROGRAMS_URL,
}

MONTH_NAMES = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
WEEKDAY_NAMES = (
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
)
MONTH_DAY_RE = re.compile(r"(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)\s+(?P<day>\d{1,2})")
TIME_RANGE_RE = re.compile(
    r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{2}))?\s*(?P<start_meridiem>AM|PM)?\s*[–-]\s*"
    r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{2}))?\s*(?P<end_meridiem>AM|PM)",
    re.IGNORECASE,
)
TIME_SINGLE_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<meridiem>AM|PM)",
    re.IGNORECASE,
)
AGE_PLUS_RE = re.compile(r"\bages?\s*(?P<age>\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)

INCLUDE_MARKERS = (
    " artist talk ",
    " curator talk ",
    " talk ",
    " workshop ",
    " make art ",
    " hidden treasures ",
)
EXCLUDE_MARKERS = (
    " farmers market ",
    " market ",
    " poetry ",
    " poet ",
    " words from the burg ",
    " soirée ",
    " soiree ",
    " cocktail ",
    " cocktails ",
    " live entertainment ",
    " dj set ",
    " after party ",
    " mingle ",
    " members only ",
    " member ",
    " art in bloom ",
    " floral interpretation ",
    " tour ",
    " tours ",
)


async def fetch_fitchburg_programs_page(
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
                response = await client.get(FITCHBURG_PROGRAMS_API_URL, params={"slug": "upcoming-programs"})
            except httpx.HTTPError as exc:
                last_exception = exc
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            if response.status_code < 400:
                payload = response.json()
                if isinstance(payload, list) and payload:
                    return payload[0]
                raise RuntimeError("Fitchburg page JSON did not include the upcoming-programs page payload.")

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError("Unable to fetch Fitchburg Art Museum programs payload") from last_exception
    raise RuntimeError("Unable to fetch Fitchburg Art Museum programs payload after retries")


async def load_fitchburg_payload() -> dict:
    return await fetch_fitchburg_programs_page()


def parse_fitchburg_payload(payload: dict) -> list[ExtractedActivity]:
    html = ((payload.get("content") or {}).get("rendered") or "").strip()
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    current_date = datetime.now(ZoneInfo(FITCHBURG_TIMEZONE)).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, datetime]] = set()

    for month_label, event_block in _iter_upcoming_event_blocks(soup):
        default_year = _extract_year(month_label)
        row_candidates = _parse_event_block(event_block, default_year=default_year)
        for row in row_candidates:
            if row.start_at.date() < current_date:
                continue
            key = (row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class FitchburgProgramsAdapter(BaseSourceAdapter):
    source_name = "fitchburg_programs"

    async def fetch(self) -> list[str]:
        payload = await fetch_fitchburg_programs_page()
        html = ((payload.get("content") or {}).get("rendered") or "").strip()
        return [html]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_fitchburg_payload({"content": {"rendered": payload}})


def _iter_upcoming_event_blocks(soup: BeautifulSoup):
    for accordion in soup.select("div.accordion"):
        head = accordion.select_one("h3.accordion-head")
        if head is None:
            continue
        month_label = _normalize_space(head.get_text(" ", strip=True))
        if not month_label or "2026" not in month_label:
            continue
        body = head.find_next_sibling("div")
        if body is None:
            continue
        for block in _split_event_blocks(body):
            yield month_label, block


def _split_event_blocks(body) -> list[list]:
    blocks: list[list] = []
    current: list = []

    for child in body.children:
        if isinstance(child, NavigableString):
            continue

        text = _normalize_space(child.get_text(" ", strip=True))
        if child.name == "hr":
            if current:
                blocks.append(current)
                current = []
            continue
        if not text:
            continue

        if child.name == "h5" and _is_event_header(child):
            if current:
                blocks.append(current)
            current = [child]
            continue

        if current:
            current.append(child)

    if current:
        blocks.append(current)

    return blocks


def _is_event_header(node) -> bool:
    lines = _extract_lines(node)
    if not lines:
        return False
    if all(line.startswith(">") for line in lines):
        return False
    return any(month in " ".join(lines) for month in MONTH_NAMES)


def _parse_event_block(block: list, *, default_year: int) -> list[ExtractedActivity]:
    header = next((node for node in block if getattr(node, "name", None) == "h5"), None)
    if header is None:
        return []

    title = _extract_title(header)
    date_lines = _extract_date_lines(header)
    description_parts, cta_texts = _extract_block_text(block[1:])
    description = " | ".join(description_parts) if description_parts else None
    keyword_blob = _keyword_blob(" ".join([title, description or "", " ".join(cta_texts)]))

    if not _should_include_event(keyword_blob):
        return []

    occurrences = _parse_occurrences(date_lines, default_year=default_year)
    rows: list[ExtractedActivity] = []
    age_min = _parse_age_min(description or "")

    for start_at, end_at in occurrences:
        source_url = f"{FITCHBURG_PROGRAMS_URL}#{_slugify(title)}-{start_at.strftime('%Y%m%d')}"
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=FITCHBURG_VENUE_NAME,
                location_text=_infer_location_text(description or ""),
                city=FITCHBURG_CITY,
                state=FITCHBURG_STATE,
                activity_type=_infer_activity_type(keyword_blob),
                age_min=age_min,
                age_max=None,
                drop_in=(" drop in " in keyword_blob),
                registration_required=_infer_registration_required(
                    description=description or "",
                    cta_texts=cta_texts,
                ),
                start_at=start_at,
                end_at=end_at,
                timezone=FITCHBURG_TIMEZONE,
                **price_classification_kwargs(description, default_is_free=None),
            )
        )

    return rows


def _extract_title(header) -> str:
    red_bits = [
        _normalize_space(node.get_text(" ", strip=True))
        for node in header.select('[style*="color: #d0021b"], [style*="color:#d0021b"]')
    ]
    red_bits = [bit for bit in red_bits if bit]
    if red_bits:
        return _normalize_space(" ".join(red_bits))

    lines = _extract_lines(header)
    non_date_lines = [line for line in lines if not _looks_like_date_line(line)]
    if non_date_lines:
        return _normalize_space(" ".join(non_date_lines))
    return _normalize_space(header.get_text(" ", strip=True))


def _extract_date_lines(header) -> list[str]:
    extracted: list[str] = []
    pending_reoccurs = False

    for line in _extract_lines(header):
        if not _looks_like_date_line(line):
            continue
        if line.lower() == "reoccurs on":
            pending_reoccurs = True
            continue
        if pending_reoccurs:
            extracted.append(f"Reoccurs on {line}")
            pending_reoccurs = False
            continue
        extracted.append(line)

    return extracted


def _extract_lines(node) -> list[str]:
    return [
        _normalize_space(line)
        for line in node.get_text("\n", strip=True).splitlines()
        if _normalize_space(line)
    ]


def _extract_block_text(nodes: list) -> tuple[list[str], list[str]]:
    description_parts: list[str] = []
    cta_texts: list[str] = []

    for node in nodes:
        text = _normalize_space(node.get_text(" ", strip=True))
        if not text:
            continue
        if text.startswith(">"):
            cta_texts.append(text)
            continue
        if node.name == "h5":
            cta_texts.append(text)
            continue
        description_parts.append(text)

    return description_parts, cta_texts


def _should_include_event(keyword_blob: str) -> bool:
    if any(marker in keyword_blob for marker in EXCLUDE_MARKERS):
        return False
    return any(marker in keyword_blob for marker in INCLUDE_MARKERS)


def _parse_occurrences(date_lines: list[str], *, default_year: int) -> list[tuple[datetime, datetime | None]]:
    occurrences: list[tuple[datetime, datetime | None]] = []

    for line in date_lines:
        normalized = _normalize_space(line).replace("Now on ", "").replace("Postponed to ", "")
        lower = normalized.lower()

        if lower.startswith("reoccurs on "):
            time_text = None
            line_body = normalized[len("Reoccurs on ") :]
            if " from " in line_body.lower():
                split_index = line_body.lower().rfind(" from ")
                time_text = line_body[split_index + len(" from ") :]
                line_body = line_body[:split_index]
            for month_name, day_text in MONTH_DAY_RE.findall(line_body):
                start_at, end_at = _build_datetime_window(
                    month_name=month_name,
                    day=int(day_text),
                    year=default_year,
                    time_text=time_text,
                )
                occurrences.append((start_at, end_at))
            continue

        date_matches = list(MONTH_DAY_RE.finditer(normalized))
        if not date_matches:
            continue

        if len(date_matches) >= 2 and "–" in normalized and "reoccurs on" not in lower:
            start_match = date_matches[0]
            end_match = date_matches[1]
            start_at = _day_start(
                month_name=start_match.group("month"),
                day=int(start_match.group("day")),
                year=default_year,
            )
            end_at = _day_start(
                month_name=end_match.group("month"),
                day=int(end_match.group("day")),
                year=default_year,
            )
            occurrences.append((start_at, end_at))
            continue

        match = date_matches[0]
        time_text = normalized[match.end() :].strip(" ,")
        start_at, end_at = _build_datetime_window(
            month_name=match.group("month"),
            day=int(match.group("day")),
            year=default_year,
            time_text=time_text,
        )
        occurrences.append((start_at, end_at))

    return occurrences


def _build_datetime_window(
    *,
    month_name: str,
    day: int,
    year: int,
    time_text: str | None,
) -> tuple[datetime, datetime | None]:
    base_day = _day_start(month_name=month_name, day=day, year=year)
    if not time_text:
        return base_day, None

    cleaned = _normalize_space(time_text)
    cleaned = cleaned.replace("Drop in between ", "").replace("drop in between ", "")
    cleaned = cleaned.replace("from ", "")

    time_range_match = TIME_RANGE_RE.search(cleaned)
    if time_range_match:
        start_meridiem = (time_range_match.group("start_meridiem") or time_range_match.group("end_meridiem") or "").upper()
        end_meridiem = (time_range_match.group("end_meridiem") or start_meridiem).upper()
        start_at = _apply_time(
            base_day,
            hour=int(time_range_match.group("start_hour")),
            minute=int(time_range_match.group("start_minute") or "0"),
            meridiem=start_meridiem,
        )
        end_at = _apply_time(
            base_day,
            hour=int(time_range_match.group("end_hour")),
            minute=int(time_range_match.group("end_minute") or "0"),
            meridiem=end_meridiem,
        )
        return start_at, end_at

    time_match = TIME_SINGLE_RE.search(cleaned)
    if time_match:
        start_at = _apply_time(
            base_day,
            hour=int(time_match.group("hour")),
            minute=int(time_match.group("minute") or "0"),
            meridiem=time_match.group("meridiem").upper(),
        )
        return start_at, None

    return base_day, None


def _apply_time(base_day: datetime, *, hour: int, minute: int, meridiem: str) -> datetime:
    if meridiem == "PM" and hour != 12:
        hour += 12
    if meridiem == "AM" and hour == 12:
        hour = 0
    return base_day.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _day_start(*, month_name: str, day: int, year: int) -> datetime:
    return datetime.strptime(f"{month_name} {day} {year}", "%B %d %Y").replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )


def _infer_activity_type(keyword_blob: str) -> str:
    if any(marker in keyword_blob for marker in (" talk ", " curator talk ", " artist talk ", " lecture ")):
        return "lecture"
    return "workshop"


def _infer_registration_required(*, description: str, cta_texts: list[str]) -> bool | None:
    token_blob = _keyword_blob(" ".join([description, " ".join(cta_texts)]))
    if " no sign up is required " in token_blob:
        return False
    if " not required " in token_blob and " sign up " in token_blob:
        return False
    if any(marker in token_blob for marker in (" advanced registration is required ", " event at capacity ", " join waitlist ")):
        return True
    if any(marker in token_blob for marker in (" program sign up ", " get tickets ", " waitlist ")):
        return True
    return None


def _infer_location_text(description: str) -> str:
    lowered = description.lower()
    if "art place on elm studio" in lowered:
        return "Art Place on Elm Studio, Fitchburg Art Museum, Fitchburg, MA"
    if "george r. wallace iii gallery" in lowered:
        return "George R. Wallace III Gallery, Fitchburg Art Museum, Fitchburg, MA"
    return FITCHBURG_DEFAULT_LOCATION


def _parse_age_min(description: str) -> int | None:
    match = AGE_PLUS_RE.search(description)
    if match is None:
        return None
    return int(match.group("age"))


def _extract_year(month_label: str) -> int:
    match = re.search(r"(20\d{2})", month_label)
    if match is None:
        return datetime.now(ZoneInfo(FITCHBURG_TIMEZONE)).year
    return int(match.group(1))


def _looks_like_date_line(line: str) -> bool:
    lowered = line.lower()
    if lowered in {"reoccurs on", "postponed to", "now on"}:
        return True
    if lowered.startswith(("reoccurs on ", "postponed to ", "now on ")):
        return True
    if any(month.lower() in lowered for month in MONTH_NAMES):
        return True
    return any(lowered.startswith(day.lower()) for day in WEEKDAY_NAMES)


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "event"


def _keyword_blob(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return f" {re.sub(r'[^a-z0-9]+', ' ', normalized.lower()).strip()} "


def _normalize_space(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())
