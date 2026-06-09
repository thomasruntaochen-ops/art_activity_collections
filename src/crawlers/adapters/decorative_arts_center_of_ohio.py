import json
import re
from datetime import date
from datetime import datetime

from bs4 import BeautifulSoup

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.adapters.oh_common import NY_TIMEZONE
from src.crawlers.adapters.oh_common import infer_activity_type
from src.crawlers.adapters.oh_common import join_non_empty
from src.crawlers.adapters.oh_common import normalize_space
from src.crawlers.adapters.oh_common import parse_age_range
from src.crawlers.adapters.oh_common import parse_time_range
from src.crawlers.adapters.oh_common import should_include_event
from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


DECORATIVE_EVENTS_URL = "https://www.decartsohio.org/classes-youth"
DECORATIVE_HIGH_SCHOOL_URL = "https://www.decartsohio.org/classes-highschool"
DECORATIVE_SUMMER_CAMP_URL = "https://www.decartsohio.org/summer-camp"
DECORATIVE_LECTURES_URL = "https://www.decartsohio.org/lectures"
DECORATIVE_SOURCE_URLS = (
    DECORATIVE_EVENTS_URL,
    DECORATIVE_HIGH_SCHOOL_URL,
    DECORATIVE_SUMMER_CAMP_URL,
    DECORATIVE_LECTURES_URL,
)
DECORATIVE_VENUE_NAME = "Decorative Arts Center of Ohio"
DECORATIVE_CITY = "Lancaster"
DECORATIVE_STATE = "OH"
DECORATIVE_LOCATION = "Decorative Arts Center of Ohio, Lancaster, OH"
DATE_TIME_RE = re.compile(
    r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+"
    r"(?P<time>\d{1,2}(?::\d{2})?(?:\s*[ap]\.?m\.?)?(?:\s*(?:-|–|—)\s*\d{1,2}(?::\d{2})?\s*[ap]\.?m\.?)?)",
    re.IGNORECASE,
)
MONTH_DAY_RE = re.compile(
    r"(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(?P<days>\d{1,2}(?:\s*(?:-|–|—|and|,)\s*\d{1,2})*)",
    re.IGNORECASE,
)
TIME_IN_LINE_RE = re.compile(
    r"(?P<time>"
    r"\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?\s*(?:-|–|—)"
    r"\s*(?:\d{1,2}(?::\d{2})?|noon)\s*(?:a\.?m\.?|p\.?m\.?|am|pm)?"
    r"|\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm)"
    r"|noon)",
    re.IGNORECASE,
)


async def load_decorative_arts_center_of_ohio_payload() -> dict[str, str]:
    from src.crawlers.adapters.oh_common import fetch_html

    return {url: await fetch_html(url) for url in DECORATIVE_SOURCE_URLS}


def parse_decorative_arts_center_of_ohio_payload(payload: dict[str, str]) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, datetime]] = set()
    today = datetime.now().date()

    for page_url, html in payload.items():
        category_hint = "lecture" if page_url == DECORATIVE_LECTURES_URL else "workshop"
        for row in _parse_page(page_url=page_url, html=html, category_hint=category_hint, today=today):
            key = (row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.start_at, row.title))
    return rows


class DecorativeArtsCenterOfOhioAdapter(BaseSourceAdapter):
    source_name = "decorative_arts_center_of_ohio_events"

    async def fetch(self) -> list[str]:
        payload = await load_decorative_arts_center_of_ohio_payload()
        return [json.dumps(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_decorative_arts_center_of_ohio_payload(json.loads(payload))


def _parse_page(
    *,
    page_url: str,
    html: str,
    category_hint: str,
    today: date,
) -> list[ExtractedActivity]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ExtractedActivity] = []
    rows.extend(_parse_current_page_text(page_url=page_url, soup=soup, category_hint=category_hint, today=today))

    for paragraph in soup.select("div.paragraph"):
        first_strong = paragraph.select_one("strong")
        if first_strong is None:
            continue

        title = normalize_space(first_strong.get_text(" ", strip=True))
        if not title or len(title) < 4:
            continue

        text_lines = [normalize_space(line) for line in paragraph.get_text("\n", strip=True).splitlines() if normalize_space(line)]
        full_text = "\n".join(text_lines)
        match = DATE_TIME_RE.search(full_text)
        if match is None:
            continue

        base_date = _infer_event_date(month_text=match.group("month"), day_text=match.group("day"), today=today)
        start_at, end_at = parse_time_range(base_date=base_date, time_text=match.group("time"))
        if start_at is None or start_at.date() < today:
            continue

        age_line = next((line for line in text_lines if "age" in line.lower()), None)
        age_min, age_max = parse_age_range(age_line)
        price_line = next((line for line in text_lines if "$" in line), None)
        description_lines = [
            line
            for line in text_lines
            if line
            and line != title
            and DATE_TIME_RE.search(line) is None
            and line != normalize_space(age_line)
            and line != normalize_space(price_line)
            and "instructor" not in line.lower()
        ]
        widget_text = _extract_register_text(paragraph)
        description = join_non_empty(description_lines + ([price_line, widget_text] if widget_text else [price_line]))
        category = category_hint
        if not should_include_event(title=title, description=description, category=category):
            continue

        rows.append(
            ExtractedActivity(
                source_url=f"{page_url}#{_slugify(title)}",
                title=title,
                description=description,
                venue_name=DECORATIVE_VENUE_NAME,
                location_text=DECORATIVE_LOCATION,
                city=DECORATIVE_CITY,
                state=DECORATIVE_STATE,
                activity_type=infer_activity_type(title, description, category),
                age_min=age_min,
                age_max=age_max,
                drop_in=False,
                registration_required=bool(widget_text),
                start_at=start_at,
                end_at=end_at,
                timezone=NY_TIMEZONE,
                audience_segment=infer_audience_segment(
                    title=title,
                    description=description,
                    category=category,
                    source_url=page_url,
                    age_min=age_min,
                    age_max=age_max,
                ),
                **price_classification_kwargs(join_non_empty([price_line, description])),
            )
        )

    return rows


def _parse_current_page_text(
    *,
    page_url: str,
    soup: BeautifulSoup,
    category_hint: str,
    today: date,
) -> list[ExtractedActivity]:
    lines = _content_lines(soup)
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, datetime]] = set()

    for index, line in enumerate(lines):
        if not _line_has_date_and_time(line):
            continue

        title = _find_title(lines, index)
        if not title:
            continue

        description_lines = _description_lines(lines, index)
        description = join_non_empty(description_lines)
        category = "lecture" if page_url == DECORATIVE_LECTURES_URL else category_hint
        if not should_include_event(title=title, description=description, category=category):
            continue

        age_min, age_max = parse_age_range(join_non_empty([title, description]))
        price_line = next((item for item in description_lines if "$" in item), None)
        time_text = _extract_time_text(line)
        for base_date in _extract_dates(line, today=today):
            if base_date < today:
                continue
            start_at, end_at = parse_time_range(base_date=base_date, time_text=time_text)
            if start_at is None:
                continue
            key = (title, start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                ExtractedActivity(
                    source_url=f"{page_url}#{_slugify(title)}",
                    title=title,
                    description=description,
                    venue_name=DECORATIVE_VENUE_NAME,
                    location_text=DECORATIVE_LOCATION,
                    city=DECORATIVE_CITY,
                    state=DECORATIVE_STATE,
                    activity_type=infer_activity_type(title, description, category),
                    age_min=age_min,
                    age_max=age_max,
                    drop_in=False,
                    registration_required=_has_registration(description_lines),
                    start_at=start_at,
                    end_at=end_at,
                    timezone=NY_TIMEZONE,
                    audience_segment=infer_audience_segment(
                        title=title,
                        description=description,
                        category=category,
                        source_url=page_url,
                        age_min=age_min,
                        age_max=age_max,
                    ),
                    **price_classification_kwargs(join_non_empty([price_line, description])),
                )
            )

    return rows


def _content_lines(soup: BeautifulSoup) -> list[str]:
    lines = [
        normalize_space(line)
        for line in soup.get_text("\n", strip=True).splitlines()
        if normalize_space(line)
    ]
    for marker in (
        "LECTURES",
        "YOUTH CLASSES",
        "YOUTH ﻿C﻿LASSES",
        "High School & Adult ﻿Classes",
        "Summer Camp at DACO",
    ):
        if marker in lines:
            lines = lines[lines.index(marker) + 1 :]
            break
    if "STAY UPDATED" in lines:
        lines = lines[: lines.index("STAY UPDATED")]
    return lines


def _line_has_date_and_time(line: str) -> bool:
    return bool(MONTH_DAY_RE.search(line) and TIME_IN_LINE_RE.search(line))


def _find_title(lines: list[str], date_line_index: int) -> str | None:
    generic = {
        "Six week sessions:",
        "Register",
        "LECTURES",
        "YOUTH CLASSES",
        "YOUTH ﻿C﻿LASSES",
        "High School & Adult ﻿Classes",
        "Summer Camp at DACO",
    }
    immediate = ""
    immediate_index = None
    for index in range(date_line_index - 1, max(-1, date_line_index - 5), -1):
        candidate = normalize_space(lines[index])
        if not candidate or candidate in generic:
            continue
        if _is_metadata_line(candidate) or candidate.lower().startswith("register for "):
            continue
        immediate = candidate
        immediate_index = index
        break
    if not immediate or len(immediate) > 160:
        return None

    parts = [immediate]
    previous = normalize_space(lines[immediate_index - 1]) if immediate_index and immediate_index > 0 else ""
    if previous and len(previous) <= 120 and ("talk" in previous.lower() or previous.rstrip().endswith(":")):
        parts.insert(0, previous)
    title = join_non_empty(parts)
    return title.replace(" | ", " ") if title else None


def _description_lines(lines: list[str], date_line_index: int) -> list[str]:
    description: list[str] = [lines[date_line_index]]
    tail = lines[date_line_index + 1 : date_line_index + 10]
    for offset, line in enumerate(tail):
        lowered = line.lower()
        if lowered.startswith("register for ") or line == "Register":
            description.append(line)
            break
        if _line_has_date_and_time(line):
            break
        if offset + 1 < len(tail) and _line_has_date_and_time(tail[offset + 1]) and not _is_metadata_line(line):
            break
        description.append(line)
    return description


def _extract_time_text(line: str) -> str | None:
    matches = list(TIME_IN_LINE_RE.finditer(line))
    if not matches:
        return None
    return normalize_space(matches[-1].group("time")).replace("Noon", "12:00 pm").replace("noon", "12:00 pm")


def _extract_dates(line: str, *, today: date) -> list[date]:
    dates: list[date] = []
    time_text = _extract_time_text(line)
    date_line = line
    if time_text:
        date_line = line.rsplit(time_text, 1)[0]
    for match in MONTH_DAY_RE.finditer(date_line):
        month = datetime.strptime(match.group("month").title(), "%B").month
        day_values = [int(value) for value in re.findall(r"\d{1,2}", match.group("days"))]
        if not day_values:
            continue
        if re.search(r"\d{1,2}\s*(?:-|–|—)\s*\d{1,2}", match.group("days")):
            day_values = day_values[:1]
        for day_value in day_values:
            try:
                candidate = date(today.year, month, day_value)
            except ValueError:
                continue
            if (today - candidate).days > 180:
                candidate = date(today.year + 1, month, day_value)
            dates.append(candidate)
    return dates


def _is_metadata_line(line: str) -> bool:
    lowered = line.lower()
    return (
        "$" in line
        or "instructor" in lowered
        or lowered.startswith("ages ")
        or lowered.startswith("age ")
        or lowered.startswith("session ")
        or lowered.startswith("d:")
        or lowered.startswith("e:")
    )


def _has_registration(description_lines: list[str]) -> bool:
    return any("register" in line.lower() for line in description_lines)


def _extract_register_text(paragraph) -> str | None:
    sibling = paragraph.find_next_sibling()
    while sibling is not None:
        classes = sibling.get("class", []) if hasattr(sibling, "get") else []
        if "paragraph" in classes:
            break
        text = normalize_space(sibling.get_text(" ", strip=True)) if hasattr(sibling, "get_text") else ""
        if "register" in text.lower():
            return text
        sibling = sibling.find_next_sibling()
    return None


def _infer_event_date(*, month_text: str, day_text: str, today: date) -> date:
    month = datetime.strptime(normalize_space(month_text), "%B").month
    day = int(day_text)
    candidate = date(today.year, month, day)
    if (today - candidate).days > 180:
        return date(today.year + 1, month, day)
    return candidate


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", normalize_space(value).lower()).strip("-")
