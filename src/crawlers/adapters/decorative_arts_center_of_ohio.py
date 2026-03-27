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
from src.crawlers.pipeline.pricing import price_classification_kwargs
from src.crawlers.pipeline.types import ExtractedActivity


DECORATIVE_EVENTS_URL = "https://www.decartsohio.org/events.html"
DECORATIVE_LECTURES_URL = "https://www.decartsohio.org/lectures.html"
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


async def load_decorative_arts_center_of_ohio_payload() -> dict[str, str]:
    from src.crawlers.adapters.oh_common import fetch_html

    return {
        DECORATIVE_EVENTS_URL: await fetch_html(DECORATIVE_EVENTS_URL),
        DECORATIVE_LECTURES_URL: await fetch_html(DECORATIVE_LECTURES_URL),
    }


def parse_decorative_arts_center_of_ohio_payload(payload: dict[str, str]) -> list[ExtractedActivity]:
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()
    today = datetime.now().date()

    for page_url, html in payload.items():
        category_hint = "lecture" if page_url == DECORATIVE_LECTURES_URL else "workshop"
        for row in _parse_page(page_url=page_url, html=html, category_hint=category_hint, today=today):
            key = (row.source_url, row.title, row.start_at)
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
                **price_classification_kwargs(join_non_empty([price_line, description])),
            )
        )

    return rows


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
