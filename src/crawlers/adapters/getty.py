import asyncio
import json
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.types import ExtractedActivity

GETTY_CALENDAR_URL = (
    "https://www.getty.edu/calendar/?category=talks&category=family&category=art-making&category=special-events"
)

LA_TIMEZONE = "America/Los_Angeles"
GETTY_VENUE_NAME = "J. Paul Getty Museum"
GETTY_CITY = "Los Angeles"
GETTY_STATE = "CA"
GETTY_DEFAULT_LOCATION = "Los Angeles, CA"

PAYLOAD_PATH_RE = re.compile(r'href="(?P<path>/_nuxt/static/[^"]+/calendar/payload\.js)"')
EXCLUDED_PRIMARY_KEYWORDS = (
    "ticket",
    "tickets",
    "tour",
    "registration",
    "camp",
    "free night",
    "fundraising",
    "admission",
    "exhibition",
    "film",
    "tv",
    "reading",
    "writing",
    "open house",
    "performance",
    "music",
    "concert",
)
INCLUDED_KEYWORDS = (
    "talk",
    "lecture",
    "conversation",
    "panel",
    "class",
    "activity",
    "workshop",
    "lab",
    "family",
    "art making",
    "art-making",
    "drawing",
    "demonstration",
)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.getty.edu/calendar/",
}


async def fetch_getty_calendar_page(
    url: str,
    *,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    print(f"[getty-fetch] start calendar url={url} max_attempts={max_attempts}")
    return await _fetch_text(
        url=url,
        label="getty-calendar",
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )


async def fetch_getty_calendar_payload(
    html: str,
    *,
    list_url: str = GETTY_CALENDAR_URL,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> tuple[str, str]:
    match = PAYLOAD_PATH_RE.search(html)
    if match is None:
        raise RuntimeError("Unable to locate Getty calendar payload URL")

    payload_url = urljoin(list_url, match.group("path"))
    print(f"[getty-fetch] resolved payload url={payload_url}")
    payload_js = await _fetch_text(
        url=payload_url,
        label="getty-payload",
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )
    return payload_js, payload_url


async def _fetch_text(
    *,
    url: str,
    label: str,
    max_attempts: int,
    base_backoff_seconds: float,
) -> str:
    last_exception: Exception | None = None
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"[{label}] attempt {attempt}/{max_attempts}: sending request")
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                print(f"[{label}] attempt {attempt}/{max_attempts}: transport error={exc}")
                if attempt < max_attempts:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                    print(f"[{label}] transient transport error, retrying after {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    continue
                break

            print(f"[{label}] attempt {attempt}/{max_attempts}: status={response.status_code}")
            if response.status_code < 400:
                print(f"[{label}] success on attempt {attempt}, bytes={len(response.text)}")
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = base_backoff_seconds * (2 ** (attempt - 1))
                print(f"[{label}] transient status={response.status_code}, retrying after {wait_seconds:.1f}s")
                await asyncio.sleep(wait_seconds)
                continue

            response.raise_for_status()

    if last_exception is not None:
        raise RuntimeError(f"Unable to fetch {label}") from last_exception
    raise RuntimeError(f"Unable to fetch {label} after retries")


class GettyCalendarAdapter(BaseSourceAdapter):
    source_name = "getty_calendar"

    def __init__(self, url: str = GETTY_CALENDAR_URL):
        self.url = url

    async def fetch(self) -> list[str]:
        html = await fetch_getty_calendar_page(self.url)
        payload_js, _ = await fetch_getty_calendar_payload(html, list_url=self.url)
        return [payload_js]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        return parse_getty_payload_js(payload, list_url=self.url)


def parse_getty_payload_js(
    payload_js: str,
    *,
    list_url: str,
    now: datetime | None = None,
) -> list[ExtractedActivity]:
    identifiers = _parse_identifier_table(payload_js)
    items_payload = _extract_items_payload(payload_js)
    items = _parse_js_value(items_payload, identifiers)
    if not isinstance(items, list):
        return []

    current_date = (now or datetime.now()).date()
    rows: list[ExtractedActivity] = []
    seen: set[tuple[str, str, datetime]] = set()

    for item in items:
        if not isinstance(item, dict):
            continue
        for row in _rows_from_item(item=item, list_url=list_url, current_date=current_date):
            key = (row.source_url, row.title, row.start_at)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    return rows


def _rows_from_item(*, item: dict, list_url: str, current_date) -> list[ExtractedActivity]:
    title = _normalize_text(item.get("title"))
    if not title:
        return []

    subtitle = _normalize_text(item.get("subtitle"))
    categories = _normalize_string_list(item.get("category"))
    audience = _normalize_string_list(item.get("audience"))
    location_labels = _normalize_string_list(item.get("locationLabels"))
    date_values = _normalize_string_list(item.get("date"))
    if not date_values:
        return []

    combined_blob = " ".join([title, subtitle or "", " ".join(categories), " ".join(audience)]).lower()
    if any(keyword in combined_blob for keyword in EXCLUDED_PRIMARY_KEYWORDS):
        return []

    if not any(keyword in combined_blob for keyword in INCLUDED_KEYWORDS):
        return []

    source_url = urljoin(list_url, _normalize_text(item.get("href")) or "")
    if not source_url:
        return []

    description = _join_non_empty(
        [
            f"Subtitle: {subtitle}" if subtitle else None,
            f"Category: {', '.join(categories)}" if categories else None,
            f"Audience: {', '.join(audience)}" if audience else None,
            f"Location: {', '.join(location_labels)}" if location_labels else None,
            f"Schedule: {_normalize_text(item.get('metaPrimary'))}" if item.get("metaPrimary") else None,
            f"Cost: {_normalize_text(item.get('cost'))}" if item.get("cost") else None,
        ]
    )
    text_blob = " ".join([combined_blob, description or ""]).lower()
    activity_type = _infer_activity_type(title=title, subtitle=subtitle, categories=categories, audience=audience)

    rows: list[ExtractedActivity] = []
    for date_value in date_values:
        start_at = _parse_datetime(date_value)
        if start_at is None or start_at.date() < current_date:
            continue
        rows.append(
            ExtractedActivity(
                source_url=source_url,
                title=title,
                description=description,
                venue_name=GETTY_VENUE_NAME,
                location_text=_location_text_from_labels(location_labels),
                city=GETTY_CITY,
                state=GETTY_STATE,
                activity_type=activity_type,
                age_min=None,
                age_max=None,
                drop_in=("drop-in" in text_blob or "drop in" in text_blob),
                registration_required=("registration" in text_blob and "not required" not in text_blob),
                start_at=start_at,
                end_at=None,
                timezone=LA_TIMEZONE,
                free_verification_status=("confirmed" if "free" in text_blob else "inferred"),
            )
        )

    return rows


def _location_text_from_labels(labels: list[str]) -> str:
    if not labels:
        return GETTY_DEFAULT_LOCATION
    return ", ".join(labels)


def _infer_activity_type(*, title: str, subtitle: str | None, categories: list[str], audience: list[str]) -> str:
    blob = " ".join([title, subtitle or "", *categories, *audience]).lower()
    if any(keyword in blob for keyword in ("talk", "lecture", "conversation", "panel")):
        return "talk"
    if any(keyword in blob for keyword in ("workshop", "family", "art making", "art-making", "drawing")):
        return "workshop"
    return "activity"


def _parse_datetime(value: str) -> datetime | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    normalized = normalized.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


def _extract_items_payload(payload_js: str) -> str:
    marker = "items:["
    start = payload_js.find(marker)
    if start < 0:
        raise RuntimeError("Unable to locate Getty payload items array")
    start += len("items:")
    return _extract_balanced(payload_js, start)


def _extract_balanced(value: str, start: int) -> str:
    opener = value[start]
    closer = "]" if opener == "[" else "}"
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(value)):
        char = value[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue
        if char == opener:
            depth += 1
            continue
        if char == closer:
            depth -= 1
            if depth == 0:
                return value[start : index + 1]
    raise RuntimeError("Unable to extract balanced JS payload")


def _parse_identifier_table(payload_js: str) -> dict[str, object]:
    function_start = payload_js.find("(function(")
    if function_start < 0:
        raise RuntimeError("Unable to locate Getty payload function")

    names_start = function_start + len("(function(")
    names_end = payload_js.find("){return", names_start)
    if names_end < 0:
        raise RuntimeError("Unable to locate Getty payload argument names")
    names = [name.strip() for name in payload_js[names_start:names_end].split(",") if name.strip()]

    call_start = payload_js.rfind("}(")
    if call_start < 0:
        raise RuntimeError("Unable to locate Getty payload argument values")
    args_start = call_start + 2
    args_end = payload_js.rfind("))")
    if args_end < 0:
        raise RuntimeError("Unable to locate Getty payload argument terminator")

    parser = _JsLiteralParser(payload_js[args_start:args_end], {})
    values = parser.parse_argument_list()
    if len(values) < len(names):
        raise RuntimeError("Getty payload argument table is shorter than expected")
    return {name: value for name, value in zip(names, values)}


def _parse_js_value(value: str, identifiers: dict[str, object]) -> object:
    parser = _JsLiteralParser(value, identifiers)
    parsed = parser.parse_value()
    parser.skip_whitespace()
    return parsed


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [text for item in value if (text := _normalize_text(item))]


def _join_non_empty(parts: list[str | None]) -> str | None:
    values = [part for part in parts if part]
    return " | ".join(values) if values else None


class _JsLiteralParser:
    def __init__(self, source: str, identifiers: dict[str, object]):
        self.source = source
        self.identifiers = identifiers
        self.index = 0

    def parse_argument_list(self) -> list[object]:
        values: list[object] = []
        while True:
            self.skip_whitespace()
            if self.index >= len(self.source):
                return values
            values.append(self.parse_value())
            self.skip_whitespace()
            if self._peek() != ",":
                return values
            self.index += 1

    def parse_value(self) -> object:
        self.skip_whitespace()
        char = self._peek()
        if char == "{":
            return self.parse_object()
        if char == "[":
            return self.parse_array()
        if char == '"':
            return self.parse_string()
        if char in "-0123456789":
            return self.parse_number()
        if self.source.startswith("!0", self.index):
            self.index += 2
            return True
        if self.source.startswith("!1", self.index):
            self.index += 2
            return False
        if self.source.startswith("void 0", self.index):
            self.index += len("void 0")
            return None
        identifier = self.parse_identifier()
        if identifier == "null":
            return None
        if identifier == "true":
            return True
        if identifier == "false":
            return False
        return self.identifiers.get(identifier, identifier)

    def parse_object(self) -> dict[str, object]:
        obj: dict[str, object] = {}
        self._expect("{")
        while True:
            self.skip_whitespace()
            if self._peek() == "}":
                self.index += 1
                return obj
            key = self.parse_key()
            self.skip_whitespace()
            self._expect(":")
            value = self.parse_value()
            obj[key] = value
            self.skip_whitespace()
            if self._peek() == ",":
                self.index += 1
                continue
            self._expect("}")
            return obj

    def parse_array(self) -> list[object]:
        values: list[object] = []
        self._expect("[")
        while True:
            self.skip_whitespace()
            if self._peek() == "]":
                self.index += 1
                return values
            values.append(self.parse_value())
            self.skip_whitespace()
            if self._peek() == ",":
                self.index += 1
                continue
            self._expect("]")
            return values

    def parse_key(self) -> str:
        self.skip_whitespace()
        if self._peek() == '"':
            return self.parse_string()
        return self.parse_identifier()

    def parse_string(self) -> str:
        self._expect('"')
        chars: list[str] = []
        while self.index < len(self.source):
            char = self.source[self.index]
            self.index += 1
            if char == "\\":
                if self.index >= len(self.source):
                    break
                escape = self.source[self.index]
                self.index += 1
                if escape == "u":
                    codepoint = self.source[self.index : self.index + 4]
                    self.index += 4
                    chars.append(chr(int(codepoint, 16)))
                else:
                    chars.append(json.loads(f'"\\{escape}"'))
                continue
            if char == '"':
                return "".join(chars)
            chars.append(char)
        raise RuntimeError("Unterminated JS string literal")

    def parse_number(self) -> int | float:
        start = self.index
        while self.index < len(self.source) and self.source[self.index] in "0123456789+-.eE":
            self.index += 1
        number_text = self.source[start:self.index]
        if any(char in number_text for char in ".eE"):
            return float(number_text)
        return int(number_text)

    def parse_identifier(self) -> str:
        start = self.index
        while self.index < len(self.source):
            char = self.source[self.index]
            if char.isalnum() or char in {"_", "$"}:
                self.index += 1
                continue
            break
        if start == self.index:
            raise RuntimeError(f"Expected identifier at offset {self.index}")
        return self.source[start:self.index]

    def skip_whitespace(self) -> None:
        while self.index < len(self.source) and self.source[self.index].isspace():
            self.index += 1

    def _expect(self, token: str) -> None:
        self.skip_whitespace()
        if not self.source.startswith(token, self.index):
            raise RuntimeError(f"Expected {token!r} at offset {self.index}")
        self.index += len(token)

    def _peek(self) -> str:
        if self.index >= len(self.source):
            return ""
        return self.source[self.index]
