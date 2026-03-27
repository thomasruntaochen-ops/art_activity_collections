import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from urllib.parse import urljoin
from urllib.parse import unquote
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from src.crawlers.adapters.base import BaseSourceAdapter
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount
from src.crawlers.pipeline.types import ExtractedActivity

NY_TIMEZONE = "America/New_York"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

STRONG_INCLUDE_PATTERNS = (
    " activity ",
    " art club ",
    " class ",
    " classes ",
    " conversation ",
    " creative aging ",
    " discussion ",
    " drawing ",
    " family day ",
    " hands on ",
    " hands-on ",
    " homeschool ",
    " lab ",
    " lecture ",
    " lectures ",
    " open studio ",
    " talk ",
    " talks ",
    " toddler ",
    " workshop ",
    " workshops ",
)
WEAK_INCLUDE_PATTERNS = (
    " art ",
    " family ",
    " families ",
    " kid ",
    " kids ",
    " teen ",
    " teens ",
    " youth ",
)
ALWAYS_REJECT_PATTERNS = (
    " admission ",
    " book club ",
    " camp ",
    " camps ",
    " concert ",
    " dinner ",
    " film ",
    " films ",
    " fundraising ",
    " gala ",
    " meditation ",
    " mindfulness ",
    " music ",
    " performance ",
    " poetry ",
    " reception ",
    " story time ",
    " storytime ",
    " tour ",
    " tours ",
    " yoga ",
)
CONTEXTUAL_REJECT_PATTERNS = (
    " exhibition ",
    " exhibitions ",
    " free admission ",
)
DROP_IN_PATTERNS = (
    " drop in ",
    " drop-in ",
    " open studio ",
)
REGISTRATION_PATTERNS = (
    " preregistration ",
    " register ",
    " registration ",
    " reserve ",
    " rsvp ",
    " seats are limited ",
)
AGE_RANGE_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b", re.IGNORECASE)
AGE_PLUS_RE = re.compile(r"\bages?\s*(\d{1,2})\s*(?:\+|and up)\b", re.IGNORECASE)
DATE_WITH_YEAR_RE = re.compile(
    r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})(?:[^0-9]+(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm))?",
    re.IGNORECASE,
)
TIME_RANGE_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)\s*(?:-|–|—|to)\s*(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
    re.IGNORECASE,
)
SIDEBAR_DATE_RE = re.compile(r"^[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}$")


@dataclass(frozen=True, slots=True)
class GaMecVenueConfig:
    slug: str
    source_name: str
    venue_name: str
    city: str
    state: str
    list_url: str
    detail_path_fragment: str
    follow_next_pages: bool = False


GA_MEC_VENUES: tuple[GaMecVenueConfig, ...] = (
    GaMecVenueConfig(
        slug="albany",
        source_name="ga_albany_events",
        venue_name="Albany Museum of Art",
        city="Albany",
        state="GA",
        list_url="https://www.albanymuseum.com/events/calendar/",
        detail_path_fragment="/event/",
    ),
    GaMecVenueConfig(
        slug="gmoa",
        source_name="ga_gmoa_events",
        venue_name="Georgia Museum of Art",
        city="Athens",
        state="GA",
        list_url="https://georgiamuseum.org/events/",
        detail_path_fragment="/events/",
        follow_next_pages=True,
    ),
)

GA_MEC_VENUES_BY_SLUG = {venue.slug: venue for venue in GA_MEC_VENUES}


async def fetch_html(
    url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 5,
    base_backoff_seconds: float = 2.0,
) -> str:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS)

    last_exception: Exception | None = None
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_exception = exc
                if attempt < max_attempts:
                    await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

            if response.status_code < 400:
                return response.text

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                await asyncio.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue

            response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()

    if last_exception is not None:
        raise RuntimeError(f"Unable to fetch GA MEC HTML: {url}") from last_exception
    raise RuntimeError(f"Unable to fetch GA MEC HTML after retries: {url}")


async def load_ga_mec_bundle_payload(
    *,
    venues: list[GaMecVenueConfig] | tuple[GaMecVenueConfig, ...] | None = None,
    page_limit: int = 6,
    max_detail_pages: int = 120,
) -> dict[str, dict]:
    selected = list(venues) if venues is not None else list(GA_MEC_VENUES)
    payload: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=DEFAULT_HEADERS) as client:
        for venue in selected:
            list_pages: dict[str, str] = {}
            detail_pages: dict[str, str] = {}
            urls_to_visit = [venue.list_url]
            seen_list_urls: set[str] = set()
            detail_urls: list[str] = []

            while urls_to_visit and len(seen_list_urls) < max(page_limit, 1):
                page_url = urls_to_visit.pop(0)
                if page_url in seen_list_urls:
                    continue
                html = await fetch_html(page_url, client=client)
                seen_list_urls.add(page_url)
                list_pages[page_url] = html

                for detail_url in _extract_detail_urls(html, list_url=page_url, venue=venue):
                    if detail_url not in detail_urls:
                        detail_urls.append(detail_url)

                if venue.follow_next_pages:
                    next_url = _extract_next_page_url(html, list_url=page_url)
                    if next_url and next_url not in seen_list_urls and next_url not in urls_to_visit:
                        urls_to_visit.append(next_url)

            for detail_url in detail_urls[:max_detail_pages]:
                try:
                    detail_pages[detail_url] = await fetch_html(detail_url, client=client)
                except Exception as exc:
                    print(f"[ga-mec-fetch] detail failed slug={venue.slug} url={detail_url}: {exc}")

            payload[venue.slug] = {
                "list_pages": list_pages,
                "detail_pages": detail_pages,
            }

    return payload


def parse_ga_mec_events(payload: dict, *, venue: GaMecVenueConfig) -> list[ExtractedActivity]:
    current_date = datetime.now(ZoneInfo(NY_TIMEZONE)).date()
    deduped: dict[tuple[str, str, datetime], ExtractedActivity] = {}

    for source_url, html in (payload.get("detail_pages") or {}).items():
        row = _build_row_from_detail_page(source_url=source_url, html=html, venue=venue)
        if row is None:
            continue
        if row.start_at.date() < current_date:
            continue
        deduped[(row.source_url, row.title, row.start_at)] = row

    rows = list(deduped.values())
    rows.sort(key=lambda row: (row.start_at, row.title, row.source_url))
    return rows


class GaMecBundleAdapter(BaseSourceAdapter):
    source_name = "ga_mec_bundle"

    async def fetch(self) -> list[str]:
        payload = await load_ga_mec_bundle_payload(page_limit=1)
        return [str(payload)]

    async def parse(self, payload: str) -> list[ExtractedActivity]:
        raise NotImplementedError("Use load_ga_mec_bundle_payload/parse_ga_mec_events from script runner.")


def _extract_detail_urls(html: str, *, list_url: str, venue: GaMecVenueConfig) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        absolute_url = urljoin(list_url, href)
        parsed = urlparse(absolute_url)
        if venue.detail_path_fragment not in parsed.path:
            continue
        if absolute_url.rstrip("/") == venue.list_url.rstrip("/"):
            continue
        if absolute_url in seen:
            continue
        seen.add(absolute_url)
        urls.append(absolute_url)

    return urls


def _extract_next_page_url(html: str, *, list_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    link = soup.find("link", attrs={"rel": lambda value: value and "next" in value})
    if link and link.get("href"):
        return urljoin(list_url, link["href"])

    anchor = soup.select_one("a.next.page-numbers[href], a[rel='next'][href]")
    if anchor:
        return urljoin(list_url, anchor.get("href") or "")

    return None


def _build_row_from_detail_page(*, source_url: str, html: str, venue: GaMecVenueConfig) -> ExtractedActivity | None:
    event_obj = _extract_event_object(html)
    title = _normalize_space(event_obj.get("name") if event_obj else None)
    description = _normalize_space(_html_to_text(event_obj.get("description")) if event_obj else "")
    start_at = _parse_datetime(event_obj.get("startDate") if event_obj else None)
    end_at = _parse_datetime(event_obj.get("endDate") if event_obj else None)
    sidebar_fields = _extract_sidebar_event_fields(html)

    fallback = _extract_fallback_event_fields(source_url=source_url, html=html)
    title = title or fallback.get("title") or ""
    description = description or fallback.get("description") or ""

    if sidebar_fields.get("start_at") is not None:
        start_at = sidebar_fields["start_at"]
    else:
        start_at = start_at or fallback.get("start_at")

    if sidebar_fields.get("end_at") is not None:
        end_at = sidebar_fields["end_at"]
    else:
        end_at = end_at or fallback.get("end_at")

    if not title or start_at is None:
        return None

    title = unquote(title)
    description = unquote(description)

    location_text = (
        _extract_location_name(event_obj.get("location")) if event_obj else None
    ) or f"{venue.city}, {venue.state}"
    price_text = _extract_offer_text(event_obj.get("offers")) if event_obj else ""
    amount = _extract_offer_amount(event_obj.get("offers")) if event_obj else None

    token_blob = _searchable_blob(" ".join([title, description, price_text or "", location_text or ""]))
    title_blob = _searchable_blob(title)
    if not _should_keep_event(title_blob=title_blob, token_blob=token_blob):
        return None

    age_min, age_max = _parse_age_range(" ".join(part for part in [title, description] if part))
    is_free, free_status = infer_price_classification_from_amount(amount, text=price_text or description)

    description_parts = [part for part in [description or None, f"Price: {price_text}" if price_text else None] if part]
    full_description = " | ".join(description_parts) if description_parts else None

    return ExtractedActivity(
        source_url=source_url,
        title=title,
        description=full_description,
        venue_name=venue.venue_name,
        location_text=location_text,
        city=venue.city,
        state=venue.state,
        activity_type=_infer_activity_type(token_blob),
        age_min=age_min,
        age_max=age_max,
        drop_in=any(pattern in token_blob for pattern in DROP_IN_PATTERNS),
        registration_required=any(pattern in token_blob for pattern in REGISTRATION_PATTERNS),
        start_at=start_at,
        end_at=end_at,
        timezone=NY_TIMEZONE,
        is_free=is_free,
        free_verification_status=free_status,
    )


def _extract_event_object(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_text = (script.get_text() or "").strip()
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except json.JSONDecodeError:
            continue
        event_obj = _find_event_object(data)
        if event_obj is not None:
            return event_obj
    return None


def _find_event_object(value: object) -> dict | None:
    if isinstance(value, dict):
        value_type = value.get("@type")
        if value_type == "Event" or (isinstance(value_type, list) and "Event" in value_type):
            return value
        for item in value.values():
            found = _find_event_object(item)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find_event_object(item)
            if found is not None:
                return found
    return None


def _extract_fallback_event_fields(*, source_url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    text = _normalize_space(soup.get_text(" ", strip=True))

    title = _normalize_space(
        (soup.find("meta", attrs={"property": "og:title"}) or {}).get("content")
        or (soup.find("title") or {}).get_text(" ", strip=True)
    )
    title = re.sub(r"\s*-\s*Georgia Museum of Art$", "", title).strip()
    title = re.sub(r"\s*–\s*Albany Museum of Art$", "", title).strip()
    description = _normalize_space(
        (soup.find("meta", attrs={"name": "description"}) or {}).get("content")
        or ""
    )

    start_at = None
    end_at = None
    date_match = DATE_WITH_YEAR_RE.search(text)
    if date_match:
        month_day_year = date_match.group(1)
        hour = date_match.group(2)
        minute = date_match.group(3) or "00"
        meridiem = date_match.group(4)
        if hour and meridiem:
            start_at = _parse_datetime_string(f"{month_day_year} {hour}:{minute} {meridiem}")
        else:
            start_at = _parse_datetime_string(month_day_year)

        if start_at is not None:
            range_match = TIME_RANGE_RE.search(text)
            if range_match:
                end_hour = range_match.group(4)
                end_minute = range_match.group(5) or "00"
                end_meridiem = range_match.group(6)
                end_at = _parse_datetime_string(
                    f"{month_day_year} {end_hour}:{end_minute} {end_meridiem}"
                )

    return {
        "title": title or source_url,
        "description": description,
        "start_at": start_at,
        "end_at": end_at,
    }


def _extract_sidebar_event_fields(html: str) -> dict[str, datetime | None]:
    soup = BeautifulSoup(html, "html.parser")
    values: dict[str, str] = {}

    for item in soup.select("ul[aria-label='Event details'] li"):
        heading = _normalize_space(item.find("h2").get_text(" ", strip=True) if item.find("h2") else "")
        value_tag = item.find("p")
        value = _normalize_space(value_tag.get_text(" ", strip=True) if value_tag else "")
        if heading and value:
            values[heading.lower()] = value

    date_text = values.get("date") or ""
    time_text = values.get("time") or ""
    if not date_text:
        date_text = _normalize_space(
            (soup.select_one(".mec-single-event-date .mec-start-date-label") or soup.select_one(".mec-single-event-date"))
            .get_text(" ", strip=True)
            if soup.select_one(".mec-single-event-date .mec-start-date-label") or soup.select_one(".mec-single-event-date")
            else ""
        )
        date_text = re.sub(r"^Date\s+", "", date_text, flags=re.IGNORECASE).strip()
    if not time_text:
        time_text = _normalize_space(
            soup.select_one(".mec-single-event-time").get_text(" ", strip=True)
            if soup.select_one(".mec-single-event-time")
            else ""
        )
        time_text = re.sub(r"^Time\s+", "", time_text, flags=re.IGNORECASE).strip()
    if not date_text or not SIDEBAR_DATE_RE.match(date_text):
        return {"start_at": None, "end_at": None}

    start_at = _parse_datetime_string(date_text)
    end_at = None
    if time_text:
        time_match = TIME_RANGE_RE.search(time_text)
        if time_match:
            start_at = _parse_datetime_string(
                f"{date_text} {time_match.group(1)}:{time_match.group(2) or '00'} {time_match.group(3)}"
            )
            end_at = _parse_datetime_string(
                f"{date_text} {time_match.group(4)}:{time_match.group(5) or '00'} {time_match.group(6)}"
            )
        else:
            single_time_match = re.search(
                r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)",
                time_text,
                re.IGNORECASE,
            )
            if single_time_match:
                start_at = _parse_datetime_string(
                    f"{date_text} {single_time_match.group(1)}:{single_time_match.group(2) or '00'} {single_time_match.group(3)}"
                )

    return {
        "start_at": start_at,
        "end_at": end_at,
    }


def _extract_offer_text(offers: object) -> str:
    if isinstance(offers, dict):
        parts = [
            _normalize_space(str(offers.get("price") or "")),
            _normalize_space(str(offers.get("priceCurrency") or "")),
        ]
        return " ".join(part for part in parts if part).strip()
    if isinstance(offers, list):
        for offer in offers:
            value = _extract_offer_text(offer)
            if value:
                return value
    if isinstance(offers, str):
        return _normalize_space(offers)
    return ""


def _extract_offer_amount(offers: object) -> Decimal | None:
    if isinstance(offers, dict):
        price = offers.get("price")
        if price is not None:
            cleaned = re.sub(r"[^0-9.]+", "", str(price))
            if cleaned:
                try:
                    return Decimal(cleaned)
                except Exception:
                    return None
    if isinstance(offers, list):
        for offer in offers:
            amount = _extract_offer_amount(offer)
            if amount is not None:
                return amount
    return None


def _extract_location_name(location: object) -> str | None:
    if isinstance(location, dict):
        name = _normalize_space(location.get("name"))
        address = location.get("address")
        if isinstance(address, dict):
            city = _normalize_space(address.get("addressLocality"))
            state = _normalize_space(address.get("addressRegion"))
            parts = [part for part in [name, city, state] if part]
            if parts:
                return ", ".join(parts)
        return name or None
    return None


def _should_keep_event(*, title_blob: str, token_blob: str) -> bool:
    strong_include = any(pattern in token_blob for pattern in STRONG_INCLUDE_PATTERNS)
    weak_include = strong_include or any(pattern in token_blob for pattern in WEAK_INCLUDE_PATTERNS)
    if not weak_include:
        return False

    if any(pattern in title_blob for pattern in ALWAYS_REJECT_PATTERNS):
        return False
    if any(pattern in token_blob for pattern in ALWAYS_REJECT_PATTERNS) and not strong_include:
        return False
    if any(pattern in token_blob for pattern in CONTEXTUAL_REJECT_PATTERNS) and not strong_include:
        return False

    return True


def _infer_activity_type(token_blob: str) -> str:
    if any(
        pattern in token_blob
        for pattern in (" lecture ", " lectures ", " talk ", " talks ", " conversation ", " discussion ")
    ):
        return "lecture"
    return "workshop"


def _searchable_blob(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return f" {' '.join(normalized.split())} "


def _parse_age_range(text: str) -> tuple[int | None, int | None]:
    match = AGE_RANGE_RE.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    plus_match = AGE_PLUS_RE.search(text)
    if plus_match:
        return int(plus_match.group(1)), None
    return None, None


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    return _parse_datetime_string(value)


def _parse_datetime_string(value: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None

    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%B %d, %Y %I:%M %p",
        "%B %d, %Y",
        "%b %d, %Y %I:%M %p",
        "%b %d, %Y",
    ):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is not None:
                return parsed.astimezone(ZoneInfo(NY_TIMEZONE)).replace(tzinfo=None)
            return parsed.replace(tzinfo=None)
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone(ZoneInfo(NY_TIMEZONE)).replace(tzinfo=None)
        return parsed.replace(tzinfo=None)
    except ValueError:
        return None


def _html_to_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return _normalize_space(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def _normalize_space(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def get_ga_mec_source_prefixes() -> tuple[str, ...]:
    prefixes: list[str] = []
    for venue in GA_MEC_VENUES:
        parsed = urlparse(venue.list_url)
        prefixes.append(f"{parsed.scheme}://{parsed.netloc}/")
    return tuple(prefixes)
