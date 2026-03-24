from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from time import monotonic, sleep
from typing import Any, Sequence

import httpx

from src.core.config import settings
from src.models.activity import Venue

_COORDINATE_QUANTIZE = Decimal("0.0000001")
_DEFAULT_COUNTRY = "USA"


@dataclass(frozen=True)
class VenueGeocodeResult:
    query: str
    lat: Decimal
    lng: Decimal
    display_name: str


@dataclass(frozen=True)
class VenueGeocodeStats:
    requested: int
    geocoded: int
    skipped: int
    failed: int


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None


def _dedupe_query_parts(parts: Sequence[str | None]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for raw_part in parts:
        part = _normalize_optional_text(raw_part)
        if part is None:
            continue
        key = part.casefold()
        if key in seen:
            continue
        deduped.append(part)
        seen.add(key)
    return deduped


def build_geocode_queries(venue: Venue) -> list[str]:
    name = _normalize_optional_text(venue.name)
    address = _normalize_optional_text(venue.address)
    city = _normalize_optional_text(venue.city)
    state = _normalize_optional_text(venue.state)

    query_variants = [
        _dedupe_query_parts([name, address, city, state, _DEFAULT_COUNTRY]),
        _dedupe_query_parts([address, city, state, _DEFAULT_COUNTRY]),
        _dedupe_query_parts([name, city, state, _DEFAULT_COUNTRY]),
        _dedupe_query_parts([name, state, _DEFAULT_COUNTRY]),
        _dedupe_query_parts([city, state, _DEFAULT_COUNTRY]),
    ]

    queries: list[str] = []
    seen_queries: set[str] = set()
    for parts in query_variants:
        if not parts:
            continue
        query = ", ".join(parts)
        key = query.casefold()
        if key in seen_queries:
            continue
        queries.append(query)
        seen_queries.add(key)
    return queries


def _score_geocode_candidate(venue: Venue, candidate: dict[str, Any]) -> int:
    haystack = " ".join(
        str(value)
        for value in (
            candidate.get("display_name"),
            candidate.get("name"),
            candidate.get("type"),
            candidate.get("class"),
            candidate.get("address"),
        )
        if value
    ).lower()

    score = 0
    venue_name = _normalize_optional_text(venue.name)
    venue_city = _normalize_optional_text(venue.city)
    venue_state = _normalize_optional_text(venue.state)
    if venue_name and venue_name.lower() in haystack:
        score += 8
    if venue_city and venue_city.lower() in haystack:
        score += 5
    if venue_state and venue_state.lower() in haystack:
        score += 2
    if candidate.get("type") == "museum":
        score += 6
    if candidate.get("class") in {"tourism", "amenity", "building"}:
        score += 2
    return score


def _to_decimal_coordinate(value: str | float | int) -> Decimal:
    return Decimal(str(value)).quantize(_COORDINATE_QUANTIZE, rounding=ROUND_HALF_UP)


class NominatimVenueGeocoder:
    def __init__(self) -> None:
        self._client = httpx.Client(
            timeout=settings.geocoding_timeout_seconds,
            headers={"User-Agent": settings.geocoding_user_agent},
            follow_redirects=True,
        )
        self._last_request_at = 0.0

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "NominatimVenueGeocoder":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _throttle(self) -> None:
        remaining = settings.geocoding_min_interval_seconds - (monotonic() - self._last_request_at)
        if remaining > 0:
            sleep(remaining)

    def geocode(self, venue: Venue) -> VenueGeocodeResult | None:
        for query in build_geocode_queries(venue):
            self._throttle()
            response = self._client.get(
                settings.geocoding_nominatim_url,
                params={
                    "q": query,
                    "format": "jsonv2",
                    "limit": 5,
                    "countrycodes": "us",
                },
            )
            self._last_request_at = monotonic()
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list) or not payload:
                continue

            ranked = sorted(
                (item for item in payload if isinstance(item, dict)),
                key=lambda item: _score_geocode_candidate(venue, item),
                reverse=True,
            )
            if not ranked:
                continue

            top_match = ranked[0]
            lat = top_match.get("lat")
            lng = top_match.get("lon")
            if lat is None or lng is None:
                continue

            return VenueGeocodeResult(
                query=query,
                lat=_to_decimal_coordinate(lat),
                lng=_to_decimal_coordinate(lng),
                display_name=str(top_match.get("display_name") or query),
            )
        return None


def populate_venue_geocodes(
    venues: Sequence[Venue],
    *,
    only_missing: bool = True,
) -> VenueGeocodeStats:
    if not settings.geocoding_enabled:
        return VenueGeocodeStats(requested=0, geocoded=0, skipped=len(venues), failed=0)

    candidates = [
        venue
        for venue in venues
        if not only_missing or venue.lat is None or venue.lng is None
    ]
    if not candidates:
        return VenueGeocodeStats(requested=0, geocoded=0, skipped=len(venues), failed=0)

    geocoded = 0
    failed = 0

    with NominatimVenueGeocoder() as geocoder:
        for venue in candidates:
            try:
                result = geocoder.geocode(venue)
            except httpx.HTTPError:
                failed += 1
                continue

            if result is None:
                failed += 1
                continue

            venue.lat = result.lat
            venue.lng = result.lng
            geocoded += 1

    skipped = len(venues) - len(candidates)
    return VenueGeocodeStats(
        requested=len(candidates),
        geocoded=geocoded,
        skipped=skipped,
        failed=failed,
    )
