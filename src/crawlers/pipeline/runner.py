from datetime import datetime
from dataclasses import dataclass
from urllib.parse import urlparse

from sqlalchemy import func, literal, select, tuple_

from src.crawlers.pipeline.types import ExtractedActivity
from src.crawlers.extractors.hardcoded import extract_from_event_page
from src.db.session import SessionLocal
from src.models.activity import Activity, FreeVerificationStatus, Source, Venue
from src.services.venue_geocoding import populate_venue_geocodes

_UPSERT_BATCH_SIZE = 500


@dataclass(frozen=True)
class UpsertStats:
    input_rows: int
    deduped_rows: int
    inserted: int
    updated: int
    unchanged: int

    @property
    def written_rows(self) -> int:
        return self.inserted + self.updated


def _to_free_status(value: str) -> FreeVerificationStatus:
    try:
        return FreeVerificationStatus(value)
    except ValueError:
        return FreeVerificationStatus.inferred


def _resolve_is_free(item: ExtractedActivity, free_status: FreeVerificationStatus) -> bool | None:
    if item.is_free is not None:
        return item.is_free
    if free_status == FreeVerificationStatus.uncertain:
        return None
    return True


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None


def _normalize_state(value: str | None) -> str | None:
    text = _normalize_optional_text(value)
    return text.upper() if text is not None else None


def _normalize_website(value: str | None) -> str | None:
    text = _normalize_optional_text(value)
    if text is None:
        return None
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def _venue_key_for(
    venue_name: str | None,
    location_text: str | None,
    city: str | None,
    state: str | None,
) -> tuple[str, str | None, str | None] | None:
    normalized_name = _normalize_optional_text(venue_name)
    normalized_location = _normalize_optional_text(location_text)
    normalized_city = _normalize_optional_text(city)
    normalized_state = _normalize_state(state)
    if not normalized_name and not normalized_location:
        return None
    return (normalized_name or "Unknown Venue", normalized_city, normalized_state)


def _chunked(values: list[tuple], size: int) -> list[list[tuple]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def _has_meaningful_activity_change(
    current: Activity,
    item: ExtractedActivity,
    *,
    venue_id: int | None,
    is_free: bool | None,
    free_status: FreeVerificationStatus,
) -> bool:
    return any(
        (
            current.description != item.description,
            current.activity_type != item.activity_type,
            current.age_min != item.age_min,
            current.age_max != item.age_max,
            current.drop_in != item.drop_in,
            current.registration_required != item.registration_required,
            current.end_at != item.end_at,
            current.timezone != item.timezone,
            current.location_text != item.location_text,
            current.venue_id != venue_id,
            current.is_free != is_free,
            current.free_verification_status != free_status,
        )
    )


def _resolve_venues(
    db,
    extracted: list[ExtractedActivity],
    *,
    website_url: str | None = None,
) -> dict[tuple[str, str | None, str | None], Venue]:
    desired_venues: dict[tuple[str, str | None, str | None], str | None] = {}
    normalized_website = _normalize_website(website_url)
    for item in extracted:
        venue_key = _venue_key_for(item.venue_name, item.location_text, item.city, item.state)
        if venue_key is None:
            continue
        address = _normalize_optional_text(item.location_text)
        if venue_key not in desired_venues or desired_venues[venue_key] is None:
            desired_venues[venue_key] = address

    if not desired_venues:
        return {}

    venue_names = list({key[0] for key in desired_venues})
    existing_venues: list[Venue] = []
    for names_chunk in _chunked([(name,) for name in venue_names], _UPSERT_BATCH_SIZE):
        names = [item[0] for item in names_chunk]
        existing_venues.extend(db.scalars(select(Venue).where(Venue.name.in_(names))).all())

    venues_by_key: dict[tuple[str, str | None, str | None], Venue] = {
        (venue.name, _normalize_optional_text(venue.city), _normalize_state(venue.state)): venue
        for venue in existing_venues
    }

    new_venues: list[Venue] = []
    venues_needing_geocodes: dict[tuple[str, str | None, str | None], Venue] = {}
    for key, address in desired_venues.items():
        if key in venues_by_key:
            venue = venues_by_key[key]
            current_address = _normalize_optional_text(venue.address)
            if current_address is None and address is not None:
                venue.address = address
            elif current_address is not None and address is not None and len(address) > len(current_address):
                venue.address = address
            if _normalize_optional_text(venue.website) is None and normalized_website is not None:
                venue.website = normalized_website
            if venue.lat is None or venue.lng is None:
                venues_needing_geocodes[key] = venue
            continue
        venue = Venue(
            name=key[0],
            address=address,
            city=key[1],
            state=key[2],
            website=normalized_website,
        )
        new_venues.append(venue)
        venues_by_key[key] = venue
        venues_needing_geocodes[key] = venue

    if new_venues:
        db.add_all(new_venues)
        db.flush()

    if venues_needing_geocodes:
        populate_venue_geocodes(list(venues_needing_geocodes.values()))

    return venues_by_key


def upsert_extracted_activities_with_stats(
    source_url: str,
    extracted: list[ExtractedActivity],
    *,
    adapter_type: str = "static_html",
) -> tuple[list[ExtractedActivity], UpsertStats]:
    """Upsert extracted activity rows and return deduped inputs plus write stats."""
    deduped = list({(a.source_url, a.title, a.start_at): a for a in extracted}.values())
    if not deduped:
        return (
            deduped,
            UpsertStats(
                input_rows=len(extracted),
                deduped_rows=0,
                inserted=0,
                updated=0,
                unchanged=0,
            ),
        )

    now = datetime.utcnow()
    inserted = 0
    updated = 0
    unchanged = 0
    with SessionLocal() as db:
        source = db.scalar(
            select(Source)
            .where(literal(source_url).like(func.concat(Source.base_url, "%")))
            .order_by(func.length(Source.base_url).desc())
            .limit(1)
        )
        if source is None:
            parsed = urlparse(source_url)
            source = Source(
                name=(parsed.netloc or "unknown_source"),
                base_url=f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else source_url,
                adapter_type=adapter_type,
                crawl_frequency="daily",
                active=True,
            )
            db.add(source)
            db.flush()

        identity_keys = list({(a.source_url, a.title, a.start_at) for a in deduped})
        existing_items: list[Activity] = []
        for key_chunk in _chunked(identity_keys, _UPSERT_BATCH_SIZE):
            existing_items.extend(
                db.scalars(
                    select(Activity).where(
                        Activity.source_id == source.id,
                        tuple_(Activity.source_url, Activity.title, Activity.start_at).in_(key_chunk),
                    )
                ).all()
            )
        existing_by_key = {(a.source_url, a.title, a.start_at): a for a in existing_items}

        venues_by_key = _resolve_venues(db, deduped, website_url=source.base_url)

        for item in deduped:
            key = (item.source_url, item.title, item.start_at)
            current = existing_by_key.get(key)
            venue_key = _venue_key_for(item.venue_name, item.location_text, item.city, item.state)
            venue = venues_by_key.get(venue_key) if venue_key is not None else None
            free_status = _to_free_status(item.free_verification_status)
            is_free = _resolve_is_free(item, free_status)
            if current is None:
                db.add(
                    Activity(
                        source_id=source.id,
                        source_url=item.source_url,
                        title=item.title,
                        description=item.description,
                        activity_type=item.activity_type,
                        age_min=item.age_min,
                        age_max=item.age_max,
                        drop_in=item.drop_in,
                        registration_required=item.registration_required,
                        start_at=item.start_at,
                        end_at=item.end_at,
                        timezone=item.timezone,
                        location_text=item.location_text,
                        venue_id=venue.id if venue else None,
                        is_free=is_free,
                        free_verification_status=free_status,
                        first_seen_at=now,
                        last_seen_at=now,
                        updated_at=now,
                    )
                )
                inserted += 1
                continue

            venue_id = venue.id if venue else None
            if not _has_meaningful_activity_change(
                current,
                item,
                venue_id=venue_id,
                is_free=is_free,
                free_status=free_status,
            ):
                unchanged += 1
                continue

            current.description = item.description
            current.activity_type = item.activity_type
            current.age_min = item.age_min
            current.age_max = item.age_max
            current.drop_in = item.drop_in
            current.registration_required = item.registration_required
            current.end_at = item.end_at
            current.timezone = item.timezone
            current.location_text = item.location_text
            current.venue_id = venue_id
            current.is_free = is_free
            current.free_verification_status = free_status
            current.last_seen_at = now
            current.updated_at = now
            updated += 1

        db.commit()

    return (
        deduped,
        UpsertStats(
            input_rows=len(extracted),
            deduped_rows=len(deduped),
            inserted=inserted,
            updated=updated,
            unchanged=unchanged,
        ),
    )


def upsert_extracted_activities(
    source_url: str,
    extracted: list[ExtractedActivity],
    *,
    adapter_type: str = "static_html",
) -> list[ExtractedActivity]:
    """Backwards-compatible wrapper returning deduplicated inputs only."""
    deduped, _ = upsert_extracted_activities_with_stats(
        source_url=source_url,
        extracted=extracted,
        adapter_type=adapter_type,
    )
    return deduped


async def run_single_page(source_url: str, html: str):
    """Ingest a single event page payload and upsert activities into MySQL.

    Current implementation keeps the extraction phase deterministic (hardcoded parser),
    then performs a lightweight upsert keyed by source_url/title/time fields.
    """
    # 1) Parse raw HTML into normalized activity objects.
    # The extractor returns a list because one page may contain multiple activities.
    extracted = extract_from_event_page(source_url=source_url, html=html)

    # 2) Persist with shared upsert logic used by other adapters.
    return upsert_extracted_activities(source_url=source_url, extracted=extracted, adapter_type="static_html")
