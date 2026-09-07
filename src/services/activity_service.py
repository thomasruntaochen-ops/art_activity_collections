from datetime import datetime
from datetime import timezone

from collections.abc import Sequence

from sqlalchemy import Select, case, func, or_, select
from sqlalchemy.orm import Session, selectinload

from src.models.activity import Activity, AudienceSegment, Venue
from src.services.venue_prominence import prominence_case


def _to_naive(value: datetime | None) -> datetime | None:
    """Drop the offset so an aware input compares cleanly against naive columns."""
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _floor_to_day(value: datetime | None) -> datetime | None:
    """Turn a client's ``date_from`` instant into a naive start-of-day cutoff.

    ``Activity.start_at`` holds naive wall-clock time in the venue's own
    timezone, but clients send ``date_from`` as a UTC instant (the app and web
    explorer both derive it from ``new Date().toISOString()``). Comparing the
    two directly dropped every activity whose local start hour had already
    passed in UTC — a 4-10 hour blind spot that widened the further west the
    venue sat, so a museum's 11am program disappeared from "Upcoming" for the
    rest of the day. Flooring to midnight reduces the comparison to a date,
    which is the granularity the two sides genuinely agree on.
    """
    value = _to_naive(value)
    if value is None:
        return None
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def _audience_segments(value: str | None) -> list[AudienceSegment]:
    """Map a requested audience to the segments that should match it.

    Activities tagged ``teens_adults`` suit both teens and adults, and the UI no
    longer offers a standalone "Teens & adults" filter. So a ``teens`` request
    matches teens + teens_adults, an ``adults`` request matches adults +
    teens_adults, and every other segment matches only itself.
    """
    if not value:
        return []
    try:
        segment = AudienceSegment(value)
    except ValueError:
        return []
    if segment is AudienceSegment.teens:
        return [AudienceSegment.teens, AudienceSegment.teens_adults]
    if segment is AudienceSegment.adults:
        return [AudienceSegment.adults, AudienceSegment.teens_adults]
    return [segment]


def list_activities(
    db: Session,
    age: int | None,
    drop_in: bool | None,
    venue: str | None,
    city: str | None,
    state: str | None,
    date_from: datetime | None,
    date_to: datetime | None,
    free_only: bool,
    audience: str | None = None,
) -> list[Activity]:
    stmt: Select[tuple[Activity]] = select(Activity).where(Activity.status.in_(("active", "needs_review")))
    if free_only:
        stmt = stmt.where(Activity.is_free.is_(True))
    stmt = stmt.options(selectinload(Activity.venue))
    has_venue_filters = bool(venue or city or state)
    if has_venue_filters:
        stmt = stmt.join(Venue, Activity.venue_id == Venue.id)

    filters = []
    if age is not None:
        filters.append(or_(Activity.age_min.is_(None), Activity.age_min <= age))
        filters.append(or_(Activity.age_max.is_(None), Activity.age_max >= age))
    audience_segments = _audience_segments(audience)
    if audience_segments:
        filters.append(Activity.audience_segment.in_(audience_segments))
    if drop_in is not None:
        filters.append(Activity.drop_in.is_(drop_in))
    if venue:
        filters.append(Venue.name == venue.strip())
    if city:
        filters.append(Venue.city == city.strip())
    if state:
        filters.append(Venue.state == state.strip().upper())
    lower_bound = _floor_to_day(date_from)
    upper_bound = _to_naive(date_to)
    if lower_bound is not None:
        # Match on the interval, not just the start, so a program already under
        # way stays listed instead of vanishing the moment it begins.
        filters.append(func.coalesce(Activity.end_at, Activity.start_at) >= lower_bound)
    if upper_bound is not None:
        filters.append(Activity.start_at <= upper_bound)

    if filters:
        stmt = stmt.where(*filters)

    stmt = stmt.order_by(Activity.start_at.asc(), Activity.id.asc()).limit(400)
    return _dedupe_activities_for_display(list(db.scalars(stmt)))[:200]


def _dedupe_activities_for_display(activities: list[Activity]) -> list[Activity]:
    unique: list[Activity] = []
    seen: set[tuple[int | None, str, datetime]] = set()
    for activity in activities:
        key = (
            activity.venue_id,
            activity.title.strip().casefold(),
            activity.start_at,
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(activity)
    return unique


def get_filter_suggestions(
    db: Session,
    *,
    field: str,
    query: str,
    limit: int = 10,
) -> list[str]:
    """Return small suggestion lists for UI autocomplete.

    Uses prefix matching (`value%`) so MySQL can use indexes efficiently.
    """
    q = query.strip()
    if not q:
        return []

    if field == "venue":
        column = Venue.name
        # Support typing after a leading article, e.g. "m" -> "The Metropolitan Museum..."
        prefixed_patterns = [f"{q}%", f"The {q}%", f"A {q}%", f"An {q}%"]
        rank = case(
            (column.like(f"{q}%"), 0),
            (column.like(f"The {q}%"), 1),
            (column.like(f"A {q}%"), 2),
            (column.like(f"An {q}%"), 3),
            else_=9,
        )
        stmt = (
            select(column)
            .distinct()
            .where(column.is_not(None), or_(*[column.like(pattern) for pattern in prefixed_patterns]))
            .order_by(rank.asc(), column.asc())
            .limit(max(1, min(limit, 20)))
        )
        return [value for value in db.scalars(stmt) if value]
    elif field == "city":
        column = Venue.city
    elif field == "state":
        column = Venue.state
    else:
        return []

    stmt = (
        select(column)
        .distinct()
        .where(column.is_not(None), column.like(f"{q}%"))
        .order_by(column.asc())
        .limit(max(1, min(limit, 20)))
    )
    return [value for value in db.scalars(stmt) if value]


def get_filter_options(
    db: Session,
    *,
    state: str | None = None,
    city: str | None = None,
    free_only: bool = False,
    audience: str | None = None,
) -> dict[str, list[str]]:
    """Return dropdown option values constrained to current activity data."""
    base_conditions = [Activity.status.in_(("active", "needs_review")), Activity.venue_id.is_not(None)]
    if free_only:
        base_conditions.append(Activity.is_free.is_(True))
    audience_segments = _audience_segments(audience)
    if audience_segments:
        base_conditions.append(Activity.audience_segment.in_(audience_segments))
    filters = list(base_conditions)
    if state:
        filters.append(Venue.state == state.strip().upper())
    if city:
        filters.append(Venue.city == city.strip())

    venue_stmt = (
        select(Venue.name)
        .distinct()
        .join(Activity, Activity.venue_id == Venue.id)
        .where(*filters, Venue.name.is_not(None))
        .order_by(Venue.name.asc())
    )
    state_stmt = (
        select(Venue.state)
        .distinct()
        .join(Activity, Activity.venue_id == Venue.id)
        .where(*filters, Venue.state.is_not(None))
        .order_by(Venue.state.asc())
    )
    city_stmt = (
        select(Venue.city)
        .distinct()
        .join(Activity, Activity.venue_id == Venue.id)
        .where(*filters, Venue.city.is_not(None))
        .order_by(Venue.city.asc())
    )

    venues = [value for value in db.scalars(venue_stmt) if value]
    states = [value for value in db.scalars(state_stmt) if value]
    cities = [value for value in db.scalars(city_stmt) if value]
    return {"venues": venues, "states": states, "cities": cities}


def list_venue_summaries(
    db: Session,
    *,
    state: str | None = None,
    city: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    free_only: bool = False,
    audience: str | None = None,
    limit: int = 150,
) -> Sequence:
    conditions = [Activity.status.in_(("active", "needs_review")), Activity.venue_id.is_not(None)]
    if free_only:
        conditions.append(Activity.is_free.is_(True))
    audience_segments = _audience_segments(audience)
    if audience_segments:
        conditions.append(Activity.audience_segment.in_(audience_segments))
    if state:
        conditions.append(Venue.state == state.strip().upper())
    if city:
        conditions.append(Venue.city == city.strip())
    # Same cutoff as list_activities, so venue cards and counts cannot disagree
    # with the activity list they drill into.
    lower_bound = _floor_to_day(date_from)
    upper_bound = _to_naive(date_to)
    if lower_bound is not None:
        conditions.append(func.coalesce(Activity.end_at, Activity.start_at) >= lower_bound)
    if upper_bound is not None:
        conditions.append(Activity.start_at <= upper_bound)

    stmt = (
        select(
            Venue.name.label("venue_name"),
            Venue.address.label("venue_address"),
            Venue.city.label("venue_city"),
            Venue.state.label("venue_state"),
            Venue.zip.label("venue_zip"),
            Venue.lat.label("venue_lat"),
            Venue.lng.label("venue_lng"),
            func.count(Activity.id).label("activity_count"),
            func.sum(case((Activity.is_free.is_(True), 1), else_=0)).label("free_activity_count"),
            func.min(Activity.start_at).label("next_activity_at"),
        )
        .join(Activity, Activity.venue_id == Venue.id)
        .where(*conditions, Venue.name.is_not(None))
        .group_by(Venue.id)
        # Curated prominence leads, program count only breaks ties inside a
        # band. Sorting by count alone buried the museums people search for --
        # the Whitney's single program put it below every regional gallery with
        # a busy calendar -- and, because this query is capped, it could drop a
        # flagship museum from the response entirely.
        .order_by(
            prominence_case(Venue.name).asc(),
            func.count(Activity.id).desc(),
            func.min(Activity.start_at).asc(),
            Venue.name.asc(),
        )
        .limit(max(1, min(limit, 300)))
    )

    return db.execute(stmt).all()
