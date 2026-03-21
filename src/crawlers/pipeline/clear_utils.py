from collections.abc import Sequence
from typing import TypeAlias

from sqlalchemy import and_, or_, select

from src.models.activity import Venue


VenueSpec: TypeAlias = tuple[str, str | None, str | None]


def lookup_venue_ids(db, venue_specs: Sequence[VenueSpec]) -> list[int]:
    normalized_specs: list[VenueSpec] = []
    for name, city, state in venue_specs:
        clean_name = (name or "").strip()
        if not clean_name:
            continue
        clean_city = city.strip() if isinstance(city, str) and city.strip() else None
        clean_state = state.strip() if isinstance(state, str) and state.strip() else None
        normalized_specs.append((clean_name, clean_city, clean_state))

    if not normalized_specs:
        return []

    venue_filters = []
    for name, city, state in normalized_specs:
        predicates = [Venue.name == name]
        predicates.append(Venue.city == city if city is not None else Venue.city.is_(None))
        predicates.append(Venue.state == state if state is not None else Venue.state.is_(None))
        venue_filters.append(and_(*predicates))

    return list(dict.fromkeys(db.scalars(select(Venue.id).where(or_(*venue_filters))).all()))
