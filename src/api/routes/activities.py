from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from src.api.deps.rate_limit import enforce_request_limits
from src.db.session import get_db
from src.schemas.activity import ActivityFilterOptions, ActivityRead, VenueSummaryRead
from src.services.activity_service import (
    get_filter_options,
    get_filter_suggestions,
    list_activities,
    list_venue_summaries,
)

router = APIRouter(tags=["activities"], dependencies=[Depends(enforce_request_limits)])


@router.get("/activities", response_model=list[ActivityRead])
def get_activities(
    age: int | None = Query(default=None, ge=0, le=120),
    drop_in: bool | None = None,
    venue: str | None = None,
    city: str | None = None,
    state: str | None = Query(default=None, min_length=2, max_length=2),
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    free_only: bool = False,
    db: Session = Depends(get_db),
) -> list[ActivityRead]:
    activities = list_activities(
        db,
        age=age,
        drop_in=drop_in,
        venue=venue,
        city=city,
        state=state,
        date_from=date_from,
        date_to=date_to,
        free_only=free_only,
    )
    result: list[ActivityRead] = []
    for activity in activities:
        venue_name = activity.venue.name if activity.venue is not None else None
        venue_city = activity.venue.city if activity.venue is not None else None
        venue_state = activity.venue.state if activity.venue is not None else None
        result.append(
            ActivityRead(
                id=activity.id,
                title=activity.title,
                source_url=activity.source_url,
                venue_name=venue_name,
                location_text=activity.location_text,
                venue_city=venue_city,
                venue_state=venue_state,
                activity_type=activity.activity_type,
                age_min=activity.age_min,
                age_max=activity.age_max,
                drop_in=activity.drop_in,
                registration_required=activity.registration_required,
                start_at=activity.start_at,
                end_at=activity.end_at,
                timezone=activity.timezone,
                is_free=activity.is_free,
                free_verification_status=activity.free_verification_status.value,
                extraction_method=activity.extraction_method.value,
                status=activity.status.value,
                confidence_score=activity.confidence_score,
            )
        )
    return result


@router.get("/activities/suggestions", response_model=list[str])
def get_activity_suggestions(
    field: Literal["venue", "city", "state"],
    q: str = Query(min_length=1, max_length=100),
    limit: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
) -> list[str]:
    return get_filter_suggestions(db, field=field, query=q, limit=limit)


@router.get("/activities/filter-options", response_model=ActivityFilterOptions)
def get_activity_filter_options(
    state: str | None = Query(default=None, min_length=2, max_length=2),
    city: str | None = None,
    free_only: bool = False,
    db: Session = Depends(get_db),
) -> ActivityFilterOptions:
    options = get_filter_options(db, state=state, city=city, free_only=free_only)
    return ActivityFilterOptions(
        venues=options["venues"],
        states=options["states"],
        cities=options["cities"],
    )


@router.get("/activities/venues", response_model=list[VenueSummaryRead])
def get_activity_venues(
    state: str | None = Query(default=None, min_length=2, max_length=2),
    city: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    free_only: bool = False,
    limit: int = Query(default=150, ge=1, le=300),
    db: Session = Depends(get_db),
) -> list[VenueSummaryRead]:
    rows = list_venue_summaries(
        db,
        state=state,
        city=city,
        date_from=date_from,
        date_to=date_to,
        free_only=free_only,
        limit=limit,
    )
    return [
        VenueSummaryRead(
            venue_name=row.venue_name,
            venue_address=row.venue_address,
            venue_city=row.venue_city,
            venue_state=row.venue_state,
            venue_zip=row.venue_zip,
            venue_lat=float(row.venue_lat) if row.venue_lat is not None else None,
            venue_lng=float(row.venue_lng) if row.venue_lng is not None else None,
            activity_count=row.activity_count,
            next_activity_at=row.next_activity_at,
        )
        for row in rows
    ]
