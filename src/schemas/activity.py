from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class ActivityRead(BaseModel):
    id: int
    title: str
    source_url: str
    venue_name: str | None
    location_text: str | None
    venue_city: str | None
    venue_state: str | None
    activity_type: str | None
    age_min: int | None
    age_max: int | None
    drop_in: bool | None
    registration_required: bool | None
    start_at: datetime
    end_at: datetime | None
    timezone: str
    free_verification_status: str
    extraction_method: str
    status: str
    confidence_score: Decimal

    model_config = {"from_attributes": True}


class ActivityFilter(BaseModel):
    age: int | None = None
    drop_in: bool | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None


class ActivityFilterOptions(BaseModel):
    venues: list[str]
    states: list[str]
    cities: list[str]
