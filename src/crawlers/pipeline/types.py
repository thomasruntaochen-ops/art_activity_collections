from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class ExtractedActivity:
    source_url: str
    title: str
    description: str | None
    venue_name: str | None
    location_text: str | None
    city: str | None
    state: str | None
    activity_type: str | None
    age_min: int | None
    age_max: int | None
    drop_in: bool | None
    registration_required: bool | None
    start_at: datetime
    end_at: datetime | None
    timezone: str
    free_verification_status: str
