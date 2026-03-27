from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError

from src.crawlers.pipeline.types import ExtractedActivity


def normalize_datetime_for_storage(value: datetime | None, *, timezone_name: str | None) -> datetime | None:
    if value is None or value.tzinfo is None:
        return value

    if timezone_name:
        try:
            value = value.astimezone(ZoneInfo(timezone_name))
        except (ValueError, ZoneInfoNotFoundError):
            pass

    return value.replace(tzinfo=None)


def normalize_extracted_activity_datetimes(activity: ExtractedActivity) -> ExtractedActivity:
    start_at = normalize_datetime_for_storage(activity.start_at, timezone_name=activity.timezone)
    end_at = normalize_datetime_for_storage(activity.end_at, timezone_name=activity.timezone)
    if start_at is activity.start_at and end_at is activity.end_at:
        return activity
    return replace(activity, start_at=start_at, end_at=end_at)


def parse_iso_datetime(value: str, *, timezone_name: str | None = None) -> datetime:
    parsed = datetime.fromisoformat(value)
    normalized = normalize_datetime_for_storage(parsed, timezone_name=timezone_name)
    return normalized if normalized is not None else parsed
