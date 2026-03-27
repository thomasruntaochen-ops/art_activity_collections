from datetime import datetime
from datetime import timezone

from src.crawlers.pipeline.datetime_utils import normalize_datetime_for_storage
from src.crawlers.pipeline.datetime_utils import normalize_extracted_activity_datetimes
from src.crawlers.pipeline.datetime_utils import parse_iso_datetime
from src.crawlers.pipeline.types import ExtractedActivity


def _sample_activity(*, start_at: datetime, end_at: datetime | None) -> ExtractedActivity:
    return ExtractedActivity(
        source_url="https://example.org/events/sample-workshop",
        title="Sample Workshop",
        description="A sample activity.",
        venue_name="Example Museum",
        location_text="Example City, CA",
        city="Example City",
        state="CA",
        activity_type="workshop",
        age_min=None,
        age_max=None,
        drop_in=False,
        registration_required=False,
        start_at=start_at,
        end_at=end_at,
        timezone="America/New_York",
        is_free=True,
        free_verification_status="inferred",
    )


def test_parse_iso_datetime_converts_aware_values_to_local_naive() -> None:
    parsed = parse_iso_datetime("2026-03-10T16:00:00+00:00", timezone_name="America/New_York")

    assert parsed == datetime(2026, 3, 10, 12, 0)
    assert parsed.tzinfo is None


def test_normalize_datetime_for_storage_leaves_naive_values_unchanged() -> None:
    naive = datetime(2026, 3, 10, 12, 0)

    assert normalize_datetime_for_storage(naive, timezone_name="America/New_York") is naive


def test_normalize_extracted_activity_datetimes_strips_timezone_info() -> None:
    activity = _sample_activity(
        start_at=datetime(2026, 3, 10, 16, 0, tzinfo=timezone.utc),
        end_at=datetime(2026, 3, 10, 18, 0, tzinfo=timezone.utc),
    )

    normalized = normalize_extracted_activity_datetimes(activity)

    assert normalized.start_at == datetime(2026, 3, 10, 12, 0)
    assert normalized.end_at == datetime(2026, 3, 10, 14, 0)
    assert normalized.start_at.tzinfo is None
    assert normalized.end_at is not None
    assert normalized.end_at.tzinfo is None
