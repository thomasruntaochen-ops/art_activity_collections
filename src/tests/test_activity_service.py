from datetime import datetime

from src.models.activity import Activity
from src.services.activity_service import _dedupe_activities_for_display


def test_dedupe_activities_for_display_keeps_first_same_venue_title_start() -> None:
    start_at = datetime(2026, 5, 15, 14, 0)
    first = Activity(id=1, venue_id=10, title="Open: Art Lab", start_at=start_at)
    duplicate = Activity(id=2, venue_id=10, title=" open: art lab ", start_at=start_at)
    distinct_venue = Activity(id=3, venue_id=11, title="Open: Art Lab", start_at=start_at)

    assert _dedupe_activities_for_display([first, duplicate, distinct_venue]) == [
        first,
        distinct_venue,
    ]
