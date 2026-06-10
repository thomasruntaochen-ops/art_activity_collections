from datetime import datetime

from src.crawlers.pipeline.audience import infer_audience_segment
from src.crawlers.pipeline.audience import normalize_audience_segment
from src.crawlers.pipeline.runner import _to_audience_segment
from src.crawlers.pipeline.types import ExtractedActivity


def test_normalize_audience_segment_accepts_common_aliases() -> None:
    assert normalize_audience_segment("family") == "kids"
    assert normalize_audience_segment("teen") == "teens"
    assert normalize_audience_segment("adult") == "adults"
    assert normalize_audience_segment("all ages") == "all_ages"
    assert normalize_audience_segment("something else") == "unknown"


def test_infer_audience_segment_from_age_range() -> None:
    assert infer_audience_segment(age_min=6, age_max=10) == "kids"
    assert infer_audience_segment(age_min=13, age_max=17) == "teens"
    assert infer_audience_segment(age_min=18, age_max=None) == "adults"


def test_infer_audience_segment_treats_general_classes_as_adult() -> None:
    assert infer_audience_segment(title="Drawing Workshop", description="A paid studio class.") == "adults"


def test_infer_audience_segment_uses_url_path_tokens() -> None:
    assert infer_audience_segment(source_url="https://example.org/events/family-programs/storytime") == "kids"
    assert infer_audience_segment(source_url="https://example.org/events/teen-studio/drawing") == "teens"


def test_infer_audience_segment_classifies_storytime_as_kids() -> None:
    assert infer_audience_segment(title="Storytime at The Met Cloisters") == "kids"


def test_infer_audience_segment_kids_default_can_be_overridden_by_teen_age_range() -> None:
    assert infer_audience_segment(title="Teen Studio", age_min=14, age_max=18, default="kids") == "teens"


def test_runner_audience_fallback_classifies_legacy_rows() -> None:
    row = ExtractedActivity(
        source_url="https://example.org/events/family-workshop",
        title="Family Workshop",
        description="A hands-on art class.",
        venue_name="Example Museum",
        location_text="Example City, CA",
        city="Example City",
        state="CA",
        activity_type="workshop",
        age_min=6,
        age_max=10,
        drop_in=False,
        registration_required=True,
        start_at=datetime(2026, 6, 12, 12, 0),
        end_at=None,
        timezone="America/Los_Angeles",
        free_verification_status="confirmed",
        is_free=True,
    )

    assert _to_audience_segment(row).value == "kids"
