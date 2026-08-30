from src.crawlers.adapters.fl_tribe_bundle import FL_TRIBE_VENUES_BY_SLUG
from src.crawlers.adapters.fl_tribe_bundle import parse_fl_tribe_events
from src.tests.fixture_helpers import future_datetime
from src.tests.fixture_helpers import future_datetime_text


def _event(
    *,
    title: str,
    slug: str,
    categories: list[str] | None = None,
    description: str = "",
    cost: str = "",
    values: list[str] | None = None,
    days: int = 30,
    hour: int = 10,
) -> dict:
    return {
        "title": title,
        "url": f"https://example.org/{slug}/",
        "start_date": future_datetime_text(days, hour=hour),
        "end_date": future_datetime_text(days, hour=hour + 2),
        "description": description,
        "excerpt": "",
        "cost": cost,
        "cost_details": {"values": values or []},
        "categories": [{"name": name} for name in categories or []],
        "venue": {"venue": "Museum", "city": "City", "state": "FL"},
    }


def test_fl_tribe_decodes_titles_and_marks_admission_required_programs_not_free() -> None:
    rows = parse_fl_tribe_events(
        [
            _event(
                title="Art&#8217;s The Spark",
                slug="spark",
                categories=["Accessibility", "Adults", "Free"],
                description="FREE, pre-registration required. Join an adult discussion.",
                cost="Free",
                values=["0"],
            ),
            _event(
                title="Sketching in the Galleries",
                slug="sketching",
                description="Drawing supplies available for artists of all ages. FREE with admission.",
                days=31,
            ),
        ],
        venue=FL_TRIBE_VENUES_BY_SLUG["orlando"],
    )

    assert [row.title for row in rows] == ["Art’s The Spark", "Sketching in the Galleries"]
    assert rows[0].audience_segment == "adults"
    assert rows[0].is_free is True
    assert rows[0].free_verification_status == "confirmed"
    assert rows[1].audience_segment == "all_ages"
    assert rows[1].is_free is False
    assert rows[1].free_verification_status == "confirmed"


def test_fl_tribe_filters_tampa_closures_and_fundraisers() -> None:
    rows = parse_fl_tribe_events(
        [
            _event(
                title="Museum Closed for Thanksgiving",
                slug="closed",
                categories=["Museum Closure"],
                description="The museum will be closed for Thanksgiving.",
            ),
            _event(
                title="Art & Aces 2026",
                slug="aces",
                categories=["Adults", "Signature"],
                description="A night of entertainment and philanthropy to raise funds.",
            ),
            _event(
                title="Ceramics Continued Study",
                slug="ceramics",
                categories=["Adults", "Studio Art Class"],
                description="Ages 16+ | Beginner Level. Build ceramics skills.",
                cost="$45",
                values=["45"],
            ),
        ],
        venue=FL_TRIBE_VENUES_BY_SLUG["tampa"],
    )

    assert len(rows) == 1
    assert rows[0].title == "Ceramics Continued Study"
    assert rows[0].audience_segment == "adults"
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"


def test_fl_tribe_bass_audience_and_known_paid_program_categories() -> None:
    rows = parse_fl_tribe_events(
        [
            _event(title="Open Studio", slug="open-studio", categories=["Open Studio"]),
            _event(
                title="Teen Studio Art Intensive",
                slug="teen-intensive",
                categories=["Teen Studio Art Intensive"],
                days=31,
                hour=9,
            ),
            _event(
                title="Workshops @ The Bass | Mirrors",
                slug="workshop",
                categories=["Workshops at The Bass"],
                days=32,
                hour=14,
            ),
            _event(
                title="Family Day | Fold & Flow",
                slug="family-day",
                categories=["Family Day"],
                days=33,
                hour=14,
            ),
        ],
        venue=FL_TRIBE_VENUES_BY_SLUG["bass"],
    )

    by_title = {row.title: row for row in rows}
    assert by_title["Open Studio"].audience_segment == "all_ages"
    assert by_title["Open Studio"].is_free is False
    assert by_title["Teen Studio Art Intensive"].audience_segment == "teens"
    assert by_title["Teen Studio Art Intensive"].is_free is False
    assert by_title["Teen Studio Art Intensive"].registration_required is True
    assert by_title["Workshops @ The Bass | Mirrors"].audience_segment == "adults"
    assert by_title["Workshops @ The Bass | Mirrors"].is_free is False
    assert by_title["Family Day | Fold & Flow"].audience_segment == "kids"
    assert by_title["Family Day | Fold & Flow"].is_free is None


def test_fl_tribe_preserves_future_datetimes() -> None:
    rows = parse_fl_tribe_events(
        [_event(title="Artist Talk", slug="talk", categories=["Adults"], description="An artist talk.")],
        venue=FL_TRIBE_VENUES_BY_SLUG["tampa"],
    )

    assert rows[0].start_at == future_datetime()


def test_fl_tribe_dedupes_same_title_and_time_across_occurrence_urls() -> None:
    rows = parse_fl_tribe_events(
        [
            _event(title="Glazing Party", slug="glazing-party-4", categories=["Adults", "Studio Art Class"]),
            _event(title="Glazing Party", slug=f"glazing-party-5/{future_datetime():%Y-%m-%d}", categories=["Adults", "Studio Art Class"]),
        ],
        venue=FL_TRIBE_VENUES_BY_SLUG["tampa"],
    )

    assert len(rows) == 1
