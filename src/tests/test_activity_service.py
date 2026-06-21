from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.session import Base
from src.models.activity import Activity
from src.models.activity import ActivityStatus
from src.models.activity import AudienceSegment
from src.models.activity import FreeVerificationStatus
from src.models.activity import Source
from src.models.activity import Venue
from src.services.activity_service import list_activities
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


def test_list_activities_filters_audience_and_keeps_paid_adults_when_not_free_only() -> None:
    engine = create_engine("sqlite:///:memory:")
    TestingSessionLocal = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    now = datetime(2026, 6, 10, 12, 0)
    with TestingSessionLocal() as db:
        source = Source(
            name="example_source",
            base_url="https://example.org",
            adapter_type="static_html",
            crawl_frequency="daily",
            active=True,
        )
        venue = Venue(name="Example Museum", city="New York", state="NY")
        db.add_all([source, venue])
        db.flush()

        adult_class = Activity(
            source_id=source.id,
            source_url="https://example.org/adult-class",
            title="Adult Drawing Workshop",
            description="A paid studio class.",
            age_min=None,
            age_max=None,
            audience_segment=AudienceSegment.adults,
            is_free=False,
            free_verification_status=FreeVerificationStatus.confirmed,
            drop_in=False,
            registration_required=True,
            start_at=now,
            timezone="America/New_York",
            venue_id=venue.id,
            status=ActivityStatus.active,
            first_seen_at=now,
            last_seen_at=now,
            updated_at=now,
        )
        kids_class = Activity(
            source_id=source.id,
            source_url="https://example.org/kids-class",
            title="Family Art Workshop",
            description="A free family program.",
            age_min=None,
            age_max=12,
            audience_segment=AudienceSegment.kids,
            is_free=True,
            free_verification_status=FreeVerificationStatus.confirmed,
            drop_in=True,
            registration_required=False,
            start_at=now,
            timezone="America/New_York",
            venue_id=venue.id,
            status=ActivityStatus.active,
            first_seen_at=now,
            last_seen_at=now,
            updated_at=now,
        )
        db.add_all([adult_class, kids_class])
        db.commit()

        adult_results = list_activities(
            db,
            age=None,
            drop_in=None,
            venue=None,
            city=None,
            state=None,
            date_from=None,
            date_to=None,
            free_only=False,
            audience="adults",
        )
        free_adult_results = list_activities(
            db,
            age=None,
            drop_in=None,
            venue=None,
            city=None,
            state=None,
            date_from=None,
            date_to=None,
            free_only=True,
            audience="adults",
        )

    assert [activity.title for activity in adult_results] == ["Adult Drawing Workshop"]
    assert free_adult_results == []


def test_list_activities_teens_and_adults_filters_include_teens_adults() -> None:
    engine = create_engine("sqlite:///:memory:")
    TestingSessionLocal = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    now = datetime(2026, 6, 10, 12, 0)

    def make_activity(slug: str, title: str, segment: AudienceSegment) -> Activity:
        return Activity(
            source_id=source.id,
            source_url=f"https://example.org/{slug}",
            title=title,
            audience_segment=segment,
            is_free=True,
            free_verification_status=FreeVerificationStatus.confirmed,
            drop_in=True,
            registration_required=False,
            start_at=now,
            timezone="America/New_York",
            venue_id=venue.id,
            status=ActivityStatus.active,
            first_seen_at=now,
            last_seen_at=now,
            updated_at=now,
        )

    with TestingSessionLocal() as db:
        source = Source(
            name="example_source",
            base_url="https://example.org",
            adapter_type="static_html",
            crawl_frequency="daily",
            active=True,
        )
        venue = Venue(name="Example Museum", city="New York", state="NY")
        db.add_all([source, venue])
        db.flush()
        db.add_all(
            [
                make_activity("teens", "Teen Studio", AudienceSegment.teens),
                make_activity("both", "Teen & Adult Studio", AudienceSegment.teens_adults),
                make_activity("adults", "Adult Studio", AudienceSegment.adults),
                make_activity("kids", "Kids Studio", AudienceSegment.kids),
            ]
        )
        db.commit()

        def titles(audience: str) -> set[str]:
            results = list_activities(
                db,
                age=None,
                drop_in=None,
                venue=None,
                city=None,
                state=None,
                date_from=None,
                date_to=None,
                free_only=False,
                audience=audience,
            )
            return {activity.title for activity in results}

        # teens_adults activities surface under both teens and adults filters.
        assert titles("teens") == {"Teen Studio", "Teen & Adult Studio"}
        assert titles("adults") == {"Adult Studio", "Teen & Adult Studio"}
        # Unrelated segments stay scoped to themselves.
        assert titles("kids") == {"Kids Studio"}
