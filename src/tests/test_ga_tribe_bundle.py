from __future__ import annotations

from src.crawlers.adapters.ga_tribe_bundle import GA_TRIBE_VENUES_BY_SLUG
from src.crawlers.adapters.ga_tribe_bundle import parse_ga_tribe_events


TELFAIR = GA_TRIBE_VENUES_BY_SLUG["telfair"]
BOOTH = GA_TRIBE_VENUES_BY_SLUG["booth"]


def _event(
    *,
    title: str,
    description: str = "",
    categories: list[str] | None = None,
    url_slug: str | None = None,
) -> dict:
    slug = url_slug or title.lower().replace(" ", "-").replace(":", "").replace(",", "")
    return {
        "title": title,
        "url": f"https://www.telfair.org/event/{slug}/",
        "start_date": "2026-07-08 10:00:00",
        "end_date": "2026-07-08 11:00:00",
        "description": description,
        "excerpt": "",
        "cost": "",
        "cost_details": {"values": []},
        "categories": [{"name": name} for name in (categories or [])],
        "venue": {"venue": "Jepson Center", "city": "Savannah", "state": "GA"},
    }


def test_telfair_rejects_seasonal_adult_class_series() -> None:
    rows = parse_ga_tribe_events(
        [
            _event(
                title="Classical Life Drawing (Summer, Class 2)",
                description="Weekly paid studio class.",
                categories=["Adult Classes"],
            )
        ],
        venue=TELFAIR,
    )

    assert rows == []


def test_telfair_rejects_youth_class_series() -> None:
    rows = parse_ga_tribe_events(
        [
            _event(
                title="Youth Painting and Drawing (Fall (2), Class 5)",
                description="Weekly paid youth class.",
                categories=["Youth Classes"],
            )
        ],
        venue=TELFAIR,
    )

    assert rows == []


def test_telfair_keeps_family_drop_in_with_all_ages_audience() -> None:
    rows = parse_ga_tribe_events(
        [
            _event(
                title="Drop-In Studio",
                description="Join us for drop-in artmaking projects for all ages!",
                categories=["Family Program"],
            )
        ],
        venue=TELFAIR,
    )

    assert len(rows) == 1
    assert rows[0].audience_segment == "all_ages"
    assert rows[0].drop_in is True


def test_telfair_keeps_veterans_open_studio_as_adults() -> None:
    rows = parse_ga_tribe_events(
        [
            _event(
                title="Veterans Open Studio",
                description="Open studio program open to all veterans.",
                categories=["Education"],
            )
        ],
        venue=TELFAIR,
    )

    assert len(rows) == 1
    assert rows[0].audience_segment == "adults"


def test_telfair_keeps_gallery_talk_as_adults() -> None:
    rows = parse_ga_tribe_events(
        [
            _event(
                title="Lunch & Learn: Gallery Talk",
                description="A curator-led gallery talk about the exhibition.",
                categories=["Lecture"],
            )
        ],
        venue=TELFAIR,
    )

    assert len(rows) == 1
    assert rows[0].activity_type == "lecture"
    assert rows[0].audience_segment == "adults"


def test_booth_rejects_summer_class_and_social_weekend_rows() -> None:
    rows = parse_ga_tribe_events(
        [
            {
                **_event(
                    title="Art Safari Summer Class",
                    description="Ages 5-7 years. Summer class with painting and drawing.",
                    categories=["Education"],
                ),
                "url": "https://boothmuseum.org/event/art-safari-summer-class/",
            },
            {
                **_event(
                    title="Cowgirl Collective",
                    description="A weekend for women who lead boldly with culture and resort events.",
                    categories=["Special Event"],
                ),
                "url": "https://boothmuseum.org/event/cowgirl-collective/",
            },
        ],
        venue=BOOTH,
    )

    assert rows == []


def test_booth_marks_artist_workshops_and_art_lunch_as_adults() -> None:
    rows = parse_ga_tribe_events(
        [
            {
                **_event(
                    title='BAA 3-Day Workshop with Artist Anna Rose Bain / "Painting Children From Photos"',
                    description="This workshop teaches painters how to paint the younger model from photographs.",
                    categories=["Booth Art Academy"],
                ),
                "url": "https://boothmuseum.org/event/painting-children-from-photos/",
            },
            {
                **_event(
                    title="Art for Lunch: Victoria Barnhill",
                    description="Join artist Victoria Barnhill for an intimate lunch as she shares her artistic journey.",
                    categories=["Education"],
                ),
                "url": "https://boothmuseum.org/event/art-for-lunch-victoria-barnhill/",
            },
        ],
        venue=BOOTH,
    )

    rows_by_title = {row.title: row for row in rows}

    assert rows_by_title['BAA 3-Day Workshop with Artist Anna Rose Bain / "Painting Children From Photos"'].audience_segment == "adults"
    assert rows_by_title['BAA 3-Day Workshop with Artist Anna Rose Bain / "Painting Children From Photos"'].activity_type == "workshop"
    assert rows_by_title["Art for Lunch: Victoria Barnhill"].audience_segment == "adults"
    assert rows_by_title["Art for Lunch: Victoria Barnhill"].activity_type == "lecture"
