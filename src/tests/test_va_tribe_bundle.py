from __future__ import annotations

from src.crawlers.adapters.va_tribe_bundle import VA_TRIBE_VENUES_BY_SLUG
from src.crawlers.adapters.va_tribe_bundle import parse_va_tribe_events


CHRYSLER = VA_TRIBE_VENUES_BY_SLUG["chrysler"]


def _event(
    *,
    title: str,
    description: str,
    categories: list[str] | None = None,
    slug: str | None = None,
) -> dict:
    event_slug = slug or title.lower().replace(" ", "-").replace(":", "").replace(",", "")
    return {
        "title": title,
        "url": f"https://chrysler.org/event/{event_slug}/",
        "start_date": "2026-07-08 10:00:00",
        "end_date": "2026-07-08 11:00:00",
        "excerpt": "",
        "description": description,
        "cost": "",
        "cost_details": {"values": []},
        "categories": [{"name": name} for name in (categories or [])],
        "venue": {"venue": "Chrysler Museum of Art", "city": "Norfolk", "state": "VA"},
    }


def test_chrysler_sets_paid_adult_glass_class_audience_and_price() -> None:
    rows = parse_va_tribe_events(
        [
            _event(
                title="Hot Glass Deluxe",
                description="Make a glass object in a hands-on class. Ages 12 + $80 Museum members, $100 for non-members.",
                categories=["Class", "Glass Studio"],
            )
        ],
        venue=CHRYSLER,
    )

    assert len(rows) == 1
    assert rows[0].audience_segment == "adults"
    assert rows[0].age_min == 12
    assert rows[0].age_max is None
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"


def test_chrysler_sets_all_ages_family_open_studio() -> None:
    rows = parse_va_tribe_events(
        [
            _event(
                title="Second Saturday Open Studio",
                description=(
                    "Try something new in this free offering for all ages. "
                    "Bring the whole family to make art. Free, registration required."
                ),
                categories=["Kids & Families", "Related Exhibitions"],
            )
        ],
        venue=CHRYSLER,
    )

    assert len(rows) == 1
    assert rows[0].audience_segment == "all_ages"
    assert rows[0].is_free is True
    assert rows[0].registration_required is True


def test_chrysler_sets_kids_art_babies_audience() -> None:
    rows = parse_va_tribe_events(
        [
            _event(
                title="Art Babies: Nature",
                description="Designed to engage little ones up to 36 months old. Free, registration not required.",
                categories=["Art Workshop", "Kids & Families"],
            )
        ],
        venue=CHRYSLER,
    )

    assert len(rows) == 1
    assert rows[0].audience_segment == "kids"
    assert rows[0].is_free is True
    assert rows[0].registration_required is False


def test_chrysler_rejects_non_target_rows() -> None:
    rows = parse_va_tribe_events(
        [
            _event(
                title="Summer Teacher Institute | Artful Design",
                description="A two-day Summer Teacher Institute for educators.",
                categories=["Class", "Teacher Professional Development"],
            ),
            _event(
                title="Mixtape First Thursday: Food and Wine Tasting",
                description="A tasting held in conjunction with Mixtape First Thursdays.",
                categories=["Zinnia"],
            ),
            _event(
                title="Camp Art Stars: Myths and Legends",
                description="A weeklong art camp for young artists.",
                categories=["Art Camp", "Kids & Families"],
            ),
            _event(
                title="Art History 101 Tour: Contemporary Art",
                description="Explore collection galleries on this docent-led tour.",
                categories=["Lectures, Tours, & Talks"],
            ),
            _event(
                title="Glassmaking Demo",
                description="Join us in the Glass Studio for a free glassmaking demonstration.",
                categories=["Glass Studio"],
            ),
        ],
        venue=CHRYSLER,
    )

    assert rows == []
