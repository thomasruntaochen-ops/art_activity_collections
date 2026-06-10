from __future__ import annotations

from src.crawlers.adapters.ar_tribe_bundle import AR_TRIBE_VENUES_BY_SLUG
from src.crawlers.adapters.ar_tribe_bundle import parse_ar_tribe_events


ARKMFA = AR_TRIBE_VENUES_BY_SLUG["arkmfa"]


def _event(
    *,
    title: str,
    description: str,
    categories: list[str] | None = None,
    cost: str = "",
    values: list[int] | None = None,
) -> dict:
    slug = title.lower().replace(" ", "-").replace(":", "").replace(",", "")
    return {
        "title": title,
        "url": f"https://arkmfa.org/event/{slug}/",
        "start_date": "2026-07-08 10:00:00",
        "end_date": "2026-07-08 11:00:00",
        "excerpt": "",
        "description": description,
        "cost": cost,
        "cost_details": {"values": values or []},
        "categories": [{"name": name} for name in (categories or [])],
        "venue": {"venue": "Arkansas Museum of Fine Arts", "city": "Little Rock", "state": "AR"},
    }


def test_arkmfa_infers_kids_all_ages_and_adult_paid_rows() -> None:
    rows = parse_ar_tribe_events(
        [
            _event(
                title="Art Start",
                description="Toddlers and preschoolers explore music, movement, and art-making with families. Free, drop in.",
                categories=["Art Start", "Families"],
            ),
            _event(
                title="Creative Saturdays",
                description="Guests of all ages and abilities are welcome for a free family-friendly art-making activity.",
                categories=["Creative Saturdays"],
            ),
            _event(
                title="Drop-in Figure Drawing",
                description="Students must be 18+ to attend this untutored drawing session.",
                categories=["Class"],
                cost="$15",
                values=[15],
            ),
        ],
        venue=ARKMFA,
    )

    assert [row.audience_segment for row in rows] == ["kids", "all_ages", "adults"]
    assert rows[0].is_free is True
    assert rows[1].is_free is True
    assert rows[2].is_free is False
    assert rows[2].free_verification_status == "confirmed"


def test_arkmfa_rejects_camps_and_tours() -> None:
    rows = parse_ar_tribe_events(
        [
            _event(
                title="Summer Art Camp",
                description="A weeklong summer camp for children.",
                categories=["Class"],
            ),
            _event(
                title="Architecture Tour",
                description="A guided tour through the building.",
                categories=["Talk"],
            ),
            _event(
                title="Art Together: Sensory-Friendly Hours",
                description="Adjusted museum hours with sensory bags available.",
                categories=["Accessible"],
            ),
        ],
        venue=ARKMFA,
    )

    assert rows == []


def test_arkmfa_complimentary_perk_does_not_make_paid_class_free() -> None:
    rows = parse_ar_tribe_events(
        [
            _event(
                title="Art For Two: Paired Printmaking",
                description=(
                    "A paid one-night class experience for two. "
                    "Present your registration receipt to receive a complimentary appetizer."
                ),
                categories=["Adults", "Art School"],
                cost="$45",
                values=[45],
            )
        ],
        venue=ARKMFA,
    )

    assert len(rows) == 1
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"
    assert rows[0].audience_segment == "adults"
