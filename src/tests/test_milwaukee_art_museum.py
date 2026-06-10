from __future__ import annotations

from src.crawlers.adapters.milwaukee_art_museum import parse_milwaukee_art_museum_payload


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
        "url": f"https://mam.org/events/event/{event_slug}/",
        "start_date": "2026-07-08 10:00:00",
        "end_date": "2026-07-08 11:00:00",
        "excerpt": "",
        "description": description,
        "cost": {"values": []},
        "cost_details": {"values": []},
        "categories": [{"name": name} for name in (categories or [])],
        "venue_details": {"venue": "Milwaukee Art Museum", "city": "Milwaukee", "state": "WI"},
    }


def test_milwaukee_sets_family_drop_in_audience() -> None:
    rows = parse_milwaukee_art_museum_payload(
        {
            "events": [
                _event(
                    title="Drop-In Art Making: Kohl's Art Studio",
                    description="Drop in for family art-making activities inspired by the Museum's collection.",
                    categories=["Free with Admission", "Youth + Family"],
                )
            ]
        }
    )

    assert len(rows) == 1
    assert rows[0].audience_segment == "kids"
    assert rows[0].drop_in is True
    assert rows[0].registration_required is False


def test_milwaukee_sets_adult_gallery_talk_audience() -> None:
    rows = parse_milwaukee_art_museum_payload(
        {
            "events": [
                _event(
                    title="Gallery Talk: Seeking Revelation",
                    description=(
                        "Join us for an in-depth exploration of the themes in the exhibition. "
                        "Admission for kids 12 and under is free."
                    ),
                    categories=["Free with Admission", "Lectures + Talks"],
                )
            ]
        }
    )

    assert len(rows) == 1
    assert rows[0].audience_segment == "adults"


def test_milwaukee_parses_child_age_range() -> None:
    rows = parse_milwaukee_art_museum_payload(
        {
            "events": [
                _event(
                    title="First Stage at MAM: Spring Bugs",
                    description="A playful, interactive experience designed for young learners (ages 2-10).",
                    categories=["Youth + Family"],
                )
            ]
        }
    )

    assert len(rows) == 1
    assert rows[0].age_min == 2
    assert rows[0].age_max == 10
    assert rows[0].audience_segment == "kids"


def test_milwaukee_rejects_non_activity_rows() -> None:
    rows = parse_milwaukee_art_museum_payload(
        {
            "events": [
                _event(title="Museum Closed", description="The Museum is closed.", categories=["Announcements"]),
                _event(title="Used Book Sale", description="Browse art history books.", categories=["Free to the Public"]),
                _event(title="Art in Bloom", description="Floral displays, music, shopping, and refreshments."),
                _event(title="Member Swap Weekend", description="Show your Member card.", categories=["For Members"]),
                _event(
                    title="Drop-In Tours: Celebrating the Collection",
                    description="Explore works from the collection.",
                    categories=["Free with Admission"],
                ),
            ]
        }
    )

    assert rows == []
