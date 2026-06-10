from __future__ import annotations

from src.crawlers.adapters.al_bundle import AL_VENUES_BY_SLUG
from src.crawlers.adapters.al_bundle import parse_al_events


MOBILE = AL_VENUES_BY_SLUG["mobile"]


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
        "url": f"https://www.mobilemuseumofart.com/event/{slug}/",
        "start_date": "2026-07-08 10:00:00",
        "end_date": "2026-07-08 11:00:00",
        "excerpt": "",
        "description": description,
        "cost": cost,
        "cost_details": {"values": values or []},
        "categories": [{"name": name} for name in (categories or [])],
        "venue": {"venue": "Mobile Museum of Art", "city": "Mobile", "state": "AL"},
    }


def test_mobile_infers_audience_and_paid_free_status() -> None:
    rows = parse_al_events(
        {
            "events": [
                _event(
                    title="Make &#038; Take Art",
                    description="Free pre-register or walk in for an art activity for all ages.",
                    categories=["Family Art"],
                ),
                _event(
                    title="Beginning Watercolor for Adults",
                    description="A watercolor class for adults.",
                    categories=["Adult Drawing & Painting", "Adults"],
                    cost="$120",
                    values=[120],
                ),
                _event(
                    title="Teen Sneaker Lab",
                    description="A custom sneaker design class for teens ages 13 to 18.",
                    categories=["Teens", "Workshop"],
                    cost="$50",
                    values=[50],
                ),
            ]
        },
        venue=MOBILE,
    )

    assert [row.title for row in rows] == [
        "Beginning Watercolor for Adults",
        "Make & Take Art",
        "Teen Sneaker Lab",
    ]
    assert [row.audience_segment for row in rows] == ["adults", "all_ages", "teens"]
    assert rows[0].is_free is False
    assert rows[1].is_free is True
    assert rows[2].is_free is False


def test_mobile_rejects_social_and_open_house_market_rows() -> None:
    rows = parse_al_events(
        {
            "events": [
                _event(
                    title="Third Annual Member-Guest Social",
                    description="A member social with drinks and light bites.",
                    categories=["Art Talk", "Programs"],
                ),
                _event(
                    title="Holiday Market &#038; Open House",
                    description="A shopping market and open house.",
                    categories=["Makers Market & Open House"],
                ),
            ]
        },
        venue=MOBILE,
    )

    assert rows == []
