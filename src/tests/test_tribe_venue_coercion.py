"""The Events Calendar returns `venue` as a dict, an empty list, or a list of dicts."""

from datetime import date

from src.crawlers.adapters.oh_common import coerce_tribe_venue
from src.crawlers.adapters.vt_bundle import VT_VENUES_BY_SLUG, _parse_tribe_events


def test_coerce_tribe_venue_accepts_every_shape_the_api_returns() -> None:
    venue = {"venue": "Pizzagalli Center", "city": "Shelburne", "state": "VT"}

    assert coerce_tribe_venue(venue) == venue
    # An event with one venue attached to an older post comes back wrapped.
    assert coerce_tribe_venue([venue]) == venue
    # An event with no venue set comes back as an empty list, not null.
    assert coerce_tribe_venue([]) == {}
    assert coerce_tribe_venue(None) == {}
    # Junk between the dicts must not shadow the real entry.
    assert coerce_tribe_venue(["", venue]) == venue


def test_vt_tribe_event_with_list_venue_keeps_its_location() -> None:
    """shelburnemuseum.org served `venue: [{...}]` and crashed the VT bundle daily."""
    payload = {
        "events_json": {
            "events": [
                {
                    "title": "Storytime and Artmaking for Families",
                    "url": "https://shelburnemuseum.org/event/storytime/",
                    "description": "Drop-in program for families.",
                    "start_date": "2099-10-10 11:00:00",
                    "end_date": "2099-10-10 12:00:00",
                    "venue": [
                        {
                            "venue": "Pizzagalli Center for Art and Education",
                            "city": "Shelburne",
                            "state": "VT",
                        }
                    ],
                }
            ]
        }
    }

    rows = _parse_tribe_events(
        payload,
        venue=VT_VENUES_BY_SLUG["shelburne"],
        start_day=date(2099, 1, 1),
    )

    assert len(rows) == 1
    assert "Pizzagalli Center for Art and Education" in rows[0].location_text
