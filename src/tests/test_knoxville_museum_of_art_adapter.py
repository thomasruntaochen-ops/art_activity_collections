from datetime import datetime

from src.crawlers.adapters.knoxville_museum_of_art import parse_knoxville_payload


def test_knoxville_parser_keeps_paid_and_free_qualifying_events() -> None:
    payload = {
        "events": [
            {
                "title": "Drop-in Figure Drawing",
                "url": "https://knoxart.org/event/drop-in-figure-drawing/2026-03-30/",
                "description": (
                    "<p>Drop-In Figure Drawing Session provides a space for individuals to work on their own "
                    "figure drawing projects.</p><p>Members: $12 | Non-Members: $15</p>"
                ),
                "excerpt": "<p>Artists of all skill levels are welcome.</p>",
                "start_date": "2026-03-30 18:00:00",
                "end_date": "2026-03-30 20:00:00",
                "cost": "",
                "venue": {
                    "venue": "Knoxville Museum of Art",
                    "city": "Knoxville",
                    "state": "TN",
                },
                "categories": [{"name": "Workshops"}],
                "tags": [],
            },
            {
                "title": "Seventeenth Annual Sarah Jane Hardrath Kramer Lecture",
                "url": "https://knoxart.org/event/sarah-jane-hardrath-kramer-lecture/",
                "description": "<p>Free lecture on contemporary glass by Black artists.</p>",
                "excerpt": "",
                "start_date": "2026-03-31 18:00:00",
                "end_date": "2026-03-31 19:30:00",
                "cost": "Free",
                "venue": {
                    "venue": "Knoxville Museum of Art",
                    "city": "Knoxville",
                    "state": "TN",
                },
                "categories": [{"name": "Lectures"}],
                "tags": [],
            },
            {
                "title": "Watercolor at the Eugenia Williams House",
                "url": "https://knoxart.org/event/watercolor-at-the-eugenia-williams-house/",
                "description": (
                    "<p>Enjoy a summer morning watercolor painting in the gardens.</p>"
                    "<p>This is not formal watercolor painting classes, however each session will be guided by "
                    "one of our professional art educators.</p><p>Tickets: $15 (KMA Member), $18 (Non-Member)</p>"
                ),
                "excerpt": "",
                "start_date": "2026-06-10 10:00:00",
                "end_date": "2026-06-10 12:00:00",
                "cost": "$15 – $18",
                "venue": {
                    "venue": "Eugenia Williams House",
                    "city": "Knoxville",
                    "state": "TN",
                },
                "categories": [{"name": "Special Events"}],
                "tags": [],
            },
        ]
    }

    rows = parse_knoxville_payload(payload)

    assert [row.title for row in rows] == [
        "Drop-in Figure Drawing",
        "Seventeenth Annual Sarah Jane Hardrath Kramer Lecture",
        "Watercolor at the Eugenia Williams House",
    ]

    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"
    assert rows[0].drop_in is True
    assert rows[0].activity_type == "workshop"
    assert rows[0].start_at == datetime(2026, 3, 30, 18, 0)

    assert rows[1].is_free is True
    assert rows[1].activity_type == "lecture"

    assert rows[2].is_free is False
    assert rows[2].registration_required is True
    assert rows[2].location_text == "Eugenia Williams House, Knoxville, TN"


def test_knoxville_parser_excludes_tours_fundraisers_films_and_brew_events() -> None:
    payload = {
        "events": [
            {
                "title": "Second Sunday Docent Tour",
                "url": "https://knoxart.org/event/second-sunday-docent-tour/",
                "description": "<p>FREE and open to the public!</p>",
                "excerpt": "",
                "start_date": "2026-04-12 14:00:00",
                "end_date": "2026-04-12 15:00:00",
                "cost": "",
                "venue": {},
                "categories": [{"name": "Tours"}],
                "tags": [],
            },
            {
                "title": "Artists on Location 2026",
                "url": "https://knoxart.org/event/artists-on-location-2026/",
                "description": "<p>A museum fundraiser.</p>",
                "excerpt": "",
                "start_date": "2026-04-24 18:00:00",
                "end_date": "2026-04-24 20:00:00",
                "cost": "",
                "venue": {},
                "categories": [{"name": "Fundraisers"}],
                "tags": [],
            },
            {
                "title": "Daughters of the Dust with introduction by Natalie Graham",
                "url": "https://knoxart.org/event/daughters-of-the-dust/",
                "description": "<p>Film screening at Central Cinema.</p>",
                "excerpt": "",
                "start_date": "2026-04-01 18:00:00",
                "end_date": "2026-04-01 20:00:00",
                "cost": "",
                "venue": {},
                "categories": [{"name": "Special Events"}],
                "tags": [],
            },
            {
                "title": "Arts & Craft Brew Night",
                "url": "https://knoxart.org/event/arts-craft-brew-night/",
                "description": "<p>Celebrate local makers while sipping local brews.</p>",
                "excerpt": "",
                "start_date": "2026-05-20 17:00:00",
                "end_date": "2026-05-20 19:00:00",
                "cost": "",
                "venue": {},
                "categories": [{"name": "Special Events"}],
                "tags": [],
            },
        ]
    }

    assert parse_knoxville_payload(payload) == []
