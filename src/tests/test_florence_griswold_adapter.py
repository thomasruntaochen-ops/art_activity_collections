from datetime import datetime

from src.crawlers.adapters.florence_griswold import parse_florence_griswold_events_payload


def test_florence_griswold_parser_keeps_paid_and_free_qualifying_events() -> None:
    payload = {
        "pages": [
            {
                "events": [
                    {
                        "title": "CERAMICS WORKSHOP: Make a Mosaic Tile",
                        "url": "https://flogris.org/calendar/ceramics-workshop-make-a-mosaic-tile/2026-03-07/",
                        "description": (
                            "<p>Saturdays, February 21-March 7, 1-3pm</p>"
                            "<p>$275 (Members 10% discount); limited enrollment</p>"
                            "<p>Explore your creative side by making a mosaic wall tile from start to finish.</p>"
                        ),
                        "excerpt": "<p>Join our three session workshop to design your own ceramic tile!</p>",
                        "start_date": "2026-03-07 13:00:00",
                        "end_date": "2026-03-07 15:00:00",
                        "venue": {
                            "venue": "Florence Griswold Museum",
                            "address": "96 Lyme Street",
                            "city": "Old Lyme",
                            "stateprovince": "CT",
                            "zip": "06371",
                        },
                        "categories": [{"name": "Adult Programs"}],
                    },
                    {
                        "title": "LECTURE: Teaching as Transformation: Women, Art, and the Power of Education",
                        "url": "https://flogris.org/calendar/lecture-teaching-as-transformation-women-art-and-the-power-of-education/",
                        "description": (
                            "<p><a href=\"https://tickets.example/lecture\">FREE but please register here</a></p>"
                            "<p>How can teachers change the story of art?</p>"
                        ),
                        "excerpt": "",
                        "start_date": "2026-03-01 13:00:00",
                        "end_date": "2026-03-01 14:00:00",
                        "venue": [],
                        "categories": [{"name": "Adult Programs"}],
                    },
                    {
                        "title": "Creative Arts Workshop for Elementary Students #1",
                        "url": "https://flogris.org/calendar/creative-arts-workshop-for-elementary-students-1-2/",
                        "description": (
                            "<p>$35 (Members 10% discount), geared for students in grades 1-3</p>"
                            "<p>Creative kids explore a variety of art projects during this half-day spring break session.</p>"
                        ),
                        "excerpt": "<p>A spring break art workshop for students in grades 1-3.</p>",
                        "start_date": "2026-04-15 09:00:00",
                        "end_date": "2026-04-15 12:00:00",
                        "venue": [],
                        "categories": [{"name": "Childrens Programs"}],
                    },
                ]
            }
        ]
    }

    rows = parse_florence_griswold_events_payload(payload)

    assert [row.title for row in rows] == [
        "LECTURE: Teaching as Transformation: Women, Art, and the Power of Education",
        "CERAMICS WORKSHOP: Make a Mosaic Tile",
        "Creative Arts Workshop for Elementary Students",
    ]

    assert rows[0].activity_type == "talk"
    assert rows[0].is_free is True
    assert rows[0].free_verification_status == "confirmed"
    assert rows[0].registration_required is True

    assert rows[1].activity_type == "workshop"
    assert rows[1].is_free is False
    assert rows[1].free_verification_status == "confirmed"
    assert rows[1].registration_required is True
    assert rows[1].start_at == datetime(2026, 3, 7, 13, 0)

    assert rows[2].title == "Creative Arts Workshop for Elementary Students"
    assert rows[2].is_free is False
    assert rows[2].free_verification_status == "confirmed"
    assert rows[2].age_min is None
    assert rows[2].age_max is None


def test_florence_griswold_parser_excludes_film_travel_camp_and_garden_events() -> None:
    payload = {
        "pages": [
            {
                "events": [
                    {
                        "title": "ART FILM AT THE KATE: Turner & Constable",
                        "url": "https://flogris.org/calendar/art-film-at-the-kate-turner-constable/",
                        "description": "<p>A screening at The Kate.</p>",
                        "excerpt": "",
                        "start_date": "2026-03-22 13:00:00",
                        "end_date": "2026-03-22 15:00:00",
                        "venue": [],
                        "categories": [{"name": "Film Events"}],
                    },
                    {
                        "title": "DAY TRIP TO NYC: Metropolitan Museum of Art",
                        "url": "https://flogris.org/calendar/day-trip-to-nyc-metropolitan-museum-of-art/",
                        "description": "<p>Travel to New York City for a museum visit.</p>",
                        "excerpt": "",
                        "start_date": "2026-04-13 07:00:00",
                        "end_date": "2026-04-13 19:00:00",
                        "venue": [],
                        "categories": [{"name": "Travel"}],
                    },
                    {
                        "title": "Wee Faerie Camp #1 2026",
                        "url": "https://flogris.org/calendar/wee-faerie-camp-1-2026/2026-06-16/",
                        "description": "<p>A three-day camp for children.</p>",
                        "excerpt": "",
                        "start_date": "2026-06-16 10:00:00",
                        "end_date": "2026-06-16 15:00:00",
                        "venue": [],
                        "categories": [{"name": "Camp"}],
                    },
                    {
                        "title": "Connecticut's Historic Gardens Day",
                        "url": "https://flogris.org/calendar/connecticuts-historic-gardens-day/",
                        "description": "<p>Garden tours, live music, scavenger hunts, and more.</p>",
                        "excerpt": "",
                        "start_date": "2026-06-28 12:00:00",
                        "end_date": "2026-06-28 16:00:00",
                        "venue": [],
                        "categories": [{"name": "Garden Events"}],
                    },
                ]
            }
        ]
    }

    assert parse_florence_griswold_events_payload(payload) == []
