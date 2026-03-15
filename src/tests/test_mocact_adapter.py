from datetime import datetime

from src.crawlers.adapters.mocact import parse_mocact_events_payload


def test_mocact_parser_keeps_paid_class_and_conversation() -> None:
    payload = {
        "pages": [
            {
                "events": [
                    {
                        "title": "Art Adventures",
                        "url": "https://mocact.org/events-calendar/art-adventures-4-2/2026-03-14/",
                        "description": (
                            "<p>Book Now: <a href=\"https://checkout.mocact.org/EventAvailability?EventId=401\">"
                            "checkout</a></p><p>Your child can join us every Saturday for a relaxed and inspiring "
                            "drop-in art class at the museum!</p><p>$25.00/per child<br />Ages 4+</p>"
                        ),
                        "excerpt": "",
                        "start_date": "2026-03-14 12:00:00",
                        "end_date": "2026-03-14 13:30:00",
                        "venue": {
                            "address": "19 Newtown Turnpike",
                            "city": "Westport",
                            "stateprovince": "CT",
                        },
                    },
                    {
                        "title": "Community Conversation: Jazz Inspirations",
                        "url": "https://mocact.org/events-calendar/community-conversation-jazz-inspirations/",
                        "description": (
                            "<p><a href=\"https://checkout.mocact.org/ChooseSeats/23601\">Register here</a>: "
                            "$10 general. $8 seniors + students, free for members</p>"
                            "<p>This engaging discussion brings together musicians, artists, educators, and scholars."
                            " This conversation will begin with a guitar performance.</p>"
                        ),
                        "excerpt": "",
                        "start_date": "2026-03-26 17:30:00",
                        "end_date": "2026-03-26 19:00:00",
                        "venue": {
                            "address": "19 Newtown Turnpike",
                            "city": "Westport",
                            "stateprovince": "CT",
                        },
                    },
                ]
            }
        ]
    }

    rows = parse_mocact_events_payload(payload)

    assert len(rows) == 2
    assert rows[0].title == "Art Adventures"
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"
    assert rows[0].registration_required is True
    assert rows[0].drop_in is True
    assert rows[0].age_min == 4
    assert rows[0].age_max is None
    assert rows[0].start_at == datetime(2026, 3, 14, 12, 0)

    assert rows[1].title == "Community Conversation: Jazz Inspirations"
    assert rows[1].activity_type == "talk"
    assert rows[1].is_free is False
    assert rows[1].free_verification_status == "confirmed"


def test_mocact_parser_excludes_tours_camps_and_writing() -> None:
    payload = {
        "pages": [
            {
                "events": [
                    {
                        "title": "Guided Tour of the Brubeck Collection (Members Only)",
                        "url": "https://mocact.org/events-calendar/guided-tour-of-the-brubeck-collection-members-only/",
                        "description": "<p>Explore the collection during a guided visit.</p>",
                        "excerpt": "",
                        "start_date": "2026-03-31 11:00:00",
                        "end_date": "2026-03-31 12:30:00",
                        "venue": {},
                    },
                    {
                        "title": "April 2026 School Recess Creativity Camp",
                        "url": "https://mocact.org/events-calendar/april-2026-school-recess-creativity-camp/",
                        "description": "<p>Camp description.</p>",
                        "excerpt": "",
                        "start_date": "2026-04-13 09:00:00",
                        "end_date": "2026-04-13 15:00:00",
                        "venue": {},
                    },
                    {
                        "title": "Westport Writers Workshop at MoCACT",
                        "url": "https://mocact.org/events-calendar/westport-writers-workshop/",
                        "description": "<p>A writing workshop for adults.</p>",
                        "excerpt": "",
                        "start_date": "2026-05-14 18:00:00",
                        "end_date": "2026-05-14 20:00:00",
                        "venue": {},
                    },
                ]
            }
        ]
    }

    assert parse_mocact_events_payload(payload) == []
