from datetime import datetime

from src.crawlers.adapters.hunter import parse_hunter_payload


def test_hunter_parser_keeps_paid_and_free_qualifying_events() -> None:
    payload = {
        "events": [
            {
                "title": "Crafts and Coffee",
                "source_url": "https://www.huntermuseum.org/events/crafts-and-coffee-3",
                "date_text": "April 3, 2026",
                "category": "Experiences",
                "detail_html": """
                    <html><body>
                    <h1>Crafts and Coffee</h1>
                    <div class="text-rich-text is-event">
                      <p>Adults 18+ are invited to join us for an afternoon of art-making and conversation.</p>
                    </div>
                    <div class="event_info-item"><div class="event_info-label">Time</div><div class="event_info-value">2:00 pm - 4:00 pm</div></div>
                    <div class="event_info-item"><div class="event_info-label">Location</div><div class="event_info-value">Hunter Museum of American Art</div></div>
                    <div class="event_info-item"><div class="event_info-label">Pricing</div><div class="event_info-value">Not-yet members: $12 Members: $10 Free for EBT card holders</div></div>
                    </body></html>
                """,
            },
            {
                "title": "Art Wise: Invitational Artist Talks",
                "source_url": "https://www.huntermuseum.org/events/art-wise-invitational-artist-talks",
                "date_text": "April 9, 2026",
                "category": "Art Talks",
                "detail_html": """
                    <html><body>
                    <h1>Art Wise: Invitational Artist Talks</h1>
                    <div class="text-rich-text is-event">
                      <p>Join artists for an in-gallery discussion of their work. This event is free and open to the public.</p>
                    </div>
                    <div class="event_info-item"><div class="event_info-label">Time</div><div class="event_info-value">6:00PM- 7:00PM</div></div>
                    <div class="event_info-item"><div class="event_info-label">Location</div><div class="event_info-value">Hunter Museum of American Art</div></div>
                    </body></html>
                """,
            },
            {
                "title": "Art After Dark",
                "source_url": "https://www.huntermuseum.org/events/art-after-dark",
                "date_text": "April 11, 2026",
                "category": "Experiences",
                "detail_html": """
                    <html><body>
                    <h1>Art After Dark</h1>
                    <div class="text-rich-text is-event">
                      <p>Teens 12-18 are invited to join us for art-making, snacks, and exclusive after-hours access.</p>
                      <p>Free to teens 12-18 years old. Pre-registration and guardian permission form required.</p>
                    </div>
                    <div class="event_info-item"><div class="event_info-label">Time</div><div class="event_info-value">6:00 pm - 8:00 pm</div></div>
                    <div class="event_info-item"><div class="event_info-label">Location</div><div class="event_info-value">Downtown Public Library</div></div>
                    <div class="event_info-item"><div class="event_info-label">Pricing</div><div class="event_info-value">FREE</div></div>
                    </body></html>
                """,
            },
        ]
    }

    rows = parse_hunter_payload(payload)

    assert [row.title for row in rows] == [
        "Crafts and Coffee",
        "Art Wise: Invitational Artist Talks",
        "Art After Dark",
    ]

    assert rows[0].is_free is False
    assert rows[0].activity_type == "workshop"

    assert rows[1].is_free is True
    assert rows[1].activity_type == "lecture"

    assert rows[2].is_free is True
    assert rows[2].registration_required is True
    assert rows[2].age_min == 12
    assert rows[2].age_max == 18
    assert rows[2].start_at == datetime(2026, 4, 11, 18, 0)


def test_hunter_parser_excludes_yoga_social_and_performance_rows() -> None:
    payload = {
        "events": [
            {
                "title": "Artful Yoga",
                "source_url": "https://www.huntermuseum.org/events/artful-yoga-18",
                "date_text": "April 27, 2026",
                "category": "Experiences",
                "detail_html": """
                    <html><body>
                    <h1>Artful Yoga</h1>
                    <div class="text-rich-text is-event"><p>Yoga in the galleries.</p></div>
                    <div class="event_info-item"><div class="event_info-label">Time</div><div class="event_info-value">10:00 am - 11:00 am</div></div>
                    </body></html>
                """,
            },
            {
                "title": "Throwback Thursday",
                "source_url": "https://www.huntermuseum.org/events/throwback-thursday-60",
                "date_text": "May 1, 2026",
                "category": "Experiences",
                "detail_html": """
                    <html><body>
                    <h1>Throwback Thursday</h1>
                    <div class="text-rich-text is-event"><p>Enjoy free admission to the museum.</p></div>
                    <div class="event_info-item"><div class="event_info-label">Time</div><div class="event_info-value">4:00 pm - 8:00 pm</div></div>
                    </body></html>
                """,
            },
            {
                "title": "Vision+Verse: Brown Girls Can",
                "source_url": "https://www.huntermuseum.org/events/vision-verse-brown-girls-can",
                "date_text": "October 2, 2026",
                "category": "Performances",
                "detail_html": """
                    <html><body>
                    <h1>Vision+Verse: Brown Girls Can</h1>
                    <div class="text-rich-text is-event"><p>This event fuses poetry, rap, and conversation.</p></div>
                    <div class="event_info-item"><div class="event_info-label">Time</div><div class="event_info-value">6:00 pm - 7:00 pm</div></div>
                    <div class="event_info-item"><div class="event_info-label">Pricing</div><div class="event_info-value">FREE</div></div>
                    </body></html>
                """,
            },
        ]
    }

    assert parse_hunter_payload(payload) == []
