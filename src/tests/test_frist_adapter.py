from datetime import datetime

from src.crawlers.adapters.frist import parse_frist_payload


def test_frist_parser_keeps_paid_free_and_family_programs() -> None:
    payload = {
        "events": [
            {
                "title": "Teen ARTlab: Words on Wear-Custom Hoodie",
                "source_url": "https://fristartmuseum.org/event/teen-artlab-words-on-wear-custom-hoodie/",
                "date_text": "Saturday, April 4, 2026",
                "time_text": "1:00-4:00 p.m.",
                "summary": "This workshop is SOLD OUT. In this hands-on workshop, participants create custom hoodies.",
                "tags": ["Teens"],
                "register_text": None,
                "detail_html": """
                    <html><body>
                    <div class="hero-bg-image__text">
                      <h1>Teen ARTlab: Words on Wear-Custom Hoodie</h1>
                      <div class="events-card__meta">
                        <span class="event-date">Saturday, April 4, 2026</span>
                        <span class="event-time">1:00-4:00 p.m.</span>
                        <p><span class="events-card__location">Studio A<br/>Free; sold out</span></p>
                      </div>
                    </div>
                    <main><div class="l-content"><div class="l-constrain--xtra-small">
                      <p>This hands-on workshop explores printmaking, painting, collage, and stitching techniques.</p>
                    </div></div></main>
                    </body></html>
                """,
            },
            {
                "title": "Adult ARTlab: Flower Glitch Acrylic Paintings",
                "source_url": "https://fristartmuseum.org/event/adult-artlab-flower-glitch-acrylic-paintings/",
                "date_text": "Sunday, April 19, 2026",
                "time_text": "1:30-4:30 p.m.",
                "summary": "Join artist Sisavanh Phouthavong Houghton to explore painting from glitched references.",
                "tags": ["Workshops"],
                "register_text": "Register",
                "detail_html": """
                    <html><body>
                    <div class="hero-bg-image__text">
                      <h1>Adult ARTlab: Flower Glitch Acrylic Paintings</h1>
                      <div class="events-card__meta">
                        <span class="event-date">Sunday, April 19, 2026</span>
                        <span class="event-time">1:30-4:30 p.m.</span>
                        <p><span class="events-card__location">Studio A<br/>$60 members; $80 not-yet-members; registration required</span></p>
                      </div>
                    </div>
                    <main><div class="l-content"><div class="l-constrain--xtra-small">
                      <p>This workshop guides students through acrylic painting fundamentals.</p>
                    </div></div></main>
                    </body></html>
                """,
            },
            {
                "title": "Family Sunday",
                "source_url": "https://fristartmuseum.org/event/family-sunday-18/",
                "date_text": "Sunday, April 12, 2026",
                "time_text": "12:00-5:30 p.m.",
                "summary": "Special kid-friendly programming, family tours, and multisensory gallery experiences.",
                "tags": ["Family Activity"],
                "register_text": None,
                "detail_html": """
                    <html><body>
                    <div class="hero-bg-image__text">
                      <h1>Family Sunday</h1>
                      <div class="events-card__meta">
                        <span class="event-date">Sunday, April 12, 2026</span>
                        <span class="event-time">12:00-5:30 p.m.</span>
                        <p><span class="events-card__location">Free for members and guests aged 18 and younger; gallery admission required for not-yet members</span></p>
                      </div>
                    </div>
                    <main><div class="l-content"><div class="l-constrain--xtra-small">
                      <p>Check out a drop-in Family ARTlab in Studio A and multisensory gallery experiences.</p>
                    </div></div></main>
                    </body></html>
                """,
            },
        ]
    }

    rows = parse_frist_payload(payload)

    assert [row.title for row in rows] == [
        "Teen ARTlab: Words on Wear-Custom Hoodie",
        "Family Sunday",
        "Adult ARTlab: Flower Glitch Acrylic Paintings",
    ]

    assert rows[0].is_free is True
    assert rows[0].registration_required is True
    assert rows[0].activity_type == "workshop"

    assert rows[1].drop_in is True
    assert rows[1].is_free is True

    assert rows[2].is_free is False
    assert rows[2].registration_required is True
    assert rows[2].location_text == "Studio A"
    assert rows[2].start_at == datetime(2026, 4, 19, 13, 30)


def test_frist_parser_excludes_music_tours_and_member_events() -> None:
    payload = {
        "events": [
            {
                "title": "Music in the Cafe: The Contrarian Ensemble",
                "source_url": "https://fristartmuseum.org/event/music-in-the-cafe/",
                "date_text": "Thursday, April 2, 2026",
                "time_text": "6:00-7:30 p.m.",
                "summary": "Acoustic music spanning centuries and genres.",
                "tags": ["Music"],
                "register_text": None,
                "detail_html": "<html><body><main><div class='l-content'><div class='l-constrain--xtra-small'><p>Music event.</p></div></div></main></body></html>",
            },
            {
                "title": "Architecture Tour",
                "source_url": "https://fristartmuseum.org/event/architecture-tour/",
                "date_text": "Saturday, April 4, 2026",
                "time_text": "3:30-4:30 p.m.",
                "summary": "Enjoy a guided tour of our landmark art deco building.",
                "tags": ["Tours"],
                "register_text": None,
                "detail_html": "<html><body><main><div class='l-content'><div class='l-constrain--xtra-small'><p>Tour event.</p></div></div></main></body></html>",
            },
            {
                "title": "Member Morning",
                "source_url": "https://fristartmuseum.org/event/member-morning/",
                "date_text": "Saturday, June 6, 2026",
                "time_text": "10:00-11:00 a.m.",
                "summary": "Member event.",
                "tags": ["Member Event"],
                "register_text": None,
                "detail_html": "<html><body><main><div class='l-content'><div class='l-constrain--xtra-small'><p>Member event.</p></div></div></main></body></html>",
            },
        ]
    }

    assert parse_frist_payload(payload) == []
