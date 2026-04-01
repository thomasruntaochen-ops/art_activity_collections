from datetime import datetime

from src.crawlers.adapters.dixon import parse_dixon_payload


def test_dixon_parser_keeps_paid_and_recurring_art_programs() -> None:
    payload = {
        "events": [
            {
                "title": "Mini Masters (ages 2-4)",
                "source_url": "https://www.dixon.org/events/event/12916/2026/03/31",
                "listing_time_text": "10:30am -11:15am",
                "instance_date": datetime(2026, 3, 31).date(),
                "detail_html": """
                    <html><body>
                    <article class="relative">
                      <h1>Mini Masters (ages 2-4)</h1>
                      <p class="font-bold">Dixon Gallery and Gardens</p>
                      <p class="mb-1">Farnsworth Education Building 4339 Park Avenue Memphis, TN 38117</p>
                      <div class="pt-4 border-t border-black/10">
                        <p>Introduce your little ones to the arts and nature with crafts, movement, and more.</p>
                        <p>Registration required | Space limited $8 | Members free Click here to register.</p>
                      </div>
                      <div class="space-y-p5"><a>Youth and Family</a></div>
                      <time>10:30am -11:15am</time>
                    </article>
                    </body></html>
                """,
            },
            {
                "title": "Youth Workshop: Figure Drawing (ages 10-13)",
                "source_url": "https://www.dixon.org/events/event/93889/",
                "listing_time_text": "1:30pm -3:30pm",
                "instance_date": None,
                "detail_html": """
                    <html><body>
                    <article class="relative">
                      <h1>Youth Workshop: Figure Drawing (ages 10-13)</h1>
                      <div class="pt-4 border-t border-black/10">
                        <p>In this workshop participants will strengthen their drawing skills with a focus on the human figure.</p>
                        <p>$15 | MEMBERS $10</p>
                        <p>REGISTRATION REQUIRED | SPACE LIMITED</p>
                      </div>
                      <div class="space-y-p5"><a>Classes and Workshops</a></div>
                          <time>April 18, 2026</time>
                          <time>1:30pm -3:30pm</time>
                    </article>
                    </body></html>
                """,
            },
            {
                "title": "Munch and Learn",
                "source_url": "https://www.dixon.org/events/event/13492/2026/07/01",
                "listing_time_text": "12:00pm -1:00pm",
                "instance_date": datetime(2026, 7, 1).date(),
                "detail_html": """
                    <html><body>
                    <article class="relative">
                      <h1>Munch and Learn</h1>
                      <div class="pt-4 border-t border-black/10">
                        <p>Presentations by local artists, scholars, and Dixon staff sharing their knowledge on a variety of topics.</p>
                        <p>FREE</p>
                      </div>
                      <div class="space-y-p5"><a>Lectures</a></div>
                      <time>12:00pm -1:00pm</time>
                    </article>
                    </body></html>
                """,
            },
        ]
    }

    rows = parse_dixon_payload(payload)

    assert [row.title for row in rows] == [
        "Mini Masters (ages 2-4)",
        "Youth Workshop: Figure Drawing (ages 10-13)",
        "Munch and Learn",
    ]

    assert rows[0].start_at == datetime(2026, 3, 31, 10, 30)
    assert rows[0].end_at == datetime(2026, 3, 31, 11, 15)
    assert rows[0].registration_required is True
    assert rows[0].is_free is False
    assert rows[0].age_min == 2
    assert rows[0].age_max == 4

    assert rows[1].activity_type == "workshop"
    assert rows[1].is_free is False
    assert rows[1].age_min == 10
    assert rows[1].age_max == 13

    assert rows[2].activity_type == "lecture"
    assert rows[2].is_free is True


def test_dixon_parser_excludes_non_art_and_social_rows() -> None:
    payload = {
        "events": [
            {
                "title": "Art for All Festival",
                "source_url": "https://www.dixon.org/events/event/93886/",
                "listing_time_text": "11:00am -3:00pm",
                "instance_date": None,
                "detail_html": """
                    <html><body>
                    <article class="relative">
                      <h1>Art for All Festival</h1>
                      <div class="pt-4 border-t border-black/10">
                        <p>This celebration of creativity, culture, and community is a day filled with live performances and hands-on art activities.</p>
                        <p>FREE</p>
                      </div>
                      <div class="space-y-p5"><a>Special Events</a></div>
                      <time>May 9, 2026</time>
                      <time>11:00am -3:00pm</time>
                    </article>
                    </body></html>
                """,
            },
            {
                "title": "Project Grow (all ages)",
                "source_url": "https://www.dixon.org/events/event/51451/2026/05/09",
                "listing_time_text": "1:00pm -3:00pm",
                "instance_date": datetime(2026, 5, 9).date(),
                "detail_html": """
                    <html><body>
                    <article class="relative">
                      <h1>Project Grow (all ages)</h1>
                      <div class="pt-4 border-t border-black/10">
                        <p>Enjoy a free garden activity for all ages.</p>
                        <p>Drop-in program | Supplies included FREE</p>
                      </div>
                      <div class="space-y-p5"><a>Youth and Family</a><a>Gardens</a></div>
                      <time>1:00pm -3:00pm</time>
                    </article>
                    </body></html>
                """,
            },
            {
                "title": "Munch and Learn: Intro to AI and Its Use in Gardening",
                "source_url": "https://www.dixon.org/events/event/93938/",
                "listing_time_text": "12:00pm -1:00pm",
                "instance_date": None,
                "detail_html": """
                    <html><body>
                    <article class="relative">
                      <h1>Munch and Learn: Intro to AI and Its Use in Gardening</h1>
                      <div class="pt-4 border-t border-black/10">
                        <p>A lunch lecture about gardening technology.</p>
                        <p>FREE</p>
                      </div>
                      <div class="space-y-p5"><a>Lectures</a></div>
                      <time>June 17, 2026</time>
                      <time>12:00pm -1:00pm</time>
                    </article>
                    </body></html>
                """,
            },
            {
                "title": "Secrets in the Garden (ages 18+)",
                "source_url": "https://www.dixon.org/events/event/93964/2026/05/27",
                "listing_time_text": "6:00pm -8:00pm",
                "instance_date": datetime(2026, 5, 27).date(),
                "detail_html": """
                    <html><body>
                    <article class="relative">
                      <h1>Secrets in the Garden (ages 18+)</h1>
                      <div class="pt-4 border-t border-black/10">
                        <p>Your ticket includes two complimentary beverages and light refreshments.</p>
                        <p>$50 | MEMBERS $45</p>
                      </div>
                      <div class="space-y-p5"><a>Special Events</a><a>Performing Arts</a></div>
                      <time>6:00pm -8:00pm</time>
                    </article>
                    </body></html>
                """,
            },
        ]
    }

    assert parse_dixon_payload(payload) == []


def test_dixon_parser_marks_plus_ages_and_excludes_garden_lectures() -> None:
    payload = {
        "events": [
            {
                "title": "Stage & Sketch (ages 18+)",
                "source_url": "https://www.dixon.org/events/event/93940/",
                "listing_time_text": "6:00pm -8:00pm",
                "instance_date": None,
                "detail_html": """
                    <html><body>
                    <article class="relative">
                      <h1>Stage & Sketch (ages 18+)</h1>
                      <div class="pt-4 border-t border-black/10">
                        <p>Enjoy an evening of figure drawing with dynamic poses.</p>
                        <p>FREE</p>
                      </div>
                      <div class="space-y-p5"><a>Classes and Workshops</a><a>Adult Programs</a></div>
                      <time>April 9, 2026</time>
                      <time>6:00pm -8:00pm</time>
                    </article>
                    </body></html>
                """,
            },
            {
                "title": "Studio Courses with Creative Aging: Narrative Painting with Carol Buchman (ages 65+)",
                "source_url": "https://www.dixon.org/events/event/93956/2026/05/05",
                "listing_time_text": "1:00pm -3:00pm",
                "instance_date": datetime(2026, 5, 5).date(),
                "detail_html": """
                    <html><body>
                    <article class="relative">
                      <h1>Studio Courses with Creative Aging: Narrative Painting with Carol Buchman (ages 65+)</h1>
                      <div class="pt-4 border-t border-black/10">
                        <p>This is a 6-week course exploring narrative painting.</p>
                        <p>Register at creativeagingmidsouth.org</p>
                      </div>
                      <div class="space-y-p5"><a>Classes and Workshops</a><a>Senior</a></div>
                      <time>1:00pm -3:00pm</time>
                    </article>
                    </body></html>
                """,
            },
            {
                "title": "Munch and Learn: Clematis - The Divine Vine",
                "source_url": "https://www.dixon.org/events/event/93930/",
                "listing_time_text": "12:00pm -1:00pm",
                "instance_date": None,
                "detail_html": """
                    <html><body>
                    <article class="relative">
                      <h1>Munch and Learn: Clematis - The Divine Vine</h1>
                      <div class="pt-4 border-t border-black/10">
                        <p>A lunch lecture about cultivating clematis and other flowering shrubs.</p>
                      </div>
                      <div class="space-y-p5"><a>Lectures</a><a>Gardens</a></div>
                      <time>April 15, 2026</time>
                      <time>12:00pm -1:00pm</time>
                    </article>
                    </body></html>
                """,
            },
        ]
    }

    rows = parse_dixon_payload(payload)

    assert [row.title for row in rows] == [
        "Stage & Sketch (ages 18+)",
        "Studio Courses with Creative Aging: Narrative Painting with Carol Buchman (ages 65+)",
    ]
    assert rows[0].age_min == 18
    assert rows[1].age_min == 65
    assert rows[1].registration_required is True
