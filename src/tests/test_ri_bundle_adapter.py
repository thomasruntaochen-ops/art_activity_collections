from datetime import datetime

from src.crawlers.adapters.ri_bundle import RI_VENUES_BY_SLUG
from src.crawlers.adapters.ri_bundle import parse_ri_events


def test_jamestown_parser_keeps_talk_and_excludes_poetry() -> None:
    payload = {
        "details": {
            "https://www.jamestownartcenter.org/events/artist-talk-franois-poisson": """
                <html>
                  <head>
                    <meta name="description" content="Ten Years of Observing the World Sunday, April 12, 2026 2-3 PM Free" />
                  </head>
                  <body>
                    <h1 class="entry-title">Artist Talk: Franois Poisson</h1>
                    <div class="blog-item-content">
                      <div class="sqs-html-content">
                        <p>Ten Years of Observing the World</p>
                        <p>Sunday, April 12, 2026</p>
                        <p>2-3 PM</p>
                        <p>Free</p>
                      </div>
                    </div>
                  </body>
                </html>
            """,
            "https://www.jamestownartcenter.org/events/jac-talk-poetry-reading": """
                <html>
                  <head>
                    <meta name="description" content="Sunday, April 19, 2026 4-6 pm Pay-what-you-can" />
                  </head>
                  <body>
                    <h1 class="entry-title">JAC Talk: Poetry Reading</h1>
                    <div class="blog-item-content">
                      <div class="sqs-html-content">
                        <p>Sunday, April 19, 2026</p>
                        <p>4-6 pm</p>
                        <p>Pay-what-you-can</p>
                      </div>
                    </div>
                  </body>
                </html>
            """,
        }
    }

    rows = parse_ri_events(payload, venue=RI_VENUES_BY_SLUG["jamestown"], start_date="2026-04-01")

    assert [row.title for row in rows] == ["Artist Talk: Franois Poisson"]
    assert rows[0].activity_type == "talk"
    assert rows[0].is_free is True
    assert rows[0].start_at == datetime(2026, 4, 12, 14, 0)


def test_newport_parser_marks_paid_event() -> None:
    payload = {
        "list_html": "",
        "details": {
            "https://newportartmuseum.org/events/building-a-career-in-the-arts/": """
                <html>
                  <body>
                    <h1 class="event-single-title">Building a Career in the Arts</h1>
                    <span class="event-date">April 2, 2026</span>
                    <span class="event-time">5:30 pm</span>
                    <span class="event-location">Ilgenfritz Gallery</span>
                    <div class="single-sidebar-event">
                      <div class="price">
                        <h5 class="price-discount-label">Non-member price</h5>
                        <p class="non-members-price">$25.00 Per person</p>
                      </div>
                    </div>
                    <div class="rich-text fc-rich-text-container wysiwyg">
                      <p>Join us for a conversation that brings together leaders of arts institutions.</p>
                    </div>
                  </body>
                </html>
            """
        },
    }

    rows = parse_ri_events(payload, venue=RI_VENUES_BY_SLUG["newport"], start_date="2026-04-01")

    assert len(rows) == 1
    assert rows[0].title == "Building a Career in the Arts"
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"
    assert rows[0].registration_required is True


def test_risd_parser_uses_calendar_rows_and_filters_performance() -> None:
    payload = {
        "months": [
            {
                "url": "https://risdmuseum.org/exhibitions-events/events",
                "html": """
                    <html>
                      <body>
                        <li class="calendar-view-day__row">
                          <a href="/exhibitions-events/events/open-studio-163">Open Studio</a>
                          <div class="field-content">Folding Pages</div>
                          Sunday, April 19 / 2:00 pm - 4:00 pm
                        </li>
                        <li class="calendar-view-day__row">
                          <a href="/exhibitions-events/events/observer-0">The Observer</a>
                          <div class="field-content">Performance</div>
                          Thursday, April 9 / 6:30 pm - 7:30 pm
                        </li>
                      </body>
                    </html>
                """,
            }
        ],
        "details": {
            "https://risdmuseum.org/exhibitions-events/events/open-studio-163": """
                <html>
                  <body>
                    <div class="field--name-field-place">RISD Museum</div>
                    <div class="field--name-field-description">
                      Open Studio invites visitors to make folded book art together.
                    </div>
                  </body>
                </html>
            """
        },
    }

    rows = parse_ri_events(payload, venue=RI_VENUES_BY_SLUG["risd"], start_date="2026-04-01")

    assert len(rows) == 1
    assert rows[0].title == "Open Studio"
    assert rows[0].activity_type == "workshop"
    assert rows[0].start_at == datetime(2026, 4, 19, 14, 0)


def test_warwick_parser_reads_jsonld_and_excludes_writing_workshop() -> None:
    payload = {
        "details": {
            "https://site.corsizio.com/event/698faad8c7f5e7ac87f1c16d": """
                <html>
                  <body>
                    <script type="application/ld+json">
                      {
                        "@context": "http://schema.org",
                        "@type": "Event",
                        "name": "Adventures in Acrylic",
                        "description": "Each month we explore acrylic painting techniques. Students aged 16+ are welcome.",
                        "startDate": "2026-05-14T18:30:00-04:00",
                        "endDate": "2026-05-14T20:30:00-04:00",
                        "location": {"@type": "Place", "name": "3259 Post Road, Warwick, RI 02886"},
                        "offers": {"@type": "Offer", "price": "45", "priceCurrency": "USD"}
                      }
                    </script>
                  </body>
                </html>
            """,
            "https://site.corsizio.com/event/696bbcb836890519ab72bdb6": """
                <html>
                  <body>
                    <script type="application/ld+json">
                      {
                        "@context": "http://schema.org",
                        "@type": "Event",
                        "name": "'Write to Grow' Writing Workshop (18+)",
                        "description": "A writing workshop for adults.",
                        "startDate": "2026-04-15T18:30:00-04:00",
                        "endDate": "2026-04-15T20:30:00-04:00",
                        "location": {"@type": "Place", "name": "3259 Post Road, Warwick, RI 02886"},
                        "offers": {"@type": "Offer", "price": "15", "priceCurrency": "USD"}
                      }
                    </script>
                  </body>
                </html>
            """,
        }
    }

    rows = parse_ri_events(payload, venue=RI_VENUES_BY_SLUG["warwick"], start_date="2026-04-01")

    assert len(rows) == 1
    assert rows[0].title == "Adventures in Acrylic"
    assert rows[0].is_free is False
    assert rows[0].age_min == 16
    assert rows[0].start_at == datetime(2026, 5, 14, 18, 30)
