import json
from datetime import datetime

from src.crawlers.adapters.met import MET_WORKSHOPS_CLASSES_URL
from src.crawlers.adapters.met import parse_met_events_html
from src.crawlers.adapters.moma import MOMA_ADULTS_CALENDAR_URL
from src.crawlers.adapters.moma import MOMA_GENERAL_CALENDAR_URL
from src.crawlers.adapters.moma import parse_moma_events_html
from src.crawlers.adapters.whitney import WHITNEY_WORKSHOPS_URL
from src.crawlers.adapters.whitney import WHITNEY_TALKS_READINGS_URL
from src.crawlers.adapters.whitney import parse_whitney_events_html


def _json_ld_html(event: dict) -> str:
    return f"""
    <html>
      <head>
        <script type="application/ld+json">{json.dumps(event)}</script>
      </head>
    </html>
    """


def test_moma_adult_calendar_keeps_paid_studio_class() -> None:
    rows = parse_moma_events_html(
        _json_ld_html(
            {
                "@type": "Event",
                "name": "Drawing from the Collection",
                "url": "https://www.moma.org/calendar/events/12345",
                "startDate": "2026-06-12T18:00:00-04:00",
                "endDate": "2026-06-12T20:00:00-04:00",
                "description": "Join this studio class for adults.",
                "offers": {"price": "$35"},
            }
        ),
        audience="adults",
        list_url=MOMA_GENERAL_CALENDAR_URL,
    )

    assert MOMA_ADULTS_CALENDAR_URL == MOMA_GENERAL_CALENDAR_URL
    assert len(rows) == 1
    assert rows[0].title == "Drawing from the Collection"
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"
    assert rows[0].audience_segment == "adults"
    assert rows[0].start_at == datetime(2026, 6, 12, 18, 0)


def test_whitney_workshops_url_keeps_paid_adult_workshop() -> None:
    rows = parse_whitney_events_html(
        _json_ld_html(
            {
                "@type": "Event",
                "name": "Studio Workshop: Painting",
                "url": "https://whitney.org/events/studio-workshop-painting",
                "startDate": "2026-06-13T14:00:00-04:00",
                "description": "A hands-on workshop for adults.",
                "offers": {"price": "$20"},
            }
        ),
        list_url=WHITNEY_WORKSHOPS_URL,
    )

    assert "teen_events" not in WHITNEY_WORKSHOPS_URL
    assert len(rows) == 1
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"
    assert rows[0].audience_segment == "adults"


def test_whitney_talks_url_keeps_adult_talk() -> None:
    rows = parse_whitney_events_html(
        _json_ld_html(
            {
                "@type": "Event",
                "name": "Artist Talk: New Directions",
                "url": "https://whitney.org/events/artist-talk-new-directions",
                "startDate": "2026-06-20T14:00:00-04:00",
                "description": "A conversation with artists and writers.",
                "keywords": "Talks & readings",
            }
        ),
        list_url=WHITNEY_TALKS_READINGS_URL,
    )

    assert "talks_and_readings" in WHITNEY_TALKS_READINGS_URL
    assert len(rows) == 1
    assert rows[0].activity_type == "talk"
    assert rows[0].audience_segment == "adults"


def test_met_workshops_classes_url_keeps_paid_adult_fallback_row() -> None:
    html = """
    <html>
      <body>
        <h2>Friday, June 19</h2>
        <a href="https://engage.metmuseum.org/events/adult-drawing-workshop">Adult Drawing Workshop</a>
        <p>Studio class with an artist instructor.</p>
        <p>6:00 PM Ruth and Harold D. Uris Center for Education</p>
        <p>$25</p>
      </body>
    </html>
    """

    rows = parse_met_events_html(html, list_url=MET_WORKSHOPS_CLASSES_URL, now=datetime(2026, 6, 1))

    assert "audience=teens" not in MET_WORKSHOPS_CLASSES_URL
    assert "price=free" not in MET_WORKSHOPS_CLASSES_URL
    assert len(rows) == 1
    assert rows[0].title == "Adult Drawing Workshop"
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"
    assert rows[0].audience_segment == "adults"
    assert rows[0].start_at == datetime(2026, 6, 19, 18, 0)
