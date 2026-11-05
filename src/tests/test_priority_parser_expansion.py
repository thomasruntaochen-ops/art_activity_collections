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
from src.crawlers.pipeline.candidates import get_candidate_count
from src.crawlers.pipeline.candidates import reset_candidate_count


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


def test_met_family_program_url_classifies_fallback_row_as_kids() -> None:
    html = """
    <html>
      <body>
        <h2>Friday, June 19</h2>
        <a href="https://engage.metmuseum.org/events/education/workshops-and-classes/family-programs/fy26/storytime-at-the-met/">
          Storytime at The Met
        </a>
        <p>Look, listen, sing, and have fun with picture-book readings connected to objects in The Met collection.</p>
        <p>10:15 AM The Met Fifth Avenue</p>
        <p>Free with Museum admission</p>
      </body>
    </html>
    """

    rows = parse_met_events_html(html, list_url=MET_WORKSHOPS_CLASSES_URL, now=datetime(2026, 6, 1))

    assert len(rows) == 1
    assert rows[0].title == "Storytime at The Met"
    assert rows[0].audience_segment == "kids"
    assert rows[0].is_free is True


def _met_calendar_page(*sources: dict) -> str:
    """One /events/calendar page: search hits inside a Next.js flight-data string."""
    hits = [
        {"_index": "events-1", "_id": f"id{n}", "_score": None, "_source": source, "sort": [n]}
        for n, source in enumerate(sources)
    ]
    flight = json.dumps({"results": hits}, ensure_ascii=False, separators=(",", ":"))
    return f"<script>self.__next_f.push([1,{json.dumps(flight, ensure_ascii=False)}])</script>"


def _met_source(title: str, start: str, end: str, *, audiences: list[str], url_slug: str) -> dict:
    return {
        "aceId": "1",
        "audiences": audiences,
        "endDate": end,
        "isPaid": False,
        "paid": "free",
        "location": '<p><a rel="noopener" href="https://maps.metmuseum.org/poi">81st Street Studio</a></p>',
        "programs": ["Free Family Programs|Ages 18 months-6 years"],
        "searchCategories": ["Family", "Workshops and Classes"],
        "startDate": start,
        "teaserText": "<p>Picture-book readings connected to the collection.&nbsp;</p>",
        "ticketRequired": True,
        "title": title,
        "url": f"https://engage.metmuseum.org/events/education/workshops-and-classes/family-programs/{url_slug}/",
    }


def test_met_reads_calendar_hits_across_pages() -> None:
    """Calendar hits end in "sort", not "highlight", and pages are joined before parsing."""
    storytime = _met_source(
        "Storytime at The Met—Color and Light",
        "2026-11-05T10:15:00-05:00",
        "2026-11-05T11:00:00-05:00",
        audiences=["Families"],
        url_slug="storytime",
    )
    painting = _met_source(
        "Thursday Painting Lab (Ages 5–8)",
        "2026-11-12T15:30:00-05:00",
        "2026-11-12T17:00:00-05:00",
        audiences=["Families"],
        url_slug="painting-lab",
    )
    # The same hit on two pages must not become two rows.
    html = "\n".join([_met_calendar_page(storytime), _met_calendar_page(storytime, painting)])

    reset_candidate_count()
    rows = parse_met_events_html(html, list_url=MET_WORKSHOPS_CLASSES_URL)

    assert get_candidate_count() == 3
    assert [row.title for row in rows] == [
        "Storytime at The Met—Color and Light",
        "Thursday Painting Lab (Ages 5–8)",
    ]
    assert rows[0].start_at == datetime(2026, 11, 5, 10, 15)
    assert rows[0].end_at == datetime(2026, 11, 5, 11, 0)
    assert rows[0].is_free is True
    assert rows[0].registration_required is True
    assert "Location: 81st Street Studio" in (rows[0].description or "")
    assert rows[1].age_min == 5 and rows[1].age_max == 8


def test_met_fallback_skips_events_without_a_date_heading() -> None:
    """The events landing page shows "NOVEMBER 5" cards with no weekday heading."""
    html = """
    <html>
      <body>
        <a href="https://engage.metmuseum.org/events/symposium">Register now</a>
        <a href="https://engage.metmuseum.org/events/painting-lab">Thursday Painting Lab (Ages 5–8)</a>
        <p>NOVEMBER 5</p>
        <p>3:30 PM</p>
      </body>
    </html>
    """

    assert parse_met_events_html(html, list_url=MET_WORKSHOPS_CLASSES_URL, now=datetime(2026, 11, 2)) == []


def test_met_calendar_page_count_follows_the_results_total() -> None:
    from src.crawlers.adapters.met import _calendar_page_count, _with_page_number

    assert _calendar_page_count("<p>20 of 177 results</p>", max_pages=12) == 9
    assert _calendar_page_count("<p>20 of 900 results</p>", max_pages=12) == 12
    assert _calendar_page_count("<p>no count here</p>", max_pages=12) == 1
    assert _with_page_number(MET_WORKSHOPS_CLASSES_URL, 3) == (
        "https://www.metmuseum.org/events/calendar?type=workshopsClasses&page=3"
    )
