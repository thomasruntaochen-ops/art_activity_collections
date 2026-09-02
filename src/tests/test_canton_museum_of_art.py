from __future__ import annotations

from datetime import date
from datetime import timedelta

from src.crawlers.adapters.canton_museum_of_art import parse_canton_museum_of_art_payload
from src.tests.fixture_helpers import assert_activity_smoke_rows


LISTING_HTML = """
    <html><body>
      <div class="grid-item">
        <h3 class="eventsummary"><a href="/node/11979">Elevate &amp; Create: Early Learning Through Art</a></h3>
        <div class="event-type">Class</div>
      </div>
    </body></html>
"""


def _detail(*, begin_text: str, days_text: str) -> str:
    return f"""
        <html><body>
          <h1 id="page-title">Elevate &amp; Create: Early Learning Through Art</h1>
          <div class="field-name-field-skill-level">Ages 3-under with Adult (siblings welcome)</div>
          <div class="field-name-body">A drawing and art making class for families with young children.</div>
          <div class="cuscls_data_begin">{begin_text}</div>
          <div class="cuscls_data_days">{days_text}</div>
          <div class="cuscls_data_room">Studio 1</div>
        </body></html>
    """


def _payload(*, begin_text: str, days_text: str) -> dict:
    return {
        "listing_html": LISTING_HTML,
        "detail_pages": {
            "https://www.cantonart.org/node/11979": _detail(begin_text=begin_text, days_text=days_text),
        },
    }


def _weekly_range(*, start_offset: int, weeks: int) -> tuple[str, str, list[date]]:
    """A Begins/Ends line whose series starts `start_offset` days from today."""
    start = date.today() + timedelta(days=start_offset)
    end = start + timedelta(weeks=weeks - 1)
    weekday_name = f"{start:%A}"
    begin_text = f"Begins {start:%m/%d/%Y}, Ends {end:%m/%d/%Y} Deadline to Register: {start:%m-%d-%Y}"
    days_text = f"{weeks} {weekday_name}s, 10:00 AM-11:00 AM"
    expected = [start + timedelta(weeks=index) for index in range(weeks)]
    return begin_text, days_text, expected


def test_series_already_under_way_still_yields_remaining_sessions() -> None:
    """The month-end regression: /calendar only shows the current month, so a
    class that started three weeks ago but runs for six was dropped entirely."""
    begin_text, days_text, expected = _weekly_range(start_offset=-21, weeks=6)

    rows = parse_canton_museum_of_art_payload(_payload(begin_text=begin_text, days_text=days_text))

    assert_activity_smoke_rows(rows)
    assert [row.start_at.date() for row in rows] == [day for day in expected if day >= date.today()]


def test_future_series_expands_every_session() -> None:
    begin_text, days_text, expected = _weekly_range(start_offset=7, weeks=4)

    rows = parse_canton_museum_of_art_payload(_payload(begin_text=begin_text, days_text=days_text))

    assert [row.start_at.date() for row in rows] == expected
    assert [row.end_at.hour for row in rows] == [11] * len(expected)


def test_finished_series_yields_nothing() -> None:
    begin_text, days_text, _ = _weekly_range(start_offset=-70, weeks=4)

    assert parse_canton_museum_of_art_payload(_payload(begin_text=begin_text, days_text=days_text)) == []


def test_no_class_dates_are_skipped() -> None:
    start = date.today() + timedelta(days=7)
    end = start + timedelta(weeks=3)
    skipped = start + timedelta(weeks=1)
    begin_text = (
        f"Begins {start:%m/%d/%Y}, Ends {end:%m/%d/%Y}, no class {skipped:%m/%d/%Y} "
        f"Deadline to Register: {start:%m-%d-%Y}"
    )
    days_text = f"4 {start:%A}s, 10:00 AM-11:00 AM"

    rows = parse_canton_museum_of_art_payload(_payload(begin_text=begin_text, days_text=days_text))

    session_days = [row.start_at.date() for row in rows]
    assert skipped not in session_days
    assert session_days == [start, start + timedelta(weeks=2), start + timedelta(weeks=3)]


def test_explicit_session_date_list_is_used_verbatim() -> None:
    first = date.today() + timedelta(days=10)
    second = first + timedelta(days=3)
    begin_text = f"Begins {first:%m/%d/%Y}, Ends {second:%m/%d/%Y} Deadline to Register: {first:%m-%d-%Y}"
    days_text = f"1 {first:%m/%d/%y},{second:%m/%d/%y}, 06:00 PM-08:00 PM"

    rows = parse_canton_museum_of_art_payload(_payload(begin_text=begin_text, days_text=days_text))

    assert [row.start_at.date() for row in rows] == [first, second]
    assert [row.start_at.hour for row in rows] == [18, 18]
