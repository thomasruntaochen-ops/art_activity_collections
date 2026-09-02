"""The Branch Museum of Design, rebuilt on Drupal after leaving WordPress.

The listing repeats the same events across four quicktabs panes (All / Today /
This Week / Past), and the museum files its own closures under a "Free Event"
category -- so deduplication and rejecting closures are the two rules that
actually decide whether this venue produces sensible rows.
"""

from datetime import date
from datetime import datetime
from datetime import timedelta

from src.crawlers.adapters.branch_museum import parse_branch_museum_payload
from src.crawlers.pipeline.candidates import get_candidate_count
from src.crawlers.pipeline.candidates import listing_was_recognized
from src.crawlers.pipeline.candidates import reset_candidate_count


def _row(*, day: date, time_text: str, title: str, href: str, location: str = "") -> str:
    return f"""
      <li class="events-list-row">
        <div class="views-field views-field-nothing"><span class="field-content">
          <div class="all-search-prod-list">
            <div class="exi-eve-info">
              <div class="date_loc">
                <div class="date">{day:%B} {day.day}, {day:%Y} | {time_text}</div>
                <div class="loc">{location}</div>
              </div>
              <div class="col"><h2>{title}</h2></div>
              <div class="header-btn"><a class="tckt-btn" href="{href}">Details</a></div>
            </div>
          </div>
        </span></div>
      </li>
    """


def _listing(*panes: str) -> str:
    bodies = "".join(f'<div class="quicktabs-tabpage"><ul>{pane}</ul></div>' for pane in panes)
    return f'<html><body><div class="quicktabs-wrapper">{bodies}</div></body></html>'


def _detail(title: str, description: str) -> str:
    return f"""
        <html><body>
          <h1>{title}</h1>
          <div class="eventdetail-left"><div class="eventdetail-leftmain">
            <div class="eventdetail-right"><p>{description}</p></div>
          </div></div>
        </body></html>
    """


WORKSHOP_URL = "https://branchmuseum.org/drawing-right-side"
CLOSURE_URL = "https://branchmuseum.org/product/4541"
CONCERT_URL = "https://branchmuseum.org/tre-charles"


def _payload(*panes: str, details: dict[str, str] | None = None) -> dict:
    return {"listing_html": _listing(*panes), "detail_pages": details or {}}


def test_workshop_is_kept_and_closure_is_rejected() -> None:
    """A "Free Event" here is often the museum telling you it is shut that day."""
    soon = date.today() + timedelta(days=10)
    panes = _row(
        day=soon,
        time_text="6:00 PM",
        title="Agnes Grochulska presents Drawing From the Right Side of the Brain",
        href=f"{WORKSHOP_URL}?v=2149",
    ) + _row(
        day=soon + timedelta(days=1),
        time_text="10:00 AM",
        title="The Branch: Closed for a Private Event",
        href=f"{CLOSURE_URL}?v=5392",
    )

    rows = parse_branch_museum_payload(
        _payload(
            panes,
            details={
                WORKSHOP_URL: _detail(
                    "Drawing From the Right Side of the Brain",
                    "A hands-on drawing workshop with artist Agnes Grochulska. Ages 16+",
                ),
                CLOSURE_URL: _detail(
                    "Closed for a Private Event",
                    "The Branch is closed for a private event.",
                ),
            },
        )
    )

    assert [row.title for row in rows] == ["Drawing From the Right Side of the Brain"]
    assert rows[0].activity_type == "workshop"
    assert rows[0].age_min == 16
    assert rows[0].start_at == datetime(soon.year, soon.month, soon.day, 18, 0)


def test_concerts_are_rejected() -> None:
    soon = date.today() + timedelta(days=7)
    panes = _row(day=soon, time_text="6:00 PM", title="Live at The Branch: Tre. Charles", href=CONCERT_URL)

    rows = parse_branch_museum_payload(
        _payload(
            panes,
            details={
                CONCERT_URL: _detail(
                    "Live at The Branch: Tre Charles",
                    "Join us for a Live at The Branch concert with artist Tre. Charles.",
                )
            },
        )
    )

    assert rows == []


def test_events_repeated_across_panes_are_deduplicated() -> None:
    """"All", "Today" and "This Week" all render the same event."""
    soon = date.today() + timedelta(days=3)
    row = _row(day=soon, time_text="2:00 PM", title="Design Conversation", href=f"{WORKSHOP_URL}?v=1")
    # The same event, re-rendered with a different cache-buster in another pane.
    same_again = _row(day=soon, time_text="2:00 PM", title="Design Conversation", href=f"{WORKSHOP_URL}?v=999")

    rows = parse_branch_museum_payload(
        _payload(
            row,
            same_again,
            details={WORKSHOP_URL: _detail("Design Conversation", "An afternoon conversation about design.")},
        )
    )

    assert len(rows) == 1
    # The ?v= cache-buster must not reach the upsert identity, or the same event
    # would be inserted again every time the site bumped the number.
    assert rows[0].source_url == WORKSHOP_URL


def test_past_events_are_dropped() -> None:
    gone = date.today() - timedelta(days=30)
    panes = _row(day=gone, time_text="6:00 PM", title="Design Conversation", href=WORKSHOP_URL)

    rows = parse_branch_museum_payload(
        _payload(
            panes,
            details={WORKSHOP_URL: _detail("Design Conversation", "An afternoon conversation about design.")},
        )
    )

    assert rows == []


def test_empty_listing_reports_a_recognized_container() -> None:
    """Nothing booked is not the same as a dead parser, and must not alert."""
    reset_candidate_count()

    rows = parse_branch_museum_payload(_payload(""))

    assert rows == []
    assert listing_was_recognized() is True
    assert get_candidate_count() == 0


def test_candidates_are_reported_before_audience_filtering() -> None:
    """Two upcoming events, neither for our audience: quiet calendar, not breakage."""
    soon = date.today() + timedelta(days=5)
    panes = _row(day=soon, time_text="6:00 PM", title="Live at The Branch: Tre. Charles", href=CONCERT_URL) + _row(
        day=soon, time_text="10:00 AM", title="The Branch: Closed for a Private Event", href=CLOSURE_URL
    )

    reset_candidate_count()
    rows = parse_branch_museum_payload(
        _payload(
            panes,
            details={
                CONCERT_URL: _detail("Tre Charles", "A Live at The Branch concert."),
                CLOSURE_URL: _detail("Closed", "The Branch is closed for a private event."),
            },
        )
    )

    assert rows == []
    assert get_candidate_count() == 2


def test_missing_container_is_not_reported_as_recognized() -> None:
    """If the theme moves and the wrapper disappears, that must still alert."""
    reset_candidate_count()

    rows = parse_branch_museum_payload({"listing_html": "<html><body><p>nothing</p></body></html>"})

    assert rows == []
    assert listing_was_recognized() is False
