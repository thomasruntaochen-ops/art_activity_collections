from datetime import datetime

from src.crawlers.adapters.clark import parse_clark_events_html


def _event_card(*, event_type: str, title: str, date_text: str, href: str = "/detail/1") -> str:
    return f"""
    <div>
      <div class="details">
        <a href="{href}">
          <div class="type">{event_type}</div>
          <div class="title">{title}</div>
          <div class="date">{date_text}</div>
        </a>
      </div>
    </div>
    """


def test_clark_parser_keeps_selected_categories() -> None:
    html = f"""
    <div id="event_list">
      {_event_card(event_type="Community Program", title="Juneteenth at the Clark", date_text="June 19, 2026", href="/detail/1")}
      {_event_card(event_type="Family Program", title="Community Day: Eye on Art!", date_text="July 12, 2026", href="/detail/2")}
      {_event_card(event_type="Lecture", title="Opening Lecture", date_text="June 13, 2026", href="/detail/3")}
      {_event_card(event_type="Nature Program", title="Foraging Walk", date_text="June 16, 2026", href="/detail/4")}
      {_event_card(event_type="Special Event", title="Drawing Closer: Ladies First", date_text="July 10, 2026", href="/detail/5")}
      {_event_card(event_type="Gallery Tour", title="Permanent Collection Gallery Tour", date_text="July 1, 2026", href="/detail/6")}
      {_event_card(event_type="Clark Society Event", title="Opening Reception: Giorgio Griffa", date_text="June 26, 2026", href="/detail/7")}
      {_event_card(event_type="Member Event", title="Member Exhibition Tour", date_text="July 24, 2026", href="/detail/8")}
      {_event_card(event_type="Special Event", title="Graduate Program Class of 2026 Symposium", date_text="June 5, 2026", href="/detail/9")}
      {_event_card(event_type="Special Event", title="Summer Opening Reception", date_text="June 12, 2026", href="/detail/10")}
    </div>
    """

    rows = parse_clark_events_html(html, list_url="https://events.clarkart.edu/")

    assert [row.title for row in rows] == [
        "Juneteenth at the Clark",
        "Community Day: Eye on Art!",
        "Opening Lecture",
        "Foraging Walk",
        "Drawing Closer: Ladies First",
    ]
    assert rows[1].start_at == datetime(2026, 7, 12)
    assert rows[1].activity_type == "workshop"
    assert rows[0].audience_segment == "all_ages"
    assert rows[1].audience_segment == "kids"
    assert rows[2].audience_segment == "adults"
    assert rows[1].venue_name == "The Clark Art Institute"
