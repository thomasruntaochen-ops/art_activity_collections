import pytest
from datetime import datetime

from src.crawlers.adapters.bampfa import _fetch_with_curl, parse_bampfa_events_html


def test_bampfa_curl_fallback_reports_missing_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.crawlers.adapters.bampfa.shutil.which", lambda _: None)

    with pytest.raises(RuntimeError, match="'curl' is not installed"):
        _fetch_with_curl("https://bampfa.org/visit/calendar")


def test_bampfa_dedupes_same_title_and_start_with_url_variants() -> None:
    html = """
    <div class="views-field"><ul class="calendar_filter"><li>Art</li></ul></div>
    <div class="popupboxthing" data-popup="one">
      <div class="event-content">
        <div class="title"><a href="/event/open-art-lab-may-2026">Open: Art Lab</a></div>
      </div>
      <div class="event-summary">Hands-on artmaking.</div>
      <div class="event-information">Drop-in Art Making!</div>
      <div class="popupboxthing-date">Friday, May 15, 2026</div>
      <div class="popupboxthing-time"><strong>2 PM-5 PM</strong></div>
      <a class="add-to-cal-link" href="https://calendar.google.com/calendar/r/eventedit?dates=20260515T140000/20260515T170000">Google Calendar</a>
    </div>
    <div class="views-field"><ul class="calendar_filter"><li>Art</li></ul></div>
    <div class="popupboxthing" data-popup="two">
      <div class="event-content">
        <div class="title"><a href="/event/open-art-lab">Open: Art Lab</a></div>
      </div>
      <div class="event-summary">Hands-on artmaking.</div>
      <div class="event-information">Drop-in Art Making!</div>
      <div class="popupboxthing-date">Friday, May 15, 2026</div>
      <div class="popupboxthing-time"><strong>2 PM-5 PM</strong></div>
      <a class="add-to-cal-link" href="https://calendar.google.com/calendar/r/eventedit?dates=20260515T140000/20260515T170000">Google Calendar</a>
    </div>
    """

    rows = parse_bampfa_events_html(
        html,
        list_url="https://bampfa.org/visit/calendar",
        now=datetime(2026, 5, 1),
    )

    assert len(rows) == 1
    assert rows[0].title == "Open: Art Lab"
    assert rows[0].start_at == datetime(2026, 5, 15, 14, 0)
