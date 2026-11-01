from datetime import datetime

from src.crawlers.adapters.bmoa import BMOA_EVENTS_URL, parse_bmoa_events_html

# Trimmed from bmoa.org/events (Nov 2026): feature blocks up top put the title
# first and the date last; the dated calendar below had stopped at October.
EVENTS_HTML = """
<div class="row sqs-row">
  <div class="col sqs-col-4 span-4">
    <div class="sqs-block html-block sqs-block-html"><div class="sqs-html-content">
      <p><strong>Second Saturday is Family Day</strong><br/><em>Artist panel + art project<br/></em>Saturday, November 14</p>
    </div></div>
    <a href="/familyday">Learn more</a>
  </div>
  <div class="col sqs-col-4 span-4">
    <div class="sqs-block html-block sqs-block-html"><div class="sqs-html-content">
      <p><strong>First Friday</strong><br/><em>Free admission &amp; extended hours</em><br/>Friday, November 6</p>
    </div></div>
    <a href="https://www.bmoa.org/hours-admission#1stfri">Learn more</a>
  </div>
</div>
<div class="sqs-html-content">
  <p><strong>Saturday, October 3</strong> <a href="/saturday-studio">Saturday Studio</a> | watercolor painting | 11:30 AM - 1:30 PM</p>
  <p><strong>Saturday, November 7</strong> <a href="/saturday-studio">Saturday Studio</a> | sculpture | 11:30 AM - 1:30 PM</p>
</div>
"""

STUDIO_HTML = """
<div class="sqs-html-content">
  <h2><strong><em>Art Classes for Kids &amp; Teens Ages 6-16</em></strong></h2>
  <p><strong><em>Members: $25 per class</em><br/><em>Non-members: $40 per class</em></strong></p>
</div>
<div class="sqs-html-content">
  <h2>Sculpture &amp; Light</h2>
  <p>Saturday, November 7<br/>11:30 AM - 1:30 PM</p>
  <p>Students will use air-dry clay to build a lantern.</p>
</div>
"""


def test_feature_block_dates_are_read_when_the_calendar_lags() -> None:
    rows = parse_bmoa_events_html(EVENTS_HTML, list_url=BMOA_EVENTS_URL, now=datetime(2026, 11, 1))

    titles = {(row.title, row.start_at) for row in rows}
    assert ("Second Saturday is Family Day", datetime(2026, 11, 14)) in titles
    family_day = next(row for row in rows if row.title == "Second Saturday is Family Day")
    assert family_day.source_url == "https://www.bmoa.org/familyday"
    # First Friday is free admission, not an activity; October is past.
    assert all("First Friday" not in row.title for row in rows)
    assert all(row.start_at >= datetime(2026, 11, 1) for row in rows)


def test_studio_sessions_replace_the_calendar_entry_for_the_same_day() -> None:
    rows = parse_bmoa_events_html(
        EVENTS_HTML, list_url=BMOA_EVENTS_URL, now=datetime(2026, 11, 1), studio_html=STUDIO_HTML
    )

    studio = [row for row in rows if row.source_url.endswith("/saturday-studio")]
    assert len(studio) == 1
    session = studio[0]
    assert session.title == "Saturday Studio: Sculpture & Light"
    assert session.start_at == datetime(2026, 11, 7, 11, 30)
    assert session.end_at == datetime(2026, 11, 7, 13, 30)
    assert (session.age_min, session.age_max) == (6, 16)
    assert "$25 per class" in session.description
