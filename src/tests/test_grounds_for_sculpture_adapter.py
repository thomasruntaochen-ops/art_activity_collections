from datetime import datetime

from src.crawlers.adapters.grounds_for_sculpture import parse_grounds_for_sculpture_events_html

CALENDAR = "https://www.groundsforsculpture.org/Calendar/"


def _teaser(*, title: str, category: str, date_text: str, time_text: str) -> str:
    return f"""
        <div class="list-teaser">
          <a class="list-teaser__title" href="/event/{title.lower().replace(' ', '-')}">{title}</a>
          <span class="list-teaser__category">{category}</span>
          <div class="list-teaser__date"><span>{date_text}</span><span>{time_text}</span></div>
        </div>
    """


def test_grounds_for_sculpture_reads_weekday_prefixed_dates() -> None:
    # Since Oct 2026 the calendar writes "Saturday, October 17, 2026".
    html = _teaser(
        title="Tots For Sculpture",
        category="Family",
        date_text="Saturday, October 17, 2026",
        time_text="10:30 — 11:45AM",
    ) + _teaser(
        title="Quilting Workshop",
        category="Workshops",
        date_text="October 25, 2026",
        time_text="1:30 – 4:30PM",
    )

    rows = parse_grounds_for_sculpture_events_html(html, list_url=CALENDAR)

    assert [(row.title, row.start_at, row.end_at) for row in rows] == [
        ("Tots For Sculpture", datetime(2026, 10, 17, 10, 30), datetime(2026, 10, 17, 11, 45)),
        ("Quilting Workshop", datetime(2026, 10, 25, 13, 30), datetime(2026, 10, 25, 16, 30)),
    ]
