from datetime import datetime

from src.crawlers.adapters.ok_bundle import OK_VENUES_BY_SLUG
from src.crawlers.adapters.ok_bundle import parse_ok_events


def test_okcontemporary_prefers_course_schedule_over_gallery_hours() -> None:
    payload = {
        "occurrences": [
            {
                "title": "Watercolor!",
                "source_url": "https://oklahomacontemporary.org/course/watercolor/",
                "date_text": "Monday, April 6, 2026",
            }
        ],
        "detail_html_by_url": {
            "https://oklahomacontemporary.org/course/watercolor/": """
                <html>
                  <body>
                    <h1 class="text-balance wrap-break-word">Watercolor!</h1>
                    <p>Eight-Week Class</p>
                    <div class="col-span-full md:col-span-4 lg:col-span-3">
                      <p class="font-medium"><span class="capitalize">mondays, </span> Apr. 6 - Jun. 1<br/>6 - 8:30 p.m.</p>
                      <h3 class="heading-6">Age Requirements</h3>
                      <p>Ages 14+</p>
                      <h6>Cost</h6>
                      <p>$295</p>
                    </div>
                    <div class="prose md:text-lg">
                      <p>Beginners welcome. Explore watercolor techniques and color palettes.</p>
                    </div>
                    <footer>
                      <dl>
                        <dt>Wednesday-Monday:</dt>
                        <dd>11 a.m. - 6 p.m.</dd>
                      </dl>
                    </footer>
                  </body>
                </html>
            """
        },
    }

    rows = parse_ok_events(
        payload,
        venue=OK_VENUES_BY_SLUG["okcontemporary"],
        start_date="2026-04-01",
    )

    assert len(rows) == 1
    assert rows[0].title == "Watercolor!"
    assert rows[0].start_at == datetime(2026, 4, 6, 18, 0)
    assert rows[0].end_at == datetime(2026, 4, 6, 20, 30)
    assert rows[0].is_free is False
