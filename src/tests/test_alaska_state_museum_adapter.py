from datetime import datetime

from src.crawlers.adapters.alaska_state_museum import ASM_EVENTS_URL
from src.crawlers.adapters.alaska_state_museum import parse_alaska_state_museum_payload


def _events_page(*, heading: str, schedule: str) -> str:
    return f"""
        <html><head><meta name="DC.Date.Modified" content="Oct 15, 2099" /></head><body>
          <div class="s-lib-box">
            <h2 class="s-lib-box-title">{heading}</h2>
            <div class="s-lib-box-content">
              <p><strong>{schedule}<br />Alaska State Museum</strong></p>
              <p>Each session, a different Juneau guest artist will share their sketching process, then we sketch!</p>
              <p>Free for members, donation-based for non-members. All donations are accepted.</p>
            </div>
          </div>
        </body></html>
    """


def test_alaska_expands_every_second_and_fourth_saturday_series() -> None:
    # Oct 2026 the series was renamed "Sketching in the Museum" and listed as a
    # recurring schedule instead of a single date.
    payload = {
        "pages": {
            ASM_EVENTS_URL: _events_page(
                heading="Sketching in the Museum returns October 10",
                schedule="Every 2nd &amp; 4th Saturday at 1:30 pm<br />October 10, 2099 through December 2099",
            )
        }
    }

    rows = parse_alaska_state_museum_payload(payload)

    assert {row.title for row in rows} == {"Sketching in the Museum"}
    assert [row.start_at for row in rows] == [
        datetime(2099, 10, 10, 13, 30),
        datetime(2099, 10, 24, 13, 30),
        datetime(2099, 11, 14, 13, 30),
        datetime(2099, 11, 28, 13, 30),
        datetime(2099, 12, 12, 13, 30),
        datetime(2099, 12, 26, 13, 30),
    ]
    assert rows[0].is_free is None
