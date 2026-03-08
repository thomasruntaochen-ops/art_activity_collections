from datetime import datetime

from src.crawlers.adapters.fresno import parse_fresno_workshops_html


def test_fresno_parser_expands_future_workshop_dates() -> None:
    html = """
    <main>
      <h3>Oil Painting Workshop with Artist Mariah Calvert</h3>
      <p>BE INSPIRED! Learn in a recurring studio workshop.</p>
      <p>Registration is open for the following dates:</p>
      <p>March 7, 2026</p>
      <p>March 21, 2026</p>
      <p>May 2. 2026</p>
      <p>Click here to register.</p>
    </main>
    """

    rows = parse_fresno_workshops_html(
        html=html,
        list_url="https://fresnoartmuseum.org/workshop",
        now=datetime(2026, 3, 8),
    )

    assert len(rows) == 2
    assert [row.start_at for row in rows] == [
        datetime(2026, 3, 21),
        datetime(2026, 5, 2),
    ]
    assert all(row.registration_required is True for row in rows)
    assert all(row.title == "Oil Painting Workshop with Artist Mariah Calvert" for row in rows)
