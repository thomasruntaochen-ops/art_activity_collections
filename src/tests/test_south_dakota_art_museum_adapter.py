from datetime import datetime

from src.crawlers.adapters.south_dakota_art_museum import parse_south_dakota_art_museum_payload


def test_south_dakota_art_museum_parser_follows_detail_pages() -> None:
    payload = {
        "occurrences": [
            {
                "title": "Community Art Day",
                "source_url": "https://www.sdstate.edu/node/310334",
                "summary": "Join us for another FREE Community Art Day!",
                "tags": ["Workshops/Training"],
            },
            {
                "title": "Community Mural Night",
                "source_url": "https://www.sdstate.edu/events/2026/04/community-mural-night",
                "summary": "Join us for a community mural paint night.",
                "tags": ["Health/Wellness", "Special Events"],
            },
        ],
        "detail_html_by_url": {
            "https://www.sdstate.edu/node/310334": """
                <html>
                  <body>
                    <div class="sds-ev__details--date">Saturday, April 11, 2026</div>
                    <div class="sds-ev__details--time">10:00 a.m. - 1:00 p.m.</div>
                    <div class="sds-ev__details--location">South Dakota Art Museum , SMU 0109 Multipurpose Room</div>
                    <div class="sds-ev__description">
                      <div>Join us for another FREE Community Art Day!</div>
                      <div>This month, we'll explore portraiture using collaged construction paper.</div>
                      <div>Come and go as you please.</div>
                    </div>
                  </body>
                </html>
            """,
            "https://www.sdstate.edu/events/2026/04/community-mural-night": """
                <html>
                  <body>
                    <div class="sds-ev__details--date">Tuesday, April 14, 2026</div>
                    <div class="sds-ev__details--time">6:00 p.m. - 8:00 p.m.</div>
                    <div class="sds-ev__details--location">South Dakota Art Museum , SMU 0109 Multipurpose Room</div>
                    <div class="sds-ev__description">
                      <div>Join us for a community mural paint night where you'll create your own tile.</div>
                      <div>Totally free. All are welcome and invited.</div>
                    </div>
                  </body>
                </html>
            """,
        },
    }

    rows = parse_south_dakota_art_museum_payload(payload, start_date="2026-04-01")

    assert [row.title for row in rows] == ["Community Art Day", "Community Mural Night"]
    assert rows[0].start_at == datetime(2026, 4, 11, 10, 0)
    assert rows[0].end_at == datetime(2026, 4, 11, 13, 0)
    assert rows[0].drop_in is True
    assert rows[0].is_free is True
    assert rows[1].start_at == datetime(2026, 4, 14, 18, 0)
    assert rows[1].end_at == datetime(2026, 4, 14, 20, 0)
    assert rows[1].is_free is True
