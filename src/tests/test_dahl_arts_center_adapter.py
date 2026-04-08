from datetime import datetime

from src.crawlers.adapters.dahl_arts_center import parse_dahl_arts_center_payload


def test_dahl_arts_center_parser_keeps_classes_and_expands_multi_dates() -> None:
    payload = {
        "html": """
            <html>
              <body>
                <table>
                  <tr>
                    <td class="wsite-multicol-col">
                      <h2 class="wsite-content-title">Saturday Art Adventure<br/>April 11, 18, 25 | 1-2:30</h2>
                      <div class="paragraph">
                        Students will explore color schemes, visual design, and creative problem-solving.
                        Saturday Art Adventure at the Dahl Arts Center is free to attend.
                      </div>
                      <a href="https://www.hisawyer.com/rapid-city-arts-council/schedules/activity-set/1844223">
                        free class - reserve your supplies
                      </a>
                    </td>
                  </tr>
                  <tr>
                    <td class="wsite-multicol-col">
                      <h2 class="wsite-content-title">Early Release Art Class<br/>May 1 | 1:30-4:30pm</h2>
                      <div class="paragraph">
                        Students will create their self-portrait in marker. Free to attend.
                      </div>
                      <a href="https://www.hisawyer.com/rapid-city-arts-council/schedules/activity-set/1801984">
                        reserve your seat
                      </a>
                    </td>
                  </tr>
                  <tr>
                    <td class="wsite-multicol-col">
                      <h2 class="wsite-content-title">Banff Centre Mountain Film Festival in Rapid City<br/>April 20-21 | 6:30pm</h2>
                      <div class="paragraph">
                        The festival brings together breathtaking cinematography.
                      </div>
                    </td>
                  </tr>
                </table>
              </body>
            </html>
        """
    }

    rows = parse_dahl_arts_center_payload(payload, start_date="2026-04-01")

    assert [row.title for row in rows] == [
        "Saturday Art Adventure",
        "Saturday Art Adventure",
        "Saturday Art Adventure",
        "Early Release Art Class",
    ]
    assert rows[0].start_at == datetime(2026, 4, 11, 13, 0)
    assert rows[0].end_at == datetime(2026, 4, 11, 14, 30)
    assert rows[0].registration_required is True
    assert rows[0].is_free is True
    assert rows[-1].start_at == datetime(2026, 5, 1, 13, 30)
    assert rows[-1].end_at == datetime(2026, 5, 1, 16, 30)
