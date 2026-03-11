from datetime import datetime

from src.crawlers.adapters.crocker import parse_crocker_events_html
from src.tests.fixture_helpers import assert_activity_smoke_rows
from src.tests.fixture_helpers import read_fixture


def test_crocker_fixture_smoke() -> None:
    rows = assert_activity_smoke_rows(
        parse_crocker_events_html(
            read_fixture("crocker", "events.html"),
            list_url="https://www.crockerart.org/events",
            now=datetime(2026, 3, 10),
        )
    )

    assert len(rows) == 29
    assert rows[0].title == "Wee Wednesday"
    assert rows[0].start_at == datetime(2026, 3, 11, 10, 30)
