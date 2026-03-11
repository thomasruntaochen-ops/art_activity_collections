from datetime import datetime

from src.crawlers.adapters.lacma import parse_lacma_events_html
from src.tests.fixture_helpers import assert_activity_smoke_rows
from src.tests.fixture_helpers import read_fixture


def test_lacma_fixture_smoke() -> None:
    rows = assert_activity_smoke_rows(
        parse_lacma_events_html(
            read_fixture("lacma", "events.html"),
            list_url="https://www.lacma.org/event",
            now=datetime(2026, 3, 10),
        )
    )

    assert len(rows) == 1
    assert rows[0].title == "Boone Children's Gallery: Pop Up Art Workshop"
    assert rows[0].activity_type == "workshop"

