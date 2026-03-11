from datetime import datetime

import pytest

from src.crawlers.adapters.nortonsimon import NORTON_SIMON_CLASSES_URL
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_FAMILY_URL
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_LECTURES_URL
from src.crawlers.adapters.nortonsimon import parse_nortonsimon_events_json
from src.tests.fixture_helpers import assert_activity_smoke_rows
from src.tests.fixture_helpers import read_fixture

FIXTURE_NOW = datetime(2026, 3, 10)


@pytest.mark.parametrize(
    ("fixture_name", "list_url", "expected_count", "expected_first_title"),
    [
        ("family.json", NORTON_SIMON_FAMILY_URL, 3, "Capturing Nature: A Plein Air Painting Workshop"),
        (
            "lectures.json",
            NORTON_SIMON_LECTURES_URL,
            2,
            "Close Circles: A Portrait of Galka Scheyer as the Artist's Friend",
        ),
        ("classes.json", NORTON_SIMON_CLASSES_URL, 3, "Second Saturday Sketching"),
    ],
)
def test_nortonsimon_fixture_smoke(
    fixture_name: str,
    list_url: str,
    expected_count: int,
    expected_first_title: str,
) -> None:
    payload = read_fixture("nortonsimon", fixture_name)

    rows = assert_activity_smoke_rows(
        parse_nortonsimon_events_json(payload, list_url=list_url, now=FIXTURE_NOW)
    )

    assert len(rows) == expected_count
    assert rows[0].title == expected_first_title

