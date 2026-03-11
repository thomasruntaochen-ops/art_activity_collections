from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlparse

from src.crawlers.pipeline.types import ExtractedActivity

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures"


def read_fixture(*parts: str) -> str:
    return FIXTURE_ROOT.joinpath(*parts).read_text(encoding="utf-8")


def assert_activity_smoke_rows(rows: Iterable[ExtractedActivity]) -> list[ExtractedActivity]:
    materialized = list(rows)
    assert materialized

    seen: set[tuple[str, str, object]] = set()
    for row in materialized:
        parsed = urlparse(row.source_url)
        assert parsed.scheme in {"http", "https"}
        assert parsed.netloc
        assert row.title
        assert row.start_at is not None
        assert row.timezone
        assert row.venue_name
        assert row.city
        assert row.state

        key = (row.source_url, row.title, row.start_at)
        assert key not in seen
        seen.add(key)

    return materialized
