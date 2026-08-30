from collections.abc import Iterable
from datetime import datetime
from datetime import timedelta
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


def future_datetime(days: int = 30, *, hour: int = 10, minute: int = 0) -> datetime:
    """Return a datetime far enough ahead of today to survive parser date filters.

    Parsers drop events that already started, so fixtures need dates that move
    with the clock instead of a hardcoded calendar day that rots into the past.
    """
    moment = datetime.now() + timedelta(days=days)
    return moment.replace(hour=hour, minute=minute, second=0, microsecond=0)


def future_datetime_text(days: int = 30, *, hour: int = 10, minute: int = 0) -> str:
    """Return :func:`future_datetime` as the ``YYYY-MM-DD HH:MM:SS`` text feeds use."""
    return future_datetime(days, hour=hour, minute=minute).strftime("%Y-%m-%d %H:%M:%S")
