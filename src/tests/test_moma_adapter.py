"""MoMA's calendar sits behind a Cloudflare challenge, so parsing is fixture-driven.

The fixture mirrors the live markup of www.moma.org/calendar/?happening_filter=For+kids
(day headings as <h2>, each event an <a href="/calendar/events/NNNNN"> whose text
carries title, time, room and category, with &nbsp; throughout).
"""

import importlib.util
from datetime import datetime
from pathlib import Path

import pytest

from src.crawlers.adapters.moma import MOMA_KIDS_CALENDAR_URL, parse_moma_events_html

FIXTURE = Path(__file__).parent / "fixtures" / "moma_kids_calendar.html"
NOW = datetime(2026, 9, 21, 10, 0)


def _load_runner():
    path = Path(__file__).resolve().parents[2] / "scripts" / "run_moma_parser.py"
    spec = importlib.util.spec_from_file_location("run_moma_parser", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_kids_calendar_rows_survive_nbsp_markup() -> None:
    rows = parse_moma_events_html(
        FIXTURE.read_text(encoding="utf-8"),
        audience="kids",
        list_url=MOMA_KIDS_CALENDAR_URL,
        now=NOW,
    )

    assert len(rows) == 2
    story_time, book_reading = rows

    # The <h2> day heading, not the anchor, carries the date.
    assert story_time.title == "Family Story Time"
    assert story_time.start_at == datetime(2026, 9, 23, 10, 30)
    assert story_time.source_url == "https://www.moma.org/calendar/events/11674"
    assert story_time.audience_segment == "kids"

    assert book_reading.title == "Family Book Reading"
    assert book_reading.start_at == datetime(2026, 9, 26, 11, 0)


def test_archive_fallback_reports_and_flags_stale_snapshots(tmp_path, capsys, monkeypatch) -> None:
    """A blocked live fetch falls back to a saved page; an old one must be called out."""
    runner = _load_runner()
    monkeypatch.setattr(runner, "ARCHIVE_DIR", tmp_path)
    monkeypatch.delenv(runner.RAWHTML_BASE_URL_ENV, raising=False)

    assert runner._load_archived_html(audience="kids") is None

    archive = tmp_path / "kids.html"
    archive.write_text(FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")

    loaded = runner._load_archived_html(audience="kids")
    assert loaded is not None
    html, path = loaded
    assert path == archive
    assert "Family Story Time" in html
    assert "[ALERT]" not in capsys.readouterr().out

    stale = (datetime.now().timestamp()) - (runner.STALE_ARCHIVE_DAYS + 1) * 86400
    import os

    os.utime(archive, (stale, stale))
    runner._load_archived_html(audience="kids")
    assert "[ALERT]" in capsys.readouterr().out
