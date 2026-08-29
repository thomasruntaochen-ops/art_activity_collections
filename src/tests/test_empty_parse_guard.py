import asyncio

import pytest

from src.crawlers.pipeline.alerts import abort_commit_on_empty_parse
from src.crawlers.pipeline.candidates import (
    get_candidate_count,
    record_candidate_count,
    reset_candidate_count,
)


def _call(**overrides):
    kwargs = dict(parser_name="run_demo_parser", commit_requested=True, parsed_count=0)
    kwargs.update(overrides)
    return abort_commit_on_empty_parse(**kwargs)


def test_alerts_when_parser_reported_no_candidates() -> None:
    # A dead parser: it saw nothing at all on the page.
    with pytest.raises(SystemExit) as exc:
        _call(candidates_found=0)
    assert exc.value.code == 2


def test_alerts_when_parser_did_not_report_at_all() -> None:
    # Unreported means unknown, so keep the original alert-on-empty behaviour.
    with pytest.raises(SystemExit) as exc:
        _call()
    assert exc.value.code == 2


def test_quiet_calendar_skips_commit_without_alerting(capsys) -> None:
    # Healthy parser, nothing matching the audience filters this month.
    assert _call(candidates_found=12) is False
    assert "12 candidate events" in capsys.readouterr().err


def test_commit_allowed_when_rows_were_parsed() -> None:
    assert _call(parsed_count=5, candidates_found=0) is True


def test_no_guard_applied_without_commit() -> None:
    assert _call(commit_requested=False, candidates_found=0) is True


def test_candidate_count_accumulates_and_resets() -> None:
    reset_candidate_count()
    assert get_candidate_count() is None

    record_candidate_count(7)
    record_candidate_count(3)
    assert get_candidate_count() == 10

    reset_candidate_count()
    assert get_candidate_count() is None


def test_counts_recorded_inside_asyncio_gather_survive() -> None:
    """Adapters record from inside gather (dixon, benton, the bundles).

    A ContextVar would copy into each Task and discard these on completion, so
    the count would come back None and the guard would raise a false alarm.
    """

    async def scan_pages() -> None:
        async def one_page() -> None:
            record_candidate_count(5)

        await asyncio.gather(one_page(), one_page(), one_page())

    reset_candidate_count()
    asyncio.run(scan_pages())

    assert get_candidate_count() == 15


def test_count_survives_across_separate_asyncio_run_calls() -> None:
    async def load() -> None:
        record_candidate_count(4)

    reset_candidate_count()
    asyncio.run(load())

    assert get_candidate_count() == 4
