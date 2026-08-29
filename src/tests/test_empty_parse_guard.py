import asyncio

import pytest

from src.crawlers.pipeline.alerts import abort_commit_on_empty_parse
from src.crawlers.pipeline.candidates import (
    get_candidate_count,
    listing_was_recognized,
    record_candidate_count,
    record_listing_recognized,
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


def test_recognized_but_empty_listing_does_not_alert(capsys) -> None:
    """Fresno's workshops page is intact but lists nothing between seasons."""
    assert _call(candidates_found=0, listing_recognized=True) is False
    assert "found but empty" in capsys.readouterr().err


def test_unrecognized_empty_listing_still_alerts() -> None:
    # No container found and nothing counted: the page changed underneath us.
    with pytest.raises(SystemExit) as exc:
        _call(candidates_found=0, listing_recognized=False)
    assert exc.value.code == 2


def test_alert_details_carry_both_signals() -> None:
    captured = {}

    def _fake_alert(*, title, message, details):
        captured.update(details)

    import src.crawlers.pipeline.alerts as alerts

    original = alerts.send_crawler_alert
    alerts.send_crawler_alert = _fake_alert
    try:
        with pytest.raises(SystemExit):
            _call(candidates_found=0)
    finally:
        alerts.send_crawler_alert = original

    assert captured["candidates_found"] == 0
    assert captured["listing_recognized"] is False


def test_recognition_flag_resets_between_targets() -> None:
    reset_candidate_count()
    record_listing_recognized()
    assert listing_was_recognized() is True

    reset_candidate_count()
    assert listing_was_recognized() is False
