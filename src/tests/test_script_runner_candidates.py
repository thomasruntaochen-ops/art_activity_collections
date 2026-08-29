import asyncio
from datetime import datetime

import pytest

from src.crawlers.pipeline.candidates import record_candidate_count
from src.crawlers.pipeline.runner import UpsertStats
from src.crawlers.pipeline.script_runner import EmptyCommitGuard, TargetRunSpec, run_targets
from src.crawlers.pipeline.types import ExtractedActivity


def _row(title: str) -> ExtractedActivity:
    return ExtractedActivity(
        source_url=f"https://example.org/events/{title}",
        title=title,
        description=None,
        venue_name="Example Museum",
        location_text="Example City, CA",
        city="Example City",
        state="CA",
        activity_type="workshop",
        age_min=None,
        age_max=None,
        drop_in=False,
        registration_required=False,
        start_at=datetime(2099, 3, 10, 12, 0),
        end_at=None,
        timezone="America/Los_Angeles",
        is_free=True,
        free_verification_status="inferred",
    )


async def _payload() -> str:
    return "payload"


def _spec(name: str, parse) -> TargetRunSpec:
    return TargetRunSpec(
        name=name,
        source_url=f"https://example.org/{name}",
        load_payload=_payload,
        parse_payload=parse,
        parser_name=f"{name}_parser",
        adapter_type=f"{name}_adapter",
        parsed_label=f"{name} rows",
    )


@pytest.fixture
def writes(monkeypatch) -> list[str]:
    calls: list[str] = []

    def _fake_upsert(source_url: str, extracted, *, adapter_type: str):
        calls.append(source_url)
        return extracted, UpsertStats(len(extracted), len(extracted), len(extracted), 0, 0)

    monkeypatch.setattr(
        "src.crawlers.pipeline.script_runner.upsert_extracted_activities_with_stats",
        _fake_upsert,
    )
    return calls


def _quiet(payload):
    """Healthy parser: saw candidates, filtered them all out."""
    record_candidate_count(9)
    return []


def _productive(payload):
    record_candidate_count(4)
    return [_row("kept")]


def test_quiet_target_does_not_block_a_sibling_from_committing(writes) -> None:
    asyncio.run(
        run_targets(
            targets=[_spec("quiet", _quiet), _spec("productive", _productive)],
            commit=True,
        )
    )

    # The quiet target is skipped; the productive one still writes.
    assert writes == ["https://example.org/productive"]


def test_dead_target_still_aborts_the_run(writes) -> None:
    def _dead(payload):
        record_candidate_count(0)
        return []

    with pytest.raises(SystemExit) as exc:
        asyncio.run(
            run_targets(targets=[_spec("dead", _dead), _spec("productive", _productive)], commit=True)
        )

    assert exc.value.code == 2
    assert writes == []


def test_aggregate_guard_skips_commit_on_a_quiet_bundle(writes) -> None:
    summary = asyncio.run(
        run_targets(
            targets=[_spec("quiet", _quiet)],
            commit=True,
            empty_commit_guard=EmptyCommitGuard(parser_name="bundle_parser"),
        )
    )

    assert writes == []
    assert summary.total_parsed == 0


def test_counts_do_not_leak_between_targets(writes) -> None:
    def _unreported(payload):
        return []

    # `quiet` reports 9; `unreported` reports nothing and must not inherit it,
    # so it falls back to the alerting path.
    with pytest.raises(SystemExit) as exc:
        asyncio.run(
            run_targets(targets=[_spec("quiet", _quiet), _spec("unreported", _unreported)], commit=True)
        )

    assert exc.value.code == 2
