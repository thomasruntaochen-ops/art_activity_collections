import asyncio
from datetime import datetime

from src.crawlers.pipeline.runner import UpsertStats
from src.crawlers.pipeline.script_runner import EmptyCommitGuard
from src.crawlers.pipeline.script_runner import TargetRunSpec
from src.crawlers.pipeline.script_runner import run_targets
from src.crawlers.pipeline.types import ExtractedActivity


def _sample_row(title: str = "Sample Workshop") -> ExtractedActivity:
    return ExtractedActivity(
        source_url="https://example.org/events/sample-workshop",
        title=title,
        description="A sample activity.",
        venue_name="Example Museum",
        location_text="Example City, CA",
        city="Example City",
        state="CA",
        activity_type="workshop",
        age_min=None,
        age_max=None,
        drop_in=False,
        registration_required=False,
        start_at=datetime(2026, 3, 10, 12, 0),
        end_at=datetime(2026, 3, 10, 13, 0),
        timezone="America/Los_Angeles",
        free_verification_status="inferred",
    )


def test_run_targets_dry_run_skips_writes(monkeypatch) -> None:
    write_calls: list[tuple[str, list[ExtractedActivity], str]] = []

    def _fake_upsert(source_url: str, extracted: list[ExtractedActivity], *, adapter_type: str):
        write_calls.append((source_url, extracted, adapter_type))
        return extracted, UpsertStats(1, 1, 1, 0, 0)

    monkeypatch.setattr("src.crawlers.pipeline.script_runner.upsert_extracted_activities_with_stats", _fake_upsert)

    async def _load_payload() -> str:
        return "payload"

    summary = asyncio.run(
        run_targets(
            targets=[
                TargetRunSpec(
                    name="example",
                    source_url="https://example.org/events",
                    load_payload=_load_payload,
                    parse_payload=lambda payload: [_sample_row()],
                    parser_name="example_parser",
                    adapter_type="example_adapter",
                    parsed_label="rows",
                )
            ],
            commit=False,
        )
    )

    assert summary.total_parsed == 1
    assert summary.total_inserted == 0
    assert write_calls == []


def test_run_targets_commit_runs_before_commit_and_aggregates_stats(monkeypatch) -> None:
    before_commit_calls: list[str] = []
    write_calls: list[tuple[str, list[ExtractedActivity], str]] = []

    def _fake_upsert(source_url: str, extracted: list[ExtractedActivity], *, adapter_type: str):
        write_calls.append((source_url, extracted, adapter_type))
        return extracted, UpsertStats(
            input_rows=len(extracted),
            deduped_rows=len(extracted),
            inserted=2,
            updated=1,
            unchanged=0,
        )

    monkeypatch.setattr("src.crawlers.pipeline.script_runner.upsert_extracted_activities_with_stats", _fake_upsert)

    async def _load_payload() -> str:
        return "payload"

    summary = asyncio.run(
        run_targets(
            targets=[
                TargetRunSpec(
                    name="example",
                    source_url="https://example.org/events",
                    load_payload=_load_payload,
                    parse_payload=lambda payload: [_sample_row("Row 1"), _sample_row("Row 2")],
                    parser_name="example_parser",
                    adapter_type="example_adapter",
                    parsed_label="rows",
                    before_commit=lambda: before_commit_calls.append("called"),
                )
            ],
            commit=True,
        )
    )

    assert before_commit_calls == ["called"]
    assert len(write_calls) == 1
    assert summary.total_parsed == 2
    assert summary.total_inserted == 2
    assert summary.total_updated == 1
    assert summary.total_unchanged == 0


def test_run_targets_aggregate_empty_guard_allows_individual_empty_batches(monkeypatch) -> None:
    write_calls: list[tuple[str, list[ExtractedActivity], str]] = []

    def _fake_upsert(source_url: str, extracted: list[ExtractedActivity], *, adapter_type: str):
        write_calls.append((source_url, extracted, adapter_type))
        return extracted, UpsertStats(
            input_rows=len(extracted),
            deduped_rows=len(extracted),
            inserted=len(extracted),
            updated=0,
            unchanged=0,
        )

    monkeypatch.setattr("src.crawlers.pipeline.script_runner.upsert_extracted_activities_with_stats", _fake_upsert)

    async def _load_payload() -> str:
        return "payload"

    summary = asyncio.run(
        run_targets(
            targets=[
                TargetRunSpec(
                    name="empty",
                    source_url="https://example.org/empty",
                    load_payload=_load_payload,
                    parse_payload=lambda payload: [],
                    parser_name="empty_parser",
                    adapter_type="example_adapter",
                    parsed_label="rows",
                ),
                TargetRunSpec(
                    name="filled",
                    source_url="https://example.org/filled",
                    load_payload=_load_payload,
                    parse_payload=lambda payload: [_sample_row()],
                    parser_name="filled_parser",
                    adapter_type="example_adapter",
                    parsed_label="rows",
                ),
            ],
            commit=True,
            empty_commit_guard=EmptyCommitGuard(parser_name="combined_parser"),
        )
    )

    assert len(write_calls) == 2
    assert write_calls[0][1] == []
    assert write_calls[1][1][0].title == "Sample Workshop"
    assert summary.total_parsed == 1
