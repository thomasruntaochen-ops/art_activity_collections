import json
import os
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from typing import Awaitable
from typing import Callable

from src.crawlers.pipeline.alerts import abort_commit_on_empty_parse
from src.crawlers.pipeline.datetime_utils import normalize_extracted_activity_datetimes
from src.crawlers.pipeline.runner import UpsertStats
from src.crawlers.pipeline.runner import prune_expired_activities
from src.crawlers.pipeline.runner import upsert_extracted_activities_with_stats
from src.crawlers.pipeline.types import ExtractedActivity


def _json_ready(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key in ("start_at", "end_at"):
        value = out.get(key)
        if isinstance(value, datetime):
            out[key] = value.isoformat()
    return out


@dataclass(slots=True)
class TargetRunSpec:
    name: str
    source_url: str
    load_payload: Callable[[], Awaitable[Any]]
    parse_payload: Callable[[Any], list[ExtractedActivity]]
    parser_name: str
    adapter_type: str
    parsed_label: str
    before_commit: Callable[[], None] | None = None
    empty_parse_details: dict[str, Any] | None = None
    serialize_row: Callable[[ExtractedActivity], dict[str, Any]] | None = None


@dataclass(slots=True)
class EmptyCommitGuard:
    parser_name: str
    source_url: str | None = None
    details: dict[str, Any] | None = None


@dataclass(slots=True)
class TargetRunOutcome:
    spec: TargetRunSpec
    parsed: list[ExtractedActivity]
    written: list[ExtractedActivity]
    stats: UpsertStats | None


@dataclass(slots=True)
class RunTargetsSummary:
    outcomes: list[TargetRunOutcome]

    @property
    def total_parsed(self) -> int:
        return sum(len(outcome.parsed) for outcome in self.outcomes)

    @property
    def total_inserted(self) -> int:
        return sum(outcome.stats.inserted for outcome in self.outcomes if outcome.stats is not None)

    @property
    def total_updated(self) -> int:
        return sum(outcome.stats.updated for outcome in self.outcomes if outcome.stats is not None)

    @property
    def total_unchanged(self) -> int:
        return sum(outcome.stats.unchanged for outcome in self.outcomes if outcome.stats is not None)


async def run_targets(
    *,
    targets: list[TargetRunSpec],
    commit: bool,
    json_ensure_ascii: bool = True,
    empty_commit_guard: EmptyCommitGuard | None = None,
) -> RunTargetsSummary:
    outcomes: list[TargetRunOutcome] = []

    for target in targets:
        payload = await target.load_payload()
        parsed = [normalize_extracted_activity_datetimes(row) for row in target.parse_payload(payload)]

        print(f"Parsed {len(parsed)} {target.parsed_label}")
        for row in parsed:
            row_payload = (
                target.serialize_row(row)
                if target.serialize_row is not None
                else _json_ready(asdict(row))
            )
            print(json.dumps(row_payload, ensure_ascii=json_ensure_ascii))

        written = parsed
        stats: UpsertStats | None = None
        outcomes.append(
            TargetRunOutcome(
                spec=target,
                parsed=parsed,
                written=written,
                stats=stats,
            )
        )

    if commit:
        if empty_commit_guard is not None:
            abort_commit_on_empty_parse(
                parser_name=empty_commit_guard.parser_name,
                commit_requested=True,
                parsed_count=sum(len(outcome.parsed) for outcome in outcomes),
                source_url=empty_commit_guard.source_url,
                details=empty_commit_guard.details,
            )
        else:
            for outcome in outcomes:
                abort_commit_on_empty_parse(
                    parser_name=outcome.spec.parser_name,
                    commit_requested=True,
                    parsed_count=len(outcome.parsed),
                    source_url=outcome.spec.source_url,
                    details=outcome.spec.empty_parse_details,
                )

        for outcome in outcomes:
            if outcome.spec.before_commit is not None:
                outcome.spec.before_commit()
            outcome.written, outcome.stats = upsert_extracted_activities_with_stats(
                source_url=outcome.spec.source_url,
                extracted=outcome.parsed,
                adapter_type=outcome.spec.adapter_type,
            )

        if os.getenv("APP_ENV") == "production":
            pruned = prune_expired_activities(cutoff_days=5)
            if pruned:
                print(f"[prune] Deleted {pruned} activities with start_at > 5 days ago")

    return RunTargetsSummary(outcomes=outcomes)
