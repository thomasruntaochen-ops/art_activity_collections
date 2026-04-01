#!/usr/bin/env python
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.crma import (  # noqa: E402
    CRMA_CALENDAR_URL,
    MONTHS_AHEAD,
    fetch_crma_events,
    parse_crma_events,
)
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch and parse Cedar Rapids Museum of Art calendar events."
    )
    parser.add_argument(
        "--months-ahead",
        type=int,
        default=MONTHS_AHEAD,
        help=f"Months of events to fetch ahead of today (default: {MONTHS_AHEAD}).",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="When set, upsert parsed rows into MySQL.",
    )
    args = parser.parse_args()

    async def _load() -> list[dict]:
        return await fetch_crma_events(months_ahead=args.months_ahead)

    run_specs = [
        TargetRunSpec(
            name="crma",
            source_url=CRMA_CALENDAR_URL,
            load_payload=_load,
            parse_payload=parse_crma_events,
            parser_name="crma",
            adapter_type="crma_fullcalendar",
            parsed_label="CRMA rows",
        )
    ]

    summary = await run_targets(
        targets=run_specs,
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="crma",
            source_url=CRMA_CALENDAR_URL,
            details={"months_ahead": args.months_ahead},
        ),
    )

    print(f"Total parsed rows: {summary.total_parsed}")
    if not args.commit:
        print("Dry run only. Pass --commit to write to DB.")
        return

    for outcome in summary.outcomes:
        assert outcome.stats is not None
        print(
            f"MySQL write summary (crma): "
            f"input={outcome.stats.input_rows}, "
            f"deduped_input={outcome.stats.deduped_rows}, "
            f"inserted={outcome.stats.inserted}, "
            f"updated={outcome.stats.updated}, "
            f"unchanged={outcome.stats.unchanged}, "
            f"written={outcome.stats.written_rows}"
        )

    print(
        "MySQL write totals: "
        f"inserted={summary.total_inserted}, "
        f"updated={summary.total_updated}, "
        f"unchanged={summary.total_unchanged}, "
        f"written={summary.total_inserted + summary.total_updated}"
    )


if __name__ == "__main__":
    asyncio.run(main())
