#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.massillon_museum import MASSILLON_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.massillon_museum import load_massillon_museum_payload  # noqa: E402
from src.crawlers.adapters.massillon_museum import parse_massillon_museum_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and parse Massillon Museum upcoming activities.")
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()

    payload = await load_massillon_museum_payload()
    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="massillon_museum",
                source_url=MASSILLON_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_massillon_museum_payload,
                parser_name="run_massillon_museum_parser",
                adapter_type="massillon_museum_events",
                parsed_label="Massillon Museum rows",
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_massillon_museum_parser",
            source_url=MASSILLON_EVENTS_URL,
        ),
    )
    if args.commit:
        print(
            f"Commit summary: parsed={summary.total_parsed}, inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
