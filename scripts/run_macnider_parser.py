#!/usr/bin/env python
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.macnider import MACNIDER_CALENDAR_URL  # noqa: E402
from src.crawlers.adapters.macnider import load_macnider_payload  # noqa: E402
from src.crawlers.adapters.macnider import parse_macnider_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and parse Charles H. MacNider Art Museum classes.")
    parser.add_argument("--commit", action="store_true", help="When set, upsert parsed rows into MySQL.")
    args = parser.parse_args()

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="macnider",
                source_url=MACNIDER_CALENDAR_URL,
                load_payload=load_macnider_payload,
                parse_payload=parse_macnider_payload,
                parser_name="macnider",
                adapter_type="macnider_events",
                parsed_label="MacNider rows",
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="macnider",
            source_url=MACNIDER_CALENDAR_URL,
        ),
    )

    print(f"Total parsed rows: {summary.total_parsed}")
    if not args.commit:
        print("Dry run only. Pass --commit to write to DB.")


if __name__ == "__main__":
    asyncio.run(main())
