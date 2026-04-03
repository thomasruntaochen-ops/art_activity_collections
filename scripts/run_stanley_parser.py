#!/usr/bin/env python
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.stanley import STANLEY_CALENDAR_URL  # noqa: E402
from src.crawlers.adapters.stanley import load_stanley_payload  # noqa: E402
from src.crawlers.adapters.stanley import parse_stanley_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and parse Stanley Museum of Art calendar events.")
    parser.add_argument("--max-pages", type=int, default=4)
    parser.add_argument("--commit", action="store_true", help="When set, upsert parsed rows into MySQL.")
    args = parser.parse_args()

    async def _load() -> dict:
        return await load_stanley_payload(max_pages=args.max_pages)

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="stanley",
                source_url=STANLEY_CALENDAR_URL,
                load_payload=_load,
                parse_payload=parse_stanley_payload,
                parser_name="stanley",
                adapter_type="stanley_events",
                parsed_label="Stanley rows",
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="stanley",
            source_url=STANLEY_CALENDAR_URL,
            details={"max_pages": args.max_pages},
        ),
    )

    print(f"Total parsed rows: {summary.total_parsed}")
    if not args.commit:
        print("Dry run only. Pass --commit to write to DB.")


if __name__ == "__main__":
    asyncio.run(main())
