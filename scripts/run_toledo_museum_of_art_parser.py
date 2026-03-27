#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.toledo_museum_of_art import TOLEDO_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.toledo_museum_of_art import load_toledo_museum_of_art_payload  # noqa: E402
from src.crawlers.adapters.toledo_museum_of_art import parse_toledo_museum_of_art_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and parse Toledo Museum of Art upcoming activities.")
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()

    payload = await load_toledo_museum_of_art_payload()
    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="toledo_museum_of_art",
                source_url=TOLEDO_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_toledo_museum_of_art_payload,
                parser_name="run_toledo_museum_of_art_parser",
                adapter_type="toledo_museum_of_art_events",
                parsed_label="Toledo Museum of Art rows",
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_toledo_museum_of_art_parser",
            source_url=TOLEDO_EVENTS_URL,
        ),
    )
    if args.commit:
        print(
            f"Commit summary: parsed={summary.total_parsed}, inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
