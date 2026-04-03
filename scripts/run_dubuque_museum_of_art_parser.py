#!/usr/bin/env python
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.dubuque_museum_of_art import DUBUQUE_EXPERIENCES_URL  # noqa: E402
from src.crawlers.adapters.dubuque_museum_of_art import load_dubuque_museum_of_art_payload  # noqa: E402
from src.crawlers.adapters.dubuque_museum_of_art import parse_dubuque_museum_of_art_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and parse Dubuque Museum of Art experiences.")
    parser.add_argument("--commit", action="store_true", help="When set, upsert parsed rows into MySQL.")
    args = parser.parse_args()

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="dubuque_museum_of_art",
                source_url=DUBUQUE_EXPERIENCES_URL,
                load_payload=load_dubuque_museum_of_art_payload,
                parse_payload=parse_dubuque_museum_of_art_payload,
                parser_name="dubuque_museum_of_art",
                adapter_type="dubuque_museum_of_art_events",
                parsed_label="Dubuque rows",
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="dubuque_museum_of_art",
            source_url=DUBUQUE_EXPERIENCES_URL,
        ),
    )

    print(f"Total parsed rows: {summary.total_parsed}")
    if not args.commit:
        print("Dry run only. Pass --commit to write to DB.")


if __name__ == "__main__":
    asyncio.run(main())
