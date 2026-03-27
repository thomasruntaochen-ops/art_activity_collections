#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.columbus_museum_of_art import COLUMBUS_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.columbus_museum_of_art import load_columbus_museum_of_art_payload  # noqa: E402
from src.crawlers.adapters.columbus_museum_of_art import parse_columbus_museum_of_art_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and parse Columbus Museum of Art upcoming activities.")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()

    payload = await load_columbus_museum_of_art_payload(max_pages=args.max_pages)
    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="columbus_museum_of_art",
                source_url=COLUMBUS_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_columbus_museum_of_art_payload,
                parser_name="run_columbus_museum_of_art_parser",
                adapter_type="columbus_museum_of_art_events",
                parsed_label="Columbus Museum of Art rows",
                empty_parse_details={"max_pages": args.max_pages},
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_columbus_museum_of_art_parser",
            source_url=COLUMBUS_EVENTS_URL,
            details={"max_pages": args.max_pages},
        ),
    )
    if args.commit:
        print(
            f"Commit summary: parsed={summary.total_parsed}, inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
