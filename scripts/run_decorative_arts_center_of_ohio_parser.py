#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.decorative_arts_center_of_ohio import DECORATIVE_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.decorative_arts_center_of_ohio import DECORATIVE_LECTURES_URL  # noqa: E402
from src.crawlers.adapters.decorative_arts_center_of_ohio import load_decorative_arts_center_of_ohio_payload  # noqa: E402
from src.crawlers.adapters.decorative_arts_center_of_ohio import parse_decorative_arts_center_of_ohio_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and parse Decorative Arts Center of Ohio activities.")
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()

    payload = await load_decorative_arts_center_of_ohio_payload()
    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="decorative_arts_center_of_ohio",
                source_url=DECORATIVE_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_decorative_arts_center_of_ohio_payload,
                parser_name="run_decorative_arts_center_of_ohio_parser",
                adapter_type="decorative_arts_center_of_ohio_events",
                parsed_label="Decorative Arts Center of Ohio rows",
                empty_parse_details={"event_links": [DECORATIVE_EVENTS_URL, DECORATIVE_LECTURES_URL]},
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_decorative_arts_center_of_ohio_parser",
            source_url=DECORATIVE_EVENTS_URL,
            details={"event_links": [DECORATIVE_EVENTS_URL, DECORATIVE_LECTURES_URL]},
        ),
    )
    if args.commit:
        print(
            f"Commit summary: parsed={summary.total_parsed}, inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
