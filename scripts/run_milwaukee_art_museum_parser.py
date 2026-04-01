#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.milwaukee_art_museum import MAM_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.milwaukee_art_museum import load_milwaukee_art_museum_payload  # noqa: E402
from src.crawlers.adapters.milwaukee_art_museum import parse_milwaukee_art_museum_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse Milwaukee Art Museum events from the Tribe events API. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--page-limit", type=int, default=None)
    parser.add_argument(
        "--commit",
        action="store_true",
        help="When set, upsert parsed rows into MySQL.",
    )
    args = parser.parse_args()

    payload = await load_milwaukee_art_museum_payload(page_limit=args.page_limit)
    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="milwaukee_art_museum_events",
                source_url=MAM_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_milwaukee_art_museum_payload,
                parser_name="run_milwaukee_art_museum_parser",
                adapter_type="milwaukee_art_museum_events",
                parsed_label="Milwaukee Art Museum rows",
                empty_parse_details={"page_limit": args.page_limit},
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_milwaukee_art_museum_parser",
            source_url=MAM_EVENTS_URL,
            details={"page_limit": args.page_limit},
        ),
    )

    if args.commit:
        print(
            "Committed Milwaukee Art Museum rows: "
            f"parsed={summary.total_parsed}, "
            f"inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, "
            f"unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
