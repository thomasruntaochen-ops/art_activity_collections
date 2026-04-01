#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.leigh_yawkey_woodson import LYWAM_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.leigh_yawkey_woodson import load_leigh_yawkey_woodson_payload  # noqa: E402
from src.crawlers.adapters.leigh_yawkey_woodson import parse_leigh_yawkey_woodson_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse Leigh Yawkey Woodson Art Museum events from the MEC REST API. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--page-limit", type=int, default=1)
    parser.add_argument(
        "--commit",
        action="store_true",
        help="When set, upsert parsed rows into MySQL.",
    )
    args = parser.parse_args()

    payload = await load_leigh_yawkey_woodson_payload(page_limit=args.page_limit)
    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="leigh_yawkey_woodson_events",
                source_url=LYWAM_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_leigh_yawkey_woodson_payload,
                parser_name="run_leigh_yawkey_woodson_parser",
                adapter_type="leigh_yawkey_woodson_events",
                parsed_label="Leigh Yawkey Woodson rows",
                empty_parse_details={"page_limit": args.page_limit},
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_leigh_yawkey_woodson_parser",
            source_url=LYWAM_EVENTS_URL,
            details={"page_limit": args.page_limit},
        ),
    )

    if args.commit:
        print(
            "Committed Leigh Yawkey Woodson rows: "
            f"parsed={summary.total_parsed}, "
            f"inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, "
            f"unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
