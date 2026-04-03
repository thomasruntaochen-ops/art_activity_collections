#!/usr/bin/env python
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.des_moines_art_center import DMAC_CALENDAR_URL  # noqa: E402
from src.crawlers.adapters.des_moines_art_center import load_des_moines_art_center_payload  # noqa: E402
from src.crawlers.adapters.des_moines_art_center import parse_des_moines_art_center_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and parse Des Moines Art Center calendar events.")
    parser.add_argument("--max-pages", type=int, default=4)
    parser.add_argument("--commit", action="store_true", help="When set, upsert parsed rows into MySQL.")
    args = parser.parse_args()

    async def _load() -> dict:
        return await load_des_moines_art_center_payload(max_pages=args.max_pages)

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="des_moines_art_center",
                source_url=DMAC_CALENDAR_URL,
                load_payload=_load,
                parse_payload=parse_des_moines_art_center_payload,
                parser_name="des_moines_art_center",
                adapter_type="des_moines_art_center_events",
                parsed_label="DMAC rows",
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="des_moines_art_center",
            source_url=DMAC_CALENDAR_URL,
            details={"max_pages": args.max_pages},
        ),
    )

    print(f"Total parsed rows: {summary.total_parsed}")
    if not args.commit:
        print("Dry run only. Pass --commit to write to DB.")


if __name__ == "__main__":
    asyncio.run(main())
