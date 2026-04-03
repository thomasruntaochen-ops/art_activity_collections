#!/usr/bin/env python
import argparse
import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.blanden import BLANDEN_CLASSES_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.blanden import load_blanden_payload  # noqa: E402
from src.crawlers.adapters.blanden import parse_blanden_html  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and parse Blanden Memorial Art Museum events.")
    parser.add_argument("--commit", action="store_true", help="When set, upsert parsed rows into MySQL.")
    args = parser.parse_args()

    async def _load() -> str:
        payload = await load_blanden_payload()
        return payload["html"]

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="blanden",
                source_url=BLANDEN_CLASSES_EVENTS_URL,
                load_payload=_load,
                parse_payload=parse_blanden_html,
                parser_name="blanden",
                adapter_type="blanden_events",
                parsed_label="Blanden rows",
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="blanden",
            source_url=BLANDEN_CLASSES_EVENTS_URL,
        ),
    )

    print(f"Total parsed rows: {summary.total_parsed}")
    if not args.commit:
        print("Dry run only. Pass --commit to write to DB.")


if __name__ == "__main__":
    asyncio.run(main())
