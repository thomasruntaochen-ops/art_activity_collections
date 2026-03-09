#!/usr/bin/env python3
import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.nortonsimon import NORTON_SIMON_CLASSES_URL  # noqa: E402
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_FAMILY_URL  # noqa: E402
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_LECTURES_URL  # noqa: E402
from src.crawlers.adapters.nortonsimon import fetch_nortonsimon_events_json  # noqa: E402
from src.crawlers.adapters.nortonsimon import parse_nortonsimon_events_json  # noqa: E402
from src.crawlers.pipeline.alerts import abort_commit_on_empty_parse  # noqa: E402
from src.crawlers.pipeline.runner import upsert_extracted_activities_with_stats  # noqa: E402


def _json_ready(row: dict) -> dict:
    out = dict(row)
    for key in ("start_at", "end_at"):
        value = out.get(key)
        if isinstance(value, datetime):
            out[key] = value.isoformat()
    return out


async def _load_payload(*, list_url: str, input_json: str | None) -> str:
    if input_json:
        input_path = Path(input_json)
        if not input_path.exists():
            print(f"Input JSON file not found: {input_path}")
            raise SystemExit(1)
        print(f"Loading Norton Simon payload from file: {input_path}")
        return input_path.read_text(encoding="utf-8")
    return await fetch_nortonsimon_events_json(list_url)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse Norton Simon Museum family, lecture, and adult art class events. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument(
        "--section",
        choices=["family", "lectures", "classes", "all"],
        default="all",
        help="Which Norton Simon event sections to parse (default: all).",
    )
    parser.add_argument("--family-url", default=NORTON_SIMON_FAMILY_URL)
    parser.add_argument("--lectures-url", default=NORTON_SIMON_LECTURES_URL)
    parser.add_argument("--classes-url", default=NORTON_SIMON_CLASSES_URL)
    parser.add_argument("--input-family-json", default=None)
    parser.add_argument("--input-lectures-json", default=None)
    parser.add_argument("--input-classes-json", default=None)
    parser.add_argument(
        "--commit",
        action="store_true",
        help="When set, upsert parsed rows into MySQL.",
    )
    args = parser.parse_args()

    targets: list[tuple[str, str, str | None]] = []
    if args.section in ("family", "all"):
        targets.append(("family", args.family_url, args.input_family_json))
    if args.section in ("lectures", "all"):
        targets.append(("lectures", args.lectures_url, args.input_lectures_json))
    if args.section in ("classes", "all"):
        targets.append(("classes", args.classes_url, args.input_classes_json))

    all_rows = []
    total_inserted = 0
    total_updated = 0
    total_unchanged = 0

    for name, list_url, input_json in targets:
        payload = await _load_payload(list_url=list_url, input_json=input_json)
        parsed = parse_nortonsimon_events_json(payload, list_url=list_url)
        print(f"Parsed {len(parsed)} Norton Simon {name} activities")
        for row in parsed:
            print(json.dumps(_json_ready(asdict(row)), ensure_ascii=False))

        all_rows.extend(parsed)
        abort_commit_on_empty_parse(
            parser_name=f"nortonsimon_{name}",
            commit_requested=args.commit,
            parsed_count=len(parsed),
            source_url=list_url,
        )

        if args.commit:
            _, stats = upsert_extracted_activities_with_stats(
                list_url,
                parsed,
                adapter_type="nortonsimon_calendar",
            )
            total_inserted += stats.inserted
            total_updated += stats.updated
            total_unchanged += stats.unchanged

    if args.commit:
        print(
            "Upsert complete: "
            f"inserted={total_inserted} updated={total_updated} unchanged={total_unchanged}"
        )
    else:
        print(f"Total parsed Norton Simon activities: {len(all_rows)}")


if __name__ == "__main__":
    asyncio.run(main())
