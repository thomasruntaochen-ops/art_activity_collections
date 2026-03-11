#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.nortonsimon import NORTON_SIMON_CLASSES_URL  # noqa: E402
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_FAMILY_URL  # noqa: E402
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_LECTURES_URL  # noqa: E402
from src.crawlers.adapters.nortonsimon import fetch_nortonsimon_events_json  # noqa: E402
from src.crawlers.adapters.nortonsimon import parse_nortonsimon_events_json  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


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

    run_specs: list[TargetRunSpec] = []
    for name, list_url, input_json in targets:
        async def load_payload(*, target_url: str = list_url, target_input: str | None = input_json) -> str:
            return await _load_payload(list_url=target_url, input_json=target_input)

        run_specs.append(
            TargetRunSpec(
                name=name,
                source_url=list_url,
                load_payload=load_payload,
                parse_payload=lambda payload, target_url=list_url: parse_nortonsimon_events_json(
                    payload,
                    list_url=target_url,
                ),
                parser_name=f"nortonsimon_{name}",
                adapter_type="nortonsimon_calendar",
                parsed_label=f"Norton Simon {name} activities",
            )
        )

    summary = await run_targets(
        targets=run_specs,
        commit=args.commit,
        json_ensure_ascii=False,
    )

    if args.commit:
        print(
            "Upsert complete: "
            f"inserted={summary.total_inserted} "
            f"updated={summary.total_updated} "
            f"unchanged={summary.total_unchanged}"
        )
    else:
        print(f"Total parsed Norton Simon activities: {summary.total_parsed}")


if __name__ == "__main__":
    asyncio.run(main())
