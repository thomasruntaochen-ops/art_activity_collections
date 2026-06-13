#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.nortonsimon import NORTON_SIMON_CLASSES_URL  # noqa: E402
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_CITY  # noqa: E402
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_FAMILY_URL  # noqa: E402
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_LECTURES_URL  # noqa: E402
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_STATE  # noqa: E402
from src.crawlers.adapters.nortonsimon import NORTON_SIMON_VENUE_NAME  # noqa: E402
from src.crawlers.adapters.nortonsimon import fetch_nortonsimon_events_json  # noqa: E402
from src.crawlers.adapters.nortonsimon import parse_nortonsimon_events_json  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity, Source  # noqa: E402


NORTON_SIMON_SOURCE_URL_PREFIXES = (
    "https://www.nortonsimon.org/calendar/",
    "https://www.nortonsimon.org/events/",
)


def clear_nortonsimon_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [(NORTON_SIMON_VENUE_NAME, NORTON_SIMON_CITY, NORTON_SIMON_STATE)],
        )
        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.in_(
                        [
                            "https://www.nortonsimon.org",
                            NORTON_SIMON_FAMILY_URL,
                            NORTON_SIMON_LECTURES_URL,
                            NORTON_SIMON_CLASSES_URL,
                        ]
                    ),
                    Source.name == "www.nortonsimon.org",
                    Source.adapter_type == "nortonsimon_calendar",
                )
            )
        ).all()

        url_filters = [Activity.source_url.like(f"{prefix}%") for prefix in NORTON_SIMON_SOURCE_URL_PREFIXES]
        activity_filter = or_(*url_filters)
        if source_ids:
            activity_filter = or_(activity_filter, Activity.source_id.in_(source_ids))
        if venue_ids:
            activity_filter = or_(activity_filter, Activity.venue_id.in_(venue_ids))

        activity_ids = db.scalars(select(Activity.id).where(activity_filter)).all()
        if activity_ids:
            delete_tags_stmt = text(
                "DELETE FROM activity_tags WHERE activity_id IN :activity_ids"
            ).bindparams(bindparam("activity_ids", expanding=True))
            deleted_activity_tags = db.execute(delete_tags_stmt, {"activity_ids": activity_ids}).rowcount or 0
            deleted_activities = db.execute(
                delete(Activity).where(Activity.id.in_(activity_ids))
            ).rowcount or 0

        if source_ids:
            delete_runs_stmt = text(
                "DELETE FROM ingestion_runs WHERE source_id IN :source_ids"
            ).bindparams(bindparam("source_ids", expanding=True))
            deleted_ingestion_runs = db.execute(delete_runs_stmt, {"source_ids": source_ids}).rowcount or 0
            deleted_sources = db.execute(
                delete(Source).where(Source.id.in_(source_ids))
            ).rowcount or 0

        db.commit()

    return {
        "activity_tags": deleted_activity_tags,
        "activities": deleted_activities,
        "ingestion_runs": deleted_ingestion_runs,
        "sources": deleted_sources,
    }


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
    parser.add_argument(
        "--clear",
        action="store_true",
        help=(
            "Delete Norton Simon DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_nortonsimon_entries()
        print(
            "Deleted Norton Simon rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    if args.clear and args.commit:
        print("Clear requested with --commit; deletion is deferred until non-empty parse is validated.")

    clear_completed = False

    def _clear_before_commit() -> None:
        nonlocal clear_completed
        if clear_completed or not args.clear:
            return
        deleted = clear_nortonsimon_entries()
        print(
            "Deleted Norton Simon rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

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
                before_commit=_clear_before_commit if args.clear else None,
                empty_parse_details={"source": NORTON_SIMON_VENUE_NAME, "section": name},
            )
        )

    summary = await run_targets(
        targets=run_specs,
        commit=args.commit,
        json_ensure_ascii=False,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_nortonsimon_parser",
            details={"sections": [name for name, _list_url, _input_json in targets]},
        ),
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
