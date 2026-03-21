#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.benton import BENTON_CALENDAR_URL  # noqa: E402
from src.crawlers.adapters.benton import load_benton_payload  # noqa: E402
from src.crawlers.adapters.benton import parse_benton_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


BENTON_SOURCE_URL_PREFIXES = (
    "https://benton.uconn.edu/%",
    "https://events.uconn.edu/benton/%",
    "https://events.uconn.edu/event/%",
)


def clear_benton_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [("William Benton Museum of Art", "Storrs", "CT")],
        )

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(BENTON_SOURCE_URL_PREFIXES[0]),
                    Source.base_url.like(BENTON_SOURCE_URL_PREFIXES[1]),
                    Source.name.like("benton_%"),
                )
            )
        ).all()

        activity_filter = or_(
            Activity.source_url.like(BENTON_SOURCE_URL_PREFIXES[0]),
            Activity.source_url.like(BENTON_SOURCE_URL_PREFIXES[1]),
            Activity.source_url.like(BENTON_SOURCE_URL_PREFIXES[2]),
        )
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


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse William Benton Museum of Art calendar events. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="When set, upsert parsed rows into MySQL.",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help=(
            "Delete all William Benton Museum of Art DB rows "
            "(activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_benton_entries()
        print(
            "Deleted William Benton rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    clear_completed = False

    def _clear_before_commit() -> None:
        nonlocal clear_completed
        if clear_completed or not args.clear:
            return
        deleted = clear_benton_entries()
        print(
            "Deleted William Benton rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    run_specs = [
        TargetRunSpec(
            name="benton_calendar",
            source_url=BENTON_CALENDAR_URL,
            load_payload=load_benton_payload,
            parse_payload=parse_benton_payload,
            parser_name="run_benton_parser",
            adapter_type="benton_calendar",
            parsed_label="Benton rows",
            before_commit=_clear_before_commit if args.clear else None,
        )
    ]

    summary = await run_targets(
        targets=run_specs,
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_benton_parser",
            source_url=BENTON_CALENDAR_URL,
        ),
    )

    if args.commit:
        print(
            "Committed Benton rows: "
            f"parsed={summary.total_parsed}, "
            f"inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, "
            f"unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
