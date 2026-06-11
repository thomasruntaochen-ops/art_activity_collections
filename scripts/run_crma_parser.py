#!/usr/bin/env python
import argparse
import asyncio
import sys
from pathlib import Path

from sqlalchemy import bindparam
from sqlalchemy import delete
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.crma import (  # noqa: E402
    CRMA_CALENDAR_URL,
    CRMA_FC_EVENTS_URL,
    MONTHS_AHEAD,
    fetch_crma_events,
    parse_crma_events,
)
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


CRMA_SOURCE_PREFIX = "https://www.crma.org/events/"


def clear_crma_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [("Cedar Rapids Museum of Art", "Cedar Rapids", "IA")],
        )

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.name == "crma",
                    Source.name.like("crma_%"),
                    Source.adapter_type == "crma_fullcalendar",
                    Source.base_url == CRMA_CALENDAR_URL,
                    Source.base_url == CRMA_FC_EVENTS_URL,
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(f"{CRMA_SOURCE_PREFIX}%")
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
            deleted_activities = db.execute(delete(Activity).where(Activity.id.in_(activity_ids))).rowcount or 0

        if source_ids:
            delete_runs_stmt = text(
                "DELETE FROM ingestion_runs WHERE source_id IN :source_ids"
            ).bindparams(bindparam("source_ids", expanding=True))
            deleted_ingestion_runs = db.execute(delete_runs_stmt, {"source_ids": source_ids}).rowcount or 0
            deleted_sources = db.execute(delete(Source).where(Source.id.in_(source_ids))).rowcount or 0

        db.commit()

    return {
        "activity_tags": deleted_activity_tags,
        "activities": deleted_activities,
        "ingestion_runs": deleted_ingestion_runs,
        "sources": deleted_sources,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch and parse Cedar Rapids Museum of Art calendar events."
    )
    parser.add_argument(
        "--months-ahead",
        type=int,
        default=MONTHS_AHEAD,
        help=f"Months of events to fetch ahead of today (default: {MONTHS_AHEAD}).",
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
            "Delete Cedar Rapids Museum of Art DB rows before repopulating. "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_crma_entries()
        print(
            "Deleted CRMA rows: "
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
        deleted = clear_crma_entries()
        print(
            "Deleted CRMA rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    async def _load() -> list[dict]:
        return await fetch_crma_events(months_ahead=args.months_ahead)

    run_specs = [
        TargetRunSpec(
            name="crma",
            source_url=CRMA_CALENDAR_URL,
            load_payload=_load,
            parse_payload=parse_crma_events,
            parser_name="crma",
            adapter_type="crma_fullcalendar",
            parsed_label="CRMA rows",
            before_commit=_clear_before_commit if args.clear else None,
        )
    ]

    summary = await run_targets(
        targets=run_specs,
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="crma",
            source_url=CRMA_CALENDAR_URL,
            details={"months_ahead": args.months_ahead},
        ),
    )

    print(f"Total parsed rows: {summary.total_parsed}")
    if not args.commit:
        print("Dry run only. Pass --commit to write to DB.")
        return

    for outcome in summary.outcomes:
        assert outcome.stats is not None
        print(
            f"MySQL write summary (crma): "
            f"input={outcome.stats.input_rows}, "
            f"deduped_input={outcome.stats.deduped_rows}, "
            f"inserted={outcome.stats.inserted}, "
            f"updated={outcome.stats.updated}, "
            f"unchanged={outcome.stats.unchanged}, "
            f"written={outcome.stats.written_rows}"
        )

    print(
        "MySQL write totals: "
        f"inserted={summary.total_inserted}, "
        f"updated={summary.total_updated}, "
        f"unchanged={summary.total_unchanged}, "
        f"written={summary.total_inserted + summary.total_updated}"
    )


if __name__ == "__main__":
    asyncio.run(main())
