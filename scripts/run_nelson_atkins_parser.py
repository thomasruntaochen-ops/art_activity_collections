#!/usr/bin/env python3
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

from src.crawlers.adapters.nelson_atkins import NELSON_ATKINS_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.nelson_atkins import load_nelson_atkins_payload  # noqa: E402
from src.crawlers.adapters.nelson_atkins import parse_nelson_atkins_payload  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


NELSON_ATKINS_SOURCE_BASE = "https://cart.nelson-atkins.org"


def clear_nelson_atkins_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [("Nelson-Atkins Museum of Art", "Kansas City", "MO")],
        )

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url == NELSON_ATKINS_SOURCE_BASE,
                    Source.base_url.like(f"{NELSON_ATKINS_SOURCE_BASE}/%"),
                    Source.name.like("nelson_atkins_%"),
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(f"{NELSON_ATKINS_SOURCE_BASE}/%")
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
        description=(
            "Fetch and parse Nelson-Atkins Museum of Art activities from the official "
            "production season API and optionally commit them to the database."
        )
    )
    parser.add_argument("--start-date", default=None, help="Override the API start date, in YYYY-MM-DD format.")
    parser.add_argument("--day-window", type=int, default=184, help="How many days ahead to request from the API.")
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--clear", action="store_true")
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_nelson_atkins_entries()
        print(
            "Deleted Nelson-Atkins rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    payload = await load_nelson_atkins_payload(
        start_date=args.start_date,
        day_window=args.day_window,
    )
    clear_completed = False

    def _clear_before_commit() -> None:
        nonlocal clear_completed
        if clear_completed or not args.clear:
            return
        deleted = clear_nelson_atkins_entries()
        print(
            "Deleted Nelson-Atkins rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="nelson_atkins_events",
                source_url=NELSON_ATKINS_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_nelson_atkins_payload,
                parser_name="run_nelson_atkins_parser",
                adapter_type="nelson_atkins_events",
                parsed_label="Nelson-Atkins rows",
                before_commit=_clear_before_commit if args.clear else None,
                empty_parse_details={"source": "Nelson-Atkins Museum of Art"},
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_nelson_atkins_parser",
            source_url=NELSON_ATKINS_EVENTS_URL,
            details={"source": "Nelson-Atkins Museum of Art"},
        ),
    )

    if args.commit:
        print(
            "Commit summary: "
            f"inserted={summary.total_inserted} "
            f"updated={summary.total_updated} "
            f"unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
