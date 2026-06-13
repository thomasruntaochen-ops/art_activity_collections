#!/usr/bin/env python
import argparse
import asyncio
import json
import sys
from pathlib import Path

from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.blanden import BLANDEN_CLASSES_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.blanden import BLANDEN_CITY  # noqa: E402
from src.crawlers.adapters.blanden import BLANDEN_STATE  # noqa: E402
from src.crawlers.adapters.blanden import BLANDEN_VENUE_NAME  # noqa: E402
from src.crawlers.adapters.blanden import load_blanden_payload  # noqa: E402
from src.crawlers.adapters.blanden import parse_blanden_html  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


BLANDEN_SOURCE_URL_PREFIX = "https://www.blanden.org/"


def clear_blanden_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(db, [(BLANDEN_VENUE_NAME, BLANDEN_CITY, BLANDEN_STATE)])
        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url == BLANDEN_CLASSES_EVENTS_URL,
                    Source.name == "blanden",
                    Source.name == "blanden_events",
                    Source.adapter_type == "blanden_events",
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(f"{BLANDEN_SOURCE_URL_PREFIX}%")
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
    parser = argparse.ArgumentParser(description="Fetch and parse Blanden Memorial Art Museum events.")
    parser.add_argument("--commit", action="store_true", help="When set, upsert parsed rows into MySQL.")
    parser.add_argument("--clear", action="store_true", help="Delete Blanden DB rows before commit.")
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_blanden_entries()
        print(
            "Deleted Blanden rows: "
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
        deleted = clear_blanden_entries()
        print(
            "Deleted Blanden rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

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
                before_commit=_clear_before_commit if args.clear else None,
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
