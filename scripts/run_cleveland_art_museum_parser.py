#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.cleveland_art_museum import CLEVELAND_EVENTS_URL_TEMPLATE  # noqa: E402
from src.crawlers.adapters.cleveland_art_museum import load_cleveland_art_museum_payload  # noqa: E402
from src.crawlers.adapters.cleveland_art_museum import parse_cleveland_art_museum_payload  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity, Source  # noqa: E402


CLEVELAND_SOURCE_URL_PREFIXES = ("https://www.clevelandart.org/",)


def clear_cleveland_art_museum_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(db, [("Cleveland Museum of Art", "Cleveland", "OH")])

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like("https://www.clevelandart.org/%"),
                    Source.name == "cleveland_art_museum_events",
                    Source.adapter_type == "cleveland_art_museum_events",
                )
            )
        ).all()

        url_filters = [Activity.source_url.like(f"{prefix}%") for prefix in CLEVELAND_SOURCE_URL_PREFIXES]
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
    parser = argparse.ArgumentParser(description="Fetch and parse Cleveland Museum of Art upcoming activities.")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--clear", action="store_true")
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_cleveland_art_museum_entries()
        print(
            "Deleted Cleveland Museum of Art rows: "
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
        deleted = clear_cleveland_art_museum_entries()
        print(
            "Deleted Cleveland Museum of Art rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    payload = await load_cleveland_art_museum_payload(max_pages=args.max_pages)
    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="cleveland_art_museum",
                source_url=CLEVELAND_EVENTS_URL_TEMPLATE.format(page=0),
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_cleveland_art_museum_payload,
                parser_name="run_cleveland_art_museum_parser",
                adapter_type="cleveland_art_museum_events",
                parsed_label="Cleveland Museum of Art rows",
                before_commit=_clear_before_commit if args.clear else None,
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_cleveland_art_museum_parser",
            source_url=CLEVELAND_EVENTS_URL_TEMPLATE.format(page=0),
        ),
    )
    if args.commit:
        print(
            f"Commit summary: parsed={summary.total_parsed}, inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
