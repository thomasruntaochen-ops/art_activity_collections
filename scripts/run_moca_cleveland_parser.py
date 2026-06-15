#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.moca_cleveland import MOCA_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.moca_cleveland import MOCA_CITY  # noqa: E402
from src.crawlers.adapters.moca_cleveland import MOCA_STATE  # noqa: E402
from src.crawlers.adapters.moca_cleveland import MOCA_VENUE_NAME  # noqa: E402
from src.crawlers.adapters.moca_cleveland import load_moca_cleveland_payload  # noqa: E402
from src.crawlers.adapters.moca_cleveland import parse_moca_cleveland_payload  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity, Source  # noqa: E402


def _source_url_prefix(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}/"
    return "https://www.mocacleveland.org/"


def clear_moca_cleveland_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0
    source_prefix = _source_url_prefix(MOCA_EVENTS_URL)

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(db, [(MOCA_VENUE_NAME, MOCA_CITY, MOCA_STATE)])
        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url == MOCA_EVENTS_URL,
                    Source.name == "moca_cleveland_events",
                    Source.adapter_type == "moca_cleveland_events",
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(f"{source_prefix}%")
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
    parser = argparse.ArgumentParser(description="Fetch and parse Museum of Contemporary Art Cleveland activities.")
    parser.add_argument("--commit", action="store_true")
    parser.add_argument(
        "--clear",
        action="store_true",
        help=(
            "Delete existing MOCA Cleveland rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_moca_cleveland_entries()
        print(
            "Deleted MOCA Cleveland rows: "
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
        deleted = clear_moca_cleveland_entries()
        print(
            "Deleted MOCA Cleveland rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    html = await load_moca_cleveland_payload()
    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="moca_cleveland",
                source_url=MOCA_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=html),
                parse_payload=parse_moca_cleveland_payload,
                parser_name="run_moca_cleveland_parser",
                adapter_type="moca_cleveland_events",
                parsed_label="Museum of Contemporary Art Cleveland rows",
                before_commit=_clear_before_commit if args.clear else None,
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_moca_cleveland_parser",
            source_url=MOCA_EVENTS_URL,
        ),
    )
    if args.commit:
        print(
            f"Commit summary: parsed={summary.total_parsed}, inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
