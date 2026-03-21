#!/usr/bin/env python3
import argparse
import asyncio
import sys
from datetime import date
from pathlib import Path

from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.yuag import YUAG_CALENDAR_URL  # noqa: E402
from src.crawlers.adapters.yuag import download_yuag_calendar_cache  # noqa: E402
from src.crawlers.adapters.yuag import load_yuag_calendar_payload_from_dir  # noqa: E402
from src.crawlers.adapters.yuag import load_yuag_calendar_payload  # noqa: E402
from src.crawlers.adapters.yuag import parse_yuag_calendar_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


YUAG_SOURCE_URL_PREFIX = "https://artgallery.yale.edu/%"


def clear_yuag_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [("Yale University Art Gallery", "New Haven", "CT")],
        )

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(YUAG_SOURCE_URL_PREFIX),
                    Source.name.like("yuag_%"),
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(YUAG_SOURCE_URL_PREFIX)
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
            "Fetch and parse Yale University Art Gallery youth calendar events. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument(
        "--start-date",
        default=date.today().isoformat(),
        help="Calendar start date filter in YYYY-MM-DD format (default: today).",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=1,
        help="Cap on fetched calendar pages (default: 1).",
    )
    parser.add_argument(
        "--input-dir",
        default="/tmp/yuag_cache",
        help=(
            "Directory for cached Yale HTML files named calendar_page_<n>.html "
            "and detail_<slug>.html (default: /tmp/yuag_cache)."
        ),
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
            "Delete all YUAG DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_yuag_entries()
        print(
            "Deleted YUAG rows: "
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
        deleted = clear_yuag_entries()
        print(
            "Deleted YUAG rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    download_summary = await asyncio.to_thread(
        download_yuag_calendar_cache,
        input_dir=args.input_dir,
        start_date=args.start_date,
        page_limit=args.page_limit,
    )
    print(
        "Downloaded YUAG cache: "
        f"pages={download_summary['pages']}, "
        f"details={download_summary['details']}, "
        f"dir={args.input_dir}"
    )

    run_specs = [
        TargetRunSpec(
            name="yuag_calendar",
            source_url=YUAG_CALENDAR_URL,
            load_payload=lambda: asyncio.to_thread(
                load_yuag_calendar_payload_from_dir,
                input_dir=args.input_dir,
                page_limit=args.page_limit,
            ),
            parse_payload=parse_yuag_calendar_payload,
            parser_name="run_yuag_parser",
            adapter_type="yuag_calendar",
            parsed_label="YUAG rows",
            before_commit=_clear_before_commit if args.clear else None,
        )
    ]

    summary = await run_targets(
        targets=run_specs,
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_yuag_parser",
            source_url=YUAG_CALENDAR_URL,
            details={"start_date": args.start_date},
        ),
    )

    if args.commit:
        print(
            "Committed YUAG rows: "
            f"parsed={summary.total_parsed}, "
            f"inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, "
            f"unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
