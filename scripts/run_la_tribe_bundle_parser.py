#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.la_tribe_bundle import LA_TRIBE_VENUES  # noqa: E402
from src.crawlers.adapters.la_tribe_bundle import LA_TRIBE_VENUES_BY_SLUG  # noqa: E402
from src.crawlers.adapters.la_tribe_bundle import get_la_tribe_source_prefixes  # noqa: E402
from src.crawlers.adapters.la_tribe_bundle import load_la_tribe_bundle_payload  # noqa: E402
from src.crawlers.adapters.la_tribe_bundle import parse_la_tribe_events  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


LA_TRIBE_SOURCE_URL_PREFIXES = get_la_tribe_source_prefixes()


def clear_la_tribe_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [(venue.venue_name, venue.city, venue.state) for venue in LA_TRIBE_VENUES],
        )

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.in_([venue.list_url for venue in LA_TRIBE_VENUES]),
                    Source.name.like("la_tribe_%"),
                    Source.name.in_([venue.source_name for venue in LA_TRIBE_VENUES]),
                )
            )
        ).all()

        url_filters = [Activity.source_url.like(f"{prefix}%") for prefix in LA_TRIBE_SOURCE_URL_PREFIXES]
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


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse Louisiana museums using Tribe events APIs. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument(
        "--venue",
        choices=["all"] + sorted(LA_TRIBE_VENUES_BY_SLUG),
        default="all",
        help="Run all Louisiana Tribe venues or one venue slug.",
    )
    parser.add_argument("--page-limit", type=int, default=None)
    parser.add_argument(
        "--commit",
        action="store_true",
        help="When set, upsert parsed rows into MySQL.",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help=(
            "Delete all Louisiana Tribe bundle DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    selected_venues = (
        list(LA_TRIBE_VENUES)
        if args.venue == "all"
        else [LA_TRIBE_VENUES_BY_SLUG[args.venue]]
    )

    if args.clear and not args.commit:
        deleted = clear_la_tribe_entries()
        print(
            "Deleted Louisiana Tribe rows: "
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
        deleted = clear_la_tribe_entries()
        print(
            "Deleted Louisiana Tribe rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    payload = await load_la_tribe_bundle_payload(
        venues=selected_venues,
        page_limit=args.page_limit,
    )
    events_by_slug = payload.get("events_by_slug") or {}
    errors_by_slug = payload.get("errors_by_slug") or {}

    targets: list[TargetRunSpec] = []
    for venue in selected_venues:
        if venue.slug in errors_by_slug:
            continue
        targets.append(
            TargetRunSpec(
                name=f"la_tribe_{venue.slug}",
                source_url=venue.list_url,
                load_payload=lambda slug=venue.slug: asyncio.sleep(
                    0,
                    result={"events": events_by_slug.get(slug) or []},
                ),
                parse_payload=lambda venue_payload, selected_venue=venue: parse_la_tribe_events(
                    venue_payload.get("events") or [],
                    venue=selected_venue,
                ),
                parser_name="run_la_tribe_bundle_parser",
                adapter_type=venue.source_name,
                parsed_label=f"{venue.slug} rows",
                before_commit=_clear_before_commit if args.clear else None,
                empty_parse_details={"source": venue.venue_name, "venue_slug": venue.slug},
            )
        )

    if errors_by_slug:
        for slug, message in sorted(errors_by_slug.items()):
            print(f"[la-tribe-fetch] skipped venue={slug}: {message}")

    if not targets:
        failed = ", ".join(f"{slug}: {message}" for slug, message in sorted(errors_by_slug.items()))
        raise RuntimeError(f"Unable to fetch any Louisiana Tribe venues. Failures: {failed}")

    summary = await run_targets(
        targets=targets,
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_la_tribe_bundle_parser",
            details={"venues": [venue.slug for venue in selected_venues if venue.slug not in errors_by_slug]},
        ),
    )

    if args.commit:
        print(
            "Commit summary: "
            f"parsed={summary.total_parsed} "
            f"inserted={summary.total_inserted} "
            f"updated={summary.total_updated} "
            f"unchanged={summary.total_unchanged}"
        )

    if errors_by_slug:
        failed = ", ".join(sorted(errors_by_slug))
        raise SystemExit(f"Completed Louisiana Tribe bundle with fetch failures: {failed}")


if __name__ == "__main__":
    asyncio.run(main())
