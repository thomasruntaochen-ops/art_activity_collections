#!/usr/bin/env python3
import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.vancouver_art_gallery import VAG_CITY  # noqa: E402
from src.crawlers.adapters.vancouver_art_gallery import VAG_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.vancouver_art_gallery import VAG_STATE  # noqa: E402
from src.crawlers.adapters.vancouver_art_gallery import VAG_VENUE_NAME  # noqa: E402
from src.crawlers.adapters.vancouver_art_gallery import load_vancouver_art_gallery_payload  # noqa: E402
from src.crawlers.adapters.vancouver_art_gallery import parse_vancouver_art_gallery_payload  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity, Source  # noqa: E402


VAG_SOURCE_URL_PREFIX = "https://www.vanartgallery.bc.ca/%"
VAG_ADAPTER_TYPE = "vancouver_art_gallery_events"
DEFAULT_CACHE_DIR = Path("data") / "html" / "vancouver_art_gallery"


def clear_vancouver_art_gallery_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(db, [(VAG_VENUE_NAME, VAG_CITY, VAG_STATE)])

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(VAG_SOURCE_URL_PREFIX),
                    Source.name == VAG_ADAPTER_TYPE,
                    Source.adapter_type == VAG_ADAPTER_TYPE,
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(VAG_SOURCE_URL_PREFIX)
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


def _print_deleted(prefix: str, deleted: dict[str, int]) -> None:
    print(
        f"{prefix}: "
        f"activity_tags={deleted['activity_tags']}, "
        f"activities={deleted['activities']}, "
        f"ingestion_runs={deleted['ingestion_runs']}, "
        f"sources={deleted['sources']}"
    )


async def _load_payload(*, input_json: str | None, save_json: bool, cache_dir: Path) -> dict[str, object]:
    if input_json:
        input_path = Path(input_json)
        if not input_path.exists():
            print(f"Input payload file not found: {input_path}")
            raise SystemExit(1)
        print(f"Loading Vancouver Art Gallery payload from file: {input_path}")
        return json.loads(input_path.read_text(encoding="utf-8"))

    payload = await load_vancouver_art_gallery_payload()
    if save_json:
        cache_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = cache_dir / f"vancouver_art_gallery_{stamp}.json"
        output_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
        print(f"Saved raw Vancouver Art Gallery payload to: {output_path}")
    return payload


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch and parse Vancouver Art Gallery family programs, talks and lectures."
    )
    parser.add_argument("--input-json", default=None, help="Parse a saved payload instead of fetching.")
    parser.add_argument("--save-json", action="store_true", help="Save the fetched payload for debugging.")
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--commit", action="store_true", help="Upsert parsed rows into MySQL.")
    parser.add_argument("--clear", action="store_true", help="Delete this venue's rows (before repopulating with --commit).")
    args = parser.parse_args()

    if args.clear and not args.commit:
        _print_deleted("Deleted Vancouver Art Gallery rows", clear_vancouver_art_gallery_entries())
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    if args.clear and args.commit:
        print("Clear requested with --commit; deletion is deferred until non-empty parse is validated.")

    clear_completed = False

    def _clear_before_commit() -> None:
        nonlocal clear_completed
        if clear_completed or not args.clear:
            return
        _print_deleted("Deleted Vancouver Art Gallery rows before repopulation", clear_vancouver_art_gallery_entries())
        clear_completed = True

    payload = await _load_payload(
        input_json=args.input_json,
        save_json=args.save_json,
        cache_dir=Path(args.cache_dir),
    )
    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="vancouver_art_gallery",
                source_url=VAG_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_vancouver_art_gallery_payload,
                parser_name="run_vancouver_art_gallery_parser",
                adapter_type=VAG_ADAPTER_TYPE,
                parsed_label="Vancouver Art Gallery rows",
                before_commit=_clear_before_commit if args.clear else None,
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_vancouver_art_gallery_parser",
            source_url=VAG_EVENTS_URL,
        ),
    )
    if args.commit:
        print(
            f"Commit summary: parsed={summary.total_parsed}, inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
