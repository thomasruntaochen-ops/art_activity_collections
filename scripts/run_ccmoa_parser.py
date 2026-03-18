#!/usr/bin/env python3
import argparse
import asyncio
import json
import sys
from datetime import datetime
from datetime import timezone
from pathlib import Path

from bs4 import BeautifulSoup
from sqlalchemy import bindparam
from sqlalchemy import delete
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.ccmoa import CCMOA_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.ccmoa import load_ccmoa_payload  # noqa: E402
from src.crawlers.adapters.ccmoa import parse_ccmoa_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "ccmoa"
CCMOA_SOURCE_URL_PREFIX = "https://www.ccmoa.org/%"


def _write_html_cache(payload: dict, cache_dir: Path) -> list[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    written: list[Path] = []

    listing_path = cache_dir / f"ccmoa_events_listing_{stamp}.html"
    listing_path.write_text(payload.get("listing_html", ""), encoding="utf-8")
    written.append(listing_path)

    for index, (source_url, html) in enumerate(sorted((payload.get("detail_pages") or {}).items())):
        slug = _slugify_detail_name(source_url)
        output_path = cache_dir / f"ccmoa_detail_{index:02d}_{slug}_{stamp}.html"
        output_path.write_text(html, encoding="utf-8")
        written.append(output_path)

    return written


def _write_text_dump(payload: dict, dump_dir: Path) -> list[Path]:
    dump_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    pages = [("listing", payload.get("listing_html", ""))]
    pages.extend((source_url, html) for source_url, html in sorted((payload.get("detail_pages") or {}).items()))
    for index, (label, html) in enumerate(pages):
        soup = BeautifulSoup(html, "html.parser")
        lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
        output_path = dump_dir / f"ccmoa_page_{index:02d}_{_slugify_detail_name(label)}.txt"
        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        written.append(output_path)

    return written


def clear_ccmoa_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(CCMOA_SOURCE_URL_PREFIX),
                    Source.name.like("ccmoa_%"),
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(CCMOA_SOURCE_URL_PREFIX)
        if source_ids:
            activity_filter = or_(activity_filter, Activity.source_id.in_(source_ids))

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


def _slugify_detail_name(label: str) -> str:
    slug = "".join(ch if ch.isalnum() else "_" for ch in label.lower()).strip("_")
    return slug[:80] or "page"


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse Cape Cod Museum of Art upcoming events. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched listing/detail HTML to cache directory for debugging.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used by --save-html and --dump-text (default: data/html/ccmoa).",
    )
    parser.add_argument(
        "--dump-text",
        action="store_true",
        help="Write normalized page text lines to .txt files for parser debugging.",
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
            "Delete all CCMoA DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_ccmoa_entries()
        print(
            "Deleted CCMoA rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    payload = await load_ccmoa_payload()

    if args.save_html:
        cache_paths = _write_html_cache(payload, Path(args.cache_dir))
        for path in cache_paths:
            print(f"Saved CCMoA cache artifact to: {path}")

    if args.dump_text:
        dump_paths = _write_text_dump(payload, Path(args.cache_dir))
        for path in dump_paths:
            print(f"Saved CCMoA text dump to: {path}")

    clear_completed = False

    def _clear_before_commit() -> None:
        nonlocal clear_completed
        if clear_completed or not args.clear:
            return
        deleted = clear_ccmoa_entries()
        print(
            "Deleted CCMoA rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="ccmoa_events",
                source_url=CCMOA_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_ccmoa_payload,
                parser_name="run_ccmoa_parser",
                adapter_type="ccmoa_events",
                parsed_label="CCMoA rows",
                before_commit=_clear_before_commit if args.clear else None,
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_ccmoa_parser",
            source_url=CCMOA_EVENTS_URL,
        ),
    )

    if args.commit:
        print(
            "Commit summary: "
            f"parsed={summary.total_parsed}, "
            f"inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, "
            f"unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
