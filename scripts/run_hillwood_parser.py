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

from src.crawlers.adapters.hillwood import HILLWOOD_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.hillwood import HILLWOOD_VENUE_NAME  # noqa: E402
from src.crawlers.adapters.hillwood import load_hillwood_events_payload  # noqa: E402
from src.crawlers.adapters.hillwood import parse_hillwood_events_payload  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "hillwood"
HILLWOOD_SOURCE_URL_PREFIX = "https://hillwoodmuseum.org/"


def _write_html_cache(payload: dict, cache_dir: Path) -> list[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    written: list[Path] = []

    for index, (url, html) in enumerate(sorted((payload.get("listing_pages") or {}).items())):
        output_path = cache_dir / f"hillwood_listing_{index:02d}_{stamp}.html"
        output_path.write_text(html, encoding="utf-8")
        written.append(output_path)

    for index, (url, html) in enumerate(sorted((payload.get("detail_pages") or {}).items())):
        output_path = cache_dir / f"hillwood_detail_{index:02d}_{_slugify(url)}_{stamp}.html"
        output_path.write_text(html, encoding="utf-8")
        written.append(output_path)

    payload_path = cache_dir / f"hillwood_payload_{stamp}.json"
    payload_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    written.append(payload_path)
    return written


def _write_text_dump(payload: dict, dump_dir: Path) -> list[Path]:
    dump_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    pages: list[tuple[str, str]] = []
    pages.extend(sorted((payload.get("listing_pages") or {}).items()))
    pages.extend(sorted((payload.get("detail_pages") or {}).items()))

    for index, (label, html) in enumerate(pages):
        soup = BeautifulSoup(html, "html.parser")
        lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
        output_path = dump_dir / f"hillwood_page_{index:02d}_{_slugify(label)}.txt"
        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        written.append(output_path)

    return written


def clear_hillwood_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [("Hillwood Estate, Museum & Gardens", "Washington", "DC")],
        )

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(HILLWOOD_SOURCE_URL_PREFIX),
                    Source.name.like("hillwood_%"),
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(HILLWOOD_SOURCE_URL_PREFIX)
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
            "Fetch and parse Hillwood Estate, Museum & Gardens events via Playwright. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum paginated event listing pages to fetch (default: 5).",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=90000,
        help="Per-page Playwright timeout in milliseconds (default: 90000).",
    )
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched raw HTML to cache directory for debugging.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used by --save-html and --dump-text (default: data/html/hillwood).",
    )
    parser.add_argument(
        "--dump-text",
        action="store_true",
        help="Write normalized page text lines to a .txt file for parser debugging.",
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
            "Delete all Hillwood DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.max_pages < 1:
        print("--max-pages must be >= 1")
        raise SystemExit(1)

    if args.clear and not args.commit:
        deleted = clear_hillwood_entries()
        print(
            "Deleted Hillwood rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    if args.clear and args.commit:
        print("Clear requested with --commit; deletion is deferred until non-empty parse is validated.")

    payload = await load_hillwood_events_payload(max_pages=args.max_pages, timeout_ms=args.timeout_ms)

    if args.save_html:
        cache_paths = _write_html_cache(payload, Path(args.cache_dir))
        for path in cache_paths:
            print(f"Saved Hillwood cache artifact to: {path}")

    if args.dump_text:
        dump_paths = _write_text_dump(payload, Path(args.cache_dir))
        for path in dump_paths:
            print(f"Saved Hillwood text dump to: {path}")

    clear_completed = False

    def _clear_before_commit() -> None:
        nonlocal clear_completed
        if clear_completed or not args.clear:
            return
        deleted = clear_hillwood_entries()
        print(
            "Deleted Hillwood rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="hillwood_events",
                source_url=HILLWOOD_EVENTS_URL,
                load_payload=lambda: asyncio.sleep(0, result=payload),
                parse_payload=parse_hillwood_events_payload,
                parser_name="run_hillwood_parser",
                adapter_type="hillwood_events",
                parsed_label="Hillwood rows",
                before_commit=_clear_before_commit if args.clear else None,
                empty_parse_details={"venue": HILLWOOD_VENUE_NAME},
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_hillwood_parser",
            source_url=HILLWOOD_EVENTS_URL,
            details={"venue": HILLWOOD_VENUE_NAME},
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


def _slugify(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value).strip("_")[:80] or "page"


if __name__ == "__main__":
    asyncio.run(main())
