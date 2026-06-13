#!/usr/bin/env python3
import argparse
import asyncio
import json
import re
import sys
from datetime import datetime
from datetime import timezone
from pathlib import Path

from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.anchorage_museum import ANCHORAGE_CATEGORY_URLS  # noqa: E402
from src.crawlers.adapters.anchorage_museum import ANCHORAGE_CITY  # noqa: E402
from src.crawlers.adapters.anchorage_museum import ANCHORAGE_STATE  # noqa: E402
from src.crawlers.adapters.anchorage_museum import ANCHORAGE_VENUE_NAME  # noqa: E402
from src.crawlers.adapters.anchorage_museum import load_anchorage_museum_payload  # noqa: E402
from src.crawlers.adapters.anchorage_museum import parse_anchorage_museum_payload  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity, Source  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "anchorage_museum"
WHITESPACE_RE = re.compile(r"\s+")
ANCHORAGE_SOURCE_URL_PREFIX = "https://www.anchoragemuseum.org/visit/calendar/%"


def clear_anchorage_museum_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(db, [(ANCHORAGE_VENUE_NAME, ANCHORAGE_CITY, ANCHORAGE_STATE)])
        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(ANCHORAGE_SOURCE_URL_PREFIX),
                    Source.name.in_(["anchorage_museum", "anchorage_museum_events"]),
                    Source.adapter_type == "anchorage_museum_events",
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(ANCHORAGE_SOURCE_URL_PREFIX)
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


def _write_html_cache(payload: dict[str, object], cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = cache_dir / f"anchorage_museum_{stamp}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    return output_path


def _write_text_dump(payload: dict[str, object], dump_dir: Path) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = dump_dir / f"anchorage_museum_{stamp}.txt"

    lines: list[str] = []
    for bucket in ("list_pages", "detail_pages"):
        pages = payload.get(bucket) or {}
        if not isinstance(pages, dict):
            continue
        for url in sorted(pages):
            html = pages[url]
            if not isinstance(html, str):
                continue
            lines.append(url)
            lines.append(WHITESPACE_RE.sub(" ", re.sub(r"<[^>]+>", " ", html)).strip())

    output_path.write_text("\n\n".join(line for line in lines if line) + "\n", encoding="utf-8")
    return output_path


async def _load_payload(
    *,
    input_json: str | None,
    save_html: bool,
    cache_dir: Path,
) -> dict[str, object]:
    if input_json:
        input_path = Path(input_json)
        if not input_path.exists():
            print(f"Input payload file not found: {input_path}")
            raise SystemExit(1)
        print(f"Loading Anchorage Museum payload from file: {input_path}")
        return json.loads(input_path.read_text(encoding="utf-8"))

    try:
        payload = await load_anchorage_museum_payload()
    except Exception as exc:
        print(f"Fetch failed: {exc}")
        raise SystemExit(1) from exc

    if save_html:
        cache_path = _write_html_cache(payload, cache_dir)
        print(f"Saved raw Anchorage Museum payload cache to: {cache_path}")

    return payload


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse Anchorage Museum activity pages. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--input-json", default=None)
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched raw payload to cache directory for debugging.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used by --save-html (default: data/html/anchorage_museum).",
    )
    parser.add_argument(
        "--dump-text",
        action="store_true",
        help="Write normalized payload text lines to a .txt file for parser debugging.",
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
            "Delete all Anchorage Museum DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_anchorage_museum_entries()
        print(
            "Deleted Anchorage Museum rows: "
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
        deleted = clear_anchorage_museum_entries()
        print(
            "Deleted Anchorage Museum rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    payload = await _load_payload(
        input_json=args.input_json,
        save_html=args.save_html,
        cache_dir=Path(args.cache_dir),
    )

    if args.dump_text:
        dump_path = _write_text_dump(payload, Path(args.cache_dir))
        print(f"Saved Anchorage Museum text dump to: {dump_path}")

    async def _payload_loader() -> dict[str, object]:
        return payload

    first_source_url = next(iter(ANCHORAGE_CATEGORY_URLS.values()))
    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="anchorage_museum",
                source_url=first_source_url,
                load_payload=_payload_loader,
                parse_payload=parse_anchorage_museum_payload,
                parser_name="anchorage_museum",
                adapter_type="anchorage_museum_events",
                parsed_label="Anchorage Museum rows",
                before_commit=_clear_before_commit if args.clear else None,
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="anchorage_museum",
            source_url=first_source_url,
            details={"cache_dir": str(args.cache_dir)},
        ),
    )

    if args.commit:
        print(
            "Anchorage Museum commit summary: "
            f"parsed={summary.total_parsed}, "
            f"inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, "
            f"unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
