#!/usr/bin/env python3
import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup
from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.bmoa import (  # noqa: E402
    BMOA_EVENTS_URL,
    fetch_bmoa_events_page,
    parse_bmoa_events_html,
)
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity, Source  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "bmoa"
BMOA_SOURCE_URL_PREFIX = "https://www.bmoa.org/%"


def _write_html_cache(html: str, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = cache_dir / f"bmoa_events_{stamp}.html"
    output_path.write_text(html, encoding="utf-8")
    return output_path


def _write_text_dump(
    html: str,
    dump_dir: Path,
    *,
    source_html_path: Path | None = None,
) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    if source_html_path is not None:
        output_path = dump_dir / f"{source_html_path.stem}.txt"
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = dump_dir / f"bmoa_events_{stamp}.txt"

    soup = BeautifulSoup(html, "html.parser")
    lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


async def _load_html(
    *,
    url: str,
    input_html: str | None,
    save_html: bool,
    cache_dir: Path,
) -> tuple[str, Path | None]:
    input_path: Path | None = None
    if input_html:
        input_path = Path(input_html)
        if not input_path.exists():
            print(f"Input HTML file not found: {input_path}")
            raise SystemExit(1)
        print(f"Loading BMoA HTML from file: {input_path}")
        html = input_path.read_text(encoding="utf-8")
        return html, input_path

    try:
        html = await fetch_bmoa_events_page(url)
    except Exception as exc:
        print(f"Fetch failed ({url}): {exc}")
        raise SystemExit(1) from exc

    if save_html:
        cache_path = _write_html_cache(html, cache_dir)
        print(f"Saved raw BMoA HTML cache to: {cache_path}")

    return html, None


def clear_bmoa_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [("Bakersfield Museum of Art", "Bakersfield", "CA")],
        )

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(BMOA_SOURCE_URL_PREFIX),
                    Source.name.like("bmoa_%"),
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(BMOA_SOURCE_URL_PREFIX)
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
            "Fetch and parse Bakersfield Museum of Art events. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--url", default=BMOA_EVENTS_URL)
    parser.add_argument("--input-html", default=None)
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched raw HTML to cache directory for debugging.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used by --save-html (default: data/html/bmoa).",
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
            "Delete all BMoA DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_bmoa_entries()
        print(
            "Deleted BMoA rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    if args.clear and args.commit:
        print("Clear requested with --commit; deletion is deferred until non-empty parse is validated.")

    html, source_html_path = await _load_html(
        url=args.url,
        input_html=args.input_html,
        save_html=args.save_html,
        cache_dir=Path(args.cache_dir),
    )

    if args.dump_text:
        dump_path = _write_text_dump(
            html,
            Path(args.cache_dir),
            source_html_path=source_html_path,
        )
        print(f"Saved BMoA text dump to: {dump_path}")

    clear_completed = False

    def _clear_before_commit() -> None:
        nonlocal clear_completed
        if clear_completed or not args.clear:
            return
        deleted = clear_bmoa_entries()
        print(
            "Deleted BMoA rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    async def _load_cached_html() -> str:
        return html

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="bmoa",
                source_url=args.url,
                load_payload=_load_cached_html,
                parse_payload=lambda payload: parse_bmoa_events_html(html=payload, list_url=args.url),
                parser_name="bmoa",
                adapter_type="bmoa_events",
                parsed_label="rows",
                before_commit=_clear_before_commit if args.clear else None,
                empty_parse_details={"cache_dir": str(args.cache_dir)},
            )
        ],
        commit=args.commit,
    )

    if not args.commit:
        print("Dry run only. Pass --commit to write to DB.")
        return

    outcome = summary.outcomes[0]
    assert outcome.stats is not None
    print(
        "MySQL write summary: "
        f"input={outcome.stats.input_rows}, "
        f"deduped_input={outcome.stats.deduped_rows}, "
        f"inserted={outcome.stats.inserted}, "
        f"updated={outcome.stats.updated}, "
        f"unchanged={outcome.stats.unchanged}, "
        f"written={outcome.stats.written_rows}"
    )


if __name__ == "__main__":
    asyncio.run(main())
