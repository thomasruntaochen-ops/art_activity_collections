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

from src.crawlers.adapters.hammer import HAMMER_PROGRAMS_URL  # noqa: E402
from src.crawlers.adapters.hammer import fetch_hammer_program_pages  # noqa: E402
from src.crawlers.adapters.hammer import parse_hammer_program_pages  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity, Source  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "hammer"
HAMMER_SOURCE_URL_PREFIX = "https://hammer.ucla.edu/%"

def _write_html_cache(html: str, cache_dir: Path, *, index: int) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = cache_dir / f"hammer_programs_page_{index:02d}_{stamp}.html"
    output_path.write_text(html, encoding="utf-8")
    return output_path


def _write_text_dump(
    html: str,
    dump_dir: Path,
    *,
    index: int,
    source_html_path: Path | None = None,
) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    if source_html_path is not None:
        output_path = dump_dir / f"{source_html_path.stem}.txt"
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = dump_dir / f"hammer_programs_page_{index:02d}_{stamp}.txt"

    soup = BeautifulSoup(html, "html.parser")
    lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


async def _load_pages(
    *,
    url: str,
    input_html: list[str] | None,
    save_html: bool,
    cache_dir: Path,
) -> list[tuple[str, str, Path | None]]:
    if input_html:
        loaded_pages: list[tuple[str, str, Path | None]] = []
        for value in input_html:
            input_path = Path(value)
            if not input_path.exists():
                print(f"Input HTML file not found: {input_path}")
                raise SystemExit(1)
            print(f"Loading Hammer HTML from file: {input_path}")
            loaded_pages.append((url, input_path.read_text(encoding="utf-8"), input_path))
        return loaded_pages

    try:
        fetched_pages = await fetch_hammer_program_pages(url)
    except Exception as exc:
        print(f"Fetch failed ({url}): {exc}")
        raise SystemExit(1) from exc

    loaded_pages = []
    for index, (page_url, html) in enumerate(fetched_pages):
        if save_html:
            cache_path = _write_html_cache(html, cache_dir, index=index)
            print(f"Saved Hammer raw HTML cache to: {cache_path}")
        loaded_pages.append((page_url, html, None))

    return loaded_pages


def clear_hammer_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(HAMMER_SOURCE_URL_PREFIX),
                    Source.name.like("hammer_%"),
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(HAMMER_SOURCE_URL_PREFIX)
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


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse Hammer Museum programs pages. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--url", default=HAMMER_PROGRAMS_URL)
    parser.add_argument(
        "--input-html",
        action="append",
        default=None,
        help="Optional local HTML file(s) to parse instead of fetching live pages.",
    )
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched raw HTML to cache directory for debugging.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used by --save-html (default: data/html/hammer).",
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
            "Delete all Hammer DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_hammer_entries()
        print(
            "Deleted Hammer rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    if args.clear and args.commit:
        print("Clear requested with --commit; deletion is deferred until non-empty parse is validated.")

    loaded_pages = await _load_pages(
        url=args.url,
        input_html=args.input_html,
        save_html=args.save_html,
        cache_dir=Path(args.cache_dir),
    )

    if args.dump_text:
        for index, (_, html, source_html_path) in enumerate(loaded_pages):
            dump_path = _write_text_dump(
                html,
                Path(args.cache_dir),
                index=index,
                source_html_path=source_html_path,
            )
            print(f"Saved Hammer text dump to: {dump_path}")

    clear_completed = False

    def _clear_before_commit() -> None:
        nonlocal clear_completed
        if clear_completed or not args.clear:
            return
        deleted = clear_hammer_entries()
        print(
            "Deleted Hammer rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    async def _load_cached_pages() -> list[tuple[str, str]]:
        return [(page_url, html) for page_url, html, _ in loaded_pages]

    def _parse_pages(pages: list[tuple[str, str]]):
        parsed = parse_hammer_program_pages(pages, list_url=args.url)
        parsed.sort(key=lambda row: (row.start_at, row.title, row.source_url))
        return parsed

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="hammer",
                source_url=args.url,
                load_payload=_load_cached_pages,
                parse_payload=_parse_pages,
                parser_name="hammer",
                adapter_type="hammer_programs",
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
