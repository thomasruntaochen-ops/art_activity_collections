#!/usr/bin/env python3
import argparse
import asyncio
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

from src.crawlers.adapters.sjma import SJMA_CALENDAR_URL  # noqa: E402
from src.crawlers.adapters.sjma import fetch_sjma_calendar_bundle  # noqa: E402
from src.crawlers.adapters.sjma import parse_sjma_events_html  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "sjma"
SJMA_SOURCE_URL_PREFIX = "https://sjmusart.org/%"


def _write_html_cache(html: str, cache_dir: Path, *, suffix: str) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = cache_dir / f"sjma_{suffix}_{stamp}.html"
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
        output_path = dump_dir / f"sjma_calendar_{stamp}.txt"

    soup = BeautifulSoup(html, "html.parser")
    lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


async def _load_bundle(
    *,
    url: str,
    input_html: str | None,
    detail_html_dir: str | None,
    save_html: bool,
    cache_dir: Path,
) -> tuple[str, dict[str, str], Path | None]:
    input_path: Path | None = None
    if input_html:
        input_path = Path(input_html)
        if not input_path.exists():
            print(f"Input HTML file not found: {input_path}")
            raise SystemExit(1)
        print(f"Loading SJMA calendar HTML from file: {input_path}")
        html = input_path.read_text(encoding="utf-8")
        detail_html_by_url: dict[str, str] = {}
        if detail_html_dir:
            for detail_path in sorted(Path(detail_html_dir).glob("*.html")):
                detail_html_by_url[detail_path.stem.replace("__", "/")] = detail_path.read_text(encoding="utf-8")
        return html, detail_html_by_url, input_path

    html, detail_html_by_url = await fetch_sjma_calendar_bundle(url)
    if save_html:
        cache_path = _write_html_cache(html, cache_dir, suffix="calendar")
        print(f"Saved raw SJMA HTML cache to: {cache_path}")
        for detail_url, detail_html in detail_html_by_url.items():
            slug = detail_url.replace("https://sjmusart.org/", "").replace("/", "__")
            detail_path = _write_html_cache(detail_html, cache_dir, suffix=slug)
            print(f"Saved SJMA detail HTML cache to: {detail_path}")
    return html, detail_html_by_url, None


def clear_sjma_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [("San Jose Museum of Art", "San Jose", "CA")],
        )

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(SJMA_SOURCE_URL_PREFIX),
                    Source.name.like("sjma_%"),
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(SJMA_SOURCE_URL_PREFIX)
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
            "Fetch and parse San Jose Museum of Art calendar events. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--url", default=SJMA_CALENDAR_URL)
    parser.add_argument("--input-html", default=None)
    parser.add_argument("--detail-html-dir", default=None)
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched raw HTML to cache directory for debugging.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used by --save-html (default: data/html/sjma).",
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
            "Delete all SJMA DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_sjma_entries()
        print(
            "Deleted SJMA rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    if args.clear and args.commit:
        print("Clear requested with --commit; deletion is deferred until non-empty parse is validated.")

    html, detail_html_by_url, source_html_path = await _load_bundle(
        url=args.url,
        input_html=args.input_html,
        detail_html_dir=args.detail_html_dir,
        save_html=args.save_html,
        cache_dir=Path(args.cache_dir),
    )

    if args.dump_text:
        dump_path = _write_text_dump(
            html,
            Path(args.cache_dir),
            source_html_path=source_html_path,
        )
        print(f"Saved SJMA text dump to: {dump_path}")

    clear_completed = False

    def _clear_before_commit() -> None:
        nonlocal clear_completed
        if clear_completed or not args.clear:
            return
        deleted = clear_sjma_entries()
        print(
            "Deleted SJMA rows before repopulating: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    async def _load_cached_bundle() -> tuple[str, dict[str, str]]:
        return html, detail_html_by_url

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="sjma",
                source_url=args.url,
                load_payload=_load_cached_bundle,
                parse_payload=lambda payload: parse_sjma_events_html(
                    html=payload[0],
                    list_url=args.url,
                    detail_html_by_url=payload[1],
                ),
                parser_name="sjma",
                adapter_type="sjma_calendar",
                parsed_label="SJMA activities",
                before_commit=_clear_before_commit if args.clear else None,
            )
        ],
        commit=args.commit,
        json_ensure_ascii=False,
    )

    if not args.commit:
        return

    outcome = summary.outcomes[0]
    assert outcome.stats is not None
    print(
        "Upsert complete: "
        f"input={outcome.stats.input_rows} deduped={outcome.stats.deduped_rows} "
        f"inserted={outcome.stats.inserted} updated={outcome.stats.updated} unchanged={outcome.stats.unchanged}"
    )


if __name__ == "__main__":
    asyncio.run(main())
