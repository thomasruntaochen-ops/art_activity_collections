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

from src.crawlers.adapters.albrecht_kemper import ALBRECHT_KEMPER_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.albrecht_kemper import fetch_albrecht_kemper_events_page  # noqa: E402
from src.crawlers.adapters.albrecht_kemper import parse_albrecht_kemper_events_html  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "albrecht_kemper"
ALBRECHT_KEMPER_SOURCE_BASE = "https://www.albrecht-kemper.org"


def _write_html_cache(html: str, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = cache_dir / f"albrecht_kemper_events_{stamp}.html"
    output_path.write_text(html, encoding="utf-8")
    return output_path


def _write_text_dump(html: str, dump_dir: Path, *, source_html_path: Path | None = None) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    if source_html_path is not None:
        output_path = dump_dir / f"{source_html_path.stem}.txt"
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = dump_dir / f"albrecht_kemper_events_{stamp}.txt"

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
        print(f"Loading Albrecht-Kemper HTML from file: {input_path}")
        return input_path.read_text(encoding="utf-8"), input_path

    html = await fetch_albrecht_kemper_events_page(url)
    if save_html:
        cache_path = _write_html_cache(html, cache_dir)
        print(f"Saved raw Albrecht-Kemper HTML cache to: {cache_path}")
    return html, None


def clear_albrecht_kemper_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [("Albrecht-Kemper Museum of Art", "St. Joseph", "MO")],
        )

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url == ALBRECHT_KEMPER_SOURCE_BASE,
                    Source.base_url.like(f"{ALBRECHT_KEMPER_SOURCE_BASE}/%"),
                    Source.name.like("albrecht_kemper_%"),
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(f"{ALBRECHT_KEMPER_SOURCE_BASE}/%")
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
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse the Albrecht-Kemper Museum of Art calendar page. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--url", default=ALBRECHT_KEMPER_EVENTS_URL)
    parser.add_argument("--input-html", default=None)
    parser.add_argument("--save-html", action="store_true")
    parser.add_argument("--dump-text", action="store_true")
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--clear", action="store_true")
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_albrecht_kemper_entries()
        print(
            "Deleted Albrecht-Kemper rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    html, source_html_path = await _load_html(
        url=args.url,
        input_html=args.input_html,
        save_html=args.save_html,
        cache_dir=Path(args.cache_dir),
    )

    if args.dump_text:
        dump_path = _write_text_dump(html, Path(args.cache_dir), source_html_path=source_html_path)
        print(f"Saved Albrecht-Kemper text dump to: {dump_path}")

    clear_completed = False

    def _clear_before_commit() -> None:
        nonlocal clear_completed
        if clear_completed or not args.clear:
            return
        deleted = clear_albrecht_kemper_entries()
        print(
            "Deleted Albrecht-Kemper rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        clear_completed = True

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="albrecht_kemper",
                source_url=args.url,
                load_payload=lambda: asyncio.sleep(0, result=html),
                parse_payload=lambda payload: parse_albrecht_kemper_events_html(payload, list_url=args.url),
                parser_name="run_albrecht_kemper_parser",
                adapter_type="albrecht_kemper_events",
                parsed_label="Albrecht-Kemper rows",
                before_commit=_clear_before_commit if args.clear else None,
                empty_parse_details={"source": "Albrecht-Kemper Museum of Art"},
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="run_albrecht_kemper_parser",
            source_url=args.url,
            details={"source": "Albrecht-Kemper Museum of Art"},
        ),
    )

    if args.commit:
        print(
            "Commit summary: "
            f"inserted={summary.total_inserted} "
            f"updated={summary.total_updated} "
            f"unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
