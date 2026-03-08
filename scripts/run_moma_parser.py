#!/usr/bin/env python3
import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup
from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.moma import (
    MOMA_KIDS_CALENDAR_URL,
    MOMA_TEENS_CALENDAR_URL,
    fetch_moma_events_page,
    parse_moma_events_html,
)
from src.crawlers.pipeline.alerts import abort_commit_on_empty_parse
from src.crawlers.pipeline.runner import upsert_extracted_activities_with_stats
from src.db.session import SessionLocal
from src.models.activity import Activity, Source


DEFAULT_CACHE_DIR = Path("data") / "html" / "moma"
MOMA_SOURCE_URL_PREFIX = "https://www.moma.org/%"


def _json_ready(row: dict) -> dict:
    out = dict(row)
    for key in ("start_at", "end_at"):
        value = out.get(key)
        if isinstance(value, datetime):
            out[key] = value.isoformat()
    return out


def _write_html_cache(html: str, cache_dir: Path, *, audience: str) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = cache_dir / f"moma_{audience}_events_{stamp}.html"
    output_path.write_text(html, encoding="utf-8")
    return output_path


def _write_text_dump(
    html: str,
    dump_dir: Path,
    *,
    audience: str,
    source_html_path: Path | None = None,
) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    if source_html_path is not None:
        output_path = dump_dir / f"{source_html_path.stem}.txt"
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = dump_dir / f"moma_{audience}_events_{stamp}.txt"

    soup = BeautifulSoup(html, "html.parser")
    lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


async def _load_html(
    *,
    audience: str,
    url: str,
    input_html: str | None,
    save_html: bool,
    cache_dir: Path,
) -> tuple[str, Path | None]:
    input_path: Path | None = None
    if input_html:
        input_path = Path(input_html)
        if not input_path.exists():
            print(f"Input HTML file not found for {audience}: {input_path}")
            raise SystemExit(1)
        print(f"Loading {audience} HTML from file: {input_path}")
        html = input_path.read_text(encoding="utf-8")
        return html, input_path

    try:
        html = await fetch_moma_events_page(url)
    except Exception as exc:
        print(f"Fetch failed for {audience} ({url}): {exc}")
        raise SystemExit(1) from exc

    if save_html:
        cache_path = _write_html_cache(html, cache_dir, audience=audience)
        print(f"Saved {audience} raw HTML cache to: {cache_path}")

    return html, None


def clear_moma_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(MOMA_SOURCE_URL_PREFIX),
                    Source.name.like("moma_%"),
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(MOMA_SOURCE_URL_PREFIX)
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
            "Fetch and parse MoMA teens/kids calendar pages. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument(
        "--audience",
        choices=["teens", "kids", "both"],
        default="both",
        help="Which MoMA audience pages to parse (default: both).",
    )
    parser.add_argument("--teens-url", default=MOMA_TEENS_CALENDAR_URL)
    parser.add_argument("--kids-url", default=MOMA_KIDS_CALENDAR_URL)
    parser.add_argument("--input-teens-html", default=None)
    parser.add_argument("--input-kids-html", default=None)
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched raw HTML to cache directory for debugging.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used by --save-html (default: data/html/moma).",
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
            "Delete all MoMA DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_moma_entries()
        print(
            "Deleted MoMA rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    if args.clear and args.commit:
        print("Clear requested with --commit; deletion is deferred until non-empty parse is validated.")

    audience_targets: list[tuple[str, str, str | None]] = []
    if args.audience in ("teens", "both"):
        audience_targets.append(("teens", args.teens_url, args.input_teens_html))
    if args.audience in ("kids", "both"):
        audience_targets.append(("kids", args.kids_url, args.input_kids_html))

    all_rows = []
    parsed_batches: list[tuple[str, str, list]] = []
    total_inserted = 0
    total_updated = 0
    total_unchanged = 0

    for audience, url, input_html in audience_targets:
        html, source_html_path = await _load_html(
            audience=audience,
            url=url,
            input_html=input_html,
            save_html=args.save_html,
            cache_dir=Path(args.cache_dir),
        )

        if args.dump_text:
            dump_path = _write_text_dump(
                html,
                Path(args.cache_dir),
                audience=audience,
                source_html_path=source_html_path,
            )
            print(f"Saved {audience} text dump to: {dump_path}")

        parsed = parse_moma_events_html(html=html, audience=audience, list_url=url)
        all_rows.extend(parsed)
        parsed_batches.append((audience, url, parsed))

        print(f"Parsed {audience} rows: {len(parsed)}")
        for row in parsed:
            item = _json_ready(asdict(row))
            item["audience"] = audience
            print(json.dumps(item, ensure_ascii=True))

    print(f"Total parsed rows: {len(all_rows)}")
    if not args.commit:
        print("Dry run only. Pass --commit to write to DB.")
        return

    abort_commit_on_empty_parse(
        parser_name="moma",
        commit_requested=args.commit,
        parsed_count=len(all_rows),
        details={"audience_mode": args.audience, "cache_dir": str(args.cache_dir)},
    )

    if args.clear:
        deleted = clear_moma_entries()
        print(
            "Deleted MoMA rows before repopulation: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )

    for audience, url, parsed in parsed_batches:
        _, stats = upsert_extracted_activities_with_stats(
            source_url=url,
            extracted=parsed,
            adapter_type="moma_calendar_audience",
        )
        total_inserted += stats.inserted
        total_updated += stats.updated
        total_unchanged += stats.unchanged
        print(
            f"MySQL write summary ({audience}): "
            f"input={stats.input_rows}, "
            f"deduped_input={stats.deduped_rows}, "
            f"inserted={stats.inserted}, "
            f"updated={stats.updated}, "
            f"unchanged={stats.unchanged}, "
            f"written={stats.written_rows}"
        )

    print(
        "MySQL write totals (all audiences): "
        f"inserted={total_inserted}, "
        f"updated={total_updated}, "
        f"unchanged={total_unchanged}, "
        f"written={total_inserted + total_updated}"
    )


if __name__ == "__main__":
    asyncio.run(main())
