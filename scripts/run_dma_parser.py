#!/usr/bin/env python3
import argparse
import asyncio
import json
import re
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import bindparam
from sqlalchemy import delete
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.dma import (  # noqa: E402
    DMA_CITY,
    DMA_EVENTS_URL,
    DMA_STATE,
    DMA_VENUE_NAME,
    load_dma_payload,
    parse_dma_payload,
)
from src.crawlers.pipeline.alerts import abort_commit_on_empty_parse  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.runner import upsert_extracted_activities_with_stats  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "dma"
WHITESPACE_RE = re.compile(r"\s+")
DMA_SOURCE_URL_PREFIXES = ("https://dma.org/%", "https://www.dma.org/%")


def _write_html_cache(payload: dict[str, object], cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = cache_dir / f"dma_events_{stamp}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    return output_path


def _write_text_dump(payload: dict[str, object], dump_dir: Path) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = dump_dir / f"dma_events_{stamp}.txt"

    lines: list[str] = []
    for result in payload.get("results", []):
        if not isinstance(result, dict):
            continue
        line = " | ".join(
            part
            for part in [
                str(result.get("field_caption_text") or "").strip(),
                str(result.get("date") or "").strip(),
                str(result.get("url") or "").strip(),
            ]
            if part
        )
        if line:
            lines.append(WHITESPACE_RE.sub(" ", line).strip())

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def clear_dma_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(
            db,
            [(DMA_VENUE_NAME, DMA_CITY, DMA_STATE)],
        )

        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url == DMA_EVENTS_URL,
                    Source.base_url.like("https://dma.org/%"),
                    Source.base_url.like("https://www.dma.org/%"),
                    Source.name.like("dma_%"),
                    Source.adapter_type == "dma_events",
                )
            )
        ).all()

        url_filters = [Activity.source_url.like(prefix) for prefix in DMA_SOURCE_URL_PREFIXES]
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


async def _load_payload(
    *,
    url: str,
    input_json: str | None,
    save_html: bool,
    cache_dir: Path,
) -> dict[str, object]:
    if input_json:
        input_path = Path(input_json)
        if not input_path.exists():
            print(f"Input payload file not found: {input_path}")
            raise SystemExit(1)
        print(f"Loading DMA payload from file: {input_path}")
        return json.loads(input_path.read_text(encoding="utf-8"))

    try:
        payload = await load_dma_payload(url)
    except Exception as exc:
        print(f"Fetch failed ({url}): {exc}")
        raise SystemExit(1) from exc

    if save_html:
        cache_path = _write_html_cache(payload, cache_dir)
        print(f"Saved raw DMA payload cache to: {cache_path}")

    return payload


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse Dallas Museum of Art calendar events. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--url", default=DMA_EVENTS_URL)
    parser.add_argument("--input-json", default=None)
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched raw payload to cache directory for debugging.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used by --save-html (default: data/html/dma).",
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
            "Delete all DMA DB rows (activity_tags, activities, ingestion_runs, sources). "
            "If used without --commit, the script exits after deletion."
        ),
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_dma_entries()
        print(
            "Deleted DMA rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    if args.clear and args.commit:
        print("Clear requested with --commit; deletion is deferred until non-empty parse is validated.")

    payload = await _load_payload(
        url=args.url,
        input_json=args.input_json,
        save_html=args.save_html,
        cache_dir=Path(args.cache_dir),
    )

    if args.dump_text:
        dump_path = _write_text_dump(payload, Path(args.cache_dir))
        print(f"Saved DMA text dump to: {dump_path}")

    parsed = parse_dma_payload(payload)
    print(f"Parsed {len(parsed)} rows")
    for row in parsed:
        row_payload = asdict(row)
        for key in ("start_at", "end_at"):
            value = row_payload.get(key)
            if isinstance(value, datetime):
                row_payload[key] = value.isoformat()
        print(json.dumps(row_payload, ensure_ascii=True))

    if args.commit:
        abort_commit_on_empty_parse(
            parser_name="dma",
            commit_requested=True,
            parsed_count=len(parsed),
            source_url=args.url,
            details={"cache_dir": str(args.cache_dir)},
        )
        if args.clear:
            deleted = clear_dma_entries()
            print(
                "Deleted DMA rows before repopulation: "
                f"activity_tags={deleted['activity_tags']}, "
                f"activities={deleted['activities']}, "
                f"ingestion_runs={deleted['ingestion_runs']}, "
                f"sources={deleted['sources']}"
            )
        _, stats = upsert_extracted_activities_with_stats(
            source_url=args.url,
            extracted=parsed,
            adapter_type="dma_events",
        )
        print(
            "DMA commit summary: "
            f"parsed={len(parsed)}, "
            f"inserted={stats.inserted}, "
            f"updated={stats.updated}, "
            f"unchanged={stats.unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
