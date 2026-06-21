#!/usr/bin/env python3
import argparse
import asyncio
import json
import re
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import bindparam, delete, or_, select, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.kimbell import (  # noqa: E402
    KIMBELL_EVENTS_URL,
    load_kimbell_payload,
    parse_kimbell_payload,
)
from src.crawlers.pipeline.alerts import abort_commit_on_empty_parse  # noqa: E402
from src.crawlers.pipeline.clear_utils import lookup_venue_ids  # noqa: E402
from src.crawlers.pipeline.runner import upsert_extracted_activities_with_stats  # noqa: E402
from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Activity  # noqa: E402
from src.models.activity import Source  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "kimbell"
WHITESPACE_RE = re.compile(r"\s+")
KIMBELL_SOURCE_URL_PREFIX = "https://kimbellart.org/%"


def clear_kimbell_entries() -> dict[str, int]:
    deleted_activity_tags = 0
    deleted_activities = 0
    deleted_ingestion_runs = 0
    deleted_sources = 0

    with SessionLocal() as db:
        venue_ids = lookup_venue_ids(db, [("Kimbell Art Museum", "Fort Worth", "TX")])
        source_ids = db.scalars(
            select(Source.id).where(
                or_(
                    Source.base_url.like(KIMBELL_SOURCE_URL_PREFIX),
                    Source.name.like("kimbell%"),
                )
            )
        ).all()

        activity_filter = Activity.source_url.like(KIMBELL_SOURCE_URL_PREFIX)
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
    output_path = cache_dir / f"kimbell_events_{stamp}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    return output_path


def _write_text_dump(payload: dict[str, object], dump_dir: Path) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = dump_dir / f"kimbell_events_{stamp}.txt"

    lines: list[str] = []
    listing_html = payload.get("listing_html")
    if isinstance(listing_html, str):
        text = WHITESPACE_RE.sub(" ", re.sub(r"<[^>]+>", " ", listing_html)).strip()
        if text:
            lines.append(text)

    output_path.write_text("\n\n".join(lines) + "\n", encoding="utf-8")
    return output_path


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
        print(f"Loading Kimbell payload from file: {input_path}")
        return json.loads(input_path.read_text(encoding="utf-8"))

    try:
        payload = await load_kimbell_payload(url)
    except Exception as exc:
        print(f"Fetch failed ({url}): {exc}")
        raise SystemExit(1) from exc

    if save_html:
        cache_path = _write_html_cache(payload, cache_dir)
        print(f"Saved raw Kimbell payload cache to: {cache_path}")

    return payload


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse Kimbell Art Museum calendar activities. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--url", default=KIMBELL_EVENTS_URL)
    parser.add_argument("--input-json", default=None)
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched raw payload to cache directory for debugging.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used by --save-html (default: data/html/kimbell).",
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
        help="Delete scoped Kimbell DB rows before commit.",
    )
    args = parser.parse_args()

    if args.clear and not args.commit:
        deleted = clear_kimbell_entries()
        print(
            "Deleted Kimbell rows: "
            f"activity_tags={deleted['activity_tags']}, "
            f"activities={deleted['activities']}, "
            f"ingestion_runs={deleted['ingestion_runs']}, "
            f"sources={deleted['sources']}"
        )
        print("Clear completed. Pass --commit with --clear to repopulate immediately.")
        return

    payload = await _load_payload(
        url=args.url,
        input_json=args.input_json,
        save_html=args.save_html,
        cache_dir=Path(args.cache_dir),
    )

    if args.dump_text:
        dump_path = _write_text_dump(payload, Path(args.cache_dir))
        print(f"Saved Kimbell text dump to: {dump_path}")

    parsed = parse_kimbell_payload(payload)
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
            parser_name="kimbell",
            commit_requested=True,
            parsed_count=len(parsed),
            source_url=args.url,
            details={"cache_dir": str(args.cache_dir)},
        )
        if args.clear:
            deleted = clear_kimbell_entries()
            print(
                "Deleted Kimbell rows before repopulation: "
                f"activity_tags={deleted['activity_tags']}, "
                f"activities={deleted['activities']}, "
                f"ingestion_runs={deleted['ingestion_runs']}, "
                f"sources={deleted['sources']}"
            )
        _, stats = upsert_extracted_activities_with_stats(
            source_url=args.url,
            extracted=parsed,
            adapter_type="kimbell_events",
        )
        print(
            "Kimbell commit summary: "
            f"parsed={len(parsed)}, "
            f"inserted={stats.inserted}, "
            f"updated={stats.updated}, "
            f"unchanged={stats.unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
