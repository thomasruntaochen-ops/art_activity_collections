#!/usr/bin/env python3
import argparse
import asyncio
import json
import re
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.mcnay import (  # noqa: E402
    MCNAY_EVENTS_URL,
    load_mcnay_payload,
    parse_mcnay_payload,
)
from src.crawlers.pipeline.alerts import abort_commit_on_empty_parse  # noqa: E402
from src.crawlers.pipeline.runner import upsert_extracted_activities_with_stats  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "mcnay"
WHITESPACE_RE = re.compile(r"\s+")


def _write_html_cache(payload: dict[str, object], cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = cache_dir / f"mcnay_events_{stamp}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    return output_path


def _write_text_dump(payload: dict[str, object], dump_dir: Path) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = dump_dir / f"mcnay_events_{stamp}.txt"

    lines: list[str] = []
    listing_html = payload.get("listing_html")
    if isinstance(listing_html, str):
        text = WHITESPACE_RE.sub(" ", re.sub(r"<[^>]+>", " ", listing_html)).strip()
        if text:
            lines.append(text)

    detail_pages = payload.get("detail_pages") or {}
    if isinstance(detail_pages, dict):
        for url in sorted(detail_pages):
            html = detail_pages[url]
            if not isinstance(html, str):
                continue
            lines.append(url)
            text = WHITESPACE_RE.sub(" ", re.sub(r"<[^>]+>", " ", html)).strip()
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
        print(f"Loading McNay payload from file: {input_path}")
        return json.loads(input_path.read_text(encoding="utf-8"))

    try:
        payload = await load_mcnay_payload(url)
    except Exception as exc:
        print(f"Fetch failed ({url}): {exc}")
        raise SystemExit(1) from exc

    if save_html:
        cache_path = _write_html_cache(payload, cache_dir)
        print(f"Saved raw McNay payload cache to: {cache_path}")

    return payload


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse McNay Art Museum event pages. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--url", default=MCNAY_EVENTS_URL)
    parser.add_argument("--input-json", default=None)
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save fetched raw payload to cache directory for debugging.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used by --save-html (default: data/html/mcnay).",
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
    args = parser.parse_args()

    payload = await _load_payload(
        url=args.url,
        input_json=args.input_json,
        save_html=args.save_html,
        cache_dir=Path(args.cache_dir),
    )

    if args.dump_text:
        dump_path = _write_text_dump(payload, Path(args.cache_dir))
        print(f"Saved McNay text dump to: {dump_path}")

    parsed = parse_mcnay_payload(payload)
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
            parser_name="mcnay",
            commit_requested=True,
            parsed_count=len(parsed),
            source_url=args.url,
            details={"cache_dir": str(args.cache_dir)},
        )
        _, stats = upsert_extracted_activities_with_stats(
            source_url=args.url,
            extracted=parsed,
            adapter_type="mcnay_events",
        )
        print(
            "McNay commit summary: "
            f"parsed={len(parsed)}, "
            f"inserted={stats.inserted}, "
            f"updated={stats.updated}, "
            f"unchanged={stats.unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
