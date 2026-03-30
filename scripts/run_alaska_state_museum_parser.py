#!/usr/bin/env python3
import argparse
import asyncio
import json
import re
import sys
from dataclasses import asdict
from datetime import datetime
from datetime import timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.alaska_state_museum import ASM_EVENTS_URL  # noqa: E402
from src.crawlers.adapters.alaska_state_museum import load_alaska_state_museum_payload  # noqa: E402
from src.crawlers.adapters.alaska_state_museum import parse_alaska_state_museum_payload  # noqa: E402
from src.crawlers.pipeline.script_runner import EmptyCommitGuard  # noqa: E402
from src.crawlers.pipeline.script_runner import TargetRunSpec  # noqa: E402
from src.crawlers.pipeline.script_runner import run_targets  # noqa: E402


DEFAULT_CACHE_DIR = Path("data") / "html" / "alaska_state_museum"
WHITESPACE_RE = re.compile(r"\s+")


def _write_html_cache(payload: dict[str, object], cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = cache_dir / f"alaska_state_museum_{stamp}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    return output_path


def _write_text_dump(payload: dict[str, object], dump_dir: Path) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = dump_dir / f"alaska_state_museum_{stamp}.txt"

    lines: list[str] = []
    pages = payload.get("pages") or {}
    if isinstance(pages, dict):
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
        print(f"Loading Alaska State Museum payload from file: {input_path}")
        return json.loads(input_path.read_text(encoding="utf-8"))

    try:
        payload = await load_alaska_state_museum_payload()
    except Exception as exc:
        print(f"Fetch failed: {exc}")
        raise SystemExit(1) from exc

    if save_html:
        cache_path = _write_html_cache(payload, cache_dir)
        print(f"Saved raw Alaska State Museum payload cache to: {cache_path}")

    return payload


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse Alaska State Museum activity pages. "
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
        help="Directory used by --save-html (default: data/html/alaska_state_museum).",
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
        input_json=args.input_json,
        save_html=args.save_html,
        cache_dir=Path(args.cache_dir),
    )

    if args.dump_text:
        dump_path = _write_text_dump(payload, Path(args.cache_dir))
        print(f"Saved Alaska State Museum text dump to: {dump_path}")

    async def _payload_loader() -> dict[str, object]:
        return payload

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="alaska_state_museum",
                source_url=ASM_EVENTS_URL,
                load_payload=_payload_loader,
                parse_payload=parse_alaska_state_museum_payload,
                parser_name="alaska_state_museum",
                adapter_type="alaska_state_museum_events",
                parsed_label="Alaska State Museum rows",
            )
        ],
        commit=args.commit,
        empty_commit_guard=EmptyCommitGuard(
            parser_name="alaska_state_museum",
            source_url=ASM_EVENTS_URL,
            details={"cache_dir": str(args.cache_dir)},
        ),
    )

    if args.commit:
        print(
            "Alaska State Museum commit summary: "
            f"parsed={summary.total_parsed}, "
            f"inserted={summary.total_inserted}, "
            f"updated={summary.total_updated}, "
            f"unchanged={summary.total_unchanged}"
        )


if __name__ == "__main__":
    asyncio.run(main())
