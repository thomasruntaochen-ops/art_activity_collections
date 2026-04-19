#!/usr/bin/env python3
import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.adapters.met import (
    MET_TEENS_FREE_WORKSHOPS_URL,
    fetch_met_events_page_playwright,
    parse_met_events_html,
)
from src.crawlers.pipeline.script_runner import TargetRunSpec
from src.crawlers.pipeline.script_runner import run_targets


DEFAULT_CACHE_DIR = PROJECT_ROOT / "data" / "rawhtml" / "met"
RAWHTML_BASE_URL_ENV = "RAWHTML_BASE_URL"
DEFAULT_REMOTE_BASE_URL = os.getenv(RAWHTML_BASE_URL_ENV, "").strip()
REMOTE_SUBDIR = "met"
REMOTE_FILENAME = "latest_events.html"


def _write_text_dump(html: str, dump_dir: Path, *, source_html_path: Path | None = None) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    if source_html_path is not None:
        output_path = dump_dir / f"{source_html_path.stem}.txt"
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = dump_dir / f"met_events_{stamp}.txt"

    soup = BeautifulSoup(html, "html.parser")
    lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def _creation_timestamp(path: Path) -> float:
    stat = path.stat()
    return getattr(stat, "st_birthtime", stat.st_ctime)


def _remote_html_url(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/{REMOTE_SUBDIR}/{REMOTE_FILENAME}"


def _download_remote_html_if_configured(*, cache_dir: Path, remote_base_url: str) -> Path | None:
    if not remote_base_url:
        return None

    remote_url = _remote_html_url(remote_base_url)
    output_path = cache_dir / REMOTE_FILENAME
    cache_dir.mkdir(parents=True, exist_ok=True)

    try:
        request = Request(
            remote_url,
            headers={
                # Cloudflare may block default Python-urllib user agents.
                "User-Agent": "Mozilla/5.0 (compatible; art-activity-collection/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        with urlopen(request, timeout=20) as response:
            html = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = ""
        try:
            detail = exc.read(200).decode("utf-8", errors="replace").strip()
        except Exception:
            pass
        if detail:
            print(f"Remote HTML download failed ({remote_url}): HTTP {exc.code} {exc.reason} | {detail}")
        else:
            print(f"Remote HTML download failed ({remote_url}): HTTP {exc.code} {exc.reason}")
        return None
    except Exception as exc:
        print(f"Remote HTML download failed ({remote_url}): {exc}")
        return None

    output_path.write_text(html, encoding="utf-8")
    print(f"Downloaded remote HTML to: {output_path}")
    return output_path


def _resolve_input_html_path(*, input_html: str | None, cache_dir: Path, remote_base_url: str) -> Path:
    if input_html:
        input_path = Path(input_html)
        if not input_path.exists():
            print(f"Input HTML file not found: {input_path}")
            raise SystemExit(1)
        return input_path

    remote_path = _download_remote_html_if_configured(cache_dir=cache_dir, remote_base_url=remote_base_url)
    if remote_path is not None:
        return remote_path

    html_files = [path for path in cache_dir.glob("*.html") if path.is_file()]
    if not html_files:
        print(f"No HTML files found in cache directory: {cache_dir}")
        if remote_base_url:
            print(f"Tried remote base URL: {remote_base_url}")
        print("Provide --input-html or place an HTML file under data/rawhtml/met.")
        raise SystemExit(1)

    return max(html_files, key=_creation_timestamp)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Parse MET teens/free/workshops events from local HTML. "
            "Print parsed rows, and optionally commit to DB."
        )
    )
    parser.add_argument("--url", default=MET_TEENS_FREE_WORKSHOPS_URL)
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used to auto-select the newest HTML file (default: data/rawhtml/met).",
    )
    parser.add_argument(
        "--input-html",
        default=None,
        help="Parse from a specific local HTML file. If omitted, load newest HTML in --cache-dir.",
    )
    parser.add_argument(
        "--remote-base-url",
        default=DEFAULT_REMOTE_BASE_URL,
        help=(
            "Optional remote base URL for raw HTML storage. "
            "When set, parser downloads met/latest_events.html before parsing. "
            "Can also be set via RAWHTML_BASE_URL."
        ),
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help=(
            "Fetch the MET events page directly via Playwright (headless browser) "
            "instead of loading from a local HTML file. Bypasses Vercel bot detection."
        ),
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
    args = parser.parse_args()

    if args.fetch:
        print("[met] Fetching live page via Playwright...")
        html = await fetch_met_events_page_playwright(args.url)
        print(f"[met] Fetched {len(html)} bytes")
        input_path = None
    else:
        input_path = _resolve_input_html_path(
            input_html=args.input_html,
            cache_dir=Path(args.cache_dir),
            remote_base_url=args.remote_base_url,
        )
        print(f"Loading HTML from file: {input_path}")
        html = input_path.read_text(encoding="utf-8")

    if args.dump_text:
        dump_path = _write_text_dump(html, Path(args.cache_dir), source_html_path=input_path if input_path else None)
        print(f"Saved text dump to: {dump_path}")

    async def _load_cached_html() -> str:
        return html

    summary = await run_targets(
        targets=[
            TargetRunSpec(
                name="met",
                source_url=args.url,
                load_payload=_load_cached_html,
                parse_payload=lambda payload: parse_met_events_html(html=payload, list_url=args.url),
                parser_name="met",
                adapter_type="met_events_filtered",
                parsed_label="rows",
                empty_parse_details={
                    "cache_dir": str(args.cache_dir),
                    "input_html": str(input_path) if input_path else "live-fetch",
                },
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
