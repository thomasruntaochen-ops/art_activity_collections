#!/usr/bin/env python3
import argparse
import glob
import os
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "crawler_venues.toml"


def _load_config(config_path: Path) -> list[dict[str, Any]]:
    if not config_path.exists():
        print(f"Crawler config file not found: {config_path}")
        raise SystemExit(1)
    data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    venues = data.get("venues")
    if not isinstance(venues, list):
        print(f"Invalid config format in {config_path}: expected [[venues]] list")
        raise SystemExit(1)
    return venues


def _matches_local_cache(pattern: str) -> bool:
    pattern_path = Path(pattern)
    search = pattern if pattern_path.is_absolute() else str(PROJECT_ROOT / pattern)
    return bool(glob.glob(search))


def _validate_venue(item: dict[str, Any], index: int) -> None:
    required = ("venue", "parser_script_name", "batch_id", "use_base_url")
    missing = [key for key in required if key not in item]
    if missing:
        print(f"Invalid venue config at index {index}: missing keys {missing}")
        raise SystemExit(1)
    if "args" in item and not isinstance(item.get("args"), list):
        print(f"Invalid venue config at index {index}: args must be a list")
        raise SystemExit(1)


def _should_run_venue(item: dict[str, Any], rawhtml_base_url: str) -> tuple[bool, str]:
    use_base_url = bool(item.get("use_base_url", False))
    local_cache_glob = item.get("local_cache_glob")

    if not use_base_url:
        return True, "use_base_url=false"

    if rawhtml_base_url:
        return True, "RAWHTML_BASE_URL is set"

    if isinstance(local_cache_glob, str) and _matches_local_cache(local_cache_glob):
        return True, f"local cache matched: {local_cache_glob}"

    return False, "RAWHTML_BASE_URL is empty and no local cache matched"


def _run_venue(*, python_bin: str, item: dict[str, Any]) -> int:
    venue = str(item["venue"])
    script_rel = str(item["parser_script_name"])
    args = [str(arg) for arg in item.get("args", ["--commit"])]
    script_path = Path(script_rel)
    if not script_path.is_absolute():
        script_path = PROJECT_ROOT / script_path
    if not script_path.exists():
        print(f"[crawler-batch] Venue={venue} script not found: {script_path}")
        return 1

    cmd = [python_bin, str(script_path), *args]
    printable = " ".join(shlex.quote(part) for part in cmd)
    print(f"[crawler-batch] Running venue={venue}: {printable}")
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        print(f"[crawler-batch] Venue={venue} failed with exit code {result.returncode}")
    else:
        print(f"[crawler-batch] Venue={venue} completed")
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run configured crawler venues with optional batch filtering."
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to crawler venues TOML config file.",
    )
    parser.add_argument(
        "--batch-id",
        default=os.getenv("CRAWLER_BATCH_ID", "").strip(),
        help="Only run venues matching this batch_id. Defaults to CRAWLER_BATCH_ID env.",
    )
    parser.add_argument(
        "--python-bin",
        default=os.getenv("PYTHON_BIN", "python"),
        help="Python binary used to run parser scripts.",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Print selected venues and exit without running parser scripts.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path

    venues = _load_config(config_path)
    rawhtml_base_url = os.getenv("RAWHTML_BASE_URL", "").strip()
    batch_id = args.batch_id

    selected: list[dict[str, Any]] = []
    for index, item in enumerate(venues):
        if not isinstance(item, dict):
            print(f"Invalid venue config at index {index}: expected table/object")
            return 1
        _validate_venue(item, index)

        if not bool(item.get("enabled", True)):
            print(f"[crawler-batch] Skipping disabled venue={item['venue']}")
            continue

        if batch_id and str(item.get("batch_id", "")) != batch_id:
            continue

        selected.append(item)

    if batch_id:
        print(f"[crawler-batch] Batch filter active: batch_id={batch_id}")
    else:
        print("[crawler-batch] No batch filter; running all enabled venues")

    if not selected:
        print("[crawler-batch] No venues selected; exiting")
        return 0

    if args.list_only:
        names = ", ".join(str(item["venue"]) for item in selected)
        print(f"[crawler-batch] list-only selected venues: {names}")
        return 0

    for item in selected:
        venue = str(item["venue"])
        should_run, reason = _should_run_venue(item, rawhtml_base_url)
        if not should_run:
            print(f"[crawler-batch] Skipping venue={venue}: {reason}")
            continue
        print(f"[crawler-batch] Venue={venue} selected: {reason}")

        exit_code = _run_venue(python_bin=args.python_bin, item=item)
        if exit_code != 0:
            return exit_code

    print("[crawler-batch] Completed selected venues")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
