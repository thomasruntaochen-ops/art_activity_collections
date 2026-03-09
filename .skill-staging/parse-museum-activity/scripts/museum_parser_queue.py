#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, NoReturn


DEFAULT_OUTPUT_ROOT = Path("data/resoures/artmuseums")
STATE_NAME_TO_CODE = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}


def _fail(message: str) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def _normalize_state(value: str) -> str:
    raw = " ".join(value.strip().split())
    if not raw:
        _fail("State must not be empty.")

    if len(raw) == 2 and raw.isalpha():
        return raw.upper()

    code = STATE_NAME_TO_CODE.get(raw.lower())
    if code is None:
        _fail(f"Unsupported state value: {value!r}")
    return code


def _state_path(state_input: str, output_root: Path) -> Path:
    state_code = _normalize_state(state_input)
    return output_root / f"{state_code}.json"


def _load_state_file(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        _fail(f"State file not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        _fail(f"Invalid JSON in {path}: {exc}")

    if not isinstance(payload, dict):
        _fail(f"State file must be a JSON object: {path}")

    normalized: dict[str, dict[str, Any]] = {}
    for museum_name, museum_value in payload.items():
        if not isinstance(museum_name, str):
            _fail("Museum names must be strings.")
        if not isinstance(museum_value, dict):
            _fail(f"Museum entry for {museum_name!r} must be an object.")
        normalized[museum_name] = dict(museum_value)
    return normalized


def _write_state_file(path: Path, payload: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _pending_museums(payload: dict[str, dict[str, Any]], limit: int | None) -> dict[str, dict[str, Any]]:
    pending: dict[str, dict[str, Any]] = {}
    for museum_name, museum_value in payload.items():
        if (
            museum_value.get("parser_script")
            or museum_value.get("fail_implement")
            or museum_value.get("failed_test_comment")
        ):
            continue
        pending[museum_name] = museum_value
        if limit is not None and len(pending) >= limit:
            break
    return pending


def _require_museum(payload: dict[str, dict[str, Any]], museum_name: str) -> dict[str, Any]:
    if museum_name not in payload:
        _fail(f"Museum not found in state file: {museum_name}")
    return payload[museum_name]


def _cmd_pending(args: argparse.Namespace) -> int:
    state_path = _state_path(args.state, Path(args.output_root))
    payload = _load_state_file(state_path)
    pending = _pending_museums(payload, args.limit)
    print(json.dumps(pending, indent=2, ensure_ascii=False))
    return 0


def _cmd_mark_success(args: argparse.Namespace) -> int:
    state_path = _state_path(args.state, Path(args.output_root))
    payload = _load_state_file(state_path)
    museum = _require_museum(payload, args.museum)
    museum["parser_script"] = args.parser_script
    museum.pop("fail_implement", None)
    museum.pop("failed_test_comment", None)
    _write_state_file(state_path, payload)
    print(f"Updated {state_path}: set parser_script for {args.museum}")
    return 0


def _cmd_mark_failure(args: argparse.Namespace) -> int:
    state_path = _state_path(args.state, Path(args.output_root))
    payload = _load_state_file(state_path)
    museum = _require_museum(payload, args.museum)
    museum["fail_implement"] = args.reason.strip()
    museum.pop("failed_test_comment", None)
    _write_state_file(state_path, payload)
    print(f"Updated {state_path}: set fail_implement for {args.museum}")
    return 0


def _cmd_mark_test_failure(args: argparse.Namespace) -> int:
    state_path = _state_path(args.state, Path(args.output_root))
    payload = _load_state_file(state_path)
    museum = _require_museum(payload, args.museum)
    museum["failed_test_comment"] = args.comment.strip()
    museum.pop("parser_script", None)
    _write_state_file(state_path, payload)
    print(f"Updated {state_path}: set failed_test_comment for {args.museum}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List pending museums from a state JSON file and update parser status keys.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Directory that contains per-state museum JSON files.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    pending_parser = subparsers.add_parser(
        "pending",
        help="Print museums that do not yet have parser_script, fail_implement, or failed_test_comment.",
    )
    pending_parser.add_argument("--state", required=True, help="US state code or full state name.")
    pending_parser.add_argument("--limit", type=int, default=None, help="Maximum museums to print.")
    pending_parser.set_defaults(func=_cmd_pending)

    success_parser = subparsers.add_parser(
        "mark-success",
        help="Set parser_script for one museum and remove fail_implement or failed_test_comment if present.",
    )
    success_parser.add_argument("--state", required=True, help="US state code or full state name.")
    success_parser.add_argument("--museum", required=True, help="Exact museum name key from the JSON file.")
    success_parser.add_argument("--parser-script", required=True, help="Runner script path, for example scripts/run_moma_parser.py.")
    success_parser.set_defaults(func=_cmd_mark_success)

    failure_parser = subparsers.add_parser(
        "mark-failure",
        help="Set fail_implement for one museum.",
    )
    failure_parser.add_argument("--state", required=True, help="US state code or full state name.")
    failure_parser.add_argument("--museum", required=True, help="Exact museum name key from the JSON file.")
    failure_parser.add_argument("--reason", required=True, help="Concrete reason the parser could not be implemented.")
    failure_parser.set_defaults(func=_cmd_mark_failure)

    test_failure_parser = subparsers.add_parser(
        "mark-test-failure",
        help="Set failed_test_comment for one museum after a zero-row dry run.",
    )
    test_failure_parser.add_argument("--state", required=True, help="US state code or full state name.")
    test_failure_parser.add_argument("--museum", required=True, help="Exact museum name key from the JSON file.")
    test_failure_parser.add_argument(
        "--comment",
        required=True,
        help="Concrete note explaining why the dry run produced zero rows and why the parser was not added.",
    )
    test_failure_parser.set_defaults(func=_cmd_mark_test_failure)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
