#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, NoReturn


DEFAULT_OUTPUT_ROOT = Path("data/resoures/artmuseums")


def _fail(message: str) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def _normalize_state(value: str) -> str:
    state = value.strip().upper()
    if not state:
        _fail("State must not be empty.")
    return state


def _normalize_museum_name(value: str) -> str:
    museum_name = " ".join(value.split())
    if not museum_name:
        _fail("Museum name must not be empty.")
    return museum_name


def _normalize_city(value: Any) -> str:
    if not isinstance(value, str):
        _fail("City must be a string.")
    city = " ".join(value.split())
    if not city:
        _fail("City must not be empty.")
    return city


def _normalize_event_links(value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        _fail("event_links must be a non-empty list.")

    unique_links: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            _fail("Each event_links item must be a string.")
        link = item.strip()
        if not link.startswith(("http://", "https://")):
            _fail(f"Invalid event link: {link!r}")
        if link not in seen:
            seen.add(link)
            unique_links.append(link)
    return unique_links


def _normalize_payload(payload: Any) -> dict[str, dict[str, Any]]:
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        _fail("JSON payload must be an object mapping museum names to metadata.")

    normalized: dict[str, dict[str, Any]] = {}
    for raw_name, raw_value in payload.items():
        if not isinstance(raw_name, str):
            _fail("Museum names must be strings.")
        if not isinstance(raw_value, dict):
            _fail(f"Museum entry for {raw_name!r} must be an object.")

        museum_name = _normalize_museum_name(raw_name)
        city = _normalize_city(raw_value.get("city"))
        event_links = _normalize_event_links(raw_value.get("event_links"))

        normalized[museum_name] = {
            "city": city,
            "event_links": event_links,
        }
    return normalized


def _load_json(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        return _normalize_payload(json.loads(path.read_text(encoding="utf-8")))
    except json.JSONDecodeError as exc:
        _fail(f"Invalid JSON in {path}: {exc}")


def _write_json(path: Path, payload: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered_payload = {name: payload[name] for name in sorted(payload)}
    path.write_text(json.dumps(ordered_payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Merge newly discovered art museums into data/resoures/artmuseums/{STATE}.json.",
    )
    parser.add_argument("--state", required=True, help="US state code such as NY or CA.")
    parser.add_argument(
        "--incoming",
        required=True,
        help="Path to a JSON file containing only newly discovered museum entries.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Directory that contains per-state museum JSON files.",
    )
    args = parser.parse_args()

    state = _normalize_state(args.state)
    incoming_path = Path(args.incoming).expanduser()
    output_root = Path(args.output_root).expanduser()
    state_path = output_root / f"{state}.json"

    if not incoming_path.exists():
        _fail(f"Incoming file not found: {incoming_path}")

    existing = _load_json(state_path)
    incoming = _load_json(incoming_path)

    added = 0
    skipped_existing = 0
    for museum_name, payload in incoming.items():
        if museum_name in existing:
            skipped_existing += 1
            continue
        existing[museum_name] = payload
        added += 1

    _write_json(state_path, existing)

    print(
        f"Wrote {state_path} | existing={len(existing) - added} added={added} skipped_existing={skipped_existing} total={len(existing)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
