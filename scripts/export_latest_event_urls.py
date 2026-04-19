#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from html import unescape
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.session import engine


QUERY = text(
    """
    WITH ranked AS (
        SELECT
            v.name AS venue,
            COALESCE(v.state, '') AS state,
            a.title AS event_name,
            a.source_url AS latest_url,
            a.start_at AS start_at,
            ROW_NUMBER() OVER (
                PARTITION BY v.id
                ORDER BY a.start_at DESC, a.updated_at DESC, a.id DESC
            ) AS rn
        FROM activities a
        JOIN venues v ON v.id = a.venue_id
        WHERE a.source_url IS NOT NULL
          AND a.source_url <> ''
    )
    SELECT venue, state, event_name, latest_url, start_at
    FROM ranked
    WHERE rn = 1
    ORDER BY state, venue
    """
)


def _escape_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def build_markdown() -> str:
    with engine.connect() as conn:
        rows = conn.execute(QUERY).fetchall()

    lines = [
        "# Event URL Test",
        "",
        f"Generated from local MySQL. Rows: {len(rows)}.",
        "",
        "| venue | state | event_name | latest_url_link |",
        "| --- | --- | --- | --- |",
    ]

    for venue, state, event_name, latest_url, _start_at in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    _escape_cell(unescape(venue or "")),
                    _escape_cell(unescape(state or "")),
                    _escape_cell(unescape(event_name or "")),
                    _escape_cell(latest_url or ""),
                ]
            )
            + " |"
        )

    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export one latest event URL row per venue from local MySQL."
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "event_url_test.md"),
        help="Output markdown path.",
    )
    args = parser.parse_args()

    output_path = Path(args.output).resolve()
    output_path.write_text(build_markdown(), encoding="utf-8")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
