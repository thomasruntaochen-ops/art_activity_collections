"""De-duplicate sources, venues, and activities created by overlapping crawl runs.

An unguarded select-then-insert (each ingest in its own transaction, no unique
constraint) let two concurrent crawl runs each insert a full copy of a source,
its venue, and its activities. This script merges those copies back together.

Order matters:
  1. Sources  — merge rows sharing a base_url; re-point activities + ingestion_runs.
  2. Venues   — merge rows sharing (name, city, state); re-point activities.
  3. Activities — now that source_id/venue_id are canonical, collapse rows sharing
                  (source_id, source_url, title, start_at); also clean activity_tags.

Run a dry-run first (default), then apply:
    source ~/.zshrc && python scripts/dedupe_venues.py
    source ~/.zshrc && python scripts/dedupe_venues.py --commit
    source ~/.zshrc && python scripts/dedupe_venues.py --commit --add-constraints
"""

import argparse
import sys
from pathlib import Path

from sqlalchemy import bindparam, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.session import engine  # noqa: E402


def _in(conn, sql: str, ids: list[int], **params) -> int:
    """Execute a statement with an expanding `:ids` IN-list; return rowcount."""
    if not ids:
        return 0
    stmt = text(sql).bindparams(bindparam("ids", expanding=True))
    return conn.execute(stmt, {"ids": ids, **params}).rowcount


def _groups(rows, key_fn, rank_fn):
    """Group rows by key_fn; for groups with >1 row return (canonical, [dup_ids])."""
    buckets: dict = {}
    for row in rows:
        buckets.setdefault(key_fn(row), []).append(row)
    out = []
    for members in buckets.values():
        if len(members) < 2:
            continue
        ordered = sorted(members, key=rank_fn)  # best (canonical) first
        out.append((ordered[0], [m.id for m in ordered[1:]]))
    return out


def dedupe_sources(conn) -> dict:
    rows = conn.execute(text("SELECT id, base_url FROM sources")).all()
    groups = _groups(rows, key_fn=lambda r: (r.base_url or "").strip(), rank_fn=lambda r: r.id)
    repointed_acts = repointed_runs = removed = 0
    for canon, dups in groups:
        repointed_acts += _in(conn, "UPDATE activities SET source_id = :c WHERE source_id IN :ids", dups, c=canon.id)
        repointed_runs += _in(conn, "UPDATE ingestion_runs SET source_id = :c WHERE source_id IN :ids", dups, c=canon.id)
        removed += _in(conn, "DELETE FROM sources WHERE id IN :ids", dups)
    return {"groups": len(groups), "removed": removed, "repointed_acts": repointed_acts, "repointed_runs": repointed_runs}


def dedupe_venues(conn) -> dict:
    rows = conn.execute(
        text(
            """
            SELECT v.id, TRIM(v.name) AS name, v.city, v.state,
                   (v.address IS NOT NULL) AS has_address,
                   (v.lat IS NOT NULL) AS has_coords,
                   (SELECT COUNT(*) FROM activities a WHERE a.venue_id = v.id) AS acts
            FROM venues v
            """
        )
    ).all()
    groups = _groups(
        rows,
        key_fn=lambda r: (r.name, (r.city or "").strip() or None, (r.state or "").strip().upper() or None),
        # canonical = has address, has coords, most activities, then lowest id
        rank_fn=lambda r: (0 if r.has_address else 1, 0 if r.has_coords else 1, -int(r.acts), r.id),
    )
    repointed = removed = 0
    detail = []
    for canon, dups in groups:
        repointed += _in(conn, "UPDATE activities SET venue_id = :c WHERE venue_id IN :ids", dups, c=canon.id)
        removed += _in(conn, "DELETE FROM venues WHERE id IN :ids", dups)
        detail.append((canon.name, canon.id, dups))
    return {"groups": len(groups), "removed": removed, "repointed": repointed, "detail": detail}


def dedupe_activities(conn) -> dict:
    rows = conn.execute(
        text("SELECT id, source_id, source_url, title, start_at FROM activities ORDER BY id")
    ).all()
    groups = _groups(
        rows,
        key_fn=lambda r: (r.source_id, r.source_url, r.title, r.start_at),
        rank_fn=lambda r: r.id,
    )
    dup_ids = [i for _, dups in groups for i in dups]
    removed_tags = _in(conn, "DELETE FROM activity_tags WHERE activity_id IN :ids", dup_ids)
    removed = _in(conn, "DELETE FROM activities WHERE id IN :ids", dup_ids)
    return {"groups": len(groups), "removed": removed, "removed_tags": removed_tags}


def add_constraints() -> None:
    # DDL auto-commits in MySQL, so run it after the data merge has committed.
    statements = [
        ("uq_venues_name_city_state", "ALTER TABLE venues ADD CONSTRAINT uq_venues_name_city_state UNIQUE (name, city, state)"),
        ("uq_sources_base_url", "ALTER TABLE sources ADD UNIQUE INDEX uq_sources_base_url (base_url(255))"),
    ]
    print("\nAdding unique constraints:")
    for name, sql in statements:
        try:
            with engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
            print(f"  added {name}")
        except Exception as exc:  # noqa: BLE001 - report and continue
            print(f"  skipped {name}: {type(exc).__name__}: {str(exc).splitlines()[0][:140]}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge duplicate sources/venues/activities.")
    parser.add_argument("--commit", action="store_true", help="Apply changes (default is a dry run).")
    parser.add_argument("--add-constraints", action="store_true", help="Also add unique constraints (with --commit).")
    args = parser.parse_args()

    conn = engine.connect()
    trans = conn.begin()
    try:
        sources = dedupe_sources(conn)
        venues = dedupe_venues(conn)
        activities = dedupe_activities(conn)

        mode = "COMMIT" if args.commit else "DRY-RUN"
        print("=" * 64)
        print(f"VENUE/SOURCE/ACTIVITY DEDUPE  ({mode})")
        print("=" * 64)
        print(f"Sources   : {sources['groups']:>3} dup groups -> remove {sources['removed']:>3} rows "
              f"(re-pointed {sources['repointed_acts']} activities, {sources['repointed_runs']} runs)")
        print(f"Venues    : {venues['groups']:>3} dup groups -> remove {venues['removed']:>3} rows "
              f"(re-pointed {venues['repointed']} activities)")
        print(f"Activities: {activities['groups']:>3} dup groups -> remove {activities['removed']:>3} rows "
              f"({activities['removed_tags']} tags)")
        if venues["detail"]:
            print("\nVenue merges (kept id <- removed ids):")
            for name, keep, dups in venues["detail"]:
                print(f"  keep {keep:<5} <- {dups}   {name}")

        if args.commit:
            trans.commit()
            print("\nCommitted.")
        else:
            trans.rollback()
            print("\nDry run only — nothing written. Re-run with --commit to apply.")
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    if args.commit and args.add_constraints:
        add_constraints()
    elif args.add_constraints:
        print("\n(--add-constraints is ignored without --commit.)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
