"""Diagnostic: report duplicate venue rows in the MySQL `venues` table.

Run with:  source ~/.zshrc && python scripts/check_duplicate_venues.py
"""

import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.session import engine  # noqa: E402


def main() -> None:
    with engine.connect() as conn:
        total_rows = conn.execute(text("SELECT COUNT(*) FROM venues")).scalar_one()
        distinct_names = conn.execute(
            text("SELECT COUNT(DISTINCT TRIM(name)) FROM venues")
        ).scalar_one()

        # Duplicate groups by trimmed name (collation is typically case-insensitive).
        dup_groups = conn.execute(
            text(
                """
                SELECT TRIM(name) AS name, COUNT(*) AS cnt,
                       COUNT(DISTINCT CONCAT(COALESCE(city,''),'|',COALESCE(state,''))) AS distinct_locations
                FROM venues
                GROUP BY TRIM(name)
                HAVING cnt > 1
                ORDER BY cnt DESC, name ASC
                """
            )
        ).all()

        redundant_rows = sum(row.cnt - 1 for row in dup_groups)

        # Same, but only venues that actually have at least one activity (what the
        # explorer/map surfaces).
        dup_groups_active = conn.execute(
            text(
                """
                SELECT TRIM(v.name) AS name, COUNT(DISTINCT v.id) AS cnt
                FROM venues v
                JOIN activities a ON a.venue_id = v.id
                GROUP BY TRIM(v.name)
                HAVING cnt > 1
                ORDER BY cnt DESC, name ASC
                """
            )
        ).all()
        redundant_rows_active = sum(row.cnt - 1 for row in dup_groups_active)

    print("=" * 64)
    print("VENUE DUPLICATE REPORT")
    print("=" * 64)
    print(f"Total venue rows .............. {total_rows}")
    print(f"Distinct venue names .......... {distinct_names}")
    print(f"Duplicated names (groups) ..... {len(dup_groups)}")
    print(f"Redundant rows (sum cnt-1) .... {redundant_rows}")
    print()
    print(f"Duplicated names WITH activities .. {len(dup_groups_active)} groups")
    print(f"Redundant rows WITH activities .... {redundant_rows_active}")
    print()

    if dup_groups:
        print("Top duplicated names (all venues):")
        print(f"  {'count':>5}  {'distinct_loc':>12}  name")
        for row in dup_groups[:30]:
            print(f"  {row.cnt:>5}  {row.distinct_locations:>12}  {row.name}")

    if dup_groups_active:
        print()
        print("Top duplicated names (venues with activities):")
        print(f"  {'count':>5}  name")
        for row in dup_groups_active[:30]:
            print(f"  {row.cnt:>5}  {row.name}")

    _print_recency_breakdown()
    _print_activity_duplication()


def _print_activity_duplication() -> None:
    """Are the activities themselves duplicated across the venue copies, or split?
    total_rows vs distinct (title, start_at) tells us."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT d.tname AS name,
                       COUNT(a.id) AS total_rows,
                       COUNT(DISTINCT a.title, a.start_at) AS distinct_events
                FROM (
                    SELECT TRIM(name) AS tname FROM venues GROUP BY TRIM(name) HAVING COUNT(*) > 1
                ) d
                JOIN venues v ON TRIM(v.name) = d.tname
                JOIN activities a ON a.venue_id = v.id
                GROUP BY d.tname
                ORDER BY d.tname ASC
                """
            )
        ).all()

    print()
    print("=" * 64)
    print("ARE THE ACTIVITIES ALSO DUPLICATED?  (total rows vs distinct events)")
    print("=" * 64)
    print(f"  {'rows':>5}  {'distinct':>8}  {'dup?':>5}  name")
    fully_dup = 0
    for row in rows:
        is_dup = row.total_rows > row.distinct_events
        if is_dup:
            fully_dup += 1
        print(
            f"  {row.total_rows:>5}  {row.distinct_events:>8}  {('YES' if is_dup else 'no'):>5}  {row.name}"
        )
    print()
    print(f"Names whose activities are duplicated (not just the venue row): {fully_dup}/{len(rows)}")


def _fmt(value) -> str:
    if value is None:
        return "—"
    return str(value)[:16]


def _print_recency_breakdown() -> None:
    """For every duplicated name, list each venue copy with how recently the
    crawler last touched its activities, so we can tell if only one copy is live."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT v.id AS venue_id, v.name AS name, v.city, v.state,
                       v.address IS NOT NULL AS has_address,
                       v.lat IS NOT NULL AS has_coords,
                       COUNT(a.id) AS activity_count,
                       MAX(a.last_seen_at) AS last_seen,
                       MAX(a.updated_at)  AS last_updated,
                       MAX(a.start_at)    AS latest_start
                FROM venues v
                LEFT JOIN activities a ON a.venue_id = v.id
                GROUP BY v.id
                ORDER BY v.name ASC
                """
            )
        ).all()

    # Keep only names that have more than one venue row (the duplicates).
    all_groups: dict[str, list] = {}
    for row in rows:
        all_groups.setdefault((row.name or "").strip(), []).append(row)
    groups = {name: copies for name, copies in all_groups.items() if len(copies) > 1}
    # Within each group, freshest crawl sighting first (NULL last_seen sorts last).
    for copies in groups.values():
        copies.sort(key=lambda c: (c.last_seen is not None, c.last_seen), reverse=True)

    print()
    print("=" * 64)
    print("RECENCY OF EACH DUPLICATE COPY (is only one copy still alive?)")
    print("=" * 64)

    single_live = 0
    for name, copies in groups.items():
        # Freshest copy = max last_seen (NULLs sort last because of ORDER BY DESC).
        freshest = copies[0].last_seen
        print(f"\n{name}")
        for copy in copies:
            tags = []
            if copy.last_seen is not None and copy.last_seen == freshest:
                tags.append("LIVE")
            if copy.activity_count == 0:
                tags.append("no activities")
            elif copy.last_seen is not None and freshest is not None and copy.last_seen != freshest:
                stale_days = (freshest - copy.last_seen).days
                tags.append(f"stale {stale_days}d")
            flags = ("addr" if copy.has_address else "----") + "/" + ("geo" if copy.has_coords else "---")
            print(
                f"  id={copy.venue_id:<6} acts={copy.activity_count:>3}  "
                f"last_seen={_fmt(copy.last_seen)}  updated={_fmt(copy.last_updated)}  "
                f"latest_start={_fmt(copy.latest_start)}  [{flags}]  {' '.join(tags)}"
            )
        live_copies = [c for c in copies if c.last_seen is not None and c.last_seen == freshest]
        # Count groups where exactly one copy carries activity that was seen most recently.
        recent_copies = [c for c in copies if c.activity_count > 0]
        if len(recent_copies) == 1 or len(live_copies) == 1:
            single_live += 1

    print()
    print(f"Groups where a single copy is clearly the live/most-recent one: {single_live}/{len(groups)}")


if __name__ == "__main__":
    main()
