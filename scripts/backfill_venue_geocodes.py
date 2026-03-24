#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from sqlalchemy import or_, select

PROJECT_ROOT = Path(__file__).resolve().parents[1]

import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.session import SessionLocal  # noqa: E402
from src.models.activity import Venue  # noqa: E402
from src.services.venue_geocoding import populate_venue_geocodes  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill venue latitude/longitude using the configured geocoder."
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Persist the fetched coordinates to MySQL. Without this flag the script rolls back.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only geocode up to this many venues.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-geocode venues even if they already have lat/lng values.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    with SessionLocal() as db:
        stmt = select(Venue).order_by(Venue.name.asc())
        if not args.force:
            stmt = stmt.where(or_(Venue.lat.is_(None), Venue.lng.is_(None)))
        if args.limit is not None:
            stmt = stmt.limit(max(1, args.limit))

        venues = list(db.scalars(stmt))
        print(f"Loaded {len(venues)} venues for geocoding")
        stats = populate_venue_geocodes(venues, only_missing=not args.force)

        if args.commit:
            db.commit()
            print("Committed coordinate updates")
        else:
            db.rollback()
            print("Dry run only. Re-run with --commit to persist.")

        print(
            "Geocode stats: "
            f"requested={stats.requested}, "
            f"geocoded={stats.geocoded}, "
            f"failed={stats.failed}, "
            f"skipped={stats.skipped}"
        )

        for venue in venues:
            print(
                f"{venue.name} | {venue.city or '-'}, {venue.state or '-'} | "
                f"{venue.lat if venue.lat is not None else '-'}, {venue.lng if venue.lng is not None else '-'}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
