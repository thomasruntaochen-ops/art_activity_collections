"""Curated display order for museum lists.

Venue lists used to be ordered by upcoming program count alone, which pushed the
museums people recognize to the bottom: the Whitney with one program landed
below a regional gallery running 194. ``config/venue_prominence.toml`` assigns
the well-known museums a rank so national institutions lead, and everything it
does not name shares one default rank where the old "most programs first"
ordering still applies.

The ranking is applied in SQL rather than after the fetch because
``list_venue_summaries`` caps its result set: ordering in Python would let the
limit cut a flagship museum before the sort ever saw it.
"""

import re
import tomllib
from functools import lru_cache
from pathlib import Path

from sqlalchemy import ColumnElement, case, func, literal

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "venue_prominence.toml"

# Museums the config does not name sort after every curated tier. Far enough
# above the configured ranks to leave room for new bands.
DEFAULT_RANK = 999

_WHITESPACE = re.compile(r"\s+")


def _normalize(name: str) -> str:
    """Fold a venue name to the form the SQL comparison sees: lowercase, trimmed."""
    return _WHITESPACE.sub(" ", name).strip().lower()


def _name_variants(name: str) -> set[str]:
    """Both spellings of a name's leading article.

    Museums are stored inconsistently across parsers ("Baltimore Museum of Art"
    vs "The Baltimore Museum of Art"), so a config entry matches either form and
    the file can spell each museum just once.
    """
    normalized = _normalize(name)
    if not normalized:
        return set()
    stripped = normalized.removeprefix("the ")
    return {normalized, stripped, f"the {stripped}"}


@lru_cache(maxsize=1)
def _ranked_names(config_path: str | None = None) -> tuple[tuple[int, tuple[str, ...]], ...]:
    """Load the config as (rank, normalized names) pairs, lowest rank first."""
    path = Path(config_path) if config_path else CONFIG_PATH
    if not path.exists():
        return ()
    data = tomllib.loads(path.read_text(encoding="utf-8"))

    by_rank: dict[int, set[str]] = {}
    for tier in data.get("tiers", []):
        rank = int(tier.get("rank", DEFAULT_RANK))
        if rank >= DEFAULT_RANK:
            continue
        names = by_rank.setdefault(rank, set())
        for venue in tier.get("venues", []):
            names.update(_name_variants(str(venue)))

    # A name listed in two tiers keeps the strongest (lowest) rank, so ordering
    # cannot depend on which tier the loader happened to read last.
    seen: set[str] = set()
    ranked: list[tuple[int, tuple[str, ...]]] = []
    for rank in sorted(by_rank):
        names = tuple(sorted(by_rank[rank] - seen))
        seen.update(names)
        if names:
            ranked.append((rank, names))
    return tuple(ranked)


def prominence_rank(name: str | None) -> int:
    """The display rank for one venue name; lower sorts first."""
    if not name:
        return DEFAULT_RANK
    normalized = _normalize(name)
    for rank, names in _ranked_names():
        if normalized in names:
            return rank
    return DEFAULT_RANK


def prominence_case(column: ColumnElement[str]) -> ColumnElement[int]:
    """A SQL expression giving ``column``'s venue name its display rank."""
    ranked = _ranked_names()
    if not ranked:
        return literal(DEFAULT_RANK)
    normalized = func.lower(func.trim(column))
    whens = [(normalized.in_(names), rank) for rank, names in ranked]
    return case(*whens, else_=DEFAULT_RANK)
