"""Lets a parser report how many candidate events it saw before filtering.

Without this, a run that finds zero rows is ambiguous: the parser may be dead
(its selectors no longer match anything), or the venue may simply have nothing
matching the kids/teens art criteria on its calendar this month. Both looked
identical to the empty-parse guard, so a venue with a healthy parser and a quiet
calendar raised the same alert as one whose site had been redesigned.

A parser records the number of candidate blocks it found *before* applying
audience/date filters. The guard then treats "0 kept out of N candidates" as a
normal quiet calendar, and reserves the alert for "0 kept out of 0 candidates".

Reporting is optional: a parser that records nothing leaves the count as None,
and the guard falls back to its original alert-on-empty behaviour.

Deliberately a module-level global rather than a ContextVar. Adapters record
from wherever they walk their listings, and several do that inside
`asyncio.gather` (dixon, benton, the bundles). A ContextVar is copied into each
Task, so counts recorded in those children are discarded on completion — the
count would silently come back as None and the guard would alert anyway, which
is the exact false alarm this module exists to remove. A crawler process handles
one venue and runs its targets sequentially, so a plain global is safe here;
`reset_candidate_count()` at the start of each target keeps them separated.
"""

_candidate_count: int | None = None


def reset_candidate_count() -> None:
    """Clear the count. Called before each target so counts never leak between them."""
    global _candidate_count
    _candidate_count = None


def record_candidate_count(count: int) -> None:
    """Report candidate blocks found before filtering.

    Additive, so a parser that sweeps several listing pages can call this once
    per page and have the totals accumulate.
    """
    global _candidate_count
    _candidate_count = count if _candidate_count is None else _candidate_count + count


def get_candidate_count() -> int | None:
    """Total reported for the current target, or None if the parser never reported."""
    return _candidate_count
