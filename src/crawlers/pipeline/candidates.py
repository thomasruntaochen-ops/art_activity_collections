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
_listing_recognized: bool = False


def reset_candidate_count() -> None:
    """Clear all reporting. Called before each target so signals never leak between them."""
    global _candidate_count, _listing_recognized
    _candidate_count = None
    _listing_recognized = False


def record_listing_recognized() -> None:
    """Assert the parser located its listing container, whatever it held.

    Some venues legitimately publish an empty listing — Fresno's workshops page
    currently says new workshops will be announced later in the season, so zero
    candidates is the honest answer rather than a symptom of breakage. A parser
    that can positively confirm its container is intact calls this, and the
    guard then treats zero candidates as a quiet calendar instead of a failure.

    Only call this after actually finding the container. Parsers whose container
    *is* the item selector should not call it: for them zero items genuinely does
    mean the page changed underneath us, and that should still raise.
    """
    global _listing_recognized
    _listing_recognized = True


def listing_was_recognized() -> bool:
    """Whether the parser confirmed its listing container this run."""
    return _listing_recognized


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
