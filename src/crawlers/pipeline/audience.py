from __future__ import annotations

from collections.abc import Iterable

AudienceSegment = str

AUDIENCE_KIDS = "kids"
AUDIENCE_TEENS = "teens"
AUDIENCE_ADULTS = "adults"
AUDIENCE_ALL_AGES = "all_ages"
AUDIENCE_UNKNOWN = "unknown"

VALID_AUDIENCE_SEGMENTS = {
    AUDIENCE_KIDS,
    AUDIENCE_TEENS,
    AUDIENCE_ADULTS,
    AUDIENCE_ALL_AGES,
    AUDIENCE_UNKNOWN,
}

_SEGMENT_ALIASES = {
    "kid": AUDIENCE_KIDS,
    "kids": AUDIENCE_KIDS,
    "child": AUDIENCE_KIDS,
    "children": AUDIENCE_KIDS,
    "family": AUDIENCE_KIDS,
    "families": AUDIENCE_KIDS,
    "youth": AUDIENCE_KIDS,
    "teen": AUDIENCE_TEENS,
    "teens": AUDIENCE_TEENS,
    "adult": AUDIENCE_ADULTS,
    "adults": AUDIENCE_ADULTS,
    "all ages": AUDIENCE_ALL_AGES,
    "all_ages": AUDIENCE_ALL_AGES,
    "all-ages": AUDIENCE_ALL_AGES,
    "unknown": AUDIENCE_UNKNOWN,
}

_ALL_AGES_MARKERS = (
    " all ages ",
    " for all ages ",
    " open to all ages ",
    " all are welcome ",
)
_ADULT_MARKERS = (
    " adult ",
    " adults ",
    " 18+ ",
    " ages 18+ ",
    " older adults ",
    " open to adults ",
)
_TEEN_MARKERS = (
    " teen ",
    " teens ",
    " teenaged ",
    " teenagers ",
    " high school ",
)
_KIDS_MARKERS = (
    " kid ",
    " kids ",
    " child ",
    " children ",
    " family ",
    " families ",
    " youth ",
    " homeschool ",
)
_GENERAL_PUBLIC_ACTIVITY_MARKERS = (
    " art class ",
    " art classes ",
    " class ",
    " classes ",
    " course ",
    " courses ",
    " drawing ",
    " lecture ",
    " lectures ",
    " studio ",
    " talk ",
    " talks ",
    " workshop ",
    " workshops ",
)


def normalize_audience_segment(value: str | None) -> AudienceSegment:
    if not value:
        return AUDIENCE_UNKNOWN
    normalized = " ".join(value.replace("-", " ").replace("_", " ").lower().split())
    return _SEGMENT_ALIASES.get(normalized, AUDIENCE_UNKNOWN)


def infer_audience_segment(
    *,
    title: str | None = None,
    description: str | None = None,
    category: str | None = None,
    source_url: str | None = None,
    tags: Iterable[str] | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
    default: str | None = None,
) -> AudienceSegment:
    default_segment = normalize_audience_segment(default)

    if default_segment != AUDIENCE_UNKNOWN:
        if default_segment == AUDIENCE_KIDS and age_min is not None and age_min >= 13 and (age_max is None or age_max <= 18):
            return AUDIENCE_TEENS
        return default_segment

    age_segment = infer_audience_segment_from_age(age_min=age_min, age_max=age_max)
    if age_segment != AUDIENCE_UNKNOWN:
        return age_segment

    blob = _searchable_blob(title, description, category, source_url, *(tags or ()))
    if not blob:
        return AUDIENCE_UNKNOWN

    if any(marker in blob for marker in _ALL_AGES_MARKERS):
        return AUDIENCE_ALL_AGES
    if any(marker in blob for marker in _ADULT_MARKERS):
        return AUDIENCE_ADULTS
    if any(marker in blob for marker in _TEEN_MARKERS):
        return AUDIENCE_TEENS
    if any(marker in blob for marker in _KIDS_MARKERS):
        return AUDIENCE_KIDS
    if any(marker in blob for marker in _GENERAL_PUBLIC_ACTIVITY_MARKERS):
        return AUDIENCE_ADULTS
    return AUDIENCE_UNKNOWN


def infer_audience_segment_from_age(*, age_min: int | None, age_max: int | None) -> AudienceSegment:
    if age_min is not None and age_min >= 18:
        return AUDIENCE_ADULTS
    if age_min is not None and age_min >= 13 and (age_max is None or age_max <= 18):
        return AUDIENCE_TEENS
    if age_max is not None and age_max <= 12:
        return AUDIENCE_KIDS
    return AUDIENCE_UNKNOWN


def _searchable_blob(*parts: object) -> str:
    text = " ".join(str(part) for part in parts if part)
    return f" {' '.join(text.lower().split())} "
