from __future__ import annotations

import re
from decimal import Decimal

FREE_MARKERS = (
    " free ",
    "no cost",
    "complimentary",
    "$0",
    "$ 0",
    "0 dollars",
    "free of charge",
)
DEFINITE_PAID_MARKERS = (
    " paid ",
    "tuition",
    "registration fee",
    "program fee",
    "ticket price:",
    "ticket prices:",
)
AMBIGUOUS_PRICE_MARKERS = (
    "admission",
    "ticket",
    "tickets",
    "price",
    "cost",
    "fee",
    "members",
    "non-members",
    "non member",
)
POSITIVE_DOLLAR_RE = re.compile(r"\$\s*(?!0(?:\.00)?\b)(\d+(?:\.\d{1,2})?)")


def infer_price_classification(
    text: str | None,
    *,
    default_is_free: bool | None = None,
) -> tuple[bool | None, str]:
    normalized = _normalize_price_text(text)
    if not normalized:
        return _default_price_classification(default_is_free)

    if POSITIVE_DOLLAR_RE.search(normalized):
        return False, "confirmed"
    if any(marker in normalized for marker in DEFINITE_PAID_MARKERS):
        return False, "confirmed"
    if any(marker in normalized for marker in FREE_MARKERS):
        return True, "confirmed"
    if any(marker in normalized for marker in AMBIGUOUS_PRICE_MARKERS):
        return None, "uncertain"
    return _default_price_classification(default_is_free)


def infer_price_classification_from_amount(
    amount: Decimal | float | int | None,
    *,
    text: str | None = None,
    default_is_free: bool | None = None,
) -> tuple[bool | None, str]:
    if amount is not None:
        return (True, "confirmed") if Decimal(str(amount)) <= 0 else (False, "confirmed")
    return infer_price_classification(text, default_is_free=default_is_free)


def price_classification_kwargs(
    text: str | None,
    *,
    default_is_free: bool | None = None,
) -> dict[str, bool | None | str]:
    is_free, free_verification_status = infer_price_classification(text, default_is_free=default_is_free)
    return {
        "is_free": is_free,
        "free_verification_status": free_verification_status,
    }


def price_classification_kwargs_from_amount(
    amount: Decimal | float | int | None,
    *,
    text: str | None = None,
    default_is_free: bool | None = None,
) -> dict[str, bool | None | str]:
    is_free, free_verification_status = infer_price_classification_from_amount(
        amount,
        text=text,
        default_is_free=default_is_free,
    )
    return {
        "is_free": is_free,
        "free_verification_status": free_verification_status,
    }


def _default_price_classification(default_is_free: bool | None) -> tuple[bool | None, str]:
    if default_is_free is None:
        return None, "uncertain"
    return default_is_free, "inferred"


def _normalize_price_text(text: str | None) -> str:
    if not text:
        return ""
    return f" {' '.join(text.lower().split())} "
