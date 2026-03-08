IRRELEVANT_ITEM_KEYWORDS = (
    "ticket",
    "tickets",
    "donate",
    "membership",
    "member",
    "shop",
)


def is_irrelevant_item_text(value: str | None) -> bool:
    if not value:
        return True
    normalized = value.strip().lower()
    if not normalized:
        return True

    # Skip top-nav and utility text that can leak into naive text parsing.
    for keyword in IRRELEVANT_ITEM_KEYWORDS:
        if normalized == keyword or normalized.startswith(f"{keyword} "):
            return True

    return False
