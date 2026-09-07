from src.services.venue_prominence import DEFAULT_RANK, prominence_rank


def test_national_museums_rank_ahead_of_regional_and_unlisted_ones() -> None:
    assert prominence_rank("The Metropolitan Museum of Art") < prominence_rank("Crocker Art Museum")
    assert prominence_rank("Crocker Art Museum") < prominence_rank("Example Community Gallery")
    assert prominence_rank("Example Community Gallery") == DEFAULT_RANK


def test_rank_ignores_case_and_a_leading_article() -> None:
    """Parsers spell the same museum with and without "The"."""
    baseline = prominence_rank("Baltimore Museum of Art")
    assert baseline < DEFAULT_RANK
    assert prominence_rank("The Baltimore Museum of Art") == baseline
    assert prominence_rank("  the baltimore museum of art ") == baseline
    # A config entry written with the article matches the bare name too.
    assert prominence_rank("Phillips Collection") == prominence_rank("The Phillips Collection")


def test_missing_name_falls_back_to_the_default_rank() -> None:
    assert prominence_rank(None) == DEFAULT_RANK
    assert prominence_rank("") == DEFAULT_RANK
