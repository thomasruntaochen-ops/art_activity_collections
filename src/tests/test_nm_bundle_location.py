from src.crawlers.adapters.nm_bundle import _extract_nmart_location
from src.crawlers.pipeline.runner import _clamp_location_text


def test_nmart_location_keeps_short_room_label() -> None:
    article = (
        "Upcoming Event Family Studio August 22, 2026 10:00 am - 12:00 pm "
        "Vladem Contemporary Join us for a morning of hands-on art making."
    )

    assert _extract_nmart_location(article) == "Vladem Contemporary"


def test_nmart_location_does_not_swallow_join_the_description() -> None:
    # "Join the ..." previously slipped past the split phrases, so the whole
    # description landed in location_text and blew the VARCHAR(512) column.
    article = (
        "Upcoming Event Route 66 Centennial August 23, 2026 10:00 am - 4:00 pm "
        "Other Join the New Mexico Museum of Art, the Museum of Indian Arts and "
        "Culture, New Mexico History Museum and the Museum of International Folk "
        "Art in celebrating the Centennial of Route 66."
    )

    assert _extract_nmart_location(article) == "Other"


def test_nmart_location_rejected_when_still_too_long() -> None:
    article = (
        "Upcoming Event Long One August 23, 2026 10:00 am - 4:00 pm "
        + "words " * 60
    )

    assert _extract_nmart_location(article) is None


def test_clamp_location_text_bounds_column_width() -> None:
    assert _clamp_location_text("Vladem Contemporary") == "Vladem Contemporary"
    assert _clamp_location_text("  ") is None
    assert _clamp_location_text(None) is None
    assert len(_clamp_location_text("x" * 900)) == 512
