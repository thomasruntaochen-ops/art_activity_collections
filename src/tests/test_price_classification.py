from decimal import Decimal

from src.crawlers.pipeline.pricing import infer_price_classification
from src.crawlers.pipeline.pricing import infer_price_classification_from_amount


def test_infer_price_classification_detects_free() -> None:
    assert infer_price_classification("Admission is free for all visitors.") == (True, "confirmed")


def test_infer_price_classification_detects_paid() -> None:
    assert infer_price_classification("Tickets: $15 for adults, $5 for youth.") == (False, "confirmed")


def test_infer_price_classification_returns_unknown_without_signal() -> None:
    assert infer_price_classification("Join us for a hands-on workshop.") == (None, "uncertain")


def test_infer_price_classification_from_amount_detects_paid() -> None:
    assert infer_price_classification_from_amount(Decimal("12.00")) == (False, "confirmed")
