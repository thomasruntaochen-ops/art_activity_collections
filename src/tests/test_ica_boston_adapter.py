from datetime import datetime

from src.crawlers.adapters.ica_boston import parse_ica_boston_calendar_payload


def _detail_html(title: str, description: str, price: str = "") -> str:
    price_html = f"<div class='field-name-field-ticket-price'>{price}</div>" if price else ""
    return (
        f"<html><body><h1 class='page-title'>{title}</h1>"
        f"<div class='entry-content'>{description}</div>"
        f"{price_html}</body></html>"
    )


def _payload() -> dict:
    listings = [
        {
            "title": "Perpetual Gratitude",
            "source_url": "https://www.icaboston.org/events/perpetual-gratitude/",
            "categories": ["Art Making"],
            "date_entries": [(datetime(2026, 7, 11, 12, 0), None)],
        },
        {
            "title": "Play Date: Free Family Art Day",
            "source_url": "https://www.icaboston.org/events/play-date-free-family-art-day/",
            "categories": ["ICA Kids", "Art Making"],
            "date_entries": [(datetime(2026, 7, 12, 10, 0), None)],
        },
        {
            "title": "Art-Making After Dark",
            "source_url": "https://www.icaboston.org/events/art-making-after-dark/",
            "categories": ["Art Making"],
            "date_entries": [(datetime(2026, 7, 16, 18, 0), None)],
        },
        {
            "title": "Gallery Talk: Curator Conversation",
            "source_url": "https://www.icaboston.org/events/gallery-talk/",
            "categories": ["Talks"],
            "date_entries": [(datetime(2026, 7, 19, 14, 0), None)],
        },
        {
            "title": "We Create The World: A Juneteenth Celebration",
            "source_url": "https://www.icaboston.org/events/we-create-the-world/",
            "categories": ["Art Making"],
            "date_entries": [(datetime(2026, 7, 20, 14, 0), None)],
        },
    ]
    return {
        "listing_url": "https://www.icaboston.org/calendar/",
        "listings": listings,
        "detail_pages": {
            "https://www.icaboston.org/events/perpetual-gratitude/": _detail_html(
                "Perpetual Gratitude",
                "Create and connect through an interactive Bank of America Art Lab installation.",
            ),
            "https://www.icaboston.org/events/play-date-free-family-art-day/": _detail_html(
                "Play Date: Free Family Art Day",
                "Free family fun for kids and families. Children 12 and under can join art-making workshops.",
                "Free",
            ),
            "https://www.icaboston.org/events/art-making-after-dark/": _detail_html(
                "Art-Making After Dark",
                "Get creative on select Free Thursday nights with art-making activities designed by local artists.",
                "Free Thursday Night",
            ),
            "https://www.icaboston.org/events/gallery-talk/": _detail_html(
                "Gallery Talk: Curator Conversation",
                "Join a curator for an exhibition conversation. Included with museum admission.",
            ),
            "https://www.icaboston.org/events/we-create-the-world/": _detail_html(
                "We Create The World: A Juneteenth Celebration",
                "A celebration with short films, DJ sets, and live musical performances.",
                "Free admission all day",
            ),
        },
    }


def test_ica_boston_audience_price_and_noise_filters() -> None:
    rows = parse_ica_boston_calendar_payload(_payload())

    by_title = {row.title: row for row in rows}
    assert "We Create The World: A Juneteenth Celebration" not in by_title

    assert by_title["Perpetual Gratitude"].audience_segment == "all_ages"
    assert by_title["Play Date: Free Family Art Day"].audience_segment == "kids"
    assert by_title["Play Date: Free Family Art Day"].is_free is True
    assert by_title["Art-Making After Dark"].audience_segment == "adults"
    assert by_title["Art-Making After Dark"].is_free is True
    assert by_title["Gallery Talk: Curator Conversation"].audience_segment == "adults"
    assert by_title["Gallery Talk: Curator Conversation"].is_free is False
    assert by_title["Gallery Talk: Curator Conversation"].free_verification_status == "confirmed"
