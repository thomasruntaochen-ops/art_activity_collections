"""slam.org blocks its REST API but serves Event JSON-LD on the public page."""

import json

from src.crawlers.adapters.slam import (
    _extract_jsonld_events,
    _html_to_text,
    _jsonld_to_event_obj,
)


def _page(*events: dict) -> str:
    return (
        '<html><head><script type="application/ld+json">'
        + json.dumps({"@context": "https://schema.org", "@graph": list(events)})
        + "</script></head><body></body></html>"
    )


EVENT = {
    "@type": "Event",
    "name": "Wee Weekend",
    "url": "https://www.slam.org/event/wee-weekend/",
    "startDate": "2099-08-29T10:00:00-05:00",
    "endDate": "2099-08-29T12:00:00-05:00",
    "description": "A drop-in art activity for young children.",
    "location": {
        "@type": "Place",
        "name": "Sculpture Hall",
        "address": {
            "@type": "PostalAddress",
            "streetAddress": "One Fine Arts Drive",
            "addressLocality": "Saint Louis",
            "addressRegion": "MO",
            "postalCode": "63110",
        },
    },
    "offers": [{"@type": "Offer", "price": "0"}],
}


def test_events_are_found_inside_a_nested_graph() -> None:
    events = _extract_jsonld_events(_page(EVENT, {"@type": "WebSite", "name": "SLAM"}))

    assert len(events) == 1
    assert events[0]["title"] == "Wee Weekend"
    assert events[0]["start_date"] == "2099-08-29T10:00:00-05:00"


def test_location_is_mapped_onto_the_api_venue_shape() -> None:
    obj = _jsonld_to_event_obj(EVENT)

    assert obj["venue"]["venue"] == "Sculpture Hall"
    assert obj["venue"]["city"] == "Saint Louis"
    assert obj["venue"]["state"] == "MO"
    assert obj["cost"] == "0"


def test_malformed_jsonld_block_is_skipped() -> None:
    html = '<script type="application/ld+json">{not json}</script>'

    assert _extract_jsonld_events(html) == []


def test_escaped_markup_is_stripped_not_just_unescaped() -> None:
    """One pass would leave a literal <em> in the title."""
    assert (
        _html_to_text("Free Fridays&mdash;&lt;em&gt;Ancient Splendor&lt;/em&gt;")
        == "Free Fridays—Ancient Splendor"
    )


def test_stripping_markup_does_not_glue_adjacent_words() -> None:
    """strip=True on the second pass would produce "Veronawith"."""
    assert (
        _html_to_text("&lt;em&gt;The Two Gentlemen of Verona&lt;/em&gt; with the Company")
        == "The Two Gentlemen of Verona with the Company"
    )
