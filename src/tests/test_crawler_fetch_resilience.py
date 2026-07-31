"""Regression cover for fetch-layer failures that silently emptied venues.

Each case here corresponds to a venue that was failing in the daily crawl while
looking like a network problem, when the real cause was a payload or header
detail.
"""

import httpx
import pytest

from src.crawlers.adapters.fl_tribe_bundle import FL_TRIBE_VENUES_BY_SLUG
from src.crawlers.adapters.fl_tribe_bundle import _iso_to_tribe_datetime
from src.crawlers.adapters.fl_tribe_bundle import _jsonld_event_to_tribe
from src.crawlers.adapters.nelson_atkins import _parse_production_response
from src.crawlers.adapters.vt_bundle import LOCALIST_WIDGET_USER_AGENT


def _json_response(payload: object) -> httpx.Response:
    return httpx.Response(
        200,
        json=payload,
        request=httpx.Request("POST", "https://cart.nelson-atkins.org/api/products/productionseasons"),
    )


def test_nelson_atkins_accepts_both_feed_shapes() -> None:
    """The feed switched from a bare list to {"productions": [...]}.

    The old shape check rejected the new wrapper, which surfaced as "unable to
    fetch ... over HTTP" and emptied the venue even though the API was fine.
    """
    records = [{"productionTitle": "Family Studio"}]

    assert _parse_production_response(_json_response(records)) == records
    assert _parse_production_response(_json_response({"productions": records})) == records


def test_nelson_atkins_still_rejects_genuinely_unexpected_payloads() -> None:
    with pytest.raises(RuntimeError):
        _parse_production_response(_json_response({"unexpected": "shape"}))


def test_harn_falls_back_to_jsonld_because_its_rest_api_is_closed() -> None:
    assert FL_TRIBE_VENUES_BY_SLUG["harn"].jsonld_url == "https://harn.ufl.edu/calendar/"


def test_jsonld_event_converts_to_the_tribe_shape() -> None:
    row = _jsonld_event_to_tribe(
        {
            "@type": "Event",
            "name": "Tot Time",
            "url": "https://harn.ufl.edu/event/tot-time-11/",
            "startDate": "2026-08-07T10:00:00-05:00",
            "endDate": "2026-08-07T11:00:00-05:00",
            "description": "A programme for young children.",
            "location": {
                "@type": "Place",
                "name": "Harn Museum of Art",
                "address": {"addressLocality": "Gainesville", "addressRegion": "FL"},
            },
        }
    )

    assert row is not None
    assert row["title"] == "Tot Time"
    assert row["url"] == "https://harn.ufl.edu/event/tot-time-11/"
    # Tribe's own format, so the existing row builder parses it unchanged.
    assert row["start_date"] == "2026-08-07 10:00:00"
    assert row["end_date"] == "2026-08-07 11:00:00"
    assert row["venue"] == {"venue": "Harn Museum of Art", "city": "Gainesville", "state": "FL"}


def test_jsonld_event_without_a_url_or_title_is_skipped() -> None:
    assert _jsonld_event_to_tribe({"@type": "Event", "name": "No link"}) is None
    assert _jsonld_event_to_tribe({"@type": "Event", "url": "https://example.org/e/1"}) is None


def test_iso_datetime_keeps_local_wall_clock_and_drops_the_offset() -> None:
    """Activities are stored as naive local time, so the offset must not shift it."""
    assert _iso_to_tribe_datetime("2026-08-07T10:00:00-05:00") == "2026-08-07 10:00:00"
    assert _iso_to_tribe_datetime("2026-08-07T10:00:00") == "2026-08-07 10:00:00"
    assert _iso_to_tribe_datetime("not a date") is None
    assert _iso_to_tribe_datetime(None) is None


def test_uvm_widget_does_not_claim_to_be_a_browser() -> None:
    """events.uvm.edu answers 403 to browser User-Agents on this endpoint."""
    assert "Mozilla" not in LOCALIST_WIDGET_USER_AGENT
    assert "art-activity-collections" in LOCALIST_WIDGET_USER_AGENT


def test_oh_common_includes_family_audience_words() -> None:
    """A programme a museum files under "Families" must not need a format word.

    Several Ohio-family venues went to zero rows because INCLUDE_MARKERS held
    only formats ("workshop", "class"), so an event whose blurb never used one
    was dropped even when its own category said Families.
    """
    from src.crawlers.adapters.oh_common import should_include_event

    assert should_include_event(
        title="Art on The Rise: Wellness & Art",
        description="Art on the Rise takes center stage on Art Climb, the museum's outdoor sculpture stairway.",
        category="Families | Special Events",
    )
    assert should_include_event(
        title="REC Reads",
        description="Bring your toddler or preschooler for a morning of art.",
        category="Families | Special Events",
    )
    # Adult tours stay out.
    assert not should_include_event(
        title="Public Tours",
        description="Tours highlight the museum's world-class collection.",
        category="Adults/General | Tours",
    )


def test_dayton_repairs_end_times_written_without_a_meridiem() -> None:
    """The calendar feed ends a 3:00 pm session at "04:30"."""
    from datetime import date as _date, datetime as _datetime

    from src.crawlers.adapters.dayton_art_institute import _build_row_from_jsonld

    row = _build_row_from_jsonld(
        {
            "name": "Family Open Studio",
            "url": "/do-see/calendar/2026/08/01/family-open-studio",
            "startDate": "2026-08-01T15:00:00-04:00",
            "endDate": "2026-08-01T04:30:00-04:00",
            "description": "Drop-in art making for families.",
        },
        today=_date(2026, 7, 30),
    )

    assert row is not None
    assert row.start_at == _datetime(2026, 8, 1, 15, 0)
    assert row.end_at == _datetime(2026, 8, 1, 16, 30)
