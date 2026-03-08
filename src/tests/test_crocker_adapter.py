import json
from datetime import datetime

from src.crawlers.adapters.crocker import parse_crocker_events_html


def _wrap_next_data(days: list[dict]) -> str:
    payload = {"props": {"pageProps": {"days": days}}}
    return (
        '<script id="__NEXT_DATA__" type="application/json">'
        f"{json.dumps(payload)}"
        "</script>"
    )


def test_crocker_next_data_uses_performance_id_in_source_url() -> None:
    html = _wrap_next_data(
        [
            {
                "items": [
                    {
                        "slug": "wee-wednesday",
                        "title": "Wee Wednesday",
                        "description": "Children engage with artmaking and visual literacy prompts.",
                        "PerformanceDate": "2026-03-11T10:30:00-07:00",
                        "PerformanceId": 4249,
                        "PerformanceType": {"Description": "Series"},
                        "PerformanceDescription": "Wee Wednesday",
                    }
                ]
            }
        ]
    )

    rows = parse_crocker_events_html(
        html=html,
        list_url="https://www.crockerart.org/events",
        now=datetime(2026, 3, 8),
    )

    assert len(rows) == 1
    assert rows[0].source_url == "https://www.crockerart.org/events/4249/wee-wednesday"


def test_crocker_next_data_falls_back_to_slug_only_url_without_performance_id() -> None:
    html = _wrap_next_data(
        [
            {
                "items": [
                    {
                        "slug": "community-artmaking",
                        "title": "Community Artmaking",
                        "description": "A family workshop with hands-on collage activities.",
                        "PerformanceDate": "2026-03-15T13:00:00-07:00",
                        "PerformanceType": {"Description": "Family Programs"},
                        "PerformanceDescription": "Community Artmaking",
                    }
                ]
            }
        ]
    )

    rows = parse_crocker_events_html(
        html=html,
        list_url="https://www.crockerart.org/events",
        now=datetime(2026, 3, 8),
    )

    assert len(rows) == 1
    assert rows[0].source_url == "https://www.crockerart.org/events/community-artmaking"


def test_crocker_next_data_normalizes_offset_datetime_to_naive_local_time() -> None:
    html = _wrap_next_data(
        [
            {
                "items": [
                    {
                        "slug": "artist-talk",
                        "title": "Artist Talk",
                        "description": "A conversation with the artist.",
                        "PerformanceDate": "2026-03-15T13:00:00-07:00",
                        "PerformanceType": {"Description": "Talks + Conversations"},
                        "PerformanceDescription": "Artist Talk",
                    }
                ]
            }
        ]
    )

    rows = parse_crocker_events_html(
        html=html,
        list_url="https://www.crockerart.org/events",
        now=datetime(2026, 3, 8),
    )

    assert len(rows) == 1
    assert rows[0].start_at == datetime(2026, 3, 15, 13, 0)
    assert rows[0].start_at.tzinfo is None
