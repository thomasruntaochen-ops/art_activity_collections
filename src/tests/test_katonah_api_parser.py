from src.crawlers.adapters.katonah import parse_katonah_events_payload


def _event(**overrides) -> dict:
    base = {
        "id": "1069",
        "visibility": "public",
        "title": "Saturday Open Studio",
        "subtitle": None,
        "start_date": "2099-09-05 10:00:00",
        "end_date": "2099-09-05 12:00:00",
        # Boilerplate that trips EXCLUDE_KEYWORDS if filtering reads descriptions.
        "full_description": "<p>Free with Museum admission. Inspired by the exhibition.</p>",
        "event_cost": None,
        "registration_link": None,
        "cancelled_at": None,
        "deleted_at": None,
    }
    base.update(overrides)
    return base


def test_keeps_family_studio_event_despite_admission_boilerplate() -> None:
    rows = parse_katonah_events_payload([_event()])

    assert len(rows) == 1
    row = rows[0]
    assert row.title == "Saturday Open Studio"
    assert row.source_url.endswith("?eid=1069")
    assert row.venue_name == "Katonah Museum of Art"
    # Description is still captured, just not used for include/exclude filtering.
    assert "admission" in (row.description or "").lower()


def test_excludes_docent_tours_by_title() -> None:
    rows = parse_katonah_events_payload([_event(title="Docent-Led Tours at the KMA")])

    assert rows == []


def test_skips_cancelled_private_and_past_events() -> None:
    events = [
        _event(id="1", cancelled_at="2026-01-01 00:00:00"),
        _event(id="2", visibility="private"),
        _event(id="3", deleted_at="2026-01-01 00:00:00"),
        _event(id="4", start_date="2000-01-01 10:00:00"),
        _event(id="5", start_date=None),
    ]

    assert parse_katonah_events_payload(events) == []


def test_registration_required_follows_registration_link() -> None:
    rows = parse_katonah_events_payload(
        [_event(registration_link="https://example.org/register")]
    )

    assert rows[0].registration_required is True
