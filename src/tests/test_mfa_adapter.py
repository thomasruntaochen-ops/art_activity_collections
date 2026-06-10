from __future__ import annotations

import json

from src.crawlers.adapters.mfa import parse_mfa_events_html


def _html_with_events(events: list[dict]) -> str:
    return (
        "<html><head><script type=\"application/ld+json\">"
        + json.dumps(events)
        + "</script></head><body></body></html>"
    )


def test_mfa_keeps_classes_and_teens_with_price_and_audience() -> None:
    rows = parse_mfa_events_html(
        _html_with_events(
            [
                {
                    "@type": "Event",
                    "name": "Drawing Online",
                    "url": "/event/studio-art-classes/drawing-online-39?event=159121",
                    "startDate": "2026-07-10T10:30:00-04:00",
                    "description": "Tuition is $240 for this online studio art class.",
                    "category": "Studio Art Classes",
                },
                {
                    "@type": "Event",
                    "name": "Beyond the Spectrum Teens",
                    "url": "/event/beyond-the-spectrum/beyond-the-spectrum-teens-15?event=165081",
                    "startDate": "2026-07-11T10:30:00-04:00",
                    "description": "A free art program for teens.",
                    "category": "Beyond the Spectrum",
                },
            ]
        ),
        list_url="https://www.mfa.org/programs?page=0",
    )

    assert [row.title for row in rows] == ["Drawing Online", "Beyond the Spectrum Teens"]
    assert rows[0].audience_segment == "adults"
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"
    assert rows[1].audience_segment == "teens"
    assert rows[1].is_free is True


def test_mfa_rejects_open_house_member_admission_special_event_and_tours() -> None:
    rows = parse_mfa_events_html(
        _html_with_events(
            [
                {
                    "@type": "Event",
                    "name": "Member Hours",
                    "url": "/event/member-hours/member-hours-framing-nature?event=161026",
                    "startDate": "2026-07-10T18:00:00-04:00",
                    "description": "Member viewing hours.",
                },
                {
                    "@type": "Event",
                    "name": "$5 Third Thursday",
                    "url": "/event/special-event/5-third-thursday?event=153071",
                    "startDate": "2026-07-16T17:00:00-04:00",
                    "description": "$5 minimum pay-what-you-wish general admission.",
                },
                {
                    "@type": "Event",
                    "name": "Juneteenth",
                    "url": "/event/open-house/juneteenth?event=153011",
                    "startDate": "2026-07-19T10:00:00-04:00",
                    "description": "Admission is free with a Massachusetts zip code.",
                },
                {
                    "@type": "Event",
                    "name": "Virtual Descriptive Tour",
                    "url": "/event/gallery-activities-and-tours/virtual-descriptive-tour?event=142326",
                    "startDate": "2026-07-20T15:00:00-04:00",
                    "description": "A virtual tour of the galleries.",
                },
                {
                    "@type": "Event",
                    "name": "Artful Evenings",
                    "url": "/event/special-event/artful-evenings?event=157576",
                    "startDate": "2026-07-12T19:00:00-04:00",
                    "description": "Spilling the Tea: MFA Edition.",
                },
            ]
        ),
        list_url="https://www.mfa.org/programs?page=0",
    )

    assert rows == []
