from datetime import datetime

from src.crawlers.adapters.slater import parse_slater_events_ics


def test_slater_parser_keeps_lecture_and_paid_workshop() -> None:
    ics_text = """BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:lecture-1
DTSTART;TZID=America/New_York:20260407T180000
DTEND;TZID=America/New_York:20260407T190000
DESCRIPTION:Lecture Series: Norwich in the Revolution - Lecture and book signing.
LOCATION:Slater Auditorium
SUMMARY:Spring Lecture Series: Norwich in the Revolution
END:VEVENT
BEGIN:VEVENT
UID:workshop-1
DTSTART;TZID=America/New_York:20260412T130000
DTEND;TZID=America/New_York:20260412T150000
DESCRIPTION:$25 per child. Ages 8-12. Register now for this clay workshop.
LOCATION:Slater Memorial Museum
SUMMARY:Clay Workshop for Young Artists
END:VEVENT
BEGIN:VEVENT
UID:storytime-1
DTSTART;TZID=America/New_York:20260417T103000
DTEND;TZID=America/New_York:20260417T113000
DESCRIPTION:Storytime with songs and crafts for ages 3-6.
LOCATION:Slater Memorial Museum
SUMMARY:Storytime at Slater / Rabbits
END:VEVENT
BEGIN:VEVENT
UID:closed-1
DTSTART;VALUE=DATE:20260403
DTEND;VALUE=DATE:20260404
DESCRIPTION:Slater Memorial Museum CLOSED for Good Friday.
SUMMARY:Slater Memorial Museum
END:VEVENT
BEGIN:VEVENT
UID:show-1
DTSTART;TZID=America/New_York:20260420T100000
DTEND;TZID=America/New_York:20260420T160000
DESCRIPTION:Annual art exhibition on view in the gallery.
LOCATION:Converse Art Gallery
SUMMARY:Student Art Show
END:VEVENT
END:VCALENDAR
"""

    rows = parse_slater_events_ics(ics_text, start_date="2026-03-15")

    assert [row.title for row in rows] == [
        "Spring Lecture Series: Norwich in the Revolution",
        "Clay Workshop for Young Artists",
    ]
    assert rows[0].activity_type == "talk"
    assert rows[0].start_at == datetime(2026, 4, 7, 18, 0)

    assert rows[1].activity_type == "workshop"
    assert rows[1].is_free is False
    assert rows[1].free_verification_status == "confirmed"
    assert rows[1].registration_required is True
    assert rows[1].age_min == 8
    assert rows[1].age_max == 12


def test_slater_parser_uses_uid_fragment_source_url() -> None:
    ics_text = """BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:lecture-uid
DTSTART;TZID=America/New_York:20260407T180000
DTEND;TZID=America/New_York:20260407T190000
DESCRIPTION:Lecture on Norwich art history.
LOCATION:Slater Auditorium
SUMMARY:Lecture Series: Norwich Art History
END:VEVENT
END:VCALENDAR
"""

    rows = parse_slater_events_ics(ics_text, start_date="2026-03-15")

    assert len(rows) == 1
    assert rows[0].source_url.endswith("#uid=lecture-uid")
