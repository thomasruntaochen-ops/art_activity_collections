from datetime import datetime

from src.crawlers.adapters.vancouver_art_gallery import parse_vancouver_art_gallery_payload

BASE = "https://www.vanartgallery.bc.ca/events/"
NOW = datetime(2099, 11, 6, 9, 0)


def _event(
    *,
    slug: str,
    title: str,
    event_type: str,
    start: str,
    end: str | None = None,
    subcategory: str | bool = False,
    show_time: bool = True,
) -> dict:
    return {
        "id": abs(hash(slug)) % 100000,
        "slug": slug,
        "link": f"{BASE}{slug}/",
        "title": {"rendered": f"{title} | dated label"},
        "acf": {
            "multi_line_event_title": f"{title} ",
            "group": {
                "event_date_and_time": start,
                "event_end": end or "",
                "event_show_time": show_time,
            },
            "event_type": event_type,
            "event_subcateogry": subcategory,
        },
    }


def _detail(*, room: str, paragraphs: list[str]) -> str:
    body = "".join(f"<p>{text}</p>" for text in paragraphs)
    return f"""
        <main>
          <div class="wp-block-group">
            <div class="wp-block-mfb-meta-field-block"><div class="value">Families</div></div>
            <div class="wp-block-acf-event-headline"><h1 class="event-headline">x</h1></div>
            <div class="wp-block-acf-event-dates"><p class="dates">Sun Nov 8 | 11 AM–4 PM</p></div>
            <div class="wp-block-mfb-meta-field-block"><div class="value">{room}</div></div>
          </div>
          <div class="written-contents">{body}<p>REGISTER NOW</p></div>
        </main>
    """


def test_vag_keeps_family_studio_and_talks_and_drops_other_programs() -> None:
    making_place = _event(
        slug="the-making-place-nov-8",
        title="The Making Place | Collage and Sound Sculptures",
        event_type="Families",
        start="2099-11-08 11:00:00",
        end="2099-11-08 16:00:00",
    )
    talk = _event(
        slug="artist-talk-example",
        title="Artist Talk: Example Artist",
        event_type="Lectures and Talks",
        subcategory="Artist Talk",
        start="2099-11-07 14:00:00",
        end="2099-11-07 15:00:00",
    )
    payload = {
        "events": [
            making_place,
            talk,
            _event(
                slug="free-first-friday",
                title="Free First Friday Nights",
                event_type="Special Events",
                start="2099-11-06 16:00:00",
            ),
            _event(
                slug="member-tour",
                title="Member Tour and Book Signing",
                event_type="Membership",
                start="2099-11-07 11:00:00",
            ),
            _event(
                slug="family-concert",
                title="Family Day Concert",
                event_type="Families",
                subcategory="Performance",
                start="2099-11-14 11:00:00",
            ),
            _event(
                slug="seniors-wellbeing",
                title="Art of Wellbeing for Seniors",
                event_type="Seniors",
                start="2099-11-10 13:30:00",
            ),
            _event(
                slug="old-making-place",
                title="The Making Place | Past Week",
                event_type="Families",
                start="2099-11-01 11:00:00",
            ),
        ],
        "detail_pages": {
            making_place["link"]: _detail(
                room="Room 4East",
                paragraphs=[
                    "Drop in to create and connect! At 2 PM, join a Family Tour of the exhibition.",
                    "The Making Place is open to all ages and is included with Gallery admission. "
                    "Admission is always free for Gallery Members and children and youth aged 18 and under.",
                ],
            ),
            talk["link"]: _detail(
                room="Courtroom 302",
                paragraphs=[
                    "Join the artist for a conversation about <a href='#'>the exhibition</a> , and more.",
                    "$5 for Ideas Members and $10 for the general public. Registration is required.",
                ],
            ),
        },
    }

    rows = parse_vancouver_art_gallery_payload(payload, now=NOW)

    assert [row.title for row in rows] == [
        "Artist Talk: Example Artist",
        "The Making Place | Collage and Sound Sculptures",
    ]
    talk_row, studio_row = rows

    assert studio_row.audience_segment == "all_ages"
    assert studio_row.is_free is True
    assert studio_row.drop_in is True
    assert studio_row.activity_type == "workshop"
    # location_text becomes the venue address, so rooms stay out of it.
    assert studio_row.location_text == "750 Hornby Street, Vancouver, BC V6Z 2H7"
    assert (studio_row.description or "").endswith("Location: Room 4East.")
    assert studio_row.start_at == datetime(2099, 11, 8, 11, 0)
    assert studio_row.end_at == datetime(2099, 11, 8, 16, 0)
    assert studio_row.state == "BC"
    assert studio_row.timezone == "America/Vancouver"
    assert "REGISTER NOW" not in (studio_row.description or "")

    assert talk_row.activity_type == "talk"
    assert talk_row.is_free is False
    assert talk_row.registration_required is True
    assert "exhibition, and more" in (talk_row.description or "")


def test_vag_hidden_time_and_multi_day_span_do_not_invent_times() -> None:
    payload = {
        "events": [
            _event(
                slug="drawing-lecture-series",
                title="Lecture Series: Drawing the Coast",
                event_type="Lectures and Talks",
                subcategory="Lecture",
                start="2099-11-10 13:30:00",
                end="2099-11-24 15:30:00",
                show_time=False,
            )
        ],
        "detail_pages": {},
    }

    rows = parse_vancouver_art_gallery_payload(payload, now=NOW)

    assert len(rows) == 1
    assert rows[0].start_at == datetime(2099, 11, 10, 0, 0)
    assert rows[0].end_at is None
    assert rows[0].location_text == "750 Hornby Street, Vancouver, BC V6Z 2H7"
