from datetime import datetime

from src.crawlers.adapters.mocact import parse_mocact_events_payload


def _calendar(*cells: tuple[str, str, str, str]) -> str:
    """Render the MEC monthly view: (yyyymmdd, time_text, title, url) per entry."""
    sections = []
    for cell, time_text, title, url in cells:
        time_html = f'<div class="mec-event-time mec-color">{time_text}</div>' if time_text else ""
        sections.append(
            f"""
            <div class="mec-calendar-events-sec" data-mec-cell="{cell}">
              <article class="mec-event-article">
                <div class="mec-monthly-contents">
                  {time_html}
                  <h4 class="mec-event-title"><a href="{url}">{title}</a></h4>
                </div>
              </article>
            </div>
            """
        )
    return f"<html><body><div class='mec-wrap'>{''.join(sections)}</div></body></html>"


def _detail(description: str) -> str:
    return f"""
        <html><head>
          <meta property="og:description" content="{description}" />
        </head><body></body></html>
    """


def test_mocact_parser_keeps_paid_class_and_conversation() -> None:
    payload = {
        "calendar_html": _calendar(
            ("20260314", "12:00 pm", "Art Adventures", "https://mocact.org/class-workshop/art-adventures-4-2/"),
            (
                "20260326",
                "5:30 pm",
                "Community Conversation: Jazz Inspirations",
                "https://mocact.org/events/community-conversation-jazz-inspirations/",
            ),
            (
                "20260410",
                "6:00 pm",
                "Adult Workshop: Encaustic Collage",
                "https://mocact.org/class-workshop/encaustic-collage/",
            ),
        ),
        "detail_pages": {
            "https://mocact.org/class-workshop/art-adventures-4-2/": _detail(
                "Book Now at checkout.mocact.org. Your child can join us every Saturday for a relaxed and "
                "inspiring drop-in art class at the museum! $25.00/per child Ages 4+"
            ),
            "https://mocact.org/events/community-conversation-jazz-inspirations/": _detail(
                "Register here: $10 general. $8 seniors + students, free for members. This engaging discussion "
                "brings together musicians, artists, educators, and scholars."
            ),
            "https://mocact.org/class-workshop/encaustic-collage/": _detail(
                "A hands-on workshop for adults. $125"
            ),
        },
    }

    rows = parse_mocact_events_payload(payload)

    assert len(rows) == 3
    assert rows[0].title == "Art Adventures"
    # "Ages 4+" has no upper bound, so the shared age rule reads it as open to every age.
    assert rows[0].audience_segment == "all_ages"
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"
    assert rows[0].registration_required is True
    assert rows[0].drop_in is True
    assert rows[0].age_min == 4
    assert rows[0].age_max is None
    assert rows[0].start_at == datetime(2026, 3, 14, 12, 0)

    assert rows[1].title == "Community Conversation: Jazz Inspirations"
    assert rows[1].activity_type == "talk"
    assert rows[1].audience_segment == "adults"
    assert rows[1].is_free is False
    assert rows[1].free_verification_status == "confirmed"
    assert rows[1].start_at == datetime(2026, 3, 26, 17, 30)

    assert rows[2].title == "Adult Workshop: Encaustic Collage"
    assert rows[2].audience_segment == "adults"
    assert rows[2].is_free is False


def test_mocact_parser_excludes_tours_camps_and_writing() -> None:
    payload = {
        "calendar_html": _calendar(
            (
                "20260331",
                "11:00 am",
                "Guided Tour of the Brubeck Collection (Members Only)",
                "https://mocact.org/events/guided-tour-brubeck/",
            ),
            (
                "20260413",
                "9:00 am",
                "April 2026 School Recess Creativity Camp",
                "https://mocact.org/class-workshop/april-2026-creativity-camp/",
            ),
            (
                "20260514",
                "6:00 pm",
                "Westport Writers Workshop at MoCACT",
                "https://mocact.org/class-workshop/westport-writers-workshop/",
            ),
        ),
        "detail_pages": {
            "https://mocact.org/events/guided-tour-brubeck/": _detail("Explore the collection during a guided visit."),
            "https://mocact.org/class-workshop/april-2026-creativity-camp/": _detail("Camp description."),
            "https://mocact.org/class-workshop/westport-writers-workshop/": _detail("A writing workshop for adults."),
        },
    }

    assert parse_mocact_events_payload(payload) == []


def test_mocact_parser_skips_undated_standing_exhibitions() -> None:
    """MEC repeats ongoing exhibitions into every day cell with no time on them."""
    payload = {
        "calendar_html": _calendar(
            ("20260901", "", "Colossi", "https://mocact.org/exhibitions/colossi/"),
            ("20260902", "", "Colossi", "https://mocact.org/exhibitions/colossi/"),
            (
                "20260905",
                "12:00 pm",
                "Art Adventures! - Drop-In Art Class for Kids",
                "https://mocact.org/class-workshop/art-adventures-9-5/",
            ),
        ),
        "detail_pages": {
            "https://mocact.org/exhibitions/colossi/": _detail("A sculpture exhibition on view in the main gallery."),
            "https://mocact.org/class-workshop/art-adventures-9-5/": _detail(
                "A drop-in art class where children explore painting, drawing and sculpture."
            ),
        },
    }

    rows = parse_mocact_events_payload(payload)

    assert [row.title for row in rows] == ["Art Adventures! - Drop-In Art Class for Kids"]
    assert rows[0].start_at == datetime(2026, 9, 5, 12, 0)
