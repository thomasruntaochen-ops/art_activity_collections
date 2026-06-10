from __future__ import annotations

from src.crawlers.adapters.columbus_museum_of_art import parse_columbus_museum_of_art_payload


def _card(
    *,
    title: str,
    source_url: str,
    month: str = "Jul",
    day: str = "10",
    time_text: str = "10:00 AM-11:00 AM",
    summary: str = "Artmaking activity.",
    tags: list[str] | None = None,
) -> dict:
    return {
        "title": title,
        "source_url": source_url,
        "month_text": month,
        "day_text": day,
        "weekday_text": "Friday",
        "time_text": time_text,
        "summary": summary,
        "tags": tags or [],
    }


def _detail(title: str, date_time: str, body: str) -> str:
    return f"""
        <html><body>
          <h1>{title}</h1>
          <div class="event-date-time">{date_time}</div>
          <div class="event-detail">{body}</div>
          <p>Location: Columbus Museum of Art</p>
        </body></html>
    """


def test_columbus_infers_audience_and_price_for_kept_rows() -> None:
    open_studio_url = "https://www.columbusmuseum.org/events/event/5699702/date/2026-07-11"
    teen_url = "https://www.columbusmuseum.org/events/event/6021046"
    talk_url = "https://www.columbusmuseum.org/events/event/6021033"

    rows = parse_columbus_museum_of_art_payload(
        {
            "cards": [
                _card(
                    title="Open Studio",
                    source_url=open_studio_url,
                    month="Jul",
                    day="11",
                    tags=["Youth & Families", "Artmaking"],
                ),
                _card(
                    title="Teen Studio",
                    source_url=teen_url,
                    month="Jul",
                    day="16",
                    tags=["Free", "Teens", "Artmaking"],
                ),
                _card(
                    title="Wednesdays@2: Curator Talk",
                    source_url=talk_url,
                    month="Jul",
                    day="22",
                    tags=["Exhibition-Inspired Talks & Conversations"],
                ),
            ],
            "detail_pages": {
                open_studio_url: _detail(
                    "Open Studio",
                    "Saturday, July 11 | 10:00 AM-1:00 PM",
                    "Individuals of all ages are welcome for artmaking. Included with museum admission.",
                ),
                teen_url: _detail(
                    "Teen Studio",
                    "Thursday, July 16 | 5:00-7:00 PM",
                    "Free art-based workshop for teens ages 13-19.",
                ),
                talk_url: _detail(
                    "Wednesdays@2: Curator Talk",
                    "Wednesday, July 22 | 2:00-3:00 PM",
                    "A lecture on the exhibition. Registration is free for members and $10 for nonmembers.",
                ),
            },
        }
    )

    assert [row.title for row in rows] == ["Open Studio", "Teen Studio", "Wednesdays@2: Curator Talk"]
    assert [row.audience_segment for row in rows] == ["all_ages", "teens", "adults"]
    assert rows[0].is_free is None
    assert rows[1].is_free is True
    assert rows[2].is_free is False


def test_columbus_rejects_summer_art_breaks_and_music_rows() -> None:
    rows = parse_columbus_museum_of_art_payload(
        {
            "cards": [
                _card(
                    title="Summer Art Breaks",
                    source_url="https://www.columbusmuseum.org/events/event/6174528/date/2026-07-01",
                    tags=["Youth & Families", "Artmaking"],
                ),
                _card(
                    title="A Tribe for Jazz Presents: Brandon Woody's Upendo",
                    source_url="https://www.columbusmuseum.org/events/event/6075513",
                    tags=["Music & Performance", "Jazz"],
                ),
            ],
            "detail_pages": {},
        }
    )

    assert rows == []
