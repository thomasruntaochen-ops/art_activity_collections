from datetime import datetime

from src.crawlers.adapters.guggenheim import GUGGENHEIM_CALENDAR_URL
from src.crawlers.adapters.guggenheim import parse_guggenheim_calendar_html


def _article(*, href: str, title: str, time_label: str, description: str, tags: list[str], datetime_attr: str) -> str:
    tag_links = "".join(f'<a href="/event/tag/{i}">{tag}</a>' for i, tag in enumerate(tags))
    return f"""
    <article class="_event_1xp2j_15">
      <a class="_image_1xp2j_32" href="{href}"></a>
      <section class="_text_1xp2j_47">
        <header><a class="_title_1xp2j_50" href="{href}">{title}</a></header>
        <time class="subtitle-2" datetime="{datetime_attr}">{time_label}</time>
        <p>{description}</p>
        {tag_links}
      </section>
    </article>
    """


CALENDAR_HTML = "<ul>" + "".join(
    [
        # The datetime attribute says T03:00:00 while the label says 3 pm.
        _article(
            href="/event/art-cart/2026-08-01",
            title="Art Cart",
            time_label="3 pm EDT",
            description="Families and visitors of all ages are invited to make art in the galleries. Drop-in; free with museum admission.",
            tags=["For Kids, Teens, and Family", "Free with Admission"],
            datetime_attr="2026-08-01T03:00:00",
        ),
        _article(
            href="/event/teen-tuesdays/2026-08-04",
            title="Teen Tuesdays",
            time_label="4 pm EDT",
            description="Drop in for FREE Teen Tuesdays, a monthly hangout where teens make art together.",
            tags=["Courses and Workshops", "For Kids, Teens, and Family", "Free with Admission"],
            datetime_attr="2026-08-04T04:00:00",
        ),
        _article(
            href="/event/curious-about-the-building/2026-08-01",
            title="Curious about the Building?",
            time_label="2 pm EDT",
            description="Join us on the rotunda floor to learn about the architecture.",
            tags=["Free with Admission", "Tours"],
            datetime_attr="2026-08-01T02:00:00",
        ),
        _article(
            href="/event/member-mondays/2026-08-03",
            title="Member Mondays",
            time_label="6 pm EDT",
            description="Members are invited to enjoy the museum during members-only hours.",
            tags=["After Hours", "For Members"],
            datetime_attr="2026-08-03T06:00:00",
        ),
        # Series landing page: no occurrence date, must be skipped.
        _article(
            href="/event/event_series/youth-programs",
            title="Youth Programs",
            time_label="10 am EDT",
            description="A series of programmes for young artists.",
            tags=["For Kids, Teens, and Family"],
            datetime_attr="2026-08-01T10:00:00",
        ),
    ]
) + "</ul>"


def _by_title(rows) -> dict:
    return {row.title: row for row in rows}


def test_guggenheim_reads_afternoon_times_from_the_visible_label() -> None:
    """The card's datetime attribute is 12-hour with no meridiem applied.

    Trusting it would file a 3 pm drop-in as a 3 am one.
    """
    rows = _by_title(parse_guggenheim_calendar_html(CALENDAR_HTML, list_url=GUGGENHEIM_CALENDAR_URL))

    assert rows["Art Cart"].start_at == datetime(2026, 8, 1, 15, 0)
    assert rows["Teen Tuesdays"].start_at == datetime(2026, 8, 4, 16, 0)


def test_guggenheim_free_with_admission_is_not_free() -> None:
    """The museum is ticketed, so "free with admission" still costs the visitor.

    An event only counts as free when its own wording says so.
    """
    rows = _by_title(parse_guggenheim_calendar_html(CALENDAR_HTML, list_url=GUGGENHEIM_CALENDAR_URL))

    assert rows["Art Cart"].is_free is False
    assert rows["Teen Tuesdays"].is_free is True


def test_guggenheim_skips_members_tours_and_series_pages() -> None:
    rows = parse_guggenheim_calendar_html(CALENDAR_HTML, list_url=GUGGENHEIM_CALENDAR_URL)
    titles = {row.title for row in rows}

    assert titles == {"Art Cart", "Teen Tuesdays"}


def test_guggenheim_audience_prefers_the_event_over_the_broad_family_tag() -> None:
    """Both events carry "For Kids, Teens, and Family"; only one is for teens."""
    rows = _by_title(parse_guggenheim_calendar_html(CALENDAR_HTML, list_url=GUGGENHEIM_CALENDAR_URL))

    assert rows["Teen Tuesdays"].audience_segment == "teens"
    assert rows["Art Cart"].audience_segment == "all_ages"


def test_guggenheim_takes_end_time_and_ages_from_the_detail_page() -> None:
    rows = _by_title(
        parse_guggenheim_calendar_html(
            CALENDAR_HTML,
            list_url=GUGGENHEIM_CALENDAR_URL,
            detail_text_by_slug={
                "art-cart": "Art Cart Saturday, August 1, 2026 3–5 pm EDT Free with admission. For ages 4–12.",
            },
        )
    )

    assert rows["Art Cart"].end_at == datetime(2026, 8, 1, 17, 0)
    assert (rows["Art Cart"].age_min, rows["Art Cart"].age_max) == (4, 12)
    # No detail text for this slug, so no end time is invented.
    assert rows["Teen Tuesdays"].end_at is None
