from src.crawlers.adapters.academy_art_museum import parse_academy_art_museum_events_html


def _card(
    *,
    title: str,
    href: str,
    categories: list[str],
    appears: str,
    start: str,
    end: str | None = None,
) -> str:
    category_html = "".join(f'<span class="single-category">{category}</span>' for category in categories)
    end_html = f'<span class="ending-date">{end}</span>' if end else ""
    return f"""
    <div class="single">
      <div class="single-title">
        <a href="{href}"><h2 class="s-title">{title}</h2></a>
      </div>
      <div class="single-dates">
        <span class="appears-date">{appears}</span>
        <span class="starting-date">{start}</span>
        {end_html}
      </div>
      <div class="single-categories">{category_html}</div>
    </div>
    """


def test_academy_infers_paid_adult_and_youth_classes() -> None:
    rows = parse_academy_art_museum_events_html(
        "\n".join(
            [
                _card(
                    title="Absolute Abstracts in Acrylic or Oil",
                    href="/absolute-abstracts-in-acrylic-or-oil/",
                    categories=["Classes And Workshops", "Painting"],
                    appears="Saturday, July 11th, 10:00AM-3:00PM",
                    start="Jul 11, 2027",
                ),
                _card(
                    title="Introduction to Drawing",
                    href="/introduction-to-drawing/",
                    categories=["Youth Classes"],
                    appears="Thursday July 23rd, 10:00AM-3:00PM",
                    start="Jul 23, 2027",
                ),
                _card(
                    title="Painting Workshop: The Costumed Figure",
                    href="/painting-workshop-the-costumed-figure/",
                    categories=["Painting"],
                    appears="Tuesday, July 24th, 10:00AM-3:00PM",
                    start="Jul 24, 2027",
                ),
            ]
        )
    )

    assert [row.audience_segment for row in rows] == ["adults", "kids", "adults"]
    assert [row.is_free for row in rows] == [False, False, False]
    assert [row.free_verification_status for row in rows] == ["inferred", "inferred", "inferred"]


def test_academy_keeps_free_family_day_as_all_ages() -> None:
    rows = parse_academy_art_museum_events_html(
        _card(
            title="FREE FAMILY ART DAY: Personalized Mexican Loteria Boards",
            href="/free-family-art-day-personalized-mexican-loteria-boards/",
            categories=["Free Events", "Special Events", "Youth Classes"],
            appears="Sunday, August 16, 2027 | 11 AM -2 PM - Drop in anytime!",
            start="Aug 16, 2027",
        )
    )

    assert len(rows) == 1
    assert rows[0].audience_segment == "all_ages"
    assert rows[0].is_free is True
    assert rows[0].free_verification_status == "confirmed"


def test_academy_rejects_receptions_and_music() -> None:
    rows = parse_academy_art_museum_events_html(
        "\n".join(
            [
                _card(
                    title="Opening Reception",
                    href="/opening-reception/",
                    categories=["Special Events"],
                    appears="Friday, July 9th, 5:00PM-7:00PM",
                    start="Jul 9, 2027",
                ),
                _card(
                    title="Music in the Courtyard",
                    href="/music-in-the-courtyard/",
                    categories=["Music"],
                    appears="Friday, July 10th, 6:00PM-8:00PM",
                    start="Jul 10, 2027",
                ),
            ]
        )
    )

    assert rows == []
