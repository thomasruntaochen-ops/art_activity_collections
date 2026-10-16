from src.crawlers.adapters.noguchi import parse_noguchi_payload

EVENT_BASE = "https://www.noguchi.org/museum/calendar/event/"


def _card(*, slug: str, title: str, categories: list[str], start: str) -> str:
    eyebrows = "".join(f'<span class="eyebrow">{name}</span>' for name in categories)
    return f"""
        <div class="grid-flexbox-layout grid-events">
          <div class="block-quarter text-gray">{eyebrows}</div>
          <h3 class="subheadline"><a href="{EVENT_BASE}{slug}/">{title}</a></h3>
          <div class="add-to-calendar">
            <span class="start">{start}</span>
            <span class="location">The Noguchi Museum</span>
          </div>
        </div>
    """


def _detail(*, description: str, price: str) -> str:
    return f"""
        <main class="calendar calendar_detail">
          <div class="subheadline-s">{price}</div>
          <div class="block-half body-text wysiwyg"><p>{description}</p></div>
        </main>
    """


def test_noguchi_keeps_family_workshop_that_mentions_admission() -> None:
    # Detail pages price family programs as "Free with admission" and describe
    # them as "in the exhibition"; neither should reject the event.
    payload = {
        "list_html": _card(
            slug="2099-11-15-1100-open-studio",
            title="Open Studio: Play with Paper",
            categories=["Workshop", "Families"],
            start="11/15/2099 11:00 AM",
        )
        + _card(
            slug="2099-11-06-free-first-friday",
            title="Free First Friday",
            categories=["Free Admission"],
            start="11/06/2099 10:00 AM",
        ),
        "detail_pages": {
            f"{EVENT_BASE}2099-11-15-1100-open-studio/": _detail(
                description="Families explore the exhibition and make art together.",
                price="Free with admission",
            ),
            f"{EVENT_BASE}2099-11-06-free-first-friday/": _detail(
                description="On the first Friday of every month, Museum admission is free.",
                price="",
            ),
        },
    }

    rows = parse_noguchi_payload(payload)

    assert [row.title for row in rows] == ["Open Studio: Play with Paper"]
    assert rows[0].audience_segment == "kids"
