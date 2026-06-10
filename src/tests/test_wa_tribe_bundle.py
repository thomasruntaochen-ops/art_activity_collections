from src.crawlers.adapters.wa_tribe_bundle import WA_TRIBE_VENUES_BY_SLUG
from src.crawlers.adapters.wa_tribe_bundle import parse_wa_tribe_events


WHATCOM = WA_TRIBE_VENUES_BY_SLUG["whatcom"]


def _event(
    *,
    title: str,
    description: str,
    categories: list[str] | None = None,
    cost: str = "",
    cost_values: list[int] | None = None,
    url_slug: str | None = None,
) -> dict:
    slug = url_slug or title.lower().replace(" ", "-").replace(":", "")
    return {
        "title": title,
        "url": f"https://www.whatcommuseum.org/event/{slug}/",
        "start_date": "2027-07-08 10:00:00",
        "end_date": "2027-07-08 11:00:00",
        "description": description,
        "excerpt": "",
        "cost": cost,
        "cost_details": {"values": cost_values or []},
        "categories": [{"name": name} for name in (categories or [])],
        "tags": [],
        "venue": {"venue": "Lightcatcher", "city": "Bellingham", "state": "WA"},
    }


def test_whatcom_decodes_title_sets_audience_and_marks_admission_paid() -> None:
    rows = parse_wa_tribe_events(
        [
            _event(
                title="Artist&#8217;s Corner: Color Lab",
                description=(
                    "Visit the Artist's Corner in the Family Interactive Gallery Studio. "
                    "Young guests create art inspired by the featured artist."
                ),
                categories=["FIG"],
                cost="Included with admission/Members free",
            )
        ],
        venue=WHATCOM,
    )

    assert len(rows) == 1
    assert "&#" not in rows[0].title
    assert rows[0].audience_segment == "kids"
    assert rows[0].is_free is False
    assert rows[0].free_verification_status == "confirmed"


def test_whatcom_keeps_family_tinker_as_all_ages() -> None:
    rows = parse_wa_tribe_events(
        [
            _event(
                title="Think & Tinker: Color Lab",
                description="Creative activities are designed for the entire family and all ages.",
                categories=["FIG"],
                cost="Included with admission/Members free",
            )
        ],
        venue=WHATCOM,
    )

    assert len(rows) == 1
    assert rows[0].audience_segment == "all_ages"


def test_whatcom_marks_in_the_studio_fig_rows_as_kids() -> None:
    rows = parse_wa_tribe_events(
        [
            _event(
                title="In the Studio: Rainbow Vision",
                description="A self-guided play-to-learn, make-and-take space for young inquisitive minds.",
                categories=["FIG"],
                cost="Included with admission/Members free",
            )
        ],
        venue=WHATCOM,
    )

    assert len(rows) == 1
    assert rows[0].audience_segment == "kids"


def test_whatcom_rejects_access_closure_and_sports_activation_rows() -> None:
    rows = parse_wa_tribe_events(
        [
            _event(
                title="Low Sensory Sunday Hours",
                description="A calmer sensory-friendly museum visit.",
                categories=["FIG"],
                cost="FREE",
                cost_values=[0],
            ),
            _event(
                title="Family Interactive Gallery Closed",
                description="The Family Interactive Gallery is closed for refresh work.",
                categories=["FIG"],
            ),
            _event(
                title="Fan Zone Friday",
                description="Summer of Soccer tote bag giveaway and decoration.",
                categories=["GENERAL CALENDAR"],
                cost="FREE",
                cost_values=[0],
            ),
        ],
        venue=WHATCOM,
    )

    assert rows == []
