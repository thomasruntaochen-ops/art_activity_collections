from src.crawlers.adapters.buffalo_akg import _infer_audience_segment as infer_buffalo_audience
from src.crawlers.adapters.buffalo_akg import _should_include_event as should_include_buffalo
from src.crawlers.adapters.mag import _build_row as build_mag_row
from src.crawlers.adapters.new_bedford import _infer_audience_segment as infer_new_bedford_audience
from src.crawlers.adapters.new_bedford import _should_include_event as should_include_new_bedford
from src.crawlers.adapters.new_bedford import _token_blob as new_bedford_token_blob
from src.crawlers.adapters.newark import parse_newark_events_html
from src.crawlers.adapters.pem import parse_pem_payload


def test_buffalo_sets_audience_and_excludes_non_activity_rows() -> None:
    kids_detail = {
        "title": "Kids' Studio Art Class: Painted Planters (Ages 8-12)",
        "description": "Young artists decorate miniature planters.",
        "source_url": "https://buffaloakg.org/events/kids-studio",
        "category_tags": [],
    }
    adult_detail = {
        "title": "Studio Clay Class: Vast Vases",
        "description": "A one-day clay workshop for all skill levels.",
        "source_url": "https://buffaloakg.org/events/studio-clay",
        "category_tags": [],
    }
    jazz_detail = {
        "title": "Lipsey Summer Jazz: The Buffalo Jazz Collective",
        "description": "A concert featuring the music of Duke Ellington.",
        "price_text": "FREE",
        "category_tags": [],
    }

    assert infer_buffalo_audience(detail=kids_detail, age_min=8, age_max=12) == "kids"
    assert infer_buffalo_audience(detail=adult_detail, age_min=None, age_max=None) == "adults"
    assert not should_include_buffalo(listing={"listing_text": "Jazz concert"}, detail=jazz_detail)


def test_pem_filters_business_noise_and_sets_drop_in_art_all_ages() -> None:
    payload = {
        "events": [
            {
                "name": "Friday Afternoon Drop-In Art: Seashell Mosaics",
                "url": "https://www.pem.org/events/drop-in-art-seashell-mosaics",
                "startDate": "2030-07-03T15:00:00",
                "endDate": "2030-07-03T16:30:00",
                "description": "Create your own seashell mosaic to bring home.",
                "offers": {"price": "0"},
            },
            {
                "name": "Salem Biz Town Hall",
                "url": "https://www.pem.org/events/salem-biz-town-hall",
                "startDate": "2030-08-11T08:30:00",
                "endDate": "2030-08-11T11:00:00",
                "description": "A networking event for Salem business owners.",
                "offers": {"price": "0"},
            },
        ],
        "detail_pages": {},
    }

    rows = parse_pem_payload(payload)

    assert [row.title for row in rows] == ["Friday Afternoon Drop-In Art: Seashell Mosaics"]
    assert rows[0].audience_segment == "all_ages"
    assert rows[0].is_free is True


def test_mag_sets_adult_audience_and_decodes_title_entities() -> None:
    row = build_mag_row(
        {
            "title": "Art Start: Paint &amp; Snack &#8211; Poppies (5ASU26)",
            "url": "https://mag.rochester.edu/event/art-start-poppies/",
            "start_date": "2030-08-11 18:00:00",
            "end_date": "2030-08-11 21:00:00",
            "categories": [{"name": "Classes"}],
            "description": "<p>Join us for a beginner painting class.</p>",
            "excerpt": "",
            "cost": "$45",
            "cost_details": {"values": ["45"]},
            "venue": {"venue": "Creative Workshop", "city": "Rochester", "state": "NY"},
        }
    )

    assert row is not None
    assert row.title == "Art Start: Paint & Snack \u2013 Poppies (5ASU26)"
    assert row.audience_segment == "adults"
    assert row.is_free is False


def test_newark_sets_family_audience_and_inferred_non_free_default() -> None:
    html = """
    <main>
      <div class="item event-item" data-terms=" onsite artmaking science" data-datetime="203006131200">
        <a href="/event/june-family-saturdays/?date=203006131200">
          <h4>June Family Saturdays</h4>
        </a>
      </div>
      <div class="item event-item" data-terms=" onsite classes-workshops" data-datetime="203006181800">
        <a href="/event/adult-workshop-drop-in-drawing/?date=203006181800">
          <h4>Adult Workshop: Drop In Drawing</h4>
        </a>
      </div>
    </main>
    """

    rows = parse_newark_events_html(html, list_url="https://newarkmuseumart.org/events/")

    assert [(row.title, row.audience_segment) for row in rows] == [
        ("June Family Saturdays", "kids"),
        ("Adult Workshop: Drop In Drawing", "adults"),
    ]
    assert all(row.is_free is False for row in rows)
    assert all(row.free_verification_status == "inferred" for row in rows)


def test_new_bedford_sets_audience_and_excludes_movement_performance() -> None:
    assert (
        infer_new_bedford_audience(
            title="Creative Canvas Club: Painting for Kids",
            description="Ages 7+",
            source_url="https://newbedfordart.org/canvas-club/",
            age_min=7,
            age_max=None,
        )
        == "kids"
    )
    assert (
        infer_new_bedford_audience(
            title="Wheel-Throwing for Everyone",
            description="A four-week ceramics class.",
            source_url="https://newbedfordart.org/wheel-throwing-2/",
            age_min=None,
            age_max=None,
        )
        == "adults"
    )
    assert not should_include_new_bedford(
        title="MOVEMENT AT THE MUSEUM: Listen",
        token_blob=new_bedford_token_blob("movement at the museum performance"),
    )
