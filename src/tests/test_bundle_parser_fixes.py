"""Regressions for three bundle parsers that had drifted from their live pages."""

from datetime import date

from src.crawlers.adapters.nm_bundle import _extract_sitesf_future_cards
from src.crawlers.adapters.ok_bundle import (
    GILCREASE_OCCURRENCE_RE,
    MGMOA_CLASS_RE,
    OK_VENUES_BY_SLUG,
    _parse_mgmoa,
)
from src.crawlers.adapters.wv_bundle import WV_VENUES_BY_SLUG, _parse_stifel_events


def test_sitesf_cards_survive_css_module_hash() -> None:
    """sitesantafe.org moved to FutureEvents-module-scss-module__<hash>__content."""
    # The anchor is a sibling inside the card's wrapper, not an ancestor.
    html = """
    <div class="FutureEvents-module-scss-module__NpbJqG__futureEvents">
      <div class="FutureEvents-module-scss-module__NpbJqG__content">
        Member Morning 06 SEP 2099 , 9 AM SITE SANTA FE READ MORE
        <a class="CtaButton-module-scss-module__1C0gdW__buttonIn"
           href="/en/events/member-morning/"><span>READ MORE</span></a>
      </div>
    </div>
    """
    cards = _extract_sitesf_future_cards(html, "https://www.sitesantafe.org/en/events/")

    assert len(cards) == 1
    assert cards[0]["title"] == "Member Morning"


def test_gilcrease_occurrence_accepts_purchase_cta() -> None:
    """The calendar renamed its call to action from "Register" to "Purchase"."""
    for cta in ("Register", "Purchase"):
        text = f"September 3, 2099 , 2:00PM , Indigenous Independence Tours , {cta}"
        assert GILCREASE_OCCURRENCE_RE.search(text) is not None, cta


def test_mgmoa_regex_is_not_pinned_to_one_term() -> None:
    """Season, year and class name all change each term."""
    text = (
        "2099 Fall Class Schedule After School Art Class "
        "This workshop is perfect for kids ages 4-14. "
        "Week 1 | October 6: Create it. "
        "Times: 5:30 - 7 p.m. Cost: $55 (includes all materials)"
    )
    match = MGMOA_CLASS_RE.search(text)

    assert match is not None
    assert match.group("year") == "2099"
    assert match.group("title") == "After School Art Class"

    # The previous term's wording must keep working too.
    older = text.replace("2099 Fall", "2098 Spring").replace(
        "After School Art Class", "After School Art Ceramic Class"
    )
    assert MGMOA_CLASS_RE.search(older) is not None


def test_mgmoa_uses_the_year_from_the_page() -> None:
    text = (
        "2099 Fall Class Schedule After School Art Class Ages 4-14. "
        "Week 1 | October 6: Create it. Times: 5:30 - 7 p.m. Cost: $55"
    )
    rows = _parse_mgmoa(
        {"html": text}, venue=OK_VENUES_BY_SLUG["mgmoa"], current_date=date(2099, 1, 1)
    )

    assert len(rows) == 1
    assert rows[0].start_at.year == 2099


def test_stifel_reads_the_newer_wpbakery_accordion_layout() -> None:
    """vc_tta-accordion wrapped in vc_tta-container moves the summary block out a level."""
    html = """
    <div>
      <div class="wpb_text_column"><strong>Kids Art Sampler</strong><p>A sampler class for 2099.</p></div>
      <div class="vc_tta-container">
        <div class="vc_general vc_tta vc_tta-accordion">
          Ages: 6 -10 Meets: Tuesdays, 4-5pm Begins: September 30 Duration: 4 weeks Cost: $60
          <a href="https://kbmc.neoncrm.com/event.jsp?event=1">Register</a>
        </div>
      </div>
    </div>
    """
    rows = _parse_stifel_events({"list_html": html}, venue=WV_VENUES_BY_SLUG["stifel"])

    assert len(rows) == 1
    assert rows[0].title == "Kids Art Sampler"
    assert (rows[0].age_min, rows[0].age_max) == (6, 10)
