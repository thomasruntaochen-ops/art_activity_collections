from datetime import datetime

from src.crawlers.adapters.moca_la import MOCA_PROGRAMS_URL, parse_moca_programs_html

# Class names carry a per-build hash; the parser must match on the stable
# "EventSquare" / "dateTime" / "__title" fragments, not the full class string.
CARD_HTML = """
<ul>
  <li>
    <a class="EventSquare-module-scss-module__XXXXX__root"
       href="/events/lita-albuquerque-in-conversation">
      <div class="EventSquare-module-scss-module__XXXXX__info">
        <div class="c1 EventSquare-module-scss-module__XXXXX__dateTime">Sept 12, 2099 3pm&ndash;4pm</div>
        <span class="h3 EventSquare-module-scss-module__XXXXX__title">Lita Albuquerque in Conversation</span>
      </div>
    </a>
  </li>
  <li>
    <a class="EventSquare-module-scss-module__XXXXX__root"
       href="/events/moca-grand-avenue-talking-tour-24">
      <div class="EventSquare-module-scss-module__XXXXX__info">
        <div class="c1 EventSquare-module-scss-module__XXXXX__dateTime">Aug 29, 2099 11:30am</div>
        <span class="h3 EventSquare-module-scss-module__XXXXX__title">MOCA Grand Avenue Talking Tour</span>
      </div>
    </a>
  </li>
</ul>
"""


def test_parses_hashed_event_square_cards() -> None:
    rows = parse_moca_programs_html(
        CARD_HTML, list_url=MOCA_PROGRAMS_URL, now=datetime(2099, 1, 1)
    )

    # The tour is excluded by keyword; the conversation is kept.
    assert len(rows) == 1
    row = rows[0]
    assert row.title == "Lita Albuquerque in Conversation"
    assert row.start_at == datetime(2099, 9, 12, 15, 0)
    assert row.source_url.endswith("/events/lita-albuquerque-in-conversation")


# The Nov 2026 build minified the module prefix to a short hash ("cM_neq_"),
# dropping the "EventSquare" name entirely; only the local names survived.
MINIFIED_CARD_HTML = """
<ul>
  <li class="m2ts1a_cardItem">
    <a class="cM_neq_root" href="/events/in-session-larry-johnson">
      <div class="cM_neq_info">
        <div class="c1 cM_neq_dateTime"><span>Nov 14, 2099</span><span>11:30am</span></div>
        <span class="c1">MOCA Grand Avenue</span>
        <div class="_0SeHga_root h3 cM_neq_title"><p>In Session: Larry Johnson</p></div>
        <span class="c1">Conversation</span>
      </div>
    </a>
  </li>
  <li class="m2ts1a_cardItem">
    <a class="cM_neq_root" href="/events/studio-visit">
      <div class="cM_neq_imageWrap">
        <div class="c1 cM_neq_tag cM_neq_absolute">Member Only</div>
      </div>
      <div class="cM_neq_info">
        <div class="c1 cM_neq_dateTime"><span>Nov 7, 2099</span></div>
        <div class="h3 cM_neq_title">Studio Visit with Guadalupe Rosales</div>
        <span class="c1">Conversation, Member Event</span>
      </div>
    </a>
  </li>
</ul>
"""


def test_parses_minified_class_cards() -> None:
    rows = parse_moca_programs_html(
        MINIFIED_CARD_HTML, list_url=MOCA_PROGRAMS_URL, now=datetime(2099, 1, 1)
    )

    # The member-only studio visit is skipped by its tag.
    assert len(rows) == 1
    row = rows[0]
    assert row.title == "In Session: Larry Johnson"
    assert row.start_at == datetime(2099, 11, 14, 11, 30)
    assert row.location_text == "MOCA Grand Avenue, Los Angeles, CA"
    assert "Category: Conversation" in row.description


def test_four_letter_month_abbreviation_parses() -> None:
    # The site writes "Sept", which %b does not accept.
    rows = parse_moca_programs_html(
        CARD_HTML.replace("Sept 12, 2099 3pm&ndash;4pm", "Sept 18&ndash;20, 2099"),
        list_url=MOCA_PROGRAMS_URL,
        now=datetime(2099, 1, 1),
    )

    assert rows[0].start_at == datetime(2099, 9, 18, 0, 0)
