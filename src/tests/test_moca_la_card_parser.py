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


def test_four_letter_month_abbreviation_parses() -> None:
    # The site writes "Sept", which %b does not accept.
    rows = parse_moca_programs_html(
        CARD_HTML.replace("Sept 12, 2099 3pm&ndash;4pm", "Sept 18&ndash;20, 2099"),
        list_url=MOCA_PROGRAMS_URL,
        now=datetime(2099, 1, 1),
    )

    assert rows[0].start_at == datetime(2099, 9, 18, 0, 0)
