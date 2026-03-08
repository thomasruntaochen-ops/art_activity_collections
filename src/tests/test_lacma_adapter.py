from datetime import datetime

from src.crawlers.adapters.lacma import parse_lacma_events_html


def test_lacma_parser_keeps_workshops_and_filters_tours() -> None:
    html = """
    <div class="card-event">
      <div class="card-event__header"><span class="card-event__type">Tours</span></div>
      <div class="card-event__body">
        <div class="card-event__name"><a href="/event/gallery-tour-modern-art">Gallery Tour: Modern Art</a></div>
        <div class="card-event__content"><p>Join a docent for a 60-minute tour.</p></div>
        <div class="card-event__date"><span>Sun Mar 8</span> | <span>12 pm PT</span></div>
      </div>
    </div>
    <div class="card-event">
      <div class="card-event__header"><span class="card-event__type">Drop-in Workshops</span></div>
      <div class="card-event__body">
        <div class="card-event__name"><a href="/event/boone-childrens-gallery-pop-art-workshop">Boone Children's Gallery: Pop Up Art Workshop</a></div>
        <div class="card-event__content"><p>Make art with us in an exhibition-based workshop.</p></div>
        <div class="card-event__date"><span>Sat Mar 14</span> | <span>11 am PT</span></div>
        <div class="card-event__location"><span>LACMA</span></div>
      </div>
    </div>
    <div class="card-event">
      <div class="card-event__header"><span class="card-event__type">Talks</span></div>
      <div class="card-event__body">
        <div class="card-event__name"><a href="/event/artist-conversation">Artist Conversation</a></div>
        <div class="card-event__content"><p>A public conversation with artists and curators.</p></div>
        <div class="card-event__date"><span>Fri Mar 13</span> | <span>4 pm PT</span></div>
        <div class="card-event__location"><span>Resnick Pavilion</span><span>LACMA</span></div>
      </div>
    </div>
    """

    rows = parse_lacma_events_html(
        html=html,
        list_url="https://www.lacma.org/event",
        now=datetime(2026, 3, 8),
    )

    assert len(rows) == 2
    assert [row.title for row in rows] == [
        "Boone Children's Gallery: Pop Up Art Workshop",
        "Artist Conversation",
    ]
    assert rows[0].activity_type == "workshop"
    assert rows[0].drop_in is True
    assert rows[1].activity_type == "talk"
