from datetime import datetime

from src.crawlers.adapters.university_of_wyoming_art_museum import (
    parse_university_of_wyoming_art_museum_payload,
)


def test_university_of_wyoming_art_museum_parser_keeps_talks_and_workshops() -> None:
    rss_text = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Tristan Duke &#8211; Artist Talk</title>
      <description>Thursday, April 9, 2026, 5&amp;nbsp;&amp;ndash;&amp;nbsp;7:30pm &lt;br/&gt;&lt;br/&gt;On Thursday, April 9 at 5:30 p.m., join visiting artist Tristan Duke for an artist talk. A reception starts at 5:30 p.m., with the talk to follow at 6 p.m. The event is free and open to everyone.&lt;br/&gt;&lt;br/&gt;&lt;b&gt;Venue&lt;/b&gt;:&amp;nbsp;Art Museum &lt;br/&gt;&lt;b&gt;Address&lt;/b&gt;:&amp;nbsp;University of Wyoming Laramie, Wyoming &lt;br/&gt;&lt;b&gt;Is there a fee to attend?&lt;/b&gt;:&amp;nbsp;No</description>
      <link>https://www.uwyo.edu/uw/calendar/?trumbaEmbed=view%3devent%26eventid%3d199087053</link>
      <category>2026/04/09 (Thu)</category>
    </item>
    <item>
      <title>Tristan Duke &#8211; Lunchtime Conversation</title>
      <description>Friday, April 10, 2026, 12&amp;nbsp;&amp;ndash;&amp;nbsp;1pm &lt;br/&gt;&lt;br/&gt;On Friday, April 10 at 12 p.m., visiting artist Tristan Duke will host a Lunchtime Conversation in the galleries. This event is free and open to the public.&lt;br/&gt;&lt;br/&gt;&lt;b&gt;Venue&lt;/b&gt;:&amp;nbsp;Art Museum &lt;br/&gt;&lt;b&gt;Address&lt;/b&gt;:&amp;nbsp;University of Wyoming Laramie, Wyoming &lt;br/&gt;&lt;b&gt;Is there a fee to attend?&lt;/b&gt;:&amp;nbsp;No</description>
      <link>https://www.uwyo.edu/uw/calendar/?trumbaEmbed=view%3devent%26eventid%3d199087434</link>
      <category>2026/04/10 (Fri)</category>
    </item>
    <item>
      <title>Tristan Duke &#8211; Artmaking Workshop</title>
      <description>Saturday, April 11, 2026, 1&amp;nbsp;&amp;ndash;&amp;nbsp;3pm &lt;br/&gt;&lt;br/&gt;On Saturday, April 11 at 1 p.m., visiting artist Tristan Duke will lead a hands-on photogram workshop. Registration is required.&lt;br/&gt;&lt;br/&gt;&lt;b&gt;Venue&lt;/b&gt;:&amp;nbsp;Art Museum &lt;br/&gt;&lt;b&gt;Address&lt;/b&gt;:&amp;nbsp;University of Wyoming Laramie, Wyoming &lt;br/&gt;&lt;b&gt;Is there a fee to attend?&lt;/b&gt;:&amp;nbsp;No</description>
      <link>https://www.uwyo.edu/uw/calendar/?trumbaEmbed=view%3devent%26eventid%3d199087436</link>
      <category>2026/04/11 (Sat)</category>
    </item>
    <item>
      <title>Outside Office Hours: UW Faculty and Staff Juried Exhibition - Opening Reception</title>
      <description>Thursday, April 16, 2026, 5:30&amp;nbsp;&amp;ndash;&amp;nbsp;7:30pm &lt;br/&gt;&lt;br/&gt;Exhibition opening and reception.&lt;br/&gt;&lt;br/&gt;&lt;b&gt;Venue&lt;/b&gt;:&amp;nbsp;Art Museum</description>
      <link>https://www.uwyo.edu/uw/calendar/?trumbaEmbed=view%3devent%26eventid%3d200492523</link>
      <category>2026/04/16 (Thu)</category>
    </item>
    <item>
      <title>UW Art Museum Gala 2026: A Pop Art Ball</title>
      <description>Saturday, May 2, 2026, 5&amp;nbsp;&amp;ndash;&amp;nbsp;8pm &lt;br/&gt;&lt;br/&gt;Purchase tickets for the gala here.&lt;br/&gt;&lt;br/&gt;&lt;b&gt;Is there a fee to attend?&lt;/b&gt;:&amp;nbsp;Yes</description>
      <link>https://www.uwyo.edu/uw/calendar/?trumbaEmbed=view%3devent%26eventid%3d196444236</link>
      <category>2026/05/02 (Sat)</category>
    </item>
  </channel>
</rss>
"""

    rows = parse_university_of_wyoming_art_museum_payload(rss_text, start_date="2026-04-01")

    assert [row.title for row in rows] == [
        "Tristan Duke – Artist Talk",
        "Tristan Duke – Lunchtime Conversation",
        "Tristan Duke – Artmaking Workshop",
    ]
    assert rows[0].start_at == datetime(2026, 4, 9, 17, 0)
    assert rows[0].end_at == datetime(2026, 4, 9, 19, 30)
    assert rows[0].activity_type == "talk"
    assert rows[0].is_free is True
    assert rows[1].start_at == datetime(2026, 4, 10, 12, 0)
    assert rows[1].end_at == datetime(2026, 4, 10, 13, 0)
    assert rows[1].activity_type == "talk"
    assert rows[2].start_at == datetime(2026, 4, 11, 13, 0)
    assert rows[2].end_at == datetime(2026, 4, 11, 15, 0)
    assert rows[2].activity_type == "workshop"
    assert rows[2].registration_required is True
