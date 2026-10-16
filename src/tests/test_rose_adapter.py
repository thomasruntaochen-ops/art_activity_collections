from src.crawlers.adapters.rose import parse_rose_payload

PROGRAMS = "https://www.brandeis.edu/rose/programs/2099/"


def test_rose_keeps_gallery_program_held_in_the_exhibition() -> None:
    payload = {
        "entries": [
            {
                "title": "Making Time: Drawing Between Us",
                "source_url": f"{PROGRAMS}making-time.html",
                "date_text": "November 14, 2099",
                "summary": "A drawing session held in the exhibition galleries.",
            },
            {
                "title": "Film Screening: One Sings, the Other Doesn't",
                "source_url": f"{PROGRAMS}film-screening.html",
                "date_text": "October 18, 2099",
                "summary": "See the film that inspired the exhibition.",
            },
        ],
        "detail_pages": {},
    }

    rows = parse_rose_payload(payload)

    assert [row.title for row in rows] == ["Making Time: Drawing Between Us"]
