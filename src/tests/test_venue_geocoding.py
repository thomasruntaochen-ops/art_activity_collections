from src.models.activity import Venue
from src.services.venue_geocoding import build_geocode_queries


def test_build_geocode_queries_prefers_specific_variants_first() -> None:
    venue = Venue(
        name="Museum of Example Art",
        address="123 Main Street",
        city="Boston",
        state="MA",
    )

    queries = build_geocode_queries(venue)

    assert queries[0] == "Museum of Example Art, 123 Main Street, Boston, MA, USA"
    assert queries[1] == "123 Main Street, Boston, MA, USA"
    assert queries[2] == "Museum of Example Art, Boston, MA, USA"


def test_build_geocode_queries_deduplicates_duplicate_parts() -> None:
    venue = Venue(
        name="Boston Museum",
        address="Boston Museum",
        city="Boston",
        state="MA",
    )

    queries = build_geocode_queries(venue)

    assert queries[0] == "Boston Museum, Boston, MA, USA"
    assert len(queries) == len(set(queries))
