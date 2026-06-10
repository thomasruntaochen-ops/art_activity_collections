from src.crawlers.adapters.nelson_atkins import parse_nelson_atkins_payload


def _performance(
    *,
    title: str,
    status: str = "",
    url: str | None = None,
) -> dict:
    slug = title.lower().replace(" ", "-")
    return {
        "isPerformanceVisible": True,
        "actionUrl": url or f"https://cart.nelson-atkins.org/test/{slug}",
        "performanceSortTitle": title,
        "iso8601DateString": "2027-01-10T10:00:00-06:00",
        "performanceStatusMessage": status,
        "displayTime": "10:00 AM",
    }


def _production(title: str, performances: list[dict]) -> dict:
    return {
        "productionTitle": title,
        "description": "",
        "performances": performances,
    }


def test_nelson_atkins_infers_audience_from_production_group() -> None:
    rows = parse_nelson_atkins_payload(
        {
            "productions": [
                _production("Free Weekend Fun", [_performance(title="Free Weekend Fun: Mini to Monumental")]),
                _production("FY27 Summer Youth Studio Class", [_performance(title="Create Fantasy Friends")]),
                _production("Teen Programs", [_performance(title="Teen Drop-In Art Making")]),
                _production("Library Programs", [_performance(title="Hidden Treasures")]),
            ]
        }
    )

    audience_by_title = {row.title: row.audience_segment for row in rows}

    assert audience_by_title["Free Weekend Fun: Mini to Monumental"] == "all_ages"
    assert audience_by_title["Create Fantasy Friends"] == "kids"
    assert audience_by_title["Teen Drop-In Art Making"] == "teens"
    assert audience_by_title["Hidden Treasures"] == "adults"


def test_nelson_atkins_excludes_sold_out_rows() -> None:
    rows = parse_nelson_atkins_payload(
        {
            "productions": [
                _production(
                    "FY27 Summer Youth Studio Class",
                    [
                        _performance(title="Create Fantasy Friends", status="Sold Out!"),
                        _performance(
                            title="Create Fantasy Friends",
                            url="https://cart.nelson-atkins.org/test/create-fantasy-friends-open",
                        ),
                    ],
                )
            ]
        }
    )

    assert [row.title for row in rows] == ["Create Fantasy Friends"]
