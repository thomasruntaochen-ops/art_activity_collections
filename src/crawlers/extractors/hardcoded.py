from datetime import datetime

from bs4 import BeautifulSoup

from src.crawlers.extractors.filters import is_irrelevant_item_text
from src.crawlers.pipeline.types import ExtractedActivity


def extract_from_event_page(source_url: str, html: str) -> list[ExtractedActivity]:
    """Simple baseline extractor for static event pages.

    This intentionally stays deterministic and avoids LLM calls.
    """
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled activity")
    if is_irrelevant_item_text(title):
        return []

    result = ExtractedActivity(
        source_url=source_url,
        title=title,
        description=None,
        venue_name=None,
        location_text=None,
        city=None,
        state=None,
        activity_type="drop-in" if "drop in" in html.lower() else None,
        age_min=None,
        age_max=None,
        drop_in=True if "drop in" in html.lower() else None,
        registration_required=False if "no registration" in html.lower() else None,
        start_at=datetime.utcnow(),
        end_at=None,
        timezone="America/Los_Angeles",
        free_verification_status="inferred",
    )
    return [result]
