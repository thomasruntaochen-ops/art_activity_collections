"""SDMA answers 403 for rate limiting on its listing and persistently on details.

Retrying the persistent ones burned the venue's whole 900s timeout on Aug 22-25
and kept hammering a host that was plainly declining, so detail fetches now fail
fast while the listing keeps its retry behaviour.
"""

import asyncio

import httpx
import pytest

from src.crawlers.adapters import sdmart


def _always_403(monkeypatch) -> list[str]:
    """Make every request 403 and record how many were attempted."""
    attempts: list[str] = []

    async def fake_get(self, url, *args, **kwargs):
        attempts.append(str(url))
        return httpx.Response(403, request=httpx.Request("GET", url), text="Forbidden")

    real_sleep = asyncio.sleep

    async def no_wait(_seconds, *args, **kwargs):
        # Skip the backoff waits; keep a real yield so the loop still schedules.
        await real_sleep(0)

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)
    monkeypatch.setattr(sdmart.asyncio, "sleep", no_wait)
    return attempts


def test_listing_403_is_retried(monkeypatch) -> None:
    attempts = _always_403(monkeypatch)

    # The final attempt calls raise_for_status(), so the 403 surfaces as-is.
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(sdmart.fetch_sdmart_events_page("https://www.sdmart.org/events/"))

    assert len(attempts) == 5


def test_detail_403_is_not_retried(monkeypatch) -> None:
    attempts = _always_403(monkeypatch)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(
            sdmart.fetch_sdmart_events_page(
                "https://www.sdmart.org/event/family-art-adventures/", retry_on_403=False
            )
        )

    assert len(attempts) == 1


def test_blocked_details_do_not_stop_the_venue(monkeypatch) -> None:
    """Rows come from the listing's JSON-LD; details only enrich them."""
    _always_403(monkeypatch)

    list_html = """
    <html><body>
    <a href="https://www.sdmart.org/event/family-art-adventures/">Family Art Adventures</a>
    </body></html>
    """
    details = asyncio.run(
        sdmart.load_sdmart_details(list_html, list_url="https://www.sdmart.org/events/")
    )

    assert details == {}
