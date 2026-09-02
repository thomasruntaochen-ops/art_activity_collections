"""brucemuseum.org sits behind a Cloudflare challenge that is scored per request.

On 2026-08-31 a single challenged response aborted the venue with a bare 403
traceback, even though the same client succeeds on retry. Challenges are treated
as transient denials -- backed off and retried like a 429 -- and a run that is
challenged every time fails with an explanation rather than an HTTPStatusError.
The challenge itself is never solved or worked around.
"""

import asyncio

import httpx
import pytest

from src.crawlers.adapters import bruce


CHALLENGE_BODY = (
    "<html><head><title>Just a moment...</title></head>"
    "<body><script src='https://challenges.cloudflare.com/turnstile/v0/api.js'></script></body></html>"
)


def _no_backoff(monkeypatch) -> None:
    real_sleep = asyncio.sleep

    async def no_wait(_seconds, *args, **kwargs):
        await real_sleep(0)

    monkeypatch.setattr(bruce.asyncio, "sleep", no_wait)


def test_challenge_is_detected_from_header_and_body() -> None:
    request = httpx.Request("GET", bruce.BRUCE_EVENTS_URL)

    header_flagged = httpx.Response(403, request=request, headers={"cf-mitigated": "challenge"}, text="")
    body_flagged = httpx.Response(403, request=request, text=CHALLENGE_BODY)
    plain_forbidden = httpx.Response(403, request=request, text="Forbidden")

    assert bruce._is_bot_challenge(header_flagged) is True
    assert bruce._is_bot_challenge(body_flagged) is True
    assert bruce._is_bot_challenge(plain_forbidden) is False


def test_challenge_is_retried_then_reported(monkeypatch) -> None:
    _no_backoff(monkeypatch)
    attempts: list[str] = []

    async def always_challenged(self, url, *args, **kwargs):
        attempts.append(str(url))
        return httpx.Response(
            403,
            request=httpx.Request("GET", url),
            headers={"cf-mitigated": "challenge"},
            text=CHALLENGE_BODY,
        )

    monkeypatch.setattr(httpx.AsyncClient, "get", always_challenged)

    with pytest.raises(RuntimeError, match="Cloudflare bot challenge"):
        asyncio.run(bruce.fetch_bruce_page(bruce.BRUCE_EVENTS_URL))

    assert len(attempts) == 5


def test_challenge_then_success_returns_the_page(monkeypatch) -> None:
    _no_backoff(monkeypatch)
    calls = {"n": 0}

    async def challenged_once(self, url, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                403,
                request=httpx.Request("GET", url),
                headers={"cf-mitigated": "challenge"},
                text=CHALLENGE_BODY,
            )
        return httpx.Response(200, request=httpx.Request("GET", url), text="<html>events</html>")

    monkeypatch.setattr(httpx.AsyncClient, "get", challenged_once)

    assert asyncio.run(bruce.fetch_bruce_page(bruce.BRUCE_EVENTS_URL)) == "<html>events</html>"
    assert calls["n"] == 2
