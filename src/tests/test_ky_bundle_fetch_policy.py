"""One transient connection failure must not take the whole Kentucky bundle down.

fetch_html has always retried; fetch_json did not, so on 2026-08-31 a single TLS
blip against artcenterky.org aborted every KY venue in the run.
"""

import asyncio

import httpx
import pytest

from src.crawlers.adapters import ky_bundle


def _no_backoff(monkeypatch) -> None:
    real_sleep = asyncio.sleep

    async def no_wait(_seconds, *args, **kwargs):
        await real_sleep(0)

    monkeypatch.setattr(ky_bundle.asyncio, "sleep", no_wait)


def test_transient_connect_error_is_retried(monkeypatch) -> None:
    _no_backoff(monkeypatch)
    calls = {"n": 0}

    async def fail_once(self, url, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("handshake failed", request=httpx.Request("GET", url))
        return httpx.Response(200, request=httpx.Request("GET", url), json={"ok": True})

    monkeypatch.setattr(httpx.AsyncClient, "get", fail_once)

    assert asyncio.run(ky_bundle.fetch_json("https://artcenterky.org/wp-json/wp/v2/pages")) == {"ok": True}
    assert calls["n"] == 2


def test_persistent_connect_error_still_raises(monkeypatch) -> None:
    _no_backoff(monkeypatch)
    attempts: list[str] = []

    async def always_fail(self, url, *args, **kwargs):
        attempts.append(str(url))
        raise httpx.ConnectError("handshake failed", request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "get", always_fail)

    with pytest.raises(RuntimeError, match="Unable to fetch JSON"):
        asyncio.run(ky_bundle.fetch_json("https://artcenterky.org/wp-json/wp/v2/pages"))

    assert len(attempts) == 5


def test_server_error_is_retried(monkeypatch) -> None:
    _no_backoff(monkeypatch)
    attempts: list[str] = []

    async def always_503(self, url, *args, **kwargs):
        attempts.append(str(url))
        return httpx.Response(503, request=httpx.Request("GET", url), text="unavailable")

    monkeypatch.setattr(httpx.AsyncClient, "get", always_503)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(ky_bundle.fetch_json("https://artcenterky.org/wp-json/wp/v2/pages"))

    assert len(attempts) == 5
