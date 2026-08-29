import asyncio

from src.crawlers.adapters.fetch_utils import fetch_page_or_none, gather_pages


def test_gather_pages_keeps_good_pages_when_one_url_fails() -> None:
    async def fetch(url: str) -> str:
        if url == "b":
            raise RuntimeError("523 origin unreachable")
        return f"html:{url}"

    pages = asyncio.run(gather_pages(["a", "b", "c"], fetch, label="test"))

    assert pages == {"a": "html:a", "c": "html:c"}


def test_gather_pages_returns_empty_when_every_url_fails() -> None:
    async def fetch(url: str) -> str:
        raise RuntimeError("boom")

    assert asyncio.run(gather_pages(["a", "b"], fetch, label="test")) == {}


def test_gather_pages_handles_no_urls() -> None:
    async def fetch(url: str) -> str:  # pragma: no cover - never called
        raise AssertionError("should not be called")

    assert asyncio.run(gather_pages([], fetch, label="test")) == {}


def test_fetch_page_or_none_returns_value_and_swallows_failure() -> None:
    async def ok() -> str:
        return "html"

    async def boom() -> str:
        raise RuntimeError("read timeout")

    assert asyncio.run(fetch_page_or_none(ok, url="u", label="test")) == "html"
    assert asyncio.run(fetch_page_or_none(boom, url="u", label="test")) is None
