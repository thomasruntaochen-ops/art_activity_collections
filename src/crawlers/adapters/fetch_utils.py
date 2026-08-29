"""Fault-tolerant helpers for adapters that fetch more than one page.

Most adapters fan out from a listing page to per-event detail pages. Without
these helpers a single unreachable page — a 5xx from the museum's CDN, a read
timeout — propagates out of the adapter and discards every page that *did*
fetch, losing the whole venue for that run instead of one event.
"""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

T = TypeVar("T")


async def gather_pages(
    urls: Sequence[str],
    fetch: Callable[[str], Awaitable[T]],
    *,
    label: str,
) -> dict[str, T]:
    """Fetch `urls` concurrently, dropping and logging the ones that fail.

    Returns a url -> payload mapping containing only the successful fetches, so
    callers must not assume it is the same length as `urls`.
    """
    results = await asyncio.gather(
        *(fetch(url) for url in urls), return_exceptions=True
    )
    pages: dict[str, T] = {}
    for url, result in zip(urls, results, strict=True):
        if isinstance(result, BaseException):
            print(f"[{label}] skipped unreachable page {url}: {result}")
            continue
        pages[url] = result
    return pages


async def fetch_page_or_none(
    fetch: Callable[[], Awaitable[T]],
    *,
    url: str,
    label: str,
) -> T | None:
    """Await a single page fetch, returning None instead of raising."""
    try:
        return await fetch()
    except Exception as exc:  # noqa: BLE001 - a bad page must not kill the venue
        print(f"[{label}] skipped unreachable page {url}: {exc}")
        return None
