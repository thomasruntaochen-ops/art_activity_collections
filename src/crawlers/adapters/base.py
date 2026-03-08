from abc import ABC, abstractmethod

from src.crawlers.pipeline.types import ExtractedActivity


class BaseSourceAdapter(ABC):
    source_name: str

    @abstractmethod
    async def fetch(self) -> list[str]:
        """Fetch raw source payloads (URLs or HTML blobs)."""

    @abstractmethod
    async def parse(self, payload: str) -> list[ExtractedActivity]:
        """Parse payload into standardized extracted activities."""
