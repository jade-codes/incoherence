"""URL discovery crawlers for public data sources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass
class DiscoveredURL:
    """A URL found by crawling a listing/archive page."""

    url: str
    source: str  # entity source_key, e.g. "hull_cc", "east_riding"
    doc_type: str  # "press_release", "minutes", "news_report", "stats"
    title: str | None = None
    date_hint: str | None = None  # ISO date if parseable from the listing


class Discoverer(Protocol):
    """Protocol for URL discovery crawlers."""

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]: ...
