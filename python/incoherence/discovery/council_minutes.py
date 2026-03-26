"""Generic council meeting minutes discoverer.

Replaces hull_minutes.py with a config-driven class.
"""

from __future__ import annotations

import logging
import re

import httpx
from bs4 import BeautifulSoup

from ..config import EntityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)


class CouncilMinutesFinder:
    """Crawl a council's committee pages for meeting minutes links."""

    def __init__(self, entity: EntityConfig, rate_limiter: DomainRateLimiter | None = None):
        if not entity.minutes:
            raise ValueError(f"Entity {entity.id} has no minutes config")
        self.entity = entity
        self.minutes = entity.minutes
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "IncoherenceDetector/0.1 (research)"},
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []

        # Start with known committees, then try to discover more from index
        committee_urls = list(self.minutes.committees)
        self.limiter.wait(self.minutes.index_url)
        try:
            resp = self.client.get(self.minutes.index_url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if self.minutes.link_pattern in href and href not in committee_urls:
                    if not href.startswith("http"):
                        href = self.minutes.base_url + href
                    committee_urls.append(href)
        except httpx.HTTPError as e:
            log.warning("Failed to fetch committee index: %s", e)

        for committee_url in committee_urls[:max_pages]:
            log.info("Crawling committee: %s", committee_url)
            self.limiter.wait(committee_url)

            try:
                resp = self.client.get(committee_url)
                resp.raise_for_status()
            except httpx.HTTPError as e:
                log.warning("Failed to fetch %s: %s", committee_url, e)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link["href"]
                text = link.get_text(strip=True).lower()

                is_minutes = (
                    "minutes" in text
                    or "minutes" in href.lower()
                    or href.lower().endswith(".pdf")
                    and ("minute" in href.lower() or "meeting" in href.lower())
                )
                if not is_minutes:
                    continue

                if not href.startswith("http"):
                    href = self.minutes.base_url + href

                title = link.get_text(strip=True) or None

                results.append(
                    DiscoveredURL(
                        url=href,
                        source=self.entity.source_key,
                        doc_type="minutes",
                        title=title,
                    )
                )

        log.info("Discovered %d %s minutes URLs", len(results), self.entity.name)
        return results

    def close(self):
        self.client.close()
