"""Discover MHCLG housing statistics from gov.uk.

These are national datasets — the same for any English city.
Individual entity filtering happens at extraction time.
"""

from __future__ import annotations

import logging
import re

import httpx
from bs4 import BeautifulSoup

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

GOV_UK = "https://www.gov.uk"

HOUSING_PAGES = [
    (
        "/government/statistical-data-sets/live-tables-on-affordable-housing-supply",
        "Affordable housing supply",
        "housing",
    ),
    (
        "/government/statistical-data-sets/live-tables-on-dwelling-stock-including-vacants",
        "Dwelling stock",
        "housing",
    ),
    (
        "/government/statistical-data-sets/live-tables-on-social-housing-sales",
        "Social housing sales",
        "housing",
    ),
    (
        "/government/statistical-data-sets/live-tables-on-house-building-new-build-dwellings",
        "House building",
        "housing",
    ),
    (
        "/government/statistical-data-sets/live-tables-on-homelessness",
        "Homelessness",
        "housing",
    ),
    (
        "/government/statistical-data-sets/live-tables-on-rents-lettings-and-tenancies",
        "Rents, lettings and tenancies",
        "housing",
    ),
]


class HousingStatsFinder:
    """Discover MHCLG housing statistics download links from gov.uk."""

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None):
        self.city = city
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "IncoherenceDetector/0.1 (research)"},
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []

        # Use the first entity's source_key; housing data covers all entities
        default_source = self.city.entities[0].source_key if self.city.entities else "unknown"

        for path, title, topic in HOUSING_PAGES[:max_pages]:
            url = GOV_UK + path
            log.info("Crawling gov.uk housing page: %s", url)
            self.limiter.wait(url)

            try:
                resp = self.client.get(url)
                resp.raise_for_status()
            except httpx.HTTPError as e:
                log.warning("Failed to fetch %s: %s", url, e)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=re.compile(
                r"\.(xlsx?|csv|ods)$", re.I
            )):
                href = link.get("href", "")
                if not href.startswith("http"):
                    href = GOV_UK + href

                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                if any(kw in text_lower for kw in [
                    "local authority", "district", "la ", "table 1",
                    "table 2", "table 3", "table 6",
                    "additional", "stock", "affordable",
                    "homelessness", "lettings",
                ]):
                    results.append(
                        DiscoveredURL(
                            url=href,
                            source=default_source,
                            doc_type="housing_stats",
                            title=f"{title}: {link_text}",
                        )
                    )

        log.info("Discovered %d MHCLG housing download URLs", len(results))
        return results

    def close(self):
        self.client.close()
