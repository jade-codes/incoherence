"""Discover house price data from the HM Land Registry linked data API.

Provides average house prices by local authority, monthly updates.
Free, no API key needed.
"""

from __future__ import annotations

import logging

import httpx

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

# Land Registry SPARQL endpoint for price paid data
LR_API = "https://landregistry.data.gov.uk/app/ukhpi"


class LandRegistryFinder:
    """Discover Land Registry house price data for configured areas."""

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None):
        self.city = city
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []

        for entity in self.city.entities:
            if not entity.ons_code:
                continue

            # UK House Price Index JSON API
            # Returns monthly average prices, index values, and transaction counts
            url = (
                f"https://landregistry.data.gov.uk/app/ukhpi/download/new.csv"
                f"?from=2020-01-01"
                f"&location=http%3A%2F%2Flandregistry.data.gov.uk%2Fid%2Fregion%2F{entity.ons_code}"
            )

            results.append(DiscoveredURL(
                url=url,
                source=entity.source_key,
                doc_type="land_registry",
                title=f"House Price Index - {entity.name}",
            ))

            # Also get the JSON summary endpoint
            json_url = (
                f"https://landregistry.data.gov.uk/app/ukhpi/region/{entity.ons_code}.json"
            )

            results.append(DiscoveredURL(
                url=json_url,
                source=entity.source_key,
                doc_type="land_registry",
                title=f"House Price Summary - {entity.name}",
            ))

        log.info("Discovered %d Land Registry endpoints", len(results))
        return results

    def close(self):
        self.client.close()
