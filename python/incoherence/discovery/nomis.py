"""Discover labour market datasets from the NOMIS API."""

from __future__ import annotations

import logging

import httpx

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

NOMIS_API = "https://www.nomisweb.co.uk/api/v01"

# Key dataset IDs on NOMIS
DATASETS = [
    {
        "id": "NM_17_5",
        "name": "Annual Population Survey - employment and unemployment",
        "topic": "economy",
    },
    {
        "id": "NM_30_1",
        "name": "Jobseekers Allowance claimants",
        "topic": "economy",
    },
    {
        "id": "NM_99_1",
        "name": "DWP benefit claimants",
        "topic": "poverty",
    },
    {
        "id": "NM_54_1",
        "name": "Annual Survey of Hours and Earnings - median earnings",
        "topic": "economy",
    },
]


class NomisFinder:
    """Discover NOMIS API endpoints for labour market data, driven by config."""

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None):
        self.city = city
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []

        for ds in DATASETS[:max_pages]:
            for entity in self.city.entities:
                if not entity.ons_code:
                    continue

                api_url = (
                    f"{NOMIS_API}/dataset/{ds['id']}.data.json"
                    f"?geography={entity.ons_code}"
                    f"&time=latest"
                    f"&select=date_name,geography_name,obs_value,measures_name"
                )

                self.limiter.wait(api_url)
                try:
                    resp = self.client.head(api_url)
                    if resp.status_code >= 400:
                        log.warning("NOMIS endpoint unavailable: %s (%d)", ds["id"], resp.status_code)
                        continue
                except httpx.HTTPError as e:
                    log.warning("Failed to check NOMIS endpoint %s: %s", ds["id"], e)
                    continue

                results.append(
                    DiscoveredURL(
                        url=api_url,
                        source=entity.source_key,
                        doc_type="stats",
                        title=f"{ds['name']} - {entity.name}",
                    )
                )

        log.info("Discovered %d NOMIS API endpoints", len(results))
        return results

    def close(self):
        self.client.close()
