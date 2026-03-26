"""Discover council performance metrics from Oflog.

Oflog (Office for Local Government) publishes standardised performance
data for all English councils covering waste, planning, roads, finance,
and more. Data is on data.gov.uk as CSV downloads.
"""

from __future__ import annotations

import logging

import httpx

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

OFLOG_API = "https://oflog.data.gov.uk/api"

# Key Oflog metric categories and their dataset slugs
# These are standardised across all English councils
OFLOG_DATASETS = [
    ("waste-collection", "Waste collection rates", "climate"),
    ("waste-recycling", "Household recycling rate", "climate"),
    ("planning-speed-major", "Planning: speed of major applications", "housing"),
    ("planning-speed-non-major", "Planning: speed of non-major applications", "housing"),
    ("planning-quality-major", "Planning: quality of major decisions", "housing"),
    ("adult-social-care-outcomes", "Adult social care outcomes", "health"),
    ("childrens-social-care", "Children's social care", "health"),
    ("council-tax-base", "Council tax base", "fiscal_responsibility"),
    ("non-decent-homes", "Non-decent council homes", "housing"),
    ("roads-maintenance", "Road maintenance condition", "transport"),
]


class OflogFinder:
    """Discover Oflog performance metric endpoints for configured councils."""

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None):
        self.city = city
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []

        for dataset_slug, name, topic in OFLOG_DATASETS:
            for entity in self.city.entities:
                if not entity.ons_code:
                    continue

                # Oflog API endpoint for a specific council's metric
                url = (
                    f"{OFLOG_API}/dataset/{dataset_slug}"
                    f"?area-code={entity.ons_code}"
                    f"&format=json"
                )

                results.append(DiscoveredURL(
                    url=url,
                    source=entity.source_key,
                    doc_type="oflog",
                    title=f"Oflog: {name} - {entity.name}",
                ))

        log.info("Discovered %d Oflog metric endpoints", len(results))
        return results

    def close(self):
        self.client.close()
