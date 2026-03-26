"""Discover planning application data from the PlanIt API.

PlanIt (planit.org.uk) aggregates planning applications from most
English councils into a single searchable API. Free, no key needed.
"""

from __future__ import annotations

import logging

import httpx

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

PLANIT_API = "https://www.planit.org.uk/api/applics/json"

# Application types of interest for incoherence detection
APP_TYPES = [
    "Large Scale Major Dwellings",
    "Small Scale Major Dwellings",
    "Large Scale Major",
    "Minor Dwellings",
]


class PlanningFinder:
    """Discover planning application data for configured areas."""

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None):
        self.city = city
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []

        # PlanIt supports searching by authority name or by lat/lng
        # Use police areas for coordinates, or search by authority name
        if self.city.police and self.city.police.areas:
            for area in self.city.police.areas:
                entity = self.city.entity(area.entity_id)
                source = entity.source_key if entity else area.entity_id

                # Recent planning applications within 5km
                url = (
                    f"{PLANIT_API}"
                    f"?lat={area.lat}&lng={area.lng}&radius=5000"
                    f"&recent=3months"
                    f"&pg_sz=100&page=1"
                    f"&sort=-decided_date"
                )

                results.append(DiscoveredURL(
                    url=url,
                    source=source,
                    doc_type="planning",
                    title=f"Planning applications - {area.name}",
                ))

                # Also get major housing applications specifically
                for app_type in APP_TYPES[:max_pages]:
                    url = (
                        f"{PLANIT_API}"
                        f"?lat={area.lat}&lng={area.lng}&radius=5000"
                        f"&app_type={app_type}"
                        f"&recent=12months"
                        f"&pg_sz=50&page=1"
                        f"&sort=-decided_date"
                    )
                    results.append(DiscoveredURL(
                        url=url,
                        source=source,
                        doc_type="planning",
                        title=f"Planning: {app_type} - {area.name}",
                    ))
        else:
            # Fallback: search by authority name
            for entity in self.city.entities:
                area_name = entity.name.split(" Council")[0].split(" City")[0].strip()
                url = (
                    f"{PLANIT_API}"
                    f"?auth={area_name}"
                    f"&recent=3months"
                    f"&pg_sz=100&page=1"
                    f"&sort=-decided_date"
                )
                results.append(DiscoveredURL(
                    url=url,
                    source=entity.source_key,
                    doc_type="planning",
                    title=f"Planning applications - {entity.name}",
                ))

        log.info("Discovered %d PlanIt planning endpoints", len(results))
        return results

    def close(self):
        self.client.close()
