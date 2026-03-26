"""Discover environmental data from the Environment Agency APIs.

Covers flood warnings, flood risk areas, water quality,
and pollution incidents — all free, no API key needed.
"""

from __future__ import annotations

import logging

import httpx

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

EA_FLOOD_API = "https://environment.data.gov.uk/flood-monitoring"
EA_ECOLOGY_API = "https://environment.data.gov.uk/ecology"
EA_WATER_API = "https://environment.data.gov.uk/water-quality"


class EnvironmentAgencyFinder:
    """Discover Environment Agency data endpoints for a city's areas."""

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None):
        self.city = city
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []

        # Use police areas for coordinates (they're the best lat/lng we have)
        areas = []
        if self.city.police and self.city.police.areas:
            areas = self.city.police.areas

        # 1. Flood warning areas near each location
        for area in areas:
            entity = self.city.entity(area.entity_id)
            source = entity.source_key if entity else area.entity_id

            # Flood warnings within 20km
            url = (
                f"{EA_FLOOD_API}/id/floods"
                f"?lat={area.lat}&long={area.lng}&dist=20"
            )
            results.append(DiscoveredURL(
                url=url,
                source=source,
                doc_type="environment",
                title=f"Flood warnings - {area.name}",
            ))

            # Flood monitoring stations
            url = (
                f"{EA_FLOOD_API}/id/stations"
                f"?lat={area.lat}&long={area.lng}&dist=15"
            )
            results.append(DiscoveredURL(
                url=url,
                source=source,
                doc_type="environment",
                title=f"Flood monitoring stations - {area.name}",
            ))

        # 2. Water quality sampling points by area
        for entity in self.city.entities:
            if not entity.ons_code:
                continue

            # Water quality — search by area name
            area_name = entity.name.split(" Council")[0].split(" City")[0].strip()
            url = (
                f"{EA_WATER_API}/id/sampling-point"
                f"?search={area_name}&_limit=50"
            )
            results.append(DiscoveredURL(
                url=url,
                source=entity.source_key,
                doc_type="environment",
                title=f"Water quality sampling - {entity.name}",
            ))

        # 3. Rainfall and river level stations (latest readings)
        for area in areas:
            entity = self.city.entity(area.entity_id)
            source = entity.source_key if entity else area.entity_id

            url = (
                f"{EA_FLOOD_API}/id/stations"
                f"?lat={area.lat}&long={area.lng}&dist=10"
                f"&parameter=rainfall&_limit=20"
            )
            results.append(DiscoveredURL(
                url=url,
                source=source,
                doc_type="environment",
                title=f"Rainfall stations - {area.name}",
            ))

        log.info("Discovered %d Environment Agency endpoints", len(results))
        return results

    def close(self):
        self.client.close()
