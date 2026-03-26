"""Discover crime data endpoints from the Police UK API."""

from __future__ import annotations

import logging

import httpx

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

POLICE_API = "https://data.police.uk/api"


class PoliceFinder:
    """Discover Police UK API endpoints for crime data, driven by config."""

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None):
        self.city = city
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []

        if not self.city.police or not self.city.police.areas:
            log.info("No police config — skipping")
            return results

        # Get available dates
        self.limiter.wait(f"{POLICE_API}/crimes-street-dates")
        try:
            resp = self.client.get(f"{POLICE_API}/crimes-street-dates")
            resp.raise_for_status()
            dates = resp.json()
        except Exception as e:
            log.warning("Failed to get crime dates: %s", e)
            return results

        recent_dates = [d["date"] for d in dates[:max_pages]]

        for date_str in recent_dates:
            for area in self.city.police.areas:
                api_url = (
                    f"{POLICE_API}/crimes-street/all-crime"
                    f"?lat={area.lat}&lng={area.lng}&date={date_str}"
                )
                # Map area entity_id back to source_key
                entity = self.city.entity(area.entity_id)
                source = entity.source_key if entity else area.entity_id

                results.append(
                    DiscoveredURL(
                        url=api_url,
                        source=source,
                        doc_type="police",
                        title=f"Crime data - {area.name} - {date_str}",
                        date_hint=f"{date_str}-01",
                    )
                )

        log.info("Discovered %d Police UK API endpoints", len(results))
        return results

    def close(self):
        self.client.close()
