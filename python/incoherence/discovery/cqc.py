"""Discover care quality data from the CQC API.

The Care Quality Commission rates care homes, hospitals, GP practices,
and social care services. Requires a free subscription key from
https://api.service.cqc.org.uk/ (set CQC_API_KEY env var).
"""

from __future__ import annotations

import logging
import os

import httpx

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

CQC_API = "https://api.service.cqc.org.uk/public/v1"


class CqcFinder:
    """Discover CQC inspection data for care providers in a city."""

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None):
        self.city = city
        self.api_key = os.environ.get("CQC_API_KEY")
        self.limiter = rate_limiter or DomainRateLimiter()
        headers = {"User-Agent": "IncoherenceDetector/0.1 (research)"}
        if self.api_key:
            headers["Ocp-Apim-Subscription-Key"] = self.api_key
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers=headers,
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        if not self.api_key:
            log.info("No CQC_API_KEY set — skipping CQC discovery")
            return []

        results: list[DiscoveredURL] = []

        for entity in self.city.entities:
            if not entity.ons_code:
                continue

            # Search area name — strip "Council" etc for better matching
            area_name = entity.name.split(" Council")[0].split(" City")[0].strip()

            # CQC locations endpoint filtered by local authority
            # The API supports filtering by localAuthority parameter
            for page in range(1, max_pages + 1):
                url = (
                    f"{CQC_API}/locations"
                    f"?localAuthority={area_name}"
                    f"&page={page}"
                    f"&perPage=50"
                )

                self.limiter.wait(url)
                try:
                    resp = self.client.get(url)
                    if resp.status_code == 404 or resp.status_code >= 500:
                        break
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    log.warning("CQC API error for %s: %s", area_name, e)
                    break

                locations = data.get("locations", [])
                if not locations:
                    break

                for loc in locations:
                    loc_id = loc.get("locationId")
                    if not loc_id:
                        continue

                    loc_url = f"{CQC_API}/locations/{loc_id}"
                    loc_name = loc.get("locationName", "Unknown")

                    results.append(DiscoveredURL(
                        url=loc_url,
                        source=entity.source_key,
                        doc_type="cqc",
                        title=f"CQC: {loc_name}",
                    ))

                # Stop if we got fewer than a full page
                if len(locations) < 50:
                    break

        log.info("Discovered %d CQC location endpoints", len(results))
        return results

    def close(self):
        self.client.close()
