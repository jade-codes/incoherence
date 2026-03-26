"""Discover health indicators from the OHID Fingertips API.

Fingertips provides Public Health England data with built-in comparisons
to England averages and trend direction — ideal for outcome extraction.
"""

from __future__ import annotations

import logging

import httpx

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

FINGERTIPS_API = "https://fingertips.phe.org.uk/api"
AREA_TYPE_UA = 401  # Unitary Authority / LA District (post-2023)

# Specific high-value indicators
# Format: (indicator_id, name, topic_hint)
KEY_INDICATORS = [
    (90366, "Life expectancy at birth (male)", "health"),
    (90366, "Life expectancy at birth (female)", "health"),
    (92901, "Healthy life expectancy at birth", "health"),
    (93553, "Inequality in life expectancy at birth", "health"),
    (90631, "Under 75 mortality rate - all causes", "health"),
    (93505, "Suicide rate", "health"),
    (92443, "Smoking prevalence in adults", "health"),
    (93014, "Hospital admissions for alcohol", "health"),
    (90810, "Childhood obesity (Year 6)", "health"),
    (10101, "Child poverty (under 16s)", "poverty"),
    (93094, "Fuel poverty", "poverty"),
    (90356, "Deprivation score (IMD)", "poverty"),
    (92313, "Employment rate", "economy"),
    (90641, "Homelessness - households owed a prevention or relief duty", "housing"),
    (93757, "Educational attainment KS4", "education"),
    (93203, "Domestic abuse related incidents and crimes", "health"),
    (91872, "Fraction of mortality attributable to particulate air pollution", "climate"),
    (93861, "Emergency hospital admissions for falls", "health"),
]


class FingertipsFinder:
    """Discover Fingertips API endpoints for health data, driven by config."""

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

        for indicator_id, name, topic in KEY_INDICATORS:
            for entity in self.city.entities:
                if not entity.ons_code:
                    continue

                unique_url = (
                    f"{FINGERTIPS_API}/all_data/csv/by_indicator_id"
                    f"?indicator_ids={indicator_id}"
                    f"&child_area_code={entity.ons_code}"
                )

                results.append(
                    DiscoveredURL(
                        url=unique_url,
                        source=entity.source_key,
                        doc_type="fingertips",
                        title=f"{name} - {entity.name}",
                    )
                )

        log.info("Discovered %d Fingertips indicator endpoints", len(results))
        return results

    def close(self):
        self.client.close()
