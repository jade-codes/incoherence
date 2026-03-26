"""Discover metrics from LG Inform / OpenDataCommunities.

Uses the ESD web services API (free, key optional for small queries)
and falls back to the public LG Inform report pages.
"""

from __future__ import annotations

import logging
import os

import httpx

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

ESD_API = "https://webservices.esd.org.uk"

# Key metric IDs from LG Inform covering common topics.
# Format: (metric_id, name, topic_hint)
KEY_METRICS = [
    # Housing
    (850, "Net additional dwellings", "housing"),
    (4103, "Affordable housing completions", "housing"),
    (3112, "Households in temporary accommodation", "housing"),
    (8926, "Homelessness acceptances per 1000 households", "housing"),
    (11, "Dwelling stock: local authority owned", "housing"),
    (9, "Dwelling stock: total", "housing"),
    (2156, "Average house price to earnings ratio", "housing"),
    # Economy
    (126, "Employment rate", "economy"),
    (127, "Unemployment rate", "economy"),
    (8277, "Median gross weekly pay", "economy"),
    (3505, "Business births rate", "economy"),
    # Education
    (14, "GCSE attainment (5+ A*-C or equiv)", "education"),
    (2855, "Attainment 8 score", "education"),
    (8200, "NEETs percentage", "education"),
    # Deprivation / Poverty
    (4558, "IMD average score", "poverty"),
    (3106, "Children in relative low income families", "poverty"),
    (3279, "Fuel poverty percentage", "poverty"),
    # Health
    (3476, "Life expectancy at birth - male", "health"),
    (3477, "Life expectancy at birth - female", "health"),
    (2893, "Healthy life expectancy at birth - male", "health"),
    (3124, "Obesity prevalence - Year 6", "health"),
    (3125, "Smoking prevalence", "health"),
    (36, "Infant mortality rate", "health"),
    # Crime
    (1127, "Total recorded crime per 1000 pop", "health"),
    (10606, "Domestic abuse incidents per 1000 pop", "health"),
    # Environment
    (4636, "CO2 emissions per capita", "climate"),
    (4635, "CO2 emissions total", "climate"),
]


class LgInformFinder:
    """Discover LG Inform metric endpoints, driven by config."""

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None, api_key: str | None = None):
        self.city = city
        self.limiter = rate_limiter or DomainRateLimiter()
        self.api_key = api_key or os.environ.get("LGINFORM_API_KEY")
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []

        for metric_id, name, topic in KEY_METRICS:
            for entity in self.city.entities:
                if not entity.ons_code:
                    continue

                if self.api_key:
                    api_url = f"{ESD_API}/data?metricType={metric_id}&area={entity.ons_code}&period=latest&ApplicationKey={self.api_key}"
                else:
                    api_url = (
                        f"https://lginform.local.gov.uk/reports/lgastandard"
                        f"?mod-metric={metric_id}&mod-area={entity.ons_code}"
                        f"&mod-group={entity.lginform_group}"
                        f"&mod-type=namedComparisonGroup"
                    )

                results.append(
                    DiscoveredURL(
                        url=api_url,
                        source=entity.source_key,
                        doc_type="lginform",
                        title=f"{name} - {entity.name}",
                    )
                )

        log.info("Discovered %d LG Inform metric endpoints", len(results))
        return results

    def close(self):
        self.client.close()
