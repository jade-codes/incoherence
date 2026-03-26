"""Discover MP statements from TheyWorkForYou API.

TheyWorkForYou tracks Hansard and provides a free API for searching
MP speeches, written answers, and debates by topic and constituency.
Requires a free API key from theyworkforyou.com/api.
"""

from __future__ import annotations

import logging
import os

import httpx

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

TWFY_API = "https://www.theyworkforyou.com/api"

# Topics to search for in parliamentary debates and statements
SEARCH_TERMS = [
    ("housing homelessness affordable homes", "housing"),
    ("poverty deprivation food bank", "poverty"),
    ("climate carbon emissions net zero", "climate"),
    ("NHS health hospital mental health", "health"),
    ("school education attainment NEET", "education"),
    ("flooding flood defences tidal surge", "flooding"),
    ("regeneration investment enterprise zone", "regeneration"),
    ("transport bus rail road cycling", "transport"),
]


class TheyWorkForYouFinder:
    """Discover MP statements from TheyWorkForYou, driven by config.

    Requires TWFY_API_KEY environment variable (free from theyworkforyou.com/api).
    """

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None):
        self.city = city
        self.api_key = os.environ.get("TWFY_API_KEY")
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        if not self.api_key:
            log.info("No TWFY_API_KEY set — skipping TheyWorkForYou discovery")
            return []

        results: list[DiscoveredURL] = []

        # Search for mentions of the city/area in parliamentary records
        for entity in self.city.entities:
            # Use area name as search context
            area_name = entity.name.split(" Council")[0].split(" City")[0].strip()

            for search_query, topic in SEARCH_TERMS[:max_pages]:
                full_query = f"{area_name} {search_query}"

                url = (
                    f"{TWFY_API}/getHansard"
                    f"?search={full_query}"
                    f"&output=json"
                    f"&num=20"
                    f"&key={self.api_key}"
                )

                self.limiter.wait(url)
                try:
                    resp = self.client.get(url)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    log.warning("TWFY API error for '%s': %s", full_query, e)
                    continue

                rows = data.get("rows", [])
                for row in rows:
                    gid = row.get("gid", "")
                    if not gid:
                        continue

                    debate_url = f"https://www.theyworkforyou.com/debates/?id={gid}"
                    speaker = row.get("speaker", {}).get("name", "Unknown MP")
                    body_text = row.get("body", "")[:100]

                    results.append(DiscoveredURL(
                        url=debate_url,
                        source=entity.source_key,
                        doc_type="parliamentary",
                        title=f"{speaker}: {body_text}...",
                        date_hint=row.get("hdate"),
                    ))

        # Deduplicate
        seen: set[str] = set()
        unique = []
        for r in results:
            if r.url not in seen:
                seen.add(r.url)
                unique.append(r)

        log.info("Discovered %d TheyWorkForYou debate URLs", len(unique))
        return unique

    def close(self):
        self.client.close()
