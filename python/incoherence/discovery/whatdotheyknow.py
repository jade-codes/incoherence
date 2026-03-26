"""Discover FOI requests from WhatDoTheyKnow for configured councils."""

from __future__ import annotations

import logging
import re

import httpx
from bs4 import BeautifulSoup

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)

BASE = "https://www.whatdotheyknow.com"


class WdtkFinder:
    """Crawl WhatDoTheyKnow for FOI requests, driven by config."""

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

        # Build authority list from config
        authorities = []
        for entity in self.city.entities:
            if entity.wdtk:
                authorities.append((entity.wdtk.slug, entity.source_key))

        for authority_slug, source in authorities:
            page = 1
            while page <= max_pages:
                url = f"{BASE}/body/{authority_slug}?page={page}"
                log.info("Crawling WDTK: %s", url)
                self.limiter.wait(url)

                try:
                    resp = self.client.get(url)
                    resp.raise_for_status()
                except httpx.HTTPError as e:
                    log.warning("Failed to fetch %s: %s", url, e)
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                request_links = soup.find_all(
                    "a", href=re.compile(r"^/request/")
                )

                if not request_links:
                    log.info("No more requests on page %d for %s", page, authority_slug)
                    break

                found = 0
                seen_hrefs: set[str] = set()
                for link in request_links:
                    href = link.get("href", "")
                    if href in seen_hrefs:
                        continue
                    seen_hrefs.add(href)

                    full_url = BASE + href
                    title = link.get_text(strip=True)

                    results.append(
                        DiscoveredURL(
                            url=full_url,
                            source=source,
                            doc_type="foi",
                            title=title,
                        )
                    )
                    found += 1

                if found == 0:
                    break

                next_link = soup.find("a", class_=re.compile(r"next", re.I)) or soup.find(
                    "a", string=re.compile(r"Next|›|»", re.I)
                )
                if not next_link:
                    break

                page += 1

        log.info("Discovered %d WDTK FOI request URLs", len(results))
        return results

    def close(self):
        self.client.close()
