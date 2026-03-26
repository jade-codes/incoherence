"""Discover statistical pages from JSNA (Joint Strategic Needs Assessment) sites."""

from __future__ import annotations

import logging
import re
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from ..config import CityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)


class JsnaFinder:
    """Crawl JSNA sites for statistical data pages, driven by config."""

    def __init__(self, city: CityConfig, rate_limiter: DomainRateLimiter | None = None):
        self.city = city
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "IncoherenceDetector/0.1 (research)"},
        )

    def _crawl_section(
        self, section_url: str, source: str, max_depth: int = 2
    ) -> list[DiscoveredURL]:
        """Crawl a JSNA section page and follow topic links."""
        results: list[DiscoveredURL] = []
        visited: set[str] = set()
        to_visit = [(section_url, 0)]

        while to_visit:
            url, depth = to_visit.pop(0)
            url = url.split("#")[0]
            if url in visited or depth > max_depth:
                continue
            visited.add(url)

            self.limiter.wait(url)
            try:
                resp = self.client.get(url)
                resp.raise_for_status()
            except httpx.HTTPError as e:
                log.warning("Failed to fetch %s: %s", url, e)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            main = soup.find("main") or soup.find("article") or soup.find("body")
            if not main:
                continue

            text = main.get_text()
            has_stats = bool(
                re.search(r"\d+\.?\d*\s*%", text)
                or re.search(r"(?:rate|average|median|index|score)\b", text, re.I)
            )
            if has_stats and url != section_url:
                title = soup.find("h1")
                results.append(
                    DiscoveredURL(
                        url=url,
                        source=source,
                        doc_type="stats",
                        title=title.get_text(strip=True) if title else None,
                    )
                )

            if depth < max_depth:
                base_domain = re.match(r"https?://[^/]+", section_url)
                if base_domain:
                    for link in main.find_all("a", href=True):
                        href = urljoin(url, link["href"])
                        href = href.split("#")[0]
                        if not href or not href.startswith(base_domain.group()):
                            continue
                        if any(skip in href for skip in [
                            "/glossary", "/wp-content/", "/tools-and-resources/",
                            "/latest/", ".xlsx", ".pdf", ".csv",
                        ]):
                            continue
                        if href not in visited:
                            to_visit.append((href, depth + 1))

        return results

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []

        for entity in self.city.entities:
            if not entity.jsna or not entity.jsna.sections:
                continue
            for section in entity.jsna.sections[:max_pages]:
                log.info("Crawling %s JSNA section: %s", entity.name, section)
                results.extend(self._crawl_section(section, source=entity.source_key))

        # Deduplicate by URL
        seen: set[str] = set()
        unique: list[DiscoveredURL] = []
        for r in results:
            if r.url not in seen:
                seen.add(r.url)
                unique.append(r)

        log.info("Discovered %d JSNA statistical page URLs", len(unique))
        return unique

    def close(self):
        self.client.close()
