"""Generic council news/press release discoverer.

Replaces the old hull_press.py and eastriding_news.py with a single
config-driven class that works for any council news archive.
"""

from __future__ import annotations

import logging
import re

import httpx
from bs4 import BeautifulSoup

from ..config import EntityConfig
from ..ratelimit import DomainRateLimiter
from . import DiscoveredURL

log = logging.getLogger(__name__)


class CouncilNewsFinder:
    """Crawl a council's news/press release listing pages."""

    def __init__(self, entity: EntityConfig, rate_limiter: DomainRateLimiter | None = None):
        if not entity.news:
            raise ValueError(f"Entity {entity.id} has no news config")
        self.entity = entity
        self.news = entity.news
        self.base_url = self.news.base_url or self.news.url
        self.limiter = rate_limiter or DomainRateLimiter()
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "IncoherenceDetector/0.1 (research)"},
        )

    def discover(self, max_pages: int = 10) -> list[DiscoveredURL]:
        results: list[DiscoveredURL] = []
        page = 1

        while page <= max_pages:
            url = self._page_url(page)
            log.info("Crawling %s", url)
            self.limiter.wait(url)

            try:
                resp = self.client.get(url)
                resp.raise_for_status()
            except httpx.HTTPError as e:
                log.warning("Failed to fetch %s: %s", url, e)
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            articles = self._find_articles(soup)

            if not articles:
                log.info("No more articles found on page %d", page)
                break

            found_any = False
            for article in articles:
                link = article.find("a", href=True) if article.name != "a" else article
                if not link or not link.get("href"):
                    continue

                href = link["href"]
                if not href.startswith("http"):
                    href = self.base_url + href

                # Skip non-article links
                if href.rstrip("/") == self.news.url.rstrip("/"):
                    continue

                title_el = article.find(["h2", "h3", "h4"]) or link
                title = title_el.get_text(strip=True) if title_el else None

                date_hint = self._extract_date(href)

                results.append(
                    DiscoveredURL(
                        url=href,
                        source=self.entity.source_key,
                        doc_type="press_release",
                        title=title,
                        date_hint=date_hint,
                    )
                )
                found_any = True

            if not found_any:
                break

            # Check for next page
            next_link = soup.find("a", class_=re.compile(r"next", re.I)) or soup.find(
                "a", string=re.compile(r"next|older|→|»", re.I)
            )
            if not next_link:
                break

            page += 1

        log.info("Discovered %d %s press release URLs", len(results), self.entity.name)
        return results

    def _page_url(self, page: int) -> str:
        """Build the URL for a given page number."""
        base = self.news.url.rstrip("/")
        if page == 1:
            return base
        # Try common pagination patterns
        if "?" in base:
            return f"{base}&page={page}"
        # WordPress-style /page/N/ or ?page=N
        if any(d in base for d in ["wordpress", "news.hull", ".gov.uk/news"]):
            pass
        # Default: try both patterns, prefer the one that matches the site
        if self.news.article_pattern:
            # Sites like eastriding use ?page=N
            return f"{base}/?page={page}"
        return f"{base}/page/{page}/"

    def _find_articles(self, soup: BeautifulSoup) -> list:
        """Find article elements in the page."""
        # Try <article> tags first
        articles = soup.find_all("article") or soup.find_all(
            "div", class_=re.compile(r"post|entry|news-item|article", re.I)
        )

        if not articles:
            main = soup.find("main") or soup.find("body")
            if main:
                # Try article pattern from config
                if self.news.article_pattern:
                    articles = main.find_all(
                        "a", href=re.compile(re.escape(self.news.article_pattern), re.I)
                    )
                # Try date pattern in URLs
                elif self.news.date_pattern:
                    articles = main.find_all(
                        "a", href=re.compile(self.news.date_pattern)
                    )

        return articles

    def _extract_date(self, href: str) -> str | None:
        """Try to extract a date from the URL."""
        if self.news.date_pattern:
            match = re.search(self.news.date_pattern, href)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    dd, mm, yyyy = groups
                    return f"{yyyy}-{mm}-{dd}"
        # Generic: try /DD/MM/YYYY/ pattern
        match = re.search(r"/(\d{2})/(\d{2})/(\d{4})/", href)
        if match:
            dd, mm, yyyy = match.groups()
            return f"{yyyy}-{mm}-{dd}"
        return None

    def close(self):
        self.client.close()
