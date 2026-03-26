"""Scraper for local news sources (Hull Daily Mail / Hull Live)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from .council import ScrapedDocument


class NewsScraper:
    """Scrapes local news articles related to Hull council decisions and outcomes."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "HullIncoherenceDetector/0.1 (research)"},
        )

    def scrape_article(self, url: str) -> ScrapedDocument | None:
        """Scrape a news article."""
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
        except httpx.HTTPError:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("h1")
        # News sites typically use article tags
        body = soup.find("article") or soup.find("main") or soup.find("body")

        return ScrapedDocument(
            url=url,
            title=title.get_text(strip=True) if title else "Unknown",
            body=body.get_text(separator="\n", strip=True) if body else "",
            date=None,
            source="news",
            doc_type="news_report",
        )

    def close(self):
        self.client.close()
