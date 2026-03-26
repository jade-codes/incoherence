"""Scraper for Hull City Council and East Riding Council public web sources."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup


@dataclass
class ScrapedDocument:
    """A raw document scraped from a council source."""

    url: str
    title: str
    body: str
    date: datetime | None
    source: str  # "hull_cc" or "east_riding"
    doc_type: str  # "minutes", "press_release", "strategy", "report"

    def save(self, output_dir: Path) -> Path:
        # Build a filesystem-safe slug from the URL path
        parsed = urlparse(self.url)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        slug = "_".join(path_parts)[:120] if path_parts else "index"
        # Append short hash to guarantee uniqueness
        url_hash = hashlib.sha256(self.url.encode()).hexdigest()[:8]
        out_path = output_dir / f"{self.source}_{slug}_{url_hash}.json"
        out_path.write_text(
            json.dumps(
                {
                    "url": self.url,
                    "title": self.title,
                    "body": self.body,
                    "date": self.date.isoformat() if self.date else None,
                    "source": self.source,
                    "doc_type": self.doc_type,
                    "scraped_at": datetime.now().isoformat(),
                },
                indent=2,
            )
        )
        return out_path


class CouncilScraper:
    """Scrapes public documents from Hull and East Riding council websites."""

    HULL_CC_BASE = "https://www.hull.gov.uk"
    EAST_RIDING_BASE = "https://www.eastriding.gov.uk"

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
                "Accept-Language": "en-GB,en;q=0.9",
            },
        )

    async def scrape_hull_minutes(self, url: str) -> ScrapedDocument | None:
        """Scrape a Hull City Council meeting minutes page."""
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
        except httpx.HTTPError:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("h1")
        body = soup.find("main") or soup.find("article") or soup.find("body")

        return ScrapedDocument(
            url=url,
            title=title.get_text(strip=True) if title else "Unknown",
            body=body.get_text(separator="\n", strip=True) if body else "",
            date=None,  # Extracted by the claims extractor
            source="hull_cc",
            doc_type="minutes",
        )

    async def scrape_press_release(self, url: str, source: str = "hull_cc") -> ScrapedDocument | None:
        """Scrape a council press release."""
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
        except httpx.HTTPError:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("h1")
        body = soup.find("main") or soup.find("article") or soup.find("body")

        return ScrapedDocument(
            url=url,
            title=title.get_text(strip=True) if title else "Unknown",
            body=body.get_text(separator="\n", strip=True) if body else "",
            date=None,
            source=source,
            doc_type="press_release",
        )

    def close(self):
        self.client.close()
