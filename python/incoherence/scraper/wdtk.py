"""Scraper for WhatDoTheyKnow FOI request pages."""

from __future__ import annotations

import logging

import httpx
from bs4 import BeautifulSoup

from .council import ScrapedDocument

log = logging.getLogger(__name__)

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    """Get a shared client with session cookie."""
    global _client
    if _client is None:
        _client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
                "Accept-Language": "en-GB,en;q=0.9",
            },
        )
        try:
            _client.get("https://www.whatdotheyknow.com/")
        except httpx.HTTPError:
            pass
    return _client


def scrape_foi_request(url: str, source: str) -> ScrapedDocument | None:
    """Scrape a WhatDoTheyKnow FOI request page."""
    client = _get_client()
    try:
        # Fetch JSON version
        json_url = url.rstrip("/") + ".json"
        resp = client.get(json_url)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        # Fallback to HTML scraping
        try:
            resp = client.get(url)
            resp.raise_for_status()
        except Exception as e:
            log.error("Failed to fetch FOI request %s: %s", url, e)
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("h1")
        body = soup.find("div", id="main_content") or soup.find("main") or soup.find("body")

        return ScrapedDocument(
            url=url,
            title=title.get_text(strip=True) if title else "FOI Request",
            body=body.get_text(separator="\n", strip=True) if body else "",
            date=None,
            source=source,
            doc_type="foi",
        )

    # Parse JSON response
    info = data.get("info_request", data)
    title = info.get("title", "FOI Request")
    status = info.get("described_state", "unknown")

    # Build text from the correspondence
    lines = [
        f"FOI Request: {title}",
        f"Status: {status}",
        f"URL: {url}",
        "",
    ]

    # Get the correspondence events
    events = info.get("info_request_events", [])
    for event in events:
        event_type = event.get("event_type", "")
        if event_type in ("sent", "response", "followup_sent", "followup_response"):
            desc = event.get("rendered_description", "")
            body_text = event.get("body", "")
            if desc:
                lines.append(f"--- {event_type} ---")
                lines.append(desc[:2000])  # Cap individual messages
            elif body_text:
                lines.append(f"--- {event_type} ---")
                lines.append(body_text[:2000])
            lines.append("")

    body = "\n".join(lines)

    return ScrapedDocument(
        url=url,
        title=title,
        body=body,
        date=None,
        source=source,
        doc_type="foi",
    )
