"""Scraper for LG Inform / ESD web services API.

With an API key, fetches structured JSON data directly.
Falls back to HTML scraping via browser if no key.
"""

from __future__ import annotations

import logging

import httpx
from bs4 import BeautifulSoup

from .council import ScrapedDocument

log = logging.getLogger(__name__)


def scrape_lginform(url: str, source: str) -> ScrapedDocument | None:
    """Fetch an LG Inform metric and produce structured text."""
    # Detect if this is an API URL (has ApplicationKey) or HTML
    is_api = "webservices.esd.org.uk" in url

    if is_api:
        return _scrape_api(url, source)
    else:
        return _scrape_html(url, source)


def _scrape_api(url: str, source: str) -> ScrapedDocument | None:
    """Fetch structured JSON from the ESD API."""
    try:
        resp = httpx.get(url, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error("Failed to fetch LG Inform API %s: %s", url, e)
        return None

    if not data or "columns" not in data:
        return None

    area_name = "Hull" if source == "hull_cc" else "East Riding"
    columns = data.get("columns", [])

    if not columns:
        return None

    # Parse the structured response
    lines = [f"LG Inform API Data: {area_name}", ""]

    for col in columns:
        metric_info = col.get("metricType", {})
        area_info = col.get("area", {})
        period_info = col.get("period", {})

        metric_label = metric_info.get("label", "Unknown metric")
        area_label = area_info.get("label", area_name)
        period_label = period_info.get("label", "Unknown period")

        lines.append(f"Metric: {metric_label}")
        lines.append(f"Area: {area_label}")
        lines.append(f"Period: {period_label}")

        # Get the value
        values = col.get("values", [])
        if values:
            val = values[0] if isinstance(values, list) else values
            lines.append(f"Value: {val}")

    # Also include row data if present
    rows = data.get("rows", [])
    for row in rows[:5]:
        row_values = row.get("values", [])
        if row_values:
            lines.append(f"  {row_values}")

    body = "\n".join(lines)

    if len(body.strip().split("\n")) <= 3:
        return None

    return ScrapedDocument(
        url=url,
        title=f"LG Inform: {columns[0].get('metricType', {}).get('label', '')} - {area_name}",
        body=body,
        date=None,
        source=source,
        doc_type="lginform",
    )


def _scrape_html(url: str, source: str) -> ScrapedDocument | None:
    """Scrape an LG Inform HTML report page (with browser-like headers or playwright fallback)."""
    # Try with session cookies first
    client = httpx.Client(
        timeout=30.0,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
            "Accept-Language": "en-GB,en;q=0.9",
        },
    )

    try:
        # Get session cookie
        client.get("https://lginform.local.gov.uk/")
        resp = client.get(url)
        resp.raise_for_status()
    except httpx.HTTPError:
        client.close()
        return None
    finally:
        client.close()

    area_name = "Hull" if source == "hull_cc" else "East Riding"
    soup = BeautifulSoup(resp.text, "html.parser")

    title_el = soup.find("h1") or soup.find("title")
    title_text = title_el.get_text(strip=True) if title_el else "LG Inform Report"

    main = (
        soup.find("div", class_="report-content")
        or soup.find("main")
        or soup.find("div", id="content")
        or soup.find("body")
    )
    if not main:
        return None

    body_text = main.get_text(separator="\n", strip=True)
    if len(body_text) < 50:
        return None

    body = f"LG Inform Report: {area_name}\nTitle: {title_text}\n\n{body_text[:5000]}"

    return ScrapedDocument(
        url=url, title=f"LG Inform: {title_text} - {area_name}",
        body=body, date=None, source=source, doc_type="lginform",
    )
