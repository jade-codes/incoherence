"""Scraper for Police UK API crime data.

Fetches street-level crime JSON, aggregates by category,
and produces a structured text summary.
"""

from __future__ import annotations

import logging
from collections import Counter

import httpx

from .council import ScrapedDocument

log = logging.getLogger(__name__)


def scrape_police(url: str, source: str) -> ScrapedDocument | None:
    """Fetch crime data from Police UK API and summarise by category."""
    try:
        resp = httpx.get(url, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        crimes = resp.json()
    except Exception as e:
        log.error("Failed to fetch police data %s: %s", url, e)
        return None

    if not crimes:
        return None

    # Parse date from URL
    date_str = ""
    if "date=" in url:
        date_str = url.split("date=")[1].split("&")[0]

    area_name = "Hull" if source == "hull_cc" else "East Riding"

    # Aggregate by crime category
    category_counts = Counter(c.get("category", "unknown") for c in crimes)
    total = len(crimes)

    lines = [
        f"Police UK Crime Data: {area_name}",
        f"Period: {date_str}",
        f"Total crimes reported: {total}",
        f"Source: data.police.uk (Humberside Police)",
        "",
        "Crime breakdown by category:",
    ]

    for category, count in category_counts.most_common():
        pct = count / total * 100
        lines.append(f"  {category.replace('-', ' ').title()}: {count} ({pct:.1f}%)")

    # Outcome summary if available
    outcomes = Counter()
    for c in crimes:
        status = c.get("outcome_status")
        if status and status.get("category"):
            outcomes[status["category"]] += 1

    if outcomes:
        lines.append("")
        lines.append("Outcome status:")
        for outcome, count in outcomes.most_common(5):
            lines.append(f"  {outcome}: {count}")

    body = "\n".join(lines)

    return ScrapedDocument(
        url=url,
        title=f"Crime data - {area_name} - {date_str}",
        body=body,
        date=None,
        source=source,
        doc_type="police",
    )
