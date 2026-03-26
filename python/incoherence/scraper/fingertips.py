"""Scraper for OHID Fingertips API data.

Fetches indicator data as CSV, filters to Hull/East Riding,
and converts to structured text with comparisons and trends.
"""

from __future__ import annotations

import csv
import io
import logging
from urllib.parse import parse_qs, urlparse

import httpx

from .council import ScrapedDocument

log = logging.getLogger(__name__)

FINGERTIPS_API = "https://fingertips.phe.org.uk/api"
HULL_CODE = "E06000010"
EAST_RIDING_CODE = "E06000011"
AREA_TYPE_UA = 401


def scrape_fingertips(url: str, source: str) -> ScrapedDocument | None:
    """Fetch a Fingertips indicator and produce a structured text document."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)

    indicator_id = params.get("indicator_ids", [None])[0]
    area_code = params.get("child_area_code", [None])[0]

    if not indicator_id:
        log.warning("No indicator_id in URL: %s", url)
        return None

    # Determine which area to filter for
    if not area_code:
        area_code = HULL_CODE if source == "hull_cc" else EAST_RIDING_CODE

    # Fetch the CSV data for this indicator
    csv_url = (
        f"{FINGERTIPS_API}/all_data/csv/by_indicator_id"
        f"?indicator_ids={indicator_id}"
        f"&area_type_id={AREA_TYPE_UA}"
        f"&parent_area_code=E92000001"
    )

    try:
        resp = httpx.get(csv_url, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        log.error("Failed to fetch Fingertips data for indicator %s: %s", indicator_id, e)
        return None

    # Parse CSV
    reader = csv.DictReader(io.StringIO(resp.text))
    rows = list(reader)

    if not rows:
        log.warning("Empty response for indicator %s", indicator_id)
        return None

    # Filter to our area
    area_rows = [r for r in rows if r.get("Area Code") == area_code]
    # Also get England rows for comparison
    england_rows = [r for r in rows if r.get("Area Code") == "E92000001"]

    if not area_rows:
        log.warning("No data for area %s in indicator %s", area_code, indicator_id)
        return None

    # Get indicator name from first row
    indicator_name = area_rows[0].get("Indicator Name", f"Indicator {indicator_id}")
    area_name = area_rows[0].get("Area Name", area_code)

    # Sort by time period (most recent first)
    area_rows.sort(key=lambda r: r.get("Time period", ""), reverse=True)
    england_rows.sort(key=lambda r: r.get("Time period", ""), reverse=True)

    # Build structured text
    lines = [
        f"Fingertips Public Health Data: {indicator_name}",
        f"Area: {area_name} ({area_code})",
        f"Source: OHID Fingertips (Office for Health Improvement and Disparities)",
        "",
    ]

    # Most recent data point
    latest = area_rows[0]
    value = latest.get("Value", "")
    period = latest.get("Time period", "")
    significance = latest.get("Compared to England value or percentiles", "")
    trend = latest.get("Recent Trend", "")
    count = latest.get("Count", "")
    denominator = latest.get("Denominator", "")

    lines.append(f"Latest period: {period}")
    lines.append(f"Value: {value}")

    if count and denominator:
        lines.append(f"Count: {count}, Denominator: {denominator}")

    if significance:
        lines.append(f"Compared to England: {significance}")

    if trend:
        lines.append(f"Recent trend: {trend}")

    # England comparison for same period
    eng_for_period = [r for r in england_rows if r.get("Time period") == period]
    if eng_for_period:
        eng_val = eng_for_period[0].get("Value", "")
        if eng_val:
            lines.append(f"England average for same period: {eng_val}")

    # Time series (last 5 data points)
    lines.append("")
    lines.append("Time series:")
    for row in area_rows[:5]:
        p = row.get("Time period", "?")
        v = row.get("Value", "?")
        sig = row.get("Compared to England value or percentiles", "")
        t = row.get("Recent Trend", "")
        parts = [f"  {p}: {v}"]
        if sig:
            parts.append(f"({sig})")
        if t:
            parts.append(f"[trend: {t}]")
        lines.append(" ".join(parts))

    body = "\n".join(lines)

    return ScrapedDocument(
        url=url,
        title=f"{indicator_name} - {area_name}",
        body=body,
        date=None,
        source=source,
        doc_type="fingertips",
    )
