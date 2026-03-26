"""Playwright-based browser scraper for Cloudflare-protected sites.

Handles WDTK, East Riding JSNA, and LG Inform when httpx gets blocked.
Uses a shared browser instance to avoid repeated startup costs.
"""

from __future__ import annotations

import logging
import time

from .council import ScrapedDocument

log = logging.getLogger(__name__)

_browser = None
_context = None


def _get_context():
    """Lazy-init a shared browser context."""
    global _browser, _context
    if _context is not None:
        return _context

    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    _browser = pw.chromium.launch(headless=True)
    _context = _browser.new_context(
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        locale="en-GB",
    )
    return _context


def browser_scrape(url: str, source: str, doc_type: str, wait_seconds: float = 3.0) -> ScrapedDocument | None:
    """Scrape a URL using a real browser to bypass Cloudflare/bot protection.

    Args:
        url: The URL to fetch.
        source: "hull_cc" or "east_riding".
        doc_type: Document type for the ScrapedDocument.
        wait_seconds: Time to wait after page load for JS to execute.
    """
    ctx = _get_context()
    page = ctx.new_page()

    try:
        log.info("Browser scraping: %s", url)
        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # Wait for Cloudflare challenge to resolve
        page.wait_for_timeout(int(wait_seconds * 1000))

        # Check if we're still on a challenge page
        content = page.content()
        if "Just a moment" in content or "Checking your browser" in content:
            # Wait longer for the challenge
            log.info("  Cloudflare challenge detected, waiting...")
            page.wait_for_timeout(5000)
            content = page.content()

        if "Just a moment" in content:
            log.warning("  Could not bypass Cloudflare challenge for %s", url)
            return None

        # Extract text content
        title = page.title()

        # Try to get the main content area
        body_text = ""
        for selector in ["main", "article", "#main_content", ".report-content", "#content", "body"]:
            el = page.query_selector(selector)
            if el:
                body_text = el.inner_text()
                if len(body_text) > 100:
                    break

        if not body_text or len(body_text) < 50:
            body_text = page.inner_text("body")

        if not body_text:
            return None

        return ScrapedDocument(
            url=url,
            title=title or "Unknown",
            body=body_text[:20000],  # Cap to avoid huge documents
            date=None,
            source=source,
            doc_type=doc_type,
        )

    except Exception as e:
        log.error("Browser scrape failed for %s: %s", url, e)
        return None
    finally:
        page.close()


def close_browser():
    """Clean up the browser instance."""
    global _browser, _context
    if _context:
        _context.close()
        _context = None
    if _browser:
        _browser.close()
        _browser = None
