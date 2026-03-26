"""Pipeline orchestrator: discovery -> scraping -> extraction -> storage."""

from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path

from .config import CityConfig
from .dedup import DeduplicationTracker
from .discovery import DiscoveredURL
from .discovery.council_news import CouncilNewsFinder
from .discovery.council_minutes import CouncilMinutesFinder
from .discovery.cqc import CqcFinder
from .discovery.environment_agency import EnvironmentAgencyFinder
from .discovery.fingertips import FingertipsFinder
from .discovery.housing import HousingStatsFinder
from .discovery.jsna import JsnaFinder
from .discovery.land_registry import LandRegistryFinder
from .discovery.lginform import LgInformFinder
from .discovery.nomis import NomisFinder
from .discovery.oflog import OflogFinder
from .discovery.planning import PlanningFinder
from .discovery.police import PoliceFinder
from .discovery.theyworkforyou import TheyWorkForYouFinder
from .discovery.whatdotheyknow import WdtkFinder
from .graph.model import SourceType, init_db
from .graph.storage import GraphStore
from .ratelimit import DomainRateLimiter
from .scraper.council import CouncilScraper, ScrapedDocument
from .scraper.news import NewsScraper

log = logging.getLogger(__name__)

# Map (source, doc_type) -> SourceType for extraction
SOURCE_TYPE_MAP = {
    "press_release": SourceType.PRESS_RELEASE,
    "minutes": SourceType.MEETING_MINUTES,
    "stats": SourceType.COUNCIL_STATS,
    "news_report": SourceType.NEWS_REPORT,
}

# Sources that produce claims vs outcomes
CLAIM_DOC_TYPES = {"press_release", "minutes", "foi", "parliamentary"}
OUTCOME_DOC_TYPES = {
    "stats", "news_report", "police", "housing_stats",
    "environment", "cqc", "oflog", "planning", "land_registry",
}


class Pipeline:
    """Coordinates the full scraping pipeline, driven by a CityConfig."""

    def __init__(
        self,
        city: CityConfig,
        output_dir: Path | None = None,
        rate_limiter: DomainRateLimiter | None = None,
        api_key: str | None = None,
        max_pages: int = 10,
    ):
        self.city = city
        db_path = city.db_path
        self.output_dir = output_dir or db_path.parent / "scraped"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.conn = init_db(db_path)
        self.store = GraphStore(self.conn)
        self.tracker = DeduplicationTracker(self.conn)
        self.limiter = rate_limiter or DomainRateLimiter(
            domain_delays=city.rate_limits
        )
        self.api_key = api_key
        self.max_pages = max_pages

    def _build_discoverers(self, sources: list[str] | None = None):
        """Build discoverer instances based on what's actually configured.

        Only enables a discoverer if the city config provides the data it needs.
        Warns when a source is requested but not configured.
        """
        discoverers = []
        source_set = set(sources) if sources else None
        city = self.city

        def _wanted(key: str) -> bool:
            return source_set is None or key in source_set

        # Per-entity discoverers (news, minutes)
        for entity in city.entities:
            if source_set and entity.source_key not in source_set:
                continue
            if entity.news:
                discoverers.append(CouncilNewsFinder(entity, rate_limiter=self.limiter))
            if entity.minutes:
                discoverers.append(CouncilMinutesFinder(entity, rate_limiter=self.limiter))

        # JSNA — only if at least one entity has JSNA sections configured
        if _wanted("jsna") and city.has_jsna:
            discoverers.append(JsnaFinder(city, rate_limiter=self.limiter))
        elif source_set and "jsna" in source_set and not city.has_jsna:
            log.warning("Source 'jsna' requested but no JSNA sections configured")

        # NOMIS, Fingertips, LG Inform — need ONS codes
        for key, cls in [("nomis", NomisFinder), ("fingertips", FingertipsFinder), ("lginform", LgInformFinder)]:
            if _wanted(key) and city.has_ons:
                discoverers.append(cls(city, rate_limiter=self.limiter))
            elif source_set and key in source_set and not city.has_ons:
                log.warning("Source '%s' requested but no ONS codes configured", key)

        # FOI (WhatDoTheyKnow) — need WDTK slugs
        if _wanted("foi") and city.has_wdtk:
            discoverers.append(WdtkFinder(city, rate_limiter=self.limiter))
        elif source_set and "foi" in source_set and not city.has_wdtk:
            log.warning("Source 'foi' requested but no WDTK slugs configured")

        # Police — need police areas with coordinates
        if _wanted("police") and city.has_police:
            discoverers.append(PoliceFinder(city, rate_limiter=self.limiter))
        elif source_set and "police" in source_set and not city.has_police:
            log.warning("Source 'police' requested but no police areas configured")

        # Housing — national stats, just need ONS codes for extraction
        if _wanted("housing") and city.has_ons:
            discoverers.append(HousingStatsFinder(city, rate_limiter=self.limiter))
        elif source_set and "housing" in source_set and not city.has_ons:
            log.warning("Source 'housing' requested but no ONS codes configured")

        # Environment Agency — needs police areas for coordinates
        if _wanted("environment") and city.has_police:
            discoverers.append(EnvironmentAgencyFinder(city, rate_limiter=self.limiter))

        # CQC — needs ONS codes
        if _wanted("cqc") and city.has_ons:
            discoverers.append(CqcFinder(city, rate_limiter=self.limiter))

        # Oflog — needs ONS codes
        if _wanted("oflog") and city.has_ons:
            discoverers.append(OflogFinder(city, rate_limiter=self.limiter))

        # TheyWorkForYou — needs TWFY_API_KEY env var
        if _wanted("parliamentary"):
            discoverers.append(TheyWorkForYouFinder(city, rate_limiter=self.limiter))

        # Planning applications — needs coordinates or entity names
        if _wanted("planning"):
            discoverers.append(PlanningFinder(city, rate_limiter=self.limiter))

        # Land Registry — needs ONS codes
        if _wanted("land_registry") and city.has_ons:
            discoverers.append(LandRegistryFinder(city, rate_limiter=self.limiter))

        if not discoverers:
            log.warning("No discoverers enabled — check your city config")

        return discoverers

    def run_discovery(self, sources: list[str] | None = None) -> int:
        """Run URL discovery. Returns count of new URLs found."""
        discoverers = self._build_discoverers(sources)
        n_new = 0

        for finder in discoverers:
            try:
                urls = finder.discover(max_pages=self.max_pages)
                for discovered in urls:
                    if self.tracker.add_discovered(
                        discovered.url, discovered.source, discovered.doc_type
                    ):
                        n_new += 1
                        log.info("New: %s", discovered.url)
            finally:
                finder.close()

        log.info("Discovery complete: %d new URLs", n_new)
        return n_new

    def run_scrape(self, source: str | None = None, limit: int = 50) -> int:
        """Scrape pending URLs. Returns count of documents scraped."""
        pending = self.tracker.pending_urls(source=source, limit=limit)
        if not pending:
            log.info("No pending URLs to scrape")
            return 0

        council_scraper = CouncilScraper(self.output_dir)
        news_scraper = NewsScraper(self.output_dir)
        n_scraped = 0

        try:
            for item in pending:
                url = item["url"]
                src = item["source"]
                doc_type = item["doc_type"]

                log.info("Scraping [%s/%s]: %s", src, doc_type, url)
                self.limiter.wait(url)

                doc = self._scrape_one(url, src, doc_type, council_scraper, news_scraper)

                # Browser fallback for sites that block httpx
                if doc is None and any(
                    domain in url for domain in [
                        "whatdotheyknow.com",
                        "jsna.com",
                        "lginform.local.gov.uk",
                    ]
                ):
                    log.info("Trying browser fallback for %s", url)
                    doc = self._browser_fallback(url, src, doc_type)

                if doc:
                    doc.save(self.output_dir)
                    self.tracker.mark_scraped(url)
                    n_scraped += 1
                else:
                    self.tracker.mark_failed(url, "scraper returned None")
        finally:
            council_scraper.close()
            news_scraper.close()

        log.info("Scrape complete: %d documents", n_scraped)
        return n_scraped

    def _browser_fallback(
        self, url: str, source: str, doc_type: str
    ) -> ScrapedDocument | None:
        """Try scraping with playwright as a fallback."""
        try:
            from .scraper.browser import browser_scrape
            return browser_scrape(url, source, doc_type)
        except ImportError:
            log.warning("Playwright not installed — cannot browser-scrape %s", url)
            return None
        except Exception as e:
            log.error("Browser fallback failed for %s: %s", url, e)
            return None

    def _scrape_one(
        self,
        url: str,
        source: str,
        doc_type: str,
        council_scraper: CouncilScraper,
        news_scraper: NewsScraper,
    ) -> ScrapedDocument | None:
        """Dispatch a single URL to the right scraper."""
        try:
            if doc_type == "minutes" and url.lower().endswith(".pdf"):
                return self._scrape_minutes_pdf(url, council_scraper)
            elif doc_type == "minutes":
                return self._scrape_html_page(url, source, doc_type, council_scraper)
            elif doc_type == "press_release":
                return self._scrape_html_page(url, source, doc_type, council_scraper)
            elif doc_type == "fingertips":
                from .scraper.fingertips import scrape_fingertips
                return scrape_fingertips(url, source)
            elif doc_type == "lginform":
                from .scraper.lginform import scrape_lginform
                return scrape_lginform(url, source)
            elif doc_type == "police":
                from .scraper.police import scrape_police
                return scrape_police(url, source)
            elif doc_type == "foi":
                from .scraper.wdtk import scrape_foi_request
                return scrape_foi_request(url, source)
            elif doc_type == "housing_stats":
                return self._scrape_html_page(url, source, doc_type, council_scraper)
            elif doc_type == "stats":
                return self._scrape_stats_page(url, source, council_scraper)
            elif doc_type == "news_report":
                return news_scraper.scrape_article(url)
            else:
                log.warning("Unknown doc_type %s for %s", doc_type, url)
                return None
        except Exception as e:
            log.error("Error scraping %s: %s", url, e)
            return None

    def _scrape_html_page(
        self,
        url: str,
        source: str,
        doc_type: str,
        council_scraper: CouncilScraper,
    ) -> ScrapedDocument | None:
        from bs4 import BeautifulSoup

        try:
            resp = council_scraper.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            log.error("Failed to fetch %s: %s", url, e)
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
            doc_type=doc_type,
        )

    def _scrape_minutes_pdf(
        self, url: str, council_scraper: CouncilScraper
    ) -> ScrapedDocument | None:
        from .scraper.minutes import parse_minutes_pdf

        try:
            resp = council_scraper.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            log.error("Failed to download PDF %s: %s", url, e)
            return None

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(resp.content)
            tmp_path = Path(f.name)

        try:
            parsed = parse_minutes_pdf(tmp_path)
            return ScrapedDocument(
                url=url,
                title=parsed.title,
                body=parsed.body,
                date=None,
                source="unknown",
                doc_type="minutes",
            )
        finally:
            tmp_path.unlink(missing_ok=True)

    def _scrape_stats_page(
        self, url: str, source: str, council_scraper: CouncilScraper
    ) -> ScrapedDocument | None:
        if "nomisweb.co.uk/api" in url:
            return self._scrape_nomis_endpoint(url, source)

        try:
            resp = council_scraper.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            log.error("Failed to fetch stats page %s: %s", url, e)
            return None

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("h1")
        body = soup.find("main") or soup.find("article") or soup.find("body")

        return ScrapedDocument(
            url=url,
            title=title.get_text(strip=True) if title else "Unknown",
            body=body.get_text(separator="\n", strip=True) if body else "",
            date=None,
            source=source,
            doc_type="stats",
        )

    def _scrape_nomis_endpoint(self, url: str, source: str) -> ScrapedDocument | None:
        import httpx

        try:
            resp = httpx.get(url, timeout=30.0, follow_redirects=True)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error("Failed to fetch NOMIS data %s: %s", url, e)
            return None

        observations = data.get("obs", [])
        if not observations:
            return None

        lines = []
        for obs in observations[:200]:
            parts = []
            for key in ["date_name", "geography_name", "measures_name", "obs_value"]:
                if key in obs:
                    parts.append(f"{key}: {obs[key]}")
            lines.append(", ".join(parts))

        body = f"NOMIS Labour Market Data\nSource: {url}\n\n" + "\n".join(lines)

        return ScrapedDocument(
            url=url,
            title=f"NOMIS data for {source}",
            body=body,
            date=None,
            source=source,
            doc_type="stats",
        )

    def run_candidate_extraction(self, source: str | None = None, limit: int = 200) -> int:
        """Rule-based candidate extraction. No API key needed."""
        from .extraction.rules import extract_candidates, store_candidates

        scraped = self.tracker.scraped_urls(source=source, limit=limit)
        if not scraped:
            log.info("No scraped documents awaiting extraction")
            return 0

        entity_map = self.city.entity_map

        total = 0
        for item in scraped:
            url = item["url"]
            src = item["source"]
            doc_type = item["doc_type"]

            doc_data = self._load_scraped_doc(url)
            if not doc_data:
                log.warning("No saved document found for %s", url)
                continue

            body = doc_data.get("body", "")
            if not body.strip():
                continue

            # Fingertips data is structured — extract outcomes directly
            if doc_type == "fingertips":
                from .extraction.fingertips import extract_outcomes_from_fingertips
                outcomes = extract_outcomes_from_fingertips(body, url, src, entity_map)
                for outcome in outcomes:
                    self.store.insert_outcome(outcome)
                total += len(outcomes)
                if outcomes:
                    log.info("  %s: %d outcomes (direct)", url, len(outcomes))
                self.tracker.mark_extracted(url)
                continue

            # LG Inform data is structured — extract outcome directly
            if doc_type == "lginform":
                from .extraction.lginform import extract_outcomes_from_lginform
                outcomes = extract_outcomes_from_lginform(body, url, src, entity_map)
                for outcome in outcomes:
                    self.store.insert_outcome(outcome)
                total += len(outcomes)
                if outcomes:
                    log.info("  %s: %d outcomes (direct)", url, len(outcomes))
                self.tracker.mark_extracted(url)
                continue

            # Police data is structured — extract outcome directly
            if doc_type == "police":
                from .extraction.police import extract_outcomes_from_police
                outcomes = extract_outcomes_from_police(body, url, src, entity_map)
                for outcome in outcomes:
                    self.store.insert_outcome(outcome)
                total += len(outcomes)
                if outcomes:
                    log.info("  %s: %d outcomes (direct)", url, len(outcomes))
                self.tracker.mark_extracted(url)
                continue

            candidates = extract_candidates(body, url, src, doc_type)
            if candidates:
                n = store_candidates(self.conn, candidates)
                total += n
                log.info("  %s: %d candidates (%d claim, %d outcome)", url,
                         n,
                         sum(1 for c in candidates if c.kind == "claim"),
                         sum(1 for c in candidates if c.kind == "outcome"))

            self.tracker.mark_extracted(url)

        log.info("Candidate extraction complete: %d candidates staged", total)
        return total

    def run_extraction(self, source: str | None = None, limit: int = 20) -> int:
        """Extract claims/outcomes from scraped documents. Returns count extracted."""
        from .extraction.claims import extract_claims_from_text
        from .extraction.outcomes import extract_outcomes_from_text

        entity_map = self.city.entity_map

        scraped = self.tracker.scraped_urls(source=source, limit=limit)
        if not scraped:
            log.info("No scraped documents awaiting extraction")
            return 0

        n_extracted = 0

        for item in scraped:
            url = item["url"]
            src = item["source"]
            doc_type = item["doc_type"]

            doc_data = self._load_scraped_doc(url)
            if not doc_data:
                log.warning("No saved document found for %s", url)
                self.tracker.mark_failed(url, "scraped doc not found on disk")
                continue

            body = doc_data.get("body", "")
            if not body.strip():
                self.tracker.mark_failed(url, "empty document body")
                continue

            entity_id = entity_map.get(src)
            source_type = SOURCE_TYPE_MAP.get(doc_type, SourceType.NEWS_REPORT)

            log.info("Extracting [%s/%s]: %s", src, doc_type, url)

            try:
                if doc_type in CLAIM_DOC_TYPES and entity_id:
                    claims = extract_claims_from_text(
                        body,
                        entity_id=entity_id,
                        source_url=url,
                        source_type=source_type,
                        api_key=self.api_key,
                    )
                    for claim in claims:
                        self.store.insert_claim(claim)
                    n_extracted += len(claims)
                    log.info("  Extracted %d claims", len(claims))

                if doc_type in OUTCOME_DOC_TYPES:
                    outcomes = extract_outcomes_from_text(
                        body,
                        entity_id=entity_id,
                        source_url=url,
                        source_type=source_type,
                        api_key=self.api_key,
                    )
                    for outcome in outcomes:
                        self.store.insert_outcome(outcome)
                    n_extracted += len(outcomes)
                    log.info("  Extracted %d outcomes", len(outcomes))

                self.tracker.mark_extracted(url)

            except Exception as e:
                log.error("Extraction failed for %s: %s", url, e)
                self.tracker.mark_failed(url, str(e))

        log.info("Extraction complete: %d claims/outcomes", n_extracted)
        return n_extracted

    def _load_scraped_doc(self, url: str) -> dict | None:
        import hashlib

        url_hash = hashlib.sha256(url.encode()).hexdigest()[:8]
        for path in self.output_dir.glob(f"*_{url_hash}.json"):
            try:
                data = json.loads(path.read_text())
                if data.get("url") == url:
                    return data
            except (json.JSONDecodeError, KeyError):
                continue

        for path in self.output_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text())
                if data.get("url") == url:
                    return data
            except (json.JSONDecodeError, KeyError):
                continue

        return None

    def run_full(self, sources: list[str] | None = None, limit: int = 50) -> dict:
        """Run the complete pipeline: discover -> scrape -> extract."""
        n_discovered = self.run_discovery(sources)
        n_scraped = self.run_scrape(limit=limit)
        n_extracted = self.run_extraction(limit=limit)
        return {
            "discovered": n_discovered,
            "scraped": n_scraped,
            "extracted": n_extracted,
        }

    def status(self) -> dict:
        return self.tracker.stats()

    def close(self):
        self.conn.close()
