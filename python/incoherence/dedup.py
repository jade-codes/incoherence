"""Deduplication tracker backed by the scraped_urls table."""

from __future__ import annotations

import sqlite3
from datetime import datetime


class DeduplicationTracker:
    """Track which URLs have been discovered, scraped, and extracted."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def is_known(self, url: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM scraped_urls WHERE url = ?", (url,)
        ).fetchone()
        return row is not None

    def add_discovered(self, url: str, source: str, doc_type: str) -> bool:
        """Register a discovered URL. Returns True if newly added."""
        if self.is_known(url):
            return False
        self.conn.execute(
            "INSERT INTO scraped_urls (url, source, doc_type) VALUES (?, ?, ?)",
            (url, source, doc_type),
        )
        self.conn.commit()
        return True

    def mark_scraped(self, url: str) -> None:
        self.conn.execute(
            "UPDATE scraped_urls SET status = 'scraped', scraped_at = ? WHERE url = ?",
            (datetime.now().isoformat(), url),
        )
        self.conn.commit()

    def mark_extracted(self, url: str) -> None:
        self.conn.execute(
            "UPDATE scraped_urls SET status = 'extracted', extracted_at = ? WHERE url = ?",
            (datetime.now().isoformat(), url),
        )
        self.conn.commit()

    def mark_failed(self, url: str, error: str) -> None:
        self.conn.execute(
            "UPDATE scraped_urls SET status = 'failed', error_message = ? WHERE url = ?",
            (error, url),
        )
        self.conn.commit()

    def pending_urls(
        self, source: str | None = None, limit: int = 100
    ) -> list[dict]:
        """Get URLs that have been discovered but not yet scraped."""
        query = "SELECT url, source, doc_type FROM scraped_urls WHERE status = 'pending'"
        params: list = []
        if source:
            query += " AND source = ?"
            params.append(source)
        query += " ORDER BY discovered_at LIMIT ?"
        params.append(limit)

        self.conn.row_factory = sqlite3.Row
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def scraped_urls(
        self, source: str | None = None, limit: int = 100
    ) -> list[dict]:
        """Get URLs that have been scraped but not yet extracted."""
        query = "SELECT url, source, doc_type FROM scraped_urls WHERE status = 'scraped'"
        params: list = []
        if source:
            query += " AND source = ?"
            params.append(source)
        query += " ORDER BY scraped_at LIMIT ?"
        params.append(limit)

        self.conn.row_factory = sqlite3.Row
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> dict[str, int]:
        self.conn.row_factory = None
        rows = self.conn.execute(
            "SELECT status, COUNT(*) FROM scraped_urls GROUP BY status"
        ).fetchall()
        return {status: count for status, count in rows}
