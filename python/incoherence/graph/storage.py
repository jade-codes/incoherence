"""SQLite storage backend for the knowledge graph."""

from __future__ import annotations

import json
import sqlite3
import struct
from datetime import date

from .model import Claim, CausalLink, Entity, Outcome


def _embed_to_blob(embedding: list[float] | None) -> bytes | None:
    if embedding is None:
        return None
    return struct.pack(f"{len(embedding)}f", *embedding)


def _blob_to_embed(blob: bytes | None) -> list[float] | None:
    if blob is None:
        return None
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


class GraphStore:
    """CRUD operations for the knowledge graph."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def insert_entity(self, entity: Entity) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO entities (id, name, kind) VALUES (?, ?, ?)",
            (entity.id, entity.name, entity.kind),
        )
        self.conn.commit()

    def insert_claim(self, claim: Claim) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO claims
               (id, entity_id, date, source_url, source_type, exact_quote,
                paraphrased, topic, embedding, confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                claim.id,
                claim.entity_id,
                claim.date.isoformat(),
                claim.source_url,
                claim.source_type.value,
                claim.exact_quote,
                claim.paraphrased,
                claim.topic.value,
                _embed_to_blob(claim.embedding),
                claim.confidence,
            ),
        )
        self.conn.commit()

    def insert_outcome(self, outcome: Outcome) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO outcomes
               (id, entity_id, date, source_url, source_type, description,
                topic, metric_name, metric_value, metric_unit, direction, embedding)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                outcome.id,
                outcome.entity_id,
                outcome.date.isoformat(),
                outcome.source_url,
                outcome.source_type.value,
                outcome.description,
                outcome.topic.value,
                outcome.metric_name,
                outcome.metric_value,
                outcome.metric_unit,
                outcome.direction.value if outcome.direction else None,
                _embed_to_blob(outcome.embedding),
            ),
        )
        self.conn.commit()

    def insert_causal_link(self, link: CausalLink) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO causal_links
               (id, claim_id, outcome_id, relationship, evidence_text,
                coherence_score, severity)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                link.id,
                link.claim_id,
                link.outcome_id,
                link.relationship.value,
                link.evidence_text,
                link.coherence_score,
                link.severity,
            ),
        )
        self.conn.commit()

    def get_claims_by_topic(self, topic: str, start: date | None = None, end: date | None = None) -> list[dict]:
        query = "SELECT * FROM claims WHERE topic = ?"
        params: list = [topic]
        if start:
            query += " AND date >= ?"
            params.append(start.isoformat())
        if end:
            query += " AND date <= ?"
            params.append(end.isoformat())
        query += " ORDER BY date"

        self.conn.row_factory = sqlite3.Row
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_outcomes_by_topic(self, topic: str, start: date | None = None, end: date | None = None) -> list[dict]:
        query = "SELECT * FROM outcomes WHERE topic = ?"
        params: list = [topic]
        if start:
            query += " AND date >= ?"
            params.append(start.isoformat())
        if end:
            query += " AND date <= ?"
            params.append(end.isoformat())
        query += " ORDER BY date"

        self.conn.row_factory = sqlite3.Row
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_contradictions(self) -> list[dict]:
        """Get all causal links marked as contradictions."""
        self.conn.row_factory = sqlite3.Row
        rows = self.conn.execute(
            """SELECT cl.*, c.paraphrased as claim_text, o.description as outcome_text
               FROM causal_links cl
               JOIN claims c ON cl.claim_id = c.id
               JOIN outcomes o ON cl.outcome_id = o.id
               WHERE cl.relationship = 'contradicted'
               ORDER BY cl.severity DESC""",
        ).fetchall()
        return [dict(r) for r in rows]

    def save_coherence_score(
        self,
        id: str,
        entity_id: str,
        topic: str,
        period_start: date,
        period_end: date,
        score: float,
        n_claims: int,
        n_outcomes: int,
        n_contradictions: int,
        analysis_json: str | None = None,
    ) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO coherence_scores
               (id, entity_id, topic, period_start, period_end, score,
                n_claims, n_outcomes, n_contradictions, analysis_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                id,
                entity_id,
                topic,
                period_start.isoformat(),
                period_end.isoformat(),
                score,
                n_claims,
                n_outcomes,
                n_contradictions,
                analysis_json,
            ),
        )
        self.conn.commit()
