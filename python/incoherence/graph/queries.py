"""Graph query helpers for the knowledge graph."""

from __future__ import annotations

import sqlite3


def entity_summary(conn: sqlite3.Connection, entity_id: str) -> dict:
    """Get a summary of claims, outcomes, and contradictions for an entity."""
    conn.row_factory = sqlite3.Row

    n_claims = conn.execute(
        "SELECT COUNT(*) as n FROM claims WHERE entity_id = ?", (entity_id,)
    ).fetchone()["n"]

    n_outcomes = conn.execute(
        "SELECT COUNT(*) as n FROM outcomes WHERE entity_id = ?", (entity_id,)
    ).fetchone()["n"]

    n_contradictions = conn.execute(
        """SELECT COUNT(*) as n FROM causal_links cl
           JOIN claims c ON cl.claim_id = c.id
           WHERE c.entity_id = ? AND cl.relationship = 'contradicted'""",
        (entity_id,),
    ).fetchone()["n"]

    topics = conn.execute(
        "SELECT DISTINCT topic FROM claims WHERE entity_id = ? ORDER BY topic",
        (entity_id,),
    ).fetchall()

    return {
        "entity_id": entity_id,
        "n_claims": n_claims,
        "n_outcomes": n_outcomes,
        "n_contradictions": n_contradictions,
        "topics": [r["topic"] for r in topics],
    }


def coherence_history(conn: sqlite3.Connection, entity_id: str, topic: str | None = None) -> list[dict]:
    """Get coherence score history for an entity, optionally filtered by topic."""
    conn.row_factory = sqlite3.Row
    query = "SELECT * FROM coherence_scores WHERE entity_id = ?"
    params: list = [entity_id]
    if topic:
        query += " AND topic = ?"
        params.append(topic)
    query += " ORDER BY period_start"
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def worst_contradictions(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    """Get the most severe contradictions across all entities."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT cl.*, c.paraphrased as claim_text, c.date as claim_date,
                  c.entity_id, o.description as outcome_text, o.date as outcome_date
           FROM causal_links cl
           JOIN claims c ON cl.claim_id = c.id
           JOIN outcomes o ON cl.outcome_id = o.id
           WHERE cl.relationship = 'contradicted'
           ORDER BY cl.severity DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]
