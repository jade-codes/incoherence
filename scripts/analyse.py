#!/usr/bin/env python3
"""Run coherence analysis on the seeded Hull data.

This script:
1. Loads claims and outcomes from the database
2. Generates embeddings using sentence-transformers
3. Computes pairwise cosine similarities
4. Identifies contradictions (claims vs worsening outcomes on same topic)
5. Scores overall institutional coherence per topic
6. Stores results back in the database

Works without the GoT web server — does the coherence analysis directly in Python
using the same geometric principles (cosine similarity with identity geometry).
"""

from __future__ import annotations

import json
import math
import sqlite3
import struct
import sys
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from incoherence.graph.model import CausalLink, Relationship, Topic, init_db
from incoherence.graph.storage import GraphStore, _blob_to_embed, _embed_to_blob
from incoherence.extraction.classifier import (
    classify_outcomes_in_db,
    is_genuine_contradiction,
)


def validate_contradiction(claim_text: str, outcome_text: str) -> tuple[bool, float]:
    """Use NLP to validate whether a claim-outcome pair is a real contradiction."""
    return is_genuine_contradiction(claim_text, outcome_text)


@dataclass
class ContradictionResult:
    claim_id: str
    claim_text: str
    outcome_id: str
    outcome_text: str
    cosine: float
    severity: float
    topic: str


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def compute_severity(cosine: float, direction_worsened: bool) -> float:
    """Compute contradiction severity.

    High severity when:
    - The claim and outcome are semantically similar (high cosine) but outcome worsened
    - Or the claim and outcome are semantically opposed (negative cosine)
    """
    if direction_worsened:
        # If outcome worsened and is semantically related to a positive claim,
        # that's a contradiction. Higher similarity = clearer contradiction.
        return min(1.0, max(0.0, cosine * 0.8 + 0.2))
    elif cosine < -0.1:
        # Semantically opposed = potential contradiction
        return min(1.0, abs(cosine))
    return 0.0


def generate_embeddings(conn: sqlite3.Connection) -> None:
    """Generate embeddings for all claims and outcomes that don't have them yet."""
    try:
        from incoherence.extraction.embeddings import EmbeddingModel
    except ImportError:
        print("  sentence-transformers not installed. Install with:")
        print("  pip install sentence-transformers")
        sys.exit(1)

    print("  Loading embedding model (all-mpnet-base-v2)...")
    model = EmbeddingModel()
    print(f"  Model loaded. Embedding dimension: {model.dim}")

    # Get claims without embeddings
    conn.row_factory = sqlite3.Row
    claims = conn.execute("SELECT id, paraphrased FROM claims WHERE embedding IS NULL").fetchall()
    outcomes = conn.execute("SELECT id, description FROM outcomes WHERE embedding IS NULL").fetchall()

    if claims:
        print(f"  Embedding {len(claims)} claims...")
        texts = [r["paraphrased"] for r in claims]
        embeddings = model.embed_batch(texts)
        for row, emb in zip(claims, embeddings):
            blob = _embed_to_blob(emb)
            conn.execute("UPDATE claims SET embedding = ? WHERE id = ?", (blob, row["id"]))
        conn.commit()

    if outcomes:
        print(f"  Embedding {len(outcomes)} outcomes...")
        texts = [r["description"] for r in outcomes]
        embeddings = model.embed_batch(texts)
        for row, emb in zip(outcomes, embeddings):
            blob = _embed_to_blob(emb)
            conn.execute("UPDATE outcomes SET embedding = ? WHERE id = ?", (blob, row["id"]))
        conn.commit()

    print(f"  Done. {len(claims)} claims + {len(outcomes)} outcomes embedded.")


def analyse_topic(
    store: GraphStore,
    conn: sqlite3.Connection,
    entity_id: str,
    topic: str,
) -> tuple[float, list[ContradictionResult]]:
    """Analyse coherence for a single entity + topic."""
    conn.row_factory = sqlite3.Row

    claims = conn.execute(
        "SELECT id, paraphrased, embedding, source_url, date FROM claims WHERE entity_id = ? AND topic = ? AND embedding IS NOT NULL",
        (entity_id, topic),
    ).fetchall()

    outcomes = conn.execute(
        "SELECT id, description, embedding, direction, source_url, date FROM outcomes WHERE (entity_id = ? OR entity_id IS NULL) AND topic = ? AND embedding IS NOT NULL",
        (entity_id, topic),
    ).fetchall()

    if not claims or not outcomes:
        return 1.0, []  # No data = assume coherent

    contradictions = []

    for claim in claims:
        claim_emb = _blob_to_embed(claim["embedding"])
        if not claim_emb:
            continue

        for outcome in outcomes:
            outcome_emb = _blob_to_embed(outcome["embedding"])
            if not outcome_emb:
                continue

            cos = cosine_similarity(claim_emb, outcome_emb)

            # Skip near-duplicates: cosine > 0.92 means they're almost
            # certainly describing the same event. Lower thresholds
            # risk filtering real contradictions (e.g. "target 2030" vs "revised to 2045").
            # BART validation below catches false matches that pass this.
            if cos > 0.92:
                continue

            # Skip if claim and outcome come from the same source URL
            claim_url = claim["source_url"] or ""
            outcome_url = outcome["source_url"] or ""
            if claim_url and outcome_url and claim_url == outcome_url:
                continue

            # Skip very short fragments
            if len(outcome["description"] or "") < 60:
                continue

            # Temporal logic: a claim can only be contradicted by an
            # outcome that was measured AFTER the claim was made.
            # If the claim came after the outcome, it can't be a contradiction.
            claim_date = claim["date"] or ""
            outcome_date = outcome["date"] or ""
            if claim_date and outcome_date and claim_date > outcome_date:
                continue

            worsened = outcome["direction"] == "worsened"
            severity = compute_severity(cos, worsened)

            if severity > 0.5:
                # NLP validation: check if this is a genuine contradiction
                is_contra, nlp_conf = validate_contradiction(
                    claim["paraphrased"], outcome["description"]
                )
                if not is_contra:
                    continue

                # Blend cosine severity with NLP confidence
                final_severity = severity * 0.6 + nlp_conf * 0.4

                contradictions.append(ContradictionResult(
                    claim_id=claim["id"],
                    claim_text=claim["paraphrased"],
                    outcome_id=outcome["id"],
                    outcome_text=outcome["description"],
                    cosine=cos,
                    severity=final_severity,
                    topic=topic,
                ))

                # Store as causal link
                store.insert_causal_link(CausalLink(
                    id=str(uuid.uuid4()),
                    claim_id=claim["id"],
                    outcome_id=outcome["id"],
                    relationship=Relationship.CONTRADICTED,
                    evidence_text=f"Cosine: {cos:.3f}, NLP confidence: {nlp_conf:.3f}, Direction: {'worsened' if worsened else 'other'}",
                    coherence_score=1.0 - final_severity,
                    severity=final_severity,
                ))

    # Coherence score: 1.0 = fully coherent, 0.0 = fully incoherent
    if contradictions:
        avg_severity = sum(c.severity for c in contradictions) / len(contradictions)
        coherence = max(0.0, 1.0 - avg_severity)
    else:
        coherence = 1.0

    return coherence, contradictions


def main():
    project_root = Path(__file__).parent.parent
    db_path = project_root / "data" / "hull.db"

    if not db_path.exists():
        print("Database not found. Run scripts/seed.py first.")
        sys.exit(1)

    conn = init_db(db_path)
    store = GraphStore(conn)

    # Step 0: Classify outcomes using NLP (topic + direction)
    # Only reclassify if outcomes haven't been classified yet
    unclassified = conn.execute(
        "SELECT COUNT(*) FROM outcomes WHERE topic IS NULL OR topic = ''"
    ).fetchone()[0]
    if unclassified > 0 or "--reclassify" in sys.argv:
        print("\n=== Step 0: NLP classification of outcomes ===")
        counts = classify_outcomes_in_db(conn)
        print(f"  Topics reclassified: {counts['topic_changed']}")
        print(f"  Directions reclassified: {counts['direction_changed']}")
    else:
        print("\n=== Step 0: Skipping classification (already done, use --reclassify to force) ===")

    # Step 1: Generate embeddings
    print("\n=== Step 1: Generating embeddings ===")
    generate_embeddings(conn)

    # Step 2: Run coherence analysis per entity per topic
    print("\n=== Step 2: Running coherence analysis ===")

    entities = conn.execute("SELECT id, name FROM entities WHERE kind = 'council'").fetchall()
    conn.row_factory = sqlite3.Row

    all_contradictions = []
    all_scores = []

    for entity in entities:
        eid = entity["id"] if isinstance(entity, sqlite3.Row) else entity[0]
        ename = entity["name"] if isinstance(entity, sqlite3.Row) else entity[1]
        print(f"\n  --- {ename} ---")

        topics = conn.execute(
            "SELECT DISTINCT topic FROM claims WHERE entity_id = ?", (eid,)
        ).fetchall()

        for topic_row in topics:
            topic = topic_row["topic"] if isinstance(topic_row, sqlite3.Row) else topic_row[0]
            coherence, contradictions = analyse_topic(store, conn, eid, topic)

            all_contradictions.extend(contradictions)
            all_scores.append((eid, topic, coherence, len(contradictions)))

            # Store coherence score
            store.save_coherence_score(
                id=str(uuid.uuid4()),
                entity_id=eid,
                topic=topic,
                period_start=date(2015, 1, 1),
                period_end=date(2025, 12, 31),
                score=coherence,
                n_claims=len(conn.execute(
                    "SELECT id FROM claims WHERE entity_id = ? AND topic = ?", (eid, topic)
                ).fetchall()),
                n_outcomes=len(conn.execute(
                    "SELECT id FROM outcomes WHERE topic = ?", (topic,)
                ).fetchall()),
                n_contradictions=len(contradictions),
            )

            status = "COHERENT" if coherence >= 0.7 else "MIXED" if coherence >= 0.4 else "INCOHERENT"
            print(f"    {topic:20s}  coherence={coherence:.2f}  [{status}]  contradictions={len(contradictions)}")

    # Step 3: Summary
    print("\n=== Summary ===")
    print(f"Total contradictions detected: {len(all_contradictions)}")

    if all_scores:
        avg = sum(s[2] for s in all_scores) / len(all_scores)
        print(f"Mean coherence score: {avg:.2f}")

    if all_contradictions:
        all_contradictions.sort(key=lambda c: c.severity, reverse=True)
        print(f"\nTop 10 contradictions by severity:")
        for i, c in enumerate(all_contradictions[:10], 1):
            print(f"\n  {i}. [{c.topic}] severity={c.severity:.2f} cosine={c.cosine:.3f}")
            print(f"     CLAIM:   {c.claim_text[:100]}...")
            print(f"     OUTCOME: {c.outcome_text[:100]}...")

    # Save full results to JSON
    results_path = project_root / "data" / "reports" / "analysis_results.json"
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.write_text(json.dumps({
        "scores": [
            {"entity": s[0], "topic": s[1], "coherence": s[2], "n_contradictions": s[3]}
            for s in all_scores
        ],
        "contradictions": [
            {
                "claim_id": c.claim_id, "claim_text": c.claim_text,
                "outcome_id": c.outcome_id, "outcome_text": c.outcome_text,
                "cosine": c.cosine, "severity": c.severity, "topic": c.topic,
            }
            for c in all_contradictions
        ],
    }, indent=2))
    print(f"\nFull results saved to {results_path}")

    conn.close()


if __name__ == "__main__":
    main()
