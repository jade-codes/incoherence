"""Institutional coherence scoring — the core analysis pipeline."""

from __future__ import annotations

import uuid
from datetime import date

from ..extraction.embeddings import EmbeddingModel
from ..graph.model import Topic
from ..graph.storage import GraphStore
from .got_bridge import CoherenceResult, GotBridge


def analyse_topic_coherence(
    store: GraphStore,
    entity_id: str,
    topic: Topic,
    start: date,
    end: date,
    embedding_model: EmbeddingModel,
    got: GotBridge,
) -> CoherenceResult | None:
    """Run GoT coherence analysis for a specific entity + topic + time period.

    1. Fetch claims and outcomes from the graph
    2. Embed them
    3. Send to GoT for coherence analysis
    4. Store the result
    """
    claims = store.get_claims_by_topic(topic.value, start, end)
    outcomes = store.get_outcomes_by_topic(topic.value, start, end)

    if not claims or not outcomes:
        return None

    claim_texts = [c["paraphrased"] for c in claims]
    outcome_texts = [o["description"] for o in outcomes]

    embeddings = embedding_model.embed_claims_and_outcomes(claim_texts, outcome_texts)
    result = got.check_coherence(embeddings)

    store.save_coherence_score(
        id=str(uuid.uuid4()),
        entity_id=entity_id,
        topic=topic.value,
        period_start=start,
        period_end=end,
        score=result.coherence_score,
        n_claims=len(claims),
        n_outcomes=len(outcomes),
        n_contradictions=len(result.contradictions),
    )

    return result


def analyse_all_topics(
    store: GraphStore,
    entity_id: str,
    start: date,
    end: date,
    embedding_model: EmbeddingModel,
    got: GotBridge,
) -> dict[Topic, CoherenceResult | None]:
    """Run coherence analysis across all topics for an entity."""
    results = {}
    for topic in Topic:
        results[topic] = analyse_topic_coherence(
            store, entity_id, topic, start, end, embedding_model, got
        )
    return results
