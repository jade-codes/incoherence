"""Temporal coherence analysis — tracking incoherence over time."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from ..graph.model import Topic
from ..graph.storage import GraphStore
from ..extraction.embeddings import EmbeddingModel
from .coherence import analyse_topic_coherence
from .got_bridge import CoherenceResult, GotBridge


@dataclass
class TemporalSlice:
    start: date
    end: date
    result: CoherenceResult | None


def yearly_coherence(
    store: GraphStore,
    entity_id: str,
    topic: Topic,
    start_year: int,
    end_year: int,
    embedding_model: EmbeddingModel,
    got: GotBridge,
) -> list[TemporalSlice]:
    """Compute yearly coherence scores for a topic, building a timeline."""
    slices = []
    for year in range(start_year, end_year + 1):
        period_start = date(year, 1, 1)
        period_end = date(year, 12, 31)

        result = analyse_topic_coherence(
            store, entity_id, topic, period_start, period_end,
            embedding_model, got,
        )

        slices.append(TemporalSlice(
            start=period_start,
            end=period_end,
            result=result,
        ))

    return slices


def detect_coherence_drops(
    slices: list[TemporalSlice],
    threshold: float = 0.2,
) -> list[tuple[TemporalSlice, TemporalSlice, float]]:
    """Find consecutive periods where coherence dropped significantly.

    Returns tuples of (before, after, drop_magnitude).
    """
    drops = []
    scored = [(s, s.result.coherence_score) for s in slices if s.result is not None]

    for i in range(1, len(scored)):
        prev_slice, prev_score = scored[i - 1]
        curr_slice, curr_score = scored[i]
        drop = prev_score - curr_score
        if drop >= threshold:
            drops.append((prev_slice, curr_slice, drop))

    return drops
