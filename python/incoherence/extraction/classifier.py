"""Semantic text classifier for topic, direction, and contradiction validation.

Uses sentence-transformers (already loaded for embeddings) for fast
embedding-based classification instead of slow BART zero-shot.
For contradiction validation, uses BART zero-shot on individual pairs only.
"""

from __future__ import annotations

import logging
import math

log = logging.getLogger(__name__)

# Topic descriptions — these get embedded and compared against outcome text
TOPIC_DESCRIPTIONS = {
    "housing": "housing supply, new homes built, affordable housing delivery, homelessness, rough sleeping, housing waiting lists, dwelling stock, social housing, council rent, tenants",
    "health": "public health outcomes, life expectancy, mortality rates, disease prevalence, mental health, NHS services, hospital admissions, smoking rates, alcohol harm, drug misuse, obesity, physical activity, suicide, cancer, diabetes, vaccination uptake, screening coverage, breastfeeding, domestic abuse",
    "poverty": "poverty and deprivation, low income families, child poverty rates, fuel poverty, food banks, free school meals eligibility, benefits claimants, financial hardship, income deprivation",
    "education": "school attainment, GCSE results, Attainment 8 scores, key stage outcomes, NEET young people, school readiness, looked after children education, special educational needs, pupil outcomes",
    "climate": "climate change, carbon emissions, net zero targets, climate emergency, renewable energy, CO2 reduction, air pollution, environmental sustainability",
    "transport": "road infrastructure, public transport, bus services, rail connections, cycling infrastructure, traffic congestion, road building projects",
    "regeneration": "urban regeneration, economic development, city of culture, enterprise zone, capital investment, urban renewal",
    "economy": "employment rate, unemployment, median earnings, wages, job creation, labour market, business growth, economic output, economic inactivity",
    "flooding": "flood risk, flood defences, tidal flooding, drainage infrastructure, sea level rise",
}

DIRECTION_DESCRIPTIONS = {
    "worsened": "the situation has deteriorated, gotten worse, rates increased for negative indicators, below average, below target, worse than comparator, gap widened, declined",
    "improved": "the situation has improved, gotten better, rates decreased for negative indicators, above average, met target, better than comparator, gap narrowed, progress made",
    "unchanged": "no significant change, remained stable, similar to previous period, comparable to average",
}

_embed_model = None
_topic_embeddings = None
_direction_embeddings = None


def _get_embed_model():
    """Lazy-load the sentence-transformers model (same one used for embeddings)."""
    global _embed_model
    if _embed_model is None:
        from sentence_transformers import SentenceTransformer
        log.info("Loading sentence-transformers model for classification...")
        _embed_model = SentenceTransformer("all-mpnet-base-v2")
    return _embed_model


def _cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _get_topic_embeddings():
    """Embed all topic descriptions once."""
    global _topic_embeddings
    if _topic_embeddings is None:
        model = _get_embed_model()
        _topic_embeddings = {
            topic: model.encode(desc).tolist()
            for topic, desc in TOPIC_DESCRIPTIONS.items()
        }
    return _topic_embeddings


def _get_direction_embeddings():
    """Embed all direction descriptions once."""
    global _direction_embeddings
    if _direction_embeddings is None:
        model = _get_embed_model()
        _direction_embeddings = {
            d: model.encode(desc).tolist()
            for d, desc in DIRECTION_DESCRIPTIONS.items()
        }
    return _direction_embeddings


def classify_topic(text: str, threshold: float = 0.25) -> str | None:
    """Classify text into a topic via embedding similarity."""
    model = _get_embed_model()
    text_emb = model.encode(text[:512]).tolist()
    topic_embs = _get_topic_embeddings()

    best_topic = None
    best_score = -1
    for topic, emb in topic_embs.items():
        score = _cosine(text_emb, emb)
        if score > best_score:
            best_score = score
            best_topic = topic

    return best_topic if best_score >= threshold else None


def classify_direction(text: str, threshold: float = 0.2) -> str | None:
    """Classify whether an outcome worsened, improved, or is unchanged."""
    model = _get_embed_model()
    text_emb = model.encode(text[:512]).tolist()
    dir_embs = _get_direction_embeddings()

    best_dir = None
    best_score = -1
    for d, emb in dir_embs.items():
        score = _cosine(text_emb, emb)
        if score > best_score:
            best_score = score
            best_dir = d

    return best_dir if best_score >= threshold else None


def is_genuine_contradiction(claim_text: str, outcome_text: str) -> tuple[bool, float]:
    """Check if a claim-outcome pair is a genuine contradiction.

    Uses BART zero-shot on individual pairs (slow but accurate).
    Only called on candidate contradictions, not all pairs.
    """
    try:
        from transformers import pipeline
    except ImportError:
        # No transformers — fall back to assuming valid
        return True, 0.7

    global _bart_clf
    try:
        _bart_clf
    except NameError:
        _bart_clf = None

    if _bart_clf is None:
        log.info("Loading BART for contradiction validation...")
        _bart_clf = pipeline(
            "zero-shot-classification",
            model="facebook/bart-large-mnli",
            device=-1,
        )

    combined = f"The council claimed: {claim_text[:300]}\n\nThe outcome: {outcome_text[:300]}"

    labels = [
        "The outcome contradicts the claim — what was promised was not delivered",
        "The outcome confirms the claim — what was promised was delivered",
        "The outcome is about a different topic and is unrelated to the claim",
        "The outcome describes the same event as the claim",
    ]

    result = _bart_clf(combined, labels, multi_label=False)
    top_label = result["labels"][0]
    top_score = result["scores"][0]

    return "contradicts" in top_label, top_score


def classify_outcomes_in_db(conn, batch_size: int = 100) -> dict[str, int]:
    """Reclassify all outcomes using embedding similarity (fast)."""
    import sqlite3
    conn.row_factory = sqlite3.Row

    model = _get_embed_model()

    rows = conn.execute("SELECT rowid, description FROM outcomes").fetchall()
    log.info("Classifying %d outcomes via embedding similarity...", len(rows))

    # Batch encode all descriptions
    descriptions = [r["description"][:512] for r in rows if r["description"] and len(r["description"]) >= 30]
    valid_rows = [r for r in rows if r["description"] and len(r["description"]) >= 30]

    if not descriptions:
        return {"topic_changed": 0, "direction_changed": 0}

    log.info("  Encoding %d descriptions...", len(descriptions))
    all_embs = model.encode(descriptions, show_progress_bar=True, batch_size=64)

    topic_embs = _get_topic_embeddings()
    dir_embs = _get_direction_embeddings()

    counts = {"topic_changed": 0, "direction_changed": 0}

    for i, (row, emb) in enumerate(zip(valid_rows, all_embs)):
        emb_list = emb.tolist()

        # Topic
        best_topic = max(topic_embs, key=lambda t: _cosine(emb_list, topic_embs[t]))
        conn.execute("UPDATE outcomes SET topic = ? WHERE rowid = ?", (best_topic, row["rowid"]))
        counts["topic_changed"] += 1

        # Direction
        best_dir = max(dir_embs, key=lambda d: _cosine(emb_list, dir_embs[d]))
        conn.execute("UPDATE outcomes SET direction = ? WHERE rowid = ?", (best_dir, row["rowid"]))
        counts["direction_changed"] += 1

        if (i + 1) % batch_size == 0:
            conn.commit()
            log.info("  Classified %d/%d", i + 1, len(valid_rows))

    conn.commit()
    log.info("Classification complete: %s", counts)
    return counts
