"""FastAPI server for the Incoherence Detector dashboard."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from .config import find_config
from .graph.model import init_db

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
WEB_DIR = PROJECT_ROOT / "web"

# Load city config
_city = find_config()
DB_PATH = PROJECT_ROOT / _city.db

app = FastAPI(title=f"{_city.name} Incoherence Detector")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_db() -> sqlite3.Connection:
    conn = init_db(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── API routes ──────────────────────────────────────────────


@app.get("/api/config")
def api_config():
    """Return city config for the frontend to dynamise titles/examples."""
    return {
        "name": _city.name,
        "slug": _city.slug,
        "entities": [
            {"id": e.id, "name": e.name, "source_key": e.source_key}
            for e in _city.entities
        ],
        "chat_examples": _city.chat_examples,
    }


@app.get("/api/graph")
def api_graph():
    """Return nodes and links for the knowledge graph visualisation."""
    conn = get_db()
    nodes = []
    links = []

    causal_rows = conn.execute(
        "SELECT claim_id, outcome_id, relationship, severity, coherence_score FROM causal_links"
    ).fetchall()

    linked_claim_ids = {r["claim_id"] for r in causal_rows}
    linked_outcome_ids = {r["outcome_id"] for r in causal_rows}

    for r in conn.execute("SELECT id, name, kind FROM entities").fetchall():
        nodes.append({"id": r["id"], "label": r["name"], "type": "entity"})

    for r in conn.execute("SELECT id, entity_id, paraphrased, topic, date, source_url FROM claims").fetchall():
        if r["id"] in linked_claim_ids:
            nodes.append({
                "id": r["id"],
                "label": r["paraphrased"][:60],
                "type": "claim",
                "topic": r["topic"],
                "date": r["date"],
                "source_url": r["source_url"],
            })
            links.append({
                "source": r["entity_id"],
                "target": r["id"],
                "relationship": "claims",
            })

    for r in conn.execute(
        "SELECT id, entity_id, description, topic, date, direction, source_url FROM outcomes"
    ).fetchall():
        if r["id"] in linked_outcome_ids:
            nodes.append({
                "id": r["id"],
                "label": r["description"][:60],
                "type": "outcome",
                "topic": r["topic"],
                "date": r["date"],
                "direction": r["direction"],
                "source_url": r["source_url"],
            })
            if r["entity_id"]:
                links.append({
                    "source": r["entity_id"],
                    "target": r["id"],
                    "relationship": "observed",
                })

    for r in causal_rows:
        links.append({
            "source": r["claim_id"],
            "target": r["outcome_id"],
            "relationship": r["relationship"],
            "severity": r["severity"],
            "coherence_score": r["coherence_score"],
        })

    conn.close()
    return {"nodes": nodes, "links": links}


@app.get("/api/coherence-history")
def api_coherence_history():
    conn = get_db()
    rows = conn.execute(
        """SELECT entity_id, topic, period_start, period_end, score,
                  n_claims, n_outcomes, n_contradictions
           FROM coherence_scores ORDER BY period_start"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/contradictions")
def api_contradictions():
    conn = get_db()
    rows = conn.execute(
        """SELECT cl.severity, cl.coherence_score as causal_cosine,
                  c.paraphrased as claim_text, c.date as claim_date, c.topic,
                  c.source_url as claim_url,
                  o.description as outcome_text, o.date as outcome_date, o.direction,
                  o.source_url as outcome_url,
                  cl.claim_id, cl.outcome_id
           FROM causal_links cl
           JOIN claims c ON cl.claim_id = c.id
           JOIN outcomes o ON cl.outcome_id = o.id
           WHERE cl.relationship = 'contradicted'
           ORDER BY cl.severity DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/timeline")
def api_timeline(topic: str = Query(default="all")):
    conn = get_db()

    claim_query = "SELECT id, entity_id, paraphrased as text, topic, date, source_url FROM claims"
    outcome_query = "SELECT id, entity_id, description as text, topic, date, direction, source_url FROM outcomes"

    params: list = []
    if topic != "all":
        claim_query += " WHERE topic = ?"
        outcome_query += " WHERE topic = ?"
        params = [topic]

    claim_query += " ORDER BY date"
    outcome_query += " ORDER BY date"

    claims = [dict(r) for r in conn.execute(claim_query, params).fetchall()]
    outcomes = [dict(r) for r in conn.execute(outcome_query, params).fetchall()]

    claim_ids = {c["id"] for c in claims}
    outcome_ids = {o["id"] for o in outcomes}

    all_links = [
        dict(r)
        for r in conn.execute(
            """SELECT claim_id, outcome_id, relationship, severity
               FROM causal_links WHERE relationship = 'contradicted'"""
        ).fetchall()
    ]
    links = [
        l for l in all_links
        if l["claim_id"] in claim_ids and l["outcome_id"] in outcome_ids
    ]

    conn.close()
    return {"claims": claims, "outcomes": outcomes, "links": links}


@app.post("/api/chat")
async def api_chat(request: Request):
    """Search the knowledge graph based on a natural language query."""
    import re

    body = await request.json()
    query = body.get("query", "").strip()

    if not query:
        return {"summary": "Please ask a question.", "claims": [], "outcomes": [], "contradictions": []}

    conn = get_db()
    query_lower = query.lower()

    # Detect topic from query
    topic_keywords = {
        "housing": ["housing", "home", "dwell", "rent", "homeless", "affordable"],
        "health": ["health", "life expectancy", "mortality", "smoking", "obesity", "hospital", "mental health", "disease", "nhs"],
        "poverty": ["poverty", "depriv", "imd", "food bank", "fuel poverty", "low income", "child poverty"],
        "climate": ["climate", "carbon", "net zero", "emission", "green"],
        "education": ["education", "school", "attainment", "gcse", "neet"],
        "transport": ["transport", "bus", "road", "cycling"],
        "regeneration": ["regeneration", "investment", "enterprise"],
        "economy": ["economy", "employment", "unemployment", "earnings", "wages", "jobs"],
        "flooding": ["flood", "tidal", "drainage"],
    }

    matched_topics = []
    for topic, keywords in topic_keywords.items():
        if any(kw in query_lower for kw in keywords):
            matched_topics.append(topic)

    # Detect entity from query using config
    entity_filter = None
    for entity in _city.entities:
        # Check if the entity name or keywords appear in the query
        name_lower = entity.name.lower()
        name_parts = name_lower.split()
        if any(part in query_lower for part in name_parts if len(part) > 3):
            entity_filter = entity.id
            break

    # Detect query type
    wants_claims = any(w in query_lower for w in ["claim", "said", "promise", "commit", "plan", "target", "council claim"])
    wants_outcomes = any(w in query_lower for w in ["outcome", "result", "happen", "actual", "data", "show", "compare"])
    wants_contradictions = any(w in query_lower for w in ["contradiction", "incoher", "worst", "gap", "fail", "broken"])

    if not wants_claims and not wants_outcomes and not wants_contradictions:
        wants_claims = wants_outcomes = wants_contradictions = True

    stop_words = {"what", "are", "the", "in", "for", "how", "does", "do", "is", "show", "me", "about", "of", "on", "to", "a", "an", "and", "or", "did", "has", "have", "with"}
    search_terms = [w for w in re.findall(r'\w+', query_lower) if w not in stop_words and len(w) > 2]

    results_claims = []
    results_outcomes = []
    results_contradictions = []

    if wants_claims:
        claim_query = "SELECT id, entity_id, paraphrased as text, topic, date, source_url FROM claims WHERE 1=1"
        params = []
        if matched_topics:
            placeholders = ",".join("?" * len(matched_topics))
            claim_query += f" AND topic IN ({placeholders})"
            params.extend(matched_topics)
        if entity_filter:
            claim_query += " AND entity_id = ?"
            params.append(entity_filter)

        rows = conn.execute(claim_query + " ORDER BY date DESC", params).fetchall()

        scored = []
        for r in rows:
            text = (r["text"] or "").lower()
            score = sum(1 for t in search_terms if t in text)
            if score > 0 or not search_terms:
                scored.append((score, dict(r)))

        scored.sort(key=lambda x: -x[0])
        results_claims = [s[1] for s in scored[:10]]

    if wants_outcomes:
        outcome_query = "SELECT id, entity_id, description as text, topic, date, direction, source_url FROM outcomes WHERE 1=1"
        params = []
        if matched_topics:
            placeholders = ",".join("?" * len(matched_topics))
            outcome_query += f" AND topic IN ({placeholders})"
            params.extend(matched_topics)
        if entity_filter:
            outcome_query += " AND (entity_id = ? OR entity_id IS NULL)"
            params.append(entity_filter)

        rows = conn.execute(outcome_query + " ORDER BY date DESC", params).fetchall()

        scored = []
        for r in rows:
            text = (r["text"] or "").lower()
            score = sum(1 for t in search_terms if t in text)
            if r["direction"]:
                score += 0.5
            if score > 0 or not search_terms:
                scored.append((score, dict(r)))

        scored.sort(key=lambda x: -x[0])
        results_outcomes = [s[1] for s in scored[:10]]

    if wants_contradictions:
        contra_query = """
            SELECT cl.severity, c.paraphrased as claim_text, c.topic,
                   c.source_url as claim_url,
                   o.description as outcome_text, o.direction,
                   o.source_url as outcome_url
            FROM causal_links cl
            JOIN claims c ON cl.claim_id = c.id
            JOIN outcomes o ON cl.outcome_id = o.id
            WHERE cl.relationship = 'contradicted'
        """
        params = []
        if matched_topics:
            placeholders = ",".join("?" * len(matched_topics))
            contra_query += f" AND c.topic IN ({placeholders})"
            params.extend(matched_topics)
        if entity_filter:
            contra_query += " AND c.entity_id = ?"
            params.append(entity_filter)

        contra_query += " ORDER BY cl.severity DESC"
        rows = conn.execute(contra_query, params).fetchall()

        scored = []
        for r in rows:
            text = ((r["claim_text"] or "") + " " + (r["outcome_text"] or "")).lower()
            score = sum(1 for t in search_terms if t in text)
            score += r["severity"] or 0
            scored.append((score, dict(r)))

        scored.sort(key=lambda x: -x[0])
        results_contradictions = [s[1] for s in scored[:10]]

    conn.close()

    parts = []
    if results_contradictions:
        worst = results_contradictions[0]
        parts.append(f"Found {len(results_contradictions)} contradiction(s)")
        topic_str = ", ".join(matched_topics) if matched_topics else "all topics"
        parts.append(f"for {topic_str}")
        parts.append(f"(worst severity: {worst['severity']*100:.0f}%).")
    elif results_claims or results_outcomes:
        parts.append(f"Found {len(results_claims)} claim(s) and {len(results_outcomes)} outcome(s).")
    else:
        parts.append("No results found for that query.")

    summary = " ".join(parts)

    return {
        "summary": summary,
        "claims": results_claims,
        "outcomes": results_outcomes,
        "contradictions": results_contradictions,
    }


# ── Static files (serve the web dashboard) ──────────────────

app.mount("/css", StaticFiles(directory=str(WEB_DIR / "css")), name="css")
app.mount("/js", StaticFiles(directory=str(WEB_DIR / "js")), name="js")


@app.get("/")
def index():
    return FileResponse(str(WEB_DIR / "index.html"))
