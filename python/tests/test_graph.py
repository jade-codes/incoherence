"""Tests for the knowledge graph model and storage."""

import sqlite3
from datetime import date
from pathlib import Path

from incoherence.graph.model import (
    Claim, CausalLink, Entity, Outcome,
    Direction, Relationship, SourceType, Topic,
    init_db,
)
from incoherence.graph.storage import GraphStore
from incoherence.graph.queries import entity_summary, worst_contradictions


def _make_db() -> sqlite3.Connection:
    return init_db(Path(":memory:"))


def test_init_db_creates_tables():
    conn = _make_db()
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = {t[0] for t in tables}
    assert "entities" in table_names
    assert "claims" in table_names
    assert "outcomes" in table_names
    assert "causal_links" in table_names
    assert "coherence_scores" in table_names


def test_insert_and_query_claim():
    conn = _make_db()
    store = GraphStore(conn)

    store.insert_entity(Entity(id="hull-cc", name="Hull City Council", kind="council"))
    store.insert_claim(Claim(
        id="c1",
        entity_id="hull-cc",
        date=date(2020, 3, 15),
        source_type=SourceType.PRESS_RELEASE,
        paraphrased="Hull City Council commits to building 1000 affordable homes by 2025",
        topic=Topic.HOUSING,
    ))

    claims = store.get_claims_by_topic("housing")
    assert len(claims) == 1
    assert claims[0]["paraphrased"] == "Hull City Council commits to building 1000 affordable homes by 2025"


def test_insert_and_query_outcome():
    conn = _make_db()
    store = GraphStore(conn)

    store.insert_entity(Entity(id="hull-cc", name="Hull City Council", kind="council"))
    store.insert_outcome(Outcome(
        id="o1",
        entity_id="hull-cc",
        date=date(2023, 6, 1),
        source_type=SourceType.ONS_DATA,
        description="Rough sleeping in Hull increased by 40% since 2020",
        topic=Topic.HOUSING,
        metric_name="rough_sleepers_pct_change",
        metric_value=40.0,
        metric_unit="percent",
        direction=Direction.WORSENED,
    ))

    outcomes = store.get_outcomes_by_topic("housing")
    assert len(outcomes) == 1


def test_contradictions_query():
    conn = _make_db()
    store = GraphStore(conn)

    store.insert_entity(Entity(id="hull-cc", name="Hull City Council", kind="council"))
    store.insert_claim(Claim(
        id="c1", entity_id="hull-cc", date=date(2020, 1, 1),
        source_type=SourceType.PRESS_RELEASE,
        paraphrased="We will end rough sleeping", topic=Topic.HOUSING,
    ))
    store.insert_outcome(Outcome(
        id="o1", entity_id="hull-cc", date=date(2023, 1, 1),
        source_type=SourceType.ONS_DATA,
        description="Rough sleeping doubled", topic=Topic.HOUSING,
        direction=Direction.WORSENED,
    ))
    store.insert_causal_link(CausalLink(
        id="l1", claim_id="c1", outcome_id="o1",
        relationship=Relationship.CONTRADICTED,
        severity=0.85,
    ))

    contradictions = store.get_contradictions()
    assert len(contradictions) == 1
    assert contradictions[0]["severity"] == 0.85


def test_entity_summary():
    conn = _make_db()
    store = GraphStore(conn)

    store.insert_entity(Entity(id="hull-cc", name="Hull City Council", kind="council"))
    store.insert_claim(Claim(
        id="c1", entity_id="hull-cc", date=date(2020, 1, 1),
        source_type=SourceType.PRESS_RELEASE,
        paraphrased="Invest in health", topic=Topic.HEALTH,
    ))

    summary = entity_summary(conn, "hull-cc")
    assert summary["n_claims"] == 1
    assert "health" in summary["topics"]
