"""Knowledge graph data model and SQLite schema for institutional claims and outcomes."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS entities (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    kind        TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS claims (
    id              TEXT PRIMARY KEY,
    entity_id       TEXT NOT NULL REFERENCES entities(id),
    date            TEXT NOT NULL,
    source_url      TEXT,
    source_type     TEXT NOT NULL,
    exact_quote     TEXT,
    paraphrased     TEXT NOT NULL,
    topic           TEXT NOT NULL,
    embedding       BLOB,
    confidence      REAL DEFAULT 1.0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS outcomes (
    id              TEXT PRIMARY KEY,
    entity_id       TEXT REFERENCES entities(id),
    date            TEXT NOT NULL,
    source_url      TEXT,
    source_type     TEXT NOT NULL,
    description     TEXT NOT NULL,
    topic           TEXT NOT NULL,
    metric_name     TEXT,
    metric_value    REAL,
    metric_unit     TEXT,
    direction       TEXT,
    embedding       BLOB,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS causal_links (
    id              TEXT PRIMARY KEY,
    claim_id        TEXT NOT NULL REFERENCES claims(id),
    outcome_id      TEXT NOT NULL REFERENCES outcomes(id),
    relationship    TEXT NOT NULL,
    evidence_text   TEXT,
    coherence_score REAL,
    severity        REAL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS coherence_scores (
    id              TEXT PRIMARY KEY,
    entity_id       TEXT NOT NULL REFERENCES entities(id),
    topic           TEXT NOT NULL,
    period_start    TEXT NOT NULL,
    period_end      TEXT NOT NULL,
    score           REAL NOT NULL,
    n_claims        INTEGER,
    n_outcomes      INTEGER,
    n_contradictions INTEGER,
    analysis_json   TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_claims_entity ON claims(entity_id);
CREATE INDEX IF NOT EXISTS idx_claims_topic ON claims(topic);
CREATE INDEX IF NOT EXISTS idx_claims_date ON claims(date);
CREATE INDEX IF NOT EXISTS idx_outcomes_topic ON outcomes(topic);
CREATE INDEX IF NOT EXISTS idx_outcomes_date ON outcomes(date);
CREATE INDEX IF NOT EXISTS idx_causal_links_claim ON causal_links(claim_id);
CREATE INDEX IF NOT EXISTS idx_causal_links_outcome ON causal_links(outcome_id);

CREATE TABLE IF NOT EXISTS scraped_urls (
    url           TEXT PRIMARY KEY,
    source        TEXT NOT NULL,
    doc_type      TEXT NOT NULL,
    discovered_at TEXT NOT NULL DEFAULT (datetime('now')),
    scraped_at    TEXT,
    extracted_at  TEXT,
    status        TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_scraped_urls_status ON scraped_urls(status);
CREATE INDEX IF NOT EXISTS idx_scraped_urls_source ON scraped_urls(source);

CREATE TABLE IF NOT EXISTS candidates (
    id              TEXT PRIMARY KEY,
    source_url      TEXT NOT NULL,
    source          TEXT NOT NULL,
    kind            TEXT NOT NULL,
    sentence        TEXT NOT NULL,
    topic_hint      TEXT,
    pattern_name    TEXT,
    confidence      REAL NOT NULL DEFAULT 0.5,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_candidates_status ON candidates(status);
CREATE INDEX IF NOT EXISTS idx_candidates_kind ON candidates(kind);
"""


class Topic(str, Enum):
    HOUSING = "housing"
    HEALTH = "health"
    POVERTY = "poverty"
    TRANSPARENCY = "transparency"
    FISCAL = "fiscal_responsibility"
    COMMUNITY = "community"
    DEMOCRACY = "democracy"
    REGENERATION = "regeneration"
    GROWTH = "growth"
    CLIMATE = "climate"
    FLOODING = "flooding"
    EDUCATION = "education"
    TRANSPORT = "transport"


class SourceType(str, Enum):
    MEETING_MINUTES = "meeting_minutes"
    PRESS_RELEASE = "press_release"
    STRATEGY_DOC = "strategy_document"
    SPEECH = "speech"
    ONS_DATA = "ons_data"
    COUNCIL_STATS = "council_stats"
    FOI_RESPONSE = "foi_response"
    NEWS_REPORT = "news_report"


class Direction(str, Enum):
    IMPROVED = "improved"
    WORSENED = "worsened"
    UNCHANGED = "unchanged"


class Relationship(str, Enum):
    FULFILLED = "fulfilled"
    CONTRADICTED = "contradicted"
    PARTIAL = "partial"
    UNRELATED = "unrelated"


@dataclass
class Entity:
    id: str
    name: str
    kind: str


@dataclass
class Claim:
    id: str
    entity_id: str
    date: date
    source_type: SourceType
    paraphrased: str
    topic: Topic
    source_url: str | None = None
    exact_quote: str | None = None
    embedding: list[float] | None = None
    confidence: float = 1.0


@dataclass
class Outcome:
    id: str
    date: date
    source_type: SourceType
    description: str
    topic: Topic
    entity_id: str | None = None
    source_url: str | None = None
    metric_name: str | None = None
    metric_value: float | None = None
    metric_unit: str | None = None
    direction: Direction | None = None
    embedding: list[float] | None = None


@dataclass
class CausalLink:
    id: str
    claim_id: str
    outcome_id: str
    relationship: Relationship
    evidence_text: str | None = None
    coherence_score: float | None = None
    severity: float | None = None


def init_db(db_path: Path) -> sqlite3.Connection:
    """Initialise the SQLite database with the knowledge graph schema."""
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA)
    return conn
