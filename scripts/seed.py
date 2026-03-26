#!/usr/bin/env python3
"""Seed the Hull Incoherence database with known institutions, claims, and outcomes."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

# Add python/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from incoherence.graph.model import (
    Claim, Entity, Outcome,
    Direction, SourceType, Topic,
    init_db,
)
from incoherence.graph.storage import GraphStore


# Map topic strings from seed data to Topic enum
TOPIC_MAP = {
    "housing": Topic.HOUSING,
    "health": Topic.HEALTH,
    "poverty": Topic.POVERTY,
    "transparency": Topic.TRANSPARENCY,
    "fiscal_responsibility": Topic.FISCAL,
    "community": Topic.COMMUNITY,
    "democracy": Topic.DEMOCRACY,
    "regeneration": Topic.REGENERATION,
    "growth": Topic.GROWTH,
    "climate": Topic.CLIMATE,
    "flooding": Topic.FLOODING,
    "education": Topic.EDUCATION,
    "transport": Topic.TRANSPORT,
    "economy": Topic.GROWTH,
    "homelessness": Topic.HOUSING,
}


ENTITIES = [
    Entity(id="hull-cc", name="Hull City Council", kind="council"),
    Entity(id="east-riding", name="East Riding of Yorkshire Council", kind="council"),
    Entity(id="humber-lep", name="Humber Local Enterprise Partnership", kind="government_agency"),
    Entity(id="nhs-hull", name="NHS Humber and North Yorkshire ICB (Hull)", kind="nhs_trust"),
    Entity(id="siemens-gamesa", name="Siemens Gamesa Renewable Energy", kind="developer"),
]


def load_seed_data(data_dir: Path) -> tuple[list[dict], list[dict]]:
    claims_path = data_dir / "seed_claims.json"
    outcomes_path = data_dir / "seed_outcomes.json"

    claims = json.loads(claims_path.read_text()) if claims_path.exists() else []
    outcomes = json.loads(outcomes_path.read_text()) if outcomes_path.exists() else []

    return claims, outcomes


def seed_entities(store: GraphStore) -> None:
    for entity in ENTITIES:
        store.insert_entity(entity)
    print(f"  Seeded {len(ENTITIES)} entities")


def seed_claims(store: GraphStore, raw_claims: list[dict]) -> int:
    count = 0
    for item in raw_claims:
        topic_str = item.get("topic", "")
        topic = TOPIC_MAP.get(topic_str)
        if topic is None:
            print(f"  Warning: unknown topic '{topic_str}' for claim {item['id']}, skipping")
            continue

        claim = Claim(
            id=item["id"],
            entity_id=item["entity"],
            date=date.fromisoformat(item["date"]),
            source_type=SourceType.PRESS_RELEASE,
            paraphrased=item["text"],
            topic=topic,
            source_url=item.get("source_url"),
            confidence=0.95,
        )
        store.insert_claim(claim)
        count += 1
    return count


def seed_outcomes(store: GraphStore, raw_outcomes: list[dict]) -> int:
    count = 0
    for item in raw_outcomes:
        topic_str = item.get("topic", "")
        topic = TOPIC_MAP.get(topic_str)
        if topic is None:
            print(f"  Warning: unknown topic '{topic_str}' for outcome {item['id']}, skipping")
            continue

        # Infer direction from text
        text_lower = item["text"].lower()
        direction = None
        if any(w in text_lower for w in ["worsened", "worse", "declined", "fell", "lower", "below", "delay", "pushed back", "shortfall", "lost"]):
            direction = Direction.WORSENED
        elif any(w in text_lower for w in ["improved", "rose", "increase", "delivered", "created", "completed"]):
            direction = Direction.IMPROVED
        else:
            direction = Direction.UNCHANGED

        outcome = Outcome(
            id=item["id"],
            entity_id=item.get("entity"),
            date=date.fromisoformat(item["date"]),
            source_type=SourceType.ONS_DATA,
            description=item["text"],
            topic=topic,
            source_url=item.get("source_url"),
            direction=direction,
        )
        store.insert_outcome(outcome)
        count += 1
    return count


def main():
    project_root = Path(__file__).parent.parent
    db_path = project_root / "data" / "hull.db"
    data_dir = project_root / "data" / "raw"

    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Initialising database at {db_path}")
    conn = init_db(db_path)
    store = GraphStore(conn)

    seed_entities(store)

    raw_claims, raw_outcomes = load_seed_data(data_dir)

    n_claims = seed_claims(store, raw_claims)
    print(f"  Seeded {n_claims} claims")

    n_outcomes = seed_outcomes(store, raw_outcomes)
    print(f"  Seeded {n_outcomes} outcomes")

    conn.close()

    print(f"\nDone. Database at {db_path}")
    print(f"  {len(ENTITIES)} entities, {n_claims} claims, {n_outcomes} outcomes")


if __name__ == "__main__":
    main()
