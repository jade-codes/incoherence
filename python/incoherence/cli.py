"""CLI entry point for the Incoherence Detector."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from .config import CityConfig, find_config
from .graph.model import Topic, init_db
from .graph.storage import GraphStore
from .graph.queries import entity_summary, worst_contradictions


def _load_config(args: argparse.Namespace) -> CityConfig:
    """Load city config from --config flag, env var, or default."""
    return find_config(getattr(args, "config", None))


def cmd_init(args: argparse.Namespace) -> None:
    """Initialise the database."""
    city = _load_config(args)
    db_path = city.db_path
    conn = init_db(db_path)
    conn.close()
    print(f"Database initialised at {db_path}")


def cmd_summary(args: argparse.Namespace) -> None:
    """Print a summary for an entity."""
    city = _load_config(args)
    conn = init_db(city.db_path)
    summary = entity_summary(conn, args.entity)
    conn.close()

    print(f"Entity: {summary['entity_id']}")
    print(f"  Claims: {summary['n_claims']}")
    print(f"  Outcomes: {summary['n_outcomes']}")
    print(f"  Contradictions: {summary['n_contradictions']}")
    print(f"  Topics: {', '.join(summary['topics'])}")


def cmd_contradictions(args: argparse.Namespace) -> None:
    """List the worst contradictions."""
    city = _load_config(args)
    conn = init_db(city.db_path)
    contradictions = worst_contradictions(conn, limit=args.limit)
    conn.close()

    if not contradictions:
        print("No contradictions found.")
        return

    for i, c in enumerate(contradictions, 1):
        print(f"\n{i}. Severity: {c.get('severity', 'N/A')}")
        print(f"   Claim: {c['claim_text']}")
        print(f"   Outcome: {c['outcome_text']}")


def _make_pipeline(args: argparse.Namespace):
    """Create a Pipeline from CLI args."""
    import logging
    from .orchestrator import Pipeline

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    city = _load_config(args)
    output_dir = city.db_path.parent / "scraped"
    max_pages = getattr(args, "max_pages", 10)
    api_key = getattr(args, "api_key", None)

    return Pipeline(
        city=city,
        output_dir=output_dir,
        api_key=api_key,
        max_pages=max_pages,
    )


def cmd_discover(args: argparse.Namespace) -> None:
    """Discover new URLs from listing pages."""
    pipeline = _make_pipeline(args)
    try:
        sources = [args.source] if args.source else None
        n = pipeline.run_discovery(sources=sources)
        print(f"Discovered {n} new URLs")
        stats = pipeline.status()
        if stats:
            print(f"Pipeline status: {stats}")
    finally:
        pipeline.close()


def cmd_scrape(args: argparse.Namespace) -> None:
    """Scrape pending discovered URLs."""
    pipeline = _make_pipeline(args)
    try:
        n = pipeline.run_scrape(source=args.source, limit=args.limit)
        print(f"Scraped {n} documents")
    finally:
        pipeline.close()


def cmd_extract(args: argparse.Namespace) -> None:
    """Extract claims/outcomes from scraped documents."""
    pipeline = _make_pipeline(args)
    try:
        n = pipeline.run_extraction(source=args.source, limit=args.limit)
        print(f"Extracted {n} claims/outcomes")
    finally:
        pipeline.close()


def cmd_pipeline(args: argparse.Namespace) -> None:
    """Run full discovery -> scrape -> extract pipeline."""
    pipeline = _make_pipeline(args)
    try:
        sources = [args.source] if args.source else None
        results = pipeline.run_full(sources=sources, limit=args.limit)
        print(f"Pipeline complete:")
        print(f"  Discovered: {results['discovered']} new URLs")
        print(f"  Scraped:    {results['scraped']} documents")
        print(f"  Extracted:  {results['extracted']} claims/outcomes")
    finally:
        pipeline.close()


def cmd_candidates(args: argparse.Namespace) -> None:
    """Extract candidates using rule-based patterns (no API key needed)."""
    pipeline = _make_pipeline(args)
    try:
        n = pipeline.run_candidate_extraction(
            source=args.source, limit=args.limit
        )
        print(f"Extracted {n} candidates")

        conn = pipeline.conn
        conn.row_factory = None
        rows = conn.execute(
            "SELECT kind, COUNT(*) FROM candidates WHERE status = 'pending' GROUP BY kind"
        ).fetchall()
        for kind, count in rows:
            print(f"  {kind}: {count}")
    finally:
        pipeline.close()


def cmd_review(args: argparse.Namespace) -> None:
    """Review staged candidates interactively."""
    import sqlite3

    city = _load_config(args)
    conn = init_db(city.db_path)
    conn.row_factory = sqlite3.Row

    kind_filter = args.kind
    query = "SELECT * FROM candidates WHERE status = 'pending'"
    params: list = []
    if kind_filter:
        query += " AND kind = ?"
        params.append(kind_filter)
    query += " ORDER BY confidence DESC LIMIT ?"
    params.append(args.limit)

    rows = conn.execute(query, params).fetchall()

    if not rows:
        print("No pending candidates to review.")
        conn.close()
        return

    print(f"\n{len(rows)} candidates to review (sorted by confidence):\n")

    accepted = 0
    rejected = 0
    for i, row in enumerate(rows, 1):
        print(f"  [{i}/{len(rows)}] ({row['kind']}) conf={row['confidence']:.2f} pattern={row['pattern_name']}")
        print(f"  topic: {row['topic_hint'] or '?'}")
        print(f"  source: {row['source_url'][:80]}")
        print(f"  >>> {row['sentence'][:200]}")

        if args.auto_accept and row["confidence"] >= args.auto_accept:
            action = "a"
            print(f"  -> auto-accepted (conf >= {args.auto_accept})")
        else:
            action = input("  [a]ccept / [r]eject / [s]kip / [q]uit? ").strip().lower()

        if action == "q":
            break
        elif action == "a":
            conn.execute(
                "UPDATE candidates SET status = 'accepted' WHERE id = ?",
                (row["id"],),
            )
            accepted += 1
        elif action == "r":
            conn.execute(
                "UPDATE candidates SET status = 'rejected' WHERE id = ?",
                (row["id"],),
            )
            rejected += 1
        print()

    conn.commit()
    print(f"\nDone: {accepted} accepted, {rejected} rejected")

    n_accepted = conn.execute(
        "SELECT COUNT(*) FROM candidates WHERE status = 'accepted'"
    ).fetchone()[0]
    if n_accepted:
        print(f"\n{n_accepted} accepted candidates ready. Run 'hull promote' to add to the knowledge graph.")
    conn.close()


def cmd_promote(args: argparse.Namespace) -> None:
    """Promote accepted candidates into the claims/outcomes tables.

    Performs semantic deduplication: skips candidates that are too similar
    (cosine > 0.9) to an existing claim or outcome on the same topic.
    """
    import math
    import sqlite3
    import struct
    import uuid as uuid_mod
    from datetime import date as date_cls

    from .graph.model import SourceType, Topic
    from .graph.storage import GraphStore

    city = _load_config(args)
    conn = init_db(city.db_path)
    conn.row_factory = sqlite3.Row
    store = GraphStore(conn)
    entity_map = city.entity_map

    rows = conn.execute(
        "SELECT * FROM candidates WHERE status = 'accepted'"
    ).fetchall()

    if not rows:
        print("No accepted candidates to promote.")
        conn.close()
        return

    # Load existing embeddings for dedup comparison
    def _blob_to_list(blob):
        if not blob:
            return None
        n = len(blob) // 4
        return list(struct.unpack(f"{n}f", blob))

    def _cosine(a, b):
        dot = sum(x * y for x, y in zip(a, b))
        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(x * x for x in b))
        if na == 0 or nb == 0:
            return 0.0
        return dot / (na * nb)

    # Load embedding model for candidate texts
    try:
        from .extraction.embeddings import EmbeddingModel
        model = EmbeddingModel()
    except ImportError:
        model = None

    # Pre-load existing embeddings by topic
    existing_embeddings: dict[str, list[tuple[str, list[float]]]] = {}
    for table in ["claims", "outcomes"]:
        text_col = "paraphrased" if table == "claims" else "description"
        db_rows = conn.execute(
            f"SELECT topic, embedding FROM {table} WHERE embedding IS NOT NULL"
        ).fetchall()
        for r in db_rows:
            topic = r["topic"]
            emb = _blob_to_list(r["embedding"])
            if emb:
                existing_embeddings.setdefault(topic, []).append(emb)

    DEDUP_THRESHOLD = 0.9
    n_claims = 0
    n_outcomes = 0
    n_skipped = 0

    for row in rows:
        entity_id = entity_map.get(row["source"])
        topic_str = row["topic_hint"]

        try:
            topic = Topic(topic_str) if topic_str else None
        except ValueError:
            topic = None

        # Semantic dedup: check if this candidate is too similar to existing data
        if model and topic_str and topic_str in existing_embeddings:
            candidate_emb = model.embed(row["sentence"][:512])
            is_duplicate = any(
                _cosine(candidate_emb, existing_emb) > DEDUP_THRESHOLD
                for existing_emb in existing_embeddings[topic_str]
            )
            if is_duplicate:
                conn.execute(
                    "UPDATE candidates SET status = 'duplicate' WHERE id = ?",
                    (row["id"],),
                )
                n_skipped += 1
                continue

        if row["kind"] == "claim" and entity_id and topic:
            from .graph.model import Claim
            store.insert_claim(Claim(
                id=str(uuid_mod.uuid4()),
                entity_id=entity_id,
                date=date_cls.today(),
                source_type=SourceType.PRESS_RELEASE,
                paraphrased=row["sentence"],
                topic=topic,
                source_url=row["source_url"],
                confidence=row["confidence"],
            ))
            n_claims += 1

        elif row["kind"] == "outcome" and topic:
            from .graph.model import Outcome
            store.insert_outcome(Outcome(
                id=str(uuid_mod.uuid4()),
                date=date_cls.today(),
                source_type=SourceType.COUNCIL_STATS,
                description=row["sentence"],
                topic=topic,
                entity_id=entity_id,
                source_url=row["source_url"],
            ))
            n_outcomes += 1

        else:
            # Can't promote (missing topic or entity) — skip
            continue

        conn.execute(
            "UPDATE candidates SET status = 'promoted' WHERE id = ?",
            (row["id"],),
        )

    conn.commit()
    conn.close()
    print(f"Promoted {n_claims} claims and {n_outcomes} outcomes into the knowledge graph.")
    if n_skipped:
        print(f"Skipped {n_skipped} duplicates (cosine > {DEDUP_THRESHOLD})")


def cmd_status(args: argparse.Namespace) -> None:
    """Show pipeline status."""
    city = _load_config(args)
    conn = init_db(city.db_path)
    from .dedup import DeduplicationTracker

    tracker = DeduplicationTracker(conn)
    stats = tracker.stats()
    conn.close()

    if not stats:
        print("No URLs tracked yet. Run 'hull discover' first.")
        return

    total = sum(stats.values())
    print(f"Scraping pipeline status ({total} total URLs):")
    for status, count in sorted(stats.items()):
        print(f"  {status:12s}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="incoherence",
        description="Incoherence Detector — institutional accountability through geometric trust",
    )
    parser.add_argument("--config", default=None, help="Path to city config TOML file")

    sub = parser.add_subparsers(dest="command")

    sub.add_parser("init", help="Initialise the database")

    p_summary = sub.add_parser("summary", help="Show entity summary")
    p_summary.add_argument("entity", help="Entity ID")

    p_contra = sub.add_parser("contradictions", help="List worst contradictions")
    p_contra.add_argument("--limit", type=int, default=20, help="Max results")

    # --- Scraping pipeline commands ---

    p_discover = sub.add_parser("discover", help="Discover new URLs from council sites")
    p_discover.add_argument("--source", help="Only discover from this source")
    p_discover.add_argument(
        "--max-pages", type=int, default=10, help="Max listing pages to crawl per source"
    )

    p_scrape = sub.add_parser("scrape", help="Scrape pending discovered URLs")
    p_scrape.add_argument("--source", help="Only scrape URLs from this source")
    p_scrape.add_argument("--limit", type=int, default=50, help="Max URLs to scrape")

    p_extract = sub.add_parser("extract", help="Extract claims/outcomes from scraped docs")
    p_extract.add_argument("--source", help="Only extract from this source")
    p_extract.add_argument("--limit", type=int, default=20, help="Max docs to extract")
    p_extract.add_argument("--api-key", help="Anthropic API key (default: ANTHROPIC_API_KEY env)")

    p_pipeline = sub.add_parser("pipeline", help="Run full discover -> scrape -> extract")
    p_pipeline.add_argument("--source", help="Only process this source")
    p_pipeline.add_argument("--limit", type=int, default=50, help="Max URLs per stage")
    p_pipeline.add_argument("--max-pages", type=int, default=10, help="Max listing pages")
    p_pipeline.add_argument("--api-key", help="Anthropic API key")

    p_status = sub.add_parser("status", help="Show scraping pipeline status")

    # --- Candidate extraction (rule-based, no API key) ---

    p_cand = sub.add_parser("candidates", help="Extract candidates using pattern matching (no API key)")
    p_cand.add_argument("--source", help="Only extract from this source")
    p_cand.add_argument("--limit", type=int, default=200, help="Max docs to process")

    p_review = sub.add_parser("review", help="Review staged candidates interactively")
    p_review.add_argument("--kind", choices=["claim", "outcome"], help="Only review this type")
    p_review.add_argument("--limit", type=int, default=50, help="Max candidates to show")
    p_review.add_argument(
        "--auto-accept", type=float, default=None,
        help="Auto-accept candidates above this confidence (e.g. 0.7)",
    )

    p_promote = sub.add_parser("promote", help="Promote accepted candidates to knowledge graph")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    commands = {
        "init": cmd_init,
        "summary": cmd_summary,
        "contradictions": cmd_contradictions,
        "discover": cmd_discover,
        "scrape": cmd_scrape,
        "extract": cmd_extract,
        "pipeline": cmd_pipeline,
        "status": cmd_status,
        "candidates": cmd_candidates,
        "review": cmd_review,
        "promote": cmd_promote,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
