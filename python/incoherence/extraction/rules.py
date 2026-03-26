"""Rule-based candidate extraction using keyword/regex patterns.

Identifies sentences likely to be institutional claims or measurable outcomes,
stages them as candidates for review or later LLM refinement.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Claim patterns — commitment language used by councils
# ---------------------------------------------------------------------------

CLAIM_PATTERNS: list[tuple[str, re.Pattern, float]] = [
    # (pattern_name, compiled_regex, base_confidence)
    ("investment_amount", re.compile(
        r"(?:invest|spending|funding|budget|allocated?)\w*\s+(?:of\s+)?£[\d,.]+ ?\w*",
        re.I,
    ), 0.7),
    ("target_number", re.compile(
        r"(?:target|goal|aim)\w*\s+(?:of|to)\s+[\d,]+",
        re.I,
    ), 0.7),
    ("will_deliver", re.compile(
        r"(?:will|shall|going to|plan(?:s|ning)? to|committed? to|pledg\w+|promis\w+)\s+"
        r"(?:deliver|build|create|provide|invest|reduce|improve|transform|develop|achiev|ensure|support)",
        re.I,
    ), 0.6),
    ("new_homes_jobs", re.compile(
        r"(?:[\d,]+)\s+(?:new\s+)?(?:homes?|dwell|jobs?|propert|affordable)",
        re.I,
    ), 0.65),
    ("carbon_climate", re.compile(
        r"(?:carbon\s+neutral|net\s+zero|climate\s+emergency|carbon\s+free)",
        re.I,
    ), 0.7),
    ("strategy_launch", re.compile(
        r"(?:launched?|adopted?|approved?|published?)\s+(?:a\s+|the\s+|its\s+)?"
        r"(?:new\s+)?(?:strategy|plan|framework|policy|programme|initiative|scheme)",
        re.I,
    ), 0.6),
    ("by_year", re.compile(
        r"by\s+20[2-4]\d",
        re.I,
    ), 0.5),
    ("million_pound", re.compile(
        r"£[\d,.]+ ?\s*(?:million|m|billion|bn)\b",
        re.I,
    ), 0.6),
]

# ---------------------------------------------------------------------------
# Outcome patterns — statistical/measurable language
# ---------------------------------------------------------------------------

OUTCOME_PATTERNS: list[tuple[str, re.Pattern, float]] = [
    ("percentage", re.compile(
        r"\b\d+\.?\d*\s*%",
    ), 0.5),
    ("ranked_nth", re.compile(
        r"(?:ranked?|position)\w*\s+(?:the\s+)?\d+(?:st|nd|rd|th)",
        re.I,
    ), 0.7),
    ("above_below_average", re.compile(
        r"(?:above|below|higher|lower|worse|better)\s+(?:than\s+)?(?:the\s+)?"
        r"(?:national|england|uk|regional|average)",
        re.I,
    ), 0.7),
    ("life_expectancy", re.compile(
        r"life\s+expectancy\b.*?\d+\.?\d*\s*(?:years?)?",
        re.I,
    ), 0.8),
    ("deprivation_index", re.compile(
        r"(?:IMD|index\s+of\s+(?:multiple\s+)?deprivation|most\s+deprived)",
        re.I,
    ), 0.75),
    ("increased_decreased", re.compile(
        r"(?:increased?|decreased?|risen?|fallen?|grew|dropped|declined?|worsened?|improved?)"
        r"\s+(?:from\s+)?[\d,.]+\s*%?\s*(?:to\s+[\d,.]+)?",
        re.I,
    ), 0.65),
    ("rate_per", re.compile(
        r"\b\d+\.?\d*\s+per\s+[\d,]+\b",
        re.I,
    ), 0.6),
    ("child_poverty", re.compile(
        r"(?:child\s+poverty|fuel\s+poverty|food\s+(?:bank|poverty)|free\s+school\s+meals)",
        re.I,
    ), 0.7),
    ("homelessness", re.compile(
        r"(?:homeless|rough\s+sleep|temporary\s+accommodation|housing\s+waiting\s+list)",
        re.I,
    ), 0.7),
    ("attainment_score", re.compile(
        r"(?:attainment\s+8|progress\s+8|GCSE|key\s+stage\s+\d)\b.*?\d+\.?\d*",
        re.I,
    ), 0.7),
    ("median_earnings", re.compile(
        r"(?:median|average)\s+(?:full[- ]time\s+)?(?:earnings?|wages?|salary|income)\b.*?£[\d,]+",
        re.I,
    ), 0.7),
    ("employment_rate", re.compile(
        r"(?:employment|unemployment|economic\s+inactivity)\s+rate\b.*?\d+\.?\d*\s*%",
        re.I,
    ), 0.7),
]

# ---------------------------------------------------------------------------
# Topic hinting — map keywords to likely topics
# ---------------------------------------------------------------------------

TOPIC_KEYWORDS: list[tuple[str, list[str]]] = [
    ("housing", ["hous", "home", "dwell", "tenant", "landlord", "rent", "affordable hous", "social hous"]),
    ("health", ["life expectancy", "health", "NHS", "hospital", "mortality", "disease", "mental health"]),
    ("poverty", ["poverty", "deprivat", "IMD", "free school meal", "food bank", "fuel poverty", "low income"]),
    ("education", ["school", "attainment", "GCSE", "key stage", "education", "SEND", "NEET"]),
    ("climate", ["carbon", "climate", "net zero", "emission", "renewable", "green"]),
    ("flooding", ["flood", "tidal", "drainage", "sea level"]),
    ("transport", ["A63", "bus", "rail", "cycling", "transport", "road", "traffic"]),
    ("regeneration", ["regenerat", "investment", "city of culture", "enterprise zone", "Siemens"]),
    ("economy", ["employment", "unemployment", "earnings", "wages", "jobs", "GDP", "labour market"]),
    ("transparency", ["FOI", "freedom of information", "budget", "spending", "audit"]),
    ("community", ["volunteer", "community", "consultation", "engagement"]),
    ("democracy", ["election", "councillor", "cabinet", "scrutiny", "vote"]),
]


def _guess_topic(text: str) -> str | None:
    """Guess the most likely topic from a sentence.

    Uses NLP zero-shot classification if available, falls back to keywords.
    """
    try:
        from .classifier import classify_topic
        return classify_topic(text)
    except Exception:
        # Fallback to keyword matching
        text_lower = text.lower()
        best_topic = None
        best_count = 0
        for topic, keywords in TOPIC_KEYWORDS:
            count = sum(1 for kw in keywords if kw.lower() in text_lower)
            if count > best_count:
                best_count = count
                best_topic = topic
        return best_topic if best_count > 0 else None


def _split_sentences(text: str) -> list[str]:
    """Split text into sentences, handling common abbreviations."""
    # Protect common abbreviations from splitting
    text = re.sub(r"(?<=[A-Z])\.(?=[A-Z])", "·", text)  # e.g. U.K.
    text = re.sub(r"\b(Mr|Mrs|Ms|Dr|Prof|St|etc|vs|approx|govt|dept)\.", r"\1·", text, flags=re.I)
    # Split on sentence boundaries
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z\d£\"'])", text)
    # Restore dots
    return [s.replace("·", ".").strip() for s in sentences if len(s.strip()) > 20]


@dataclass
class Candidate:
    """A candidate claim or outcome extracted by pattern matching."""

    id: str
    source_url: str
    source: str
    kind: str  # "claim" or "outcome"
    sentence: str
    topic_hint: str | None
    pattern_name: str
    confidence: float

    def to_row(self) -> tuple:
        return (
            self.id,
            self.source_url,
            self.source,
            self.kind,
            self.sentence,
            self.topic_hint,
            self.pattern_name,
            self.confidence,
        )


def extract_candidates(
    text: str,
    source_url: str,
    source: str,
    doc_type: str,
) -> list[Candidate]:
    """Extract candidate claims and outcomes from document text using patterns."""
    sentences = _split_sentences(text)
    candidates: list[Candidate] = []
    seen: set[str] = set()  # Deduplicate by sentence text

    for sentence in sentences:
        # Skip very short or very long sentences
        if len(sentence) < 30 or len(sentence) > 500:
            continue

        # Try claim patterns (primarily for press releases and minutes)
        if doc_type in ("press_release", "minutes"):
            for name, pattern, base_conf in CLAIM_PATTERNS:
                if pattern.search(sentence):
                    key = ("claim", sentence)
                    if key not in seen:
                        seen.add(key)
                        candidates.append(Candidate(
                            id=str(uuid.uuid4()),
                            source_url=source_url,
                            source=source,
                            kind="claim",
                            sentence=sentence,
                            topic_hint=_guess_topic(sentence),
                            pattern_name=name,
                            confidence=base_conf,
                        ))
                    break  # One match per sentence is enough

        # Try outcome patterns (primarily for stats pages)
        if doc_type in ("stats", "news_report", "press_release"):
            for name, pattern, base_conf in OUTCOME_PATTERNS:
                if pattern.search(sentence):
                    key = ("outcome", sentence)
                    if key not in seen:
                        seen.add(key)
                        candidates.append(Candidate(
                            id=str(uuid.uuid4()),
                            source_url=source_url,
                            source=source,
                            kind="outcome",
                            sentence=sentence,
                            topic_hint=_guess_topic(sentence),
                            pattern_name=name,
                            confidence=base_conf,
                        ))
                    break

    return candidates


def store_candidates(conn, candidates: list[Candidate]) -> int:
    """Insert candidates into the staging table. Returns count inserted."""
    n = 0
    for c in candidates:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO candidates
                   (id, source_url, source, kind, sentence, topic_hint, pattern_name, confidence)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                c.to_row(),
            )
            n += 1
        except Exception:
            continue
    conn.commit()
    return n
