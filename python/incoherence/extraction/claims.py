"""LLM-assisted claim extraction from raw council documents."""

from __future__ import annotations

import json
import uuid
from datetime import date

import anthropic

from ..graph.model import Claim, SourceType, Topic

# System prompt for structured claim extraction
EXTRACTION_PROMPT = """You are analysing documents from Hull City Council and East Riding of Yorkshire Council.
Extract institutional claims — statements where the council commits to, promises, or prioritises something.

For each claim found, return a JSON array of objects with these fields:
- "date": ISO date string (YYYY-MM-DD) if identifiable, or null
- "exact_quote": the verbatim text of the claim
- "paraphrased": a normalised one-sentence summary of what was claimed
- "topic": one of: housing, health, poverty, transparency, fiscal_responsibility, community, democracy, regeneration, growth, climate, flooding, education, transport
- "confidence": 0.0-1.0 how confident you are this is a genuine institutional claim (not just reporting)

Only extract claims made BY the institution (council, committee, cabinet member), not claims reported about them by others.
Return ONLY the JSON array, no other text."""


def extract_claims_from_text(
    text: str,
    entity_id: str,
    source_url: str | None,
    source_type: SourceType,
    api_key: str | None = None,
) -> list[Claim]:
    """Use Claude to extract structured claims from raw document text.

    Falls back to empty list if API is unavailable.
    """
    client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=EXTRACTION_PROMPT,
            messages=[{"role": "user", "content": f"Extract claims from this document:\n\n{text[:15000]}"}],
        )
    except Exception:
        return []

    try:
        raw = json.loads(response.content[0].text)
    except (json.JSONDecodeError, IndexError):
        return []

    claims = []
    for item in raw:
        try:
            topic = Topic(item["topic"])
        except ValueError:
            continue

        claim_date = None
        if item.get("date"):
            try:
                claim_date = date.fromisoformat(item["date"])
            except ValueError:
                claim_date = date.today()
        else:
            claim_date = date.today()

        claims.append(
            Claim(
                id=str(uuid.uuid4()),
                entity_id=entity_id,
                date=claim_date,
                source_url=source_url,
                source_type=source_type,
                exact_quote=item.get("exact_quote"),
                paraphrased=item["paraphrased"],
                topic=topic,
                confidence=float(item.get("confidence", 0.8)),
            )
        )

    return claims
