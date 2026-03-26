"""Outcome extraction from statistical data and reports."""

from __future__ import annotations

import json
import uuid
from datetime import date

import anthropic

from ..graph.model import Direction, Outcome, SourceType, Topic

OUTCOME_EXTRACTION_PROMPT = """You are analysing statistical data and reports about Hull and East Riding of Yorkshire.
Extract measurable outcomes — things that actually happened, with data to back them up.

For each outcome found, return a JSON array of objects with these fields:
- "date": ISO date string (YYYY-MM-DD) when the outcome was measured/reported
- "description": a clear one-sentence description of what happened
- "topic": one of: housing, health, poverty, transparency, fiscal_responsibility, community, democracy, regeneration, growth, climate, flooding, education, transport
- "metric_name": the specific metric being measured (e.g. "rough_sleepers_count", "nhs_wait_weeks")
- "metric_value": the numeric value, or null if not quantifiable
- "metric_unit": the unit of measurement (e.g. "people", "weeks", "£millions")
- "direction": "improved", "worsened", or "unchanged" relative to the previous period

Return ONLY the JSON array, no other text."""


def extract_outcomes_from_text(
    text: str,
    entity_id: str | None,
    source_url: str | None,
    source_type: SourceType,
    api_key: str | None = None,
) -> list[Outcome]:
    """Use Claude to extract structured outcomes from statistical reports."""
    client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=OUTCOME_EXTRACTION_PROMPT,
            messages=[{"role": "user", "content": f"Extract outcomes from this data:\n\n{text[:15000]}"}],
        )
    except Exception:
        return []

    try:
        raw = json.loads(response.content[0].text)
    except (json.JSONDecodeError, IndexError):
        return []

    outcomes = []
    for item in raw:
        try:
            topic = Topic(item["topic"])
        except ValueError:
            continue

        direction = None
        if item.get("direction"):
            try:
                direction = Direction(item["direction"])
            except ValueError:
                pass

        outcome_date = date.today()
        if item.get("date"):
            try:
                outcome_date = date.fromisoformat(item["date"])
            except ValueError:
                pass

        outcomes.append(
            Outcome(
                id=str(uuid.uuid4()),
                entity_id=entity_id,
                date=outcome_date,
                source_url=source_url,
                source_type=source_type,
                description=item["description"],
                topic=topic,
                metric_name=item.get("metric_name"),
                metric_value=item.get("metric_value"),
                metric_unit=item.get("metric_unit"),
                direction=direction,
            )
        )

    return outcomes
