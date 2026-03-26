"""Direct extraction of outcomes from Fingertips structured data.

Since Fingertips data is already structured (value, comparison, trend),
we can extract outcomes directly without LLM or regex — just parse the fields.
"""

from __future__ import annotations

import re
import uuid
from datetime import date

from ..graph.model import Direction, Outcome, SourceType, Topic

# Map Fingertips significance strings to Direction
SIGNIFICANCE_TO_DIRECTION = {
    "worse": Direction.WORSENED,
    "significantly worse than england": Direction.WORSENED,
    "higher": Direction.WORSENED,
    "lower": Direction.IMPROVED,
    "better": Direction.IMPROVED,
    "significantly better than england": Direction.IMPROVED,
    "similar": Direction.UNCHANGED,
    "similar to england": Direction.UNCHANGED,
    "not compared": None,
}

# For some indicators, "higher" is actually better
HIGHER_IS_BETTER = {
    "employment", "life expectancy", "healthy life expectancy",
    "breastfeeding", "vaccination", "screening", "school readiness",
    "physical activity", "attainment",
}

# Map indicator keywords to topics
INDICATOR_TO_TOPIC = {
    "life expectancy": "health",
    "healthy life": "health",
    "mortality": "health",
    "suicide": "health",
    "smoking": "health",
    "alcohol": "health",
    "obesity": "health",
    "hospital admission": "health",
    "mental health": "health",
    "dementia": "health",
    "falls": "health",
    "air pollution": "climate",
    "child poverty": "poverty",
    "fuel poverty": "poverty",
    "deprivation": "poverty",
    "free school meals": "poverty",
    "employment": "economy",
    "unemployment": "economy",
    "homelessness": "housing",
    "overcrowding": "housing",
    "attainment": "education",
    "school readiness": "education",
    "NEET": "education",
    "domestic abuse": "health",
    "violent crime": "health",
    "breastfeeding": "health",
    "vaccination": "health",
    "screening": "health",
}


def _guess_topic(indicator_name: str) -> str | None:
    name_lower = indicator_name.lower()
    for keyword, topic in INDICATOR_TO_TOPIC.items():
        if keyword.lower() in name_lower:
            return topic
    return "health"


def _infer_direction(significance: str, indicator_name: str) -> Direction | None:
    sig_lower = significance.lower().strip()
    name_lower = indicator_name.lower()
    flip = any(kw in name_lower for kw in HIGHER_IS_BETTER)

    for key, direction in SIGNIFICANCE_TO_DIRECTION.items():
        if key in sig_lower:
            if flip and direction == Direction.WORSENED:
                return Direction.IMPROVED
            elif flip and direction == Direction.IMPROVED:
                return Direction.WORSENED
            return direction

    return None


def extract_outcomes_from_fingertips(
    body: str,
    source_url: str,
    source: str,
    entity_map: dict[str, str] | None = None,
) -> list[Outcome]:
    """Parse structured Fingertips text and extract outcomes directly."""
    outcomes: list[Outcome] = []

    lines = body.split("\n")
    indicator_name = ""
    area_name = ""
    value = ""
    period = ""
    significance = ""
    trend = ""
    england_value = ""

    for line in lines:
        line = line.strip()
        if line.startswith("Fingertips Public Health Data:"):
            indicator_name = line.split(":", 1)[1].strip()
        elif line.startswith("Area:"):
            area_name = line.split(":", 1)[1].strip().split("(")[0].strip()
        elif line.startswith("Latest period:"):
            period = line.split(":", 1)[1].strip()
        elif line.startswith("Value:"):
            value = line.split(":", 1)[1].strip()
        elif line.startswith("Compared to England:"):
            significance = line.split(":", 1)[1].strip()
        elif line.startswith("Recent trend:"):
            trend = line.split(":", 1)[1].strip()
        elif line.startswith("England average"):
            england_value = line.split(":", 1)[1].strip()

    if not indicator_name or not value or value == "":
        return []

    parts = [f"{indicator_name} in {area_name} ({period}): {value}"]
    if england_value:
        parts.append(f"(England: {england_value})")
    if significance:
        parts.append(f"— {significance}")
    if trend:
        parts.append(f"[trend: {trend}]")

    description = " ".join(parts)
    direction = _infer_direction(significance, indicator_name)

    if not direction and trend:
        trend_lower = trend.lower()
        if "increasing" in trend_lower:
            flip = any(kw in indicator_name.lower() for kw in HIGHER_IS_BETTER)
            direction = Direction.IMPROVED if flip else Direction.WORSENED
        elif "decreasing" in trend_lower:
            flip = any(kw in indicator_name.lower() for kw in HIGHER_IS_BETTER)
            direction = Direction.WORSENED if flip else Direction.IMPROVED
        elif "no significant change" in trend_lower:
            direction = Direction.UNCHANGED

    topic_str = _guess_topic(indicator_name)
    try:
        topic = Topic(topic_str) if topic_str else Topic.HEALTH
    except ValueError:
        topic = Topic.HEALTH

    metric_value = None
    try:
        metric_value = float(value.replace(",", ""))
    except (ValueError, TypeError):
        pass

    _entity_map = entity_map or {}

    outcomes.append(Outcome(
        id=str(uuid.uuid4()),
        date=date.today(),
        source_type=SourceType.ONS_DATA,
        description=description,
        topic=topic,
        entity_id=_entity_map.get(source),
        source_url=source_url,
        metric_name=indicator_name,
        metric_value=metric_value,
        metric_unit=None,
        direction=direction,
    ))

    return outcomes
