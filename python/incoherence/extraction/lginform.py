"""Direct extraction of outcomes from LG Inform structured data."""

from __future__ import annotations

import re
import uuid
from datetime import date
from urllib.parse import parse_qs, urlparse

from ..graph.model import Direction, Outcome, SourceType, Topic

# Map metric IDs to topics and names (mirrors discovery/lginform.py)
METRIC_INFO = {
    850: ("housing", "Net additional dwellings"),
    4103: ("housing", "Affordable housing completions"),
    3112: ("housing", "Households in temporary accommodation"),
    8926: ("housing", "Homelessness acceptances per 1000 households"),
    11: ("housing", "Dwelling stock: local authority owned"),
    9: ("housing", "Dwelling stock: total"),
    2156: ("housing", "Average house price to earnings ratio"),
    126: ("economy", "Employment rate"),
    127: ("economy", "Unemployment rate"),
    8277: ("economy", "Median gross weekly pay"),
    3505: ("economy", "Business births rate"),
    14: ("education", "GCSE attainment"),
    2855: ("education", "Attainment 8 score"),
    8200: ("education", "NEETs percentage"),
    4558: ("poverty", "IMD average score"),
    3106: ("poverty", "Children in relative low income families"),
    3279: ("poverty", "Fuel poverty percentage"),
    3476: ("health", "Life expectancy at birth - male"),
    3477: ("health", "Life expectancy at birth - female"),
    2893: ("health", "Healthy life expectancy at birth - male"),
    3124: ("health", "Obesity prevalence - Year 6"),
    3125: ("health", "Smoking prevalence"),
    36: ("health", "Infant mortality rate"),
    1127: ("health", "Total recorded crime per 1000 pop"),
    10606: ("health", "Domestic abuse incidents per 1000 pop"),
    4636: ("climate", "CO2 emissions per capita"),
    4635: ("climate", "CO2 emissions total"),
}


def extract_outcomes_from_lginform(
    body: str,
    source_url: str,
    source: str,
    entity_map: dict[str, str] | None = None,
) -> list[Outcome]:
    """Parse LG Inform structured text and extract outcomes."""
    parsed = urlparse(source_url)
    params = parse_qs(parsed.query)
    metric_id = params.get("metricType", [None])[0]

    topic_str = "health"
    metric_name = "LG Inform metric"
    if metric_id:
        try:
            info = METRIC_INFO.get(int(metric_id))
            if info:
                topic_str, metric_name = info
        except (ValueError, TypeError):
            pass

    try:
        topic = Topic(topic_str)
    except ValueError:
        topic = Topic.HEALTH

    _entity_map = entity_map or {}
    entity_id = _entity_map.get(source)

    # Derive area name from entity config if available, else from source key
    area_name = source.replace("_", " ").title()

    values = re.findall(r'(?:value|Value|amount)\s*[:\s]+\s*([\d,.]+)', body)
    if not values:
        values = re.findall(r'\b(\d+\.?\d*)\b', body)

    metric_value = None
    if values:
        try:
            metric_value = float(values[0].replace(",", ""))
        except ValueError:
            pass

    description = f"{metric_name} in {area_name}: {metric_value if metric_value is not None else 'see data'} (source: LG Inform)"

    return [Outcome(
        id=str(uuid.uuid4()),
        date=date.today(),
        source_type=SourceType.COUNCIL_STATS,
        description=description,
        topic=topic,
        entity_id=entity_id,
        source_url=source_url,
        metric_name=metric_name,
        metric_value=metric_value,
        direction=None,
    )]
