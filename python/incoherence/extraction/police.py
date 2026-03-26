"""Direct extraction of outcomes from Police UK crime data."""

from __future__ import annotations

import re
import uuid
from datetime import date

from ..graph.model import Direction, Outcome, SourceType, Topic


def extract_outcomes_from_police(
    body: str,
    source_url: str,
    source: str,
    entity_map: dict[str, str] | None = None,
) -> list[Outcome]:
    """Parse structured police crime text and create an outcome."""
    lines = body.split("\n")
    area_name = ""
    period = ""
    total_crimes = 0
    categories: list[tuple[str, int, float]] = []

    for line in lines:
        line = line.strip()
        if line.startswith("Police UK Crime Data:"):
            area_name = line.split(":", 1)[1].strip()
        elif line.startswith("Period:"):
            period = line.split(":", 1)[1].strip()
        elif line.startswith("Total crimes reported:"):
            try:
                total_crimes = int(line.split(":")[1].strip())
            except ValueError:
                pass
        elif line.startswith("  ") and ":" in line and "%" in line:
            match = re.match(r"\s+(.+?):\s+(\d+)\s+\((\d+\.?\d*)%\)", line)
            if match:
                cat_name, count, pct = match.groups()
                categories.append((cat_name, int(count), float(pct)))

    if not total_crimes:
        return []

    _entity_map = entity_map or {}
    entity_id = _entity_map.get(source)

    top_3 = categories[:3]
    top_str = ", ".join(f"{c[0]} ({c[2]:.0f}%)" for c in top_3)
    description = (
        f"{total_crimes} crimes reported in {area_name} ({period}). "
        f"Top categories: {top_str}."
    )

    return [Outcome(
        id=str(uuid.uuid4()),
        date=date.today(),
        source_type=SourceType.ONS_DATA,
        description=description,
        topic=Topic.HEALTH,
        entity_id=entity_id,
        source_url=source_url,
        metric_name="total_crimes_reported",
        metric_value=float(total_crimes),
        metric_unit="crimes",
        direction=Direction.WORSENED,
    )]
