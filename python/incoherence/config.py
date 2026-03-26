"""City configuration loader.

Reads a TOML file describing a city/region and provides typed access
to entities, data sources, geography codes, and other settings needed
by the discovery, extraction, and serving layers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class NewsConfig:
    url: str
    base_url: str | None = None
    date_pattern: str | None = None
    article_pattern: str | None = None
    # "path" = /page/2/, "query" = ?page=2
    pagination: str = "path"


@dataclass
class MinutesConfig:
    index_url: str
    base_url: str
    committees: list[str] = field(default_factory=list)
    # URL path segment that identifies committee/meeting links
    link_pattern: str = "/meetings/"


@dataclass
class JsnaConfig:
    sections: list[str] = field(default_factory=list)


@dataclass
class WdtkConfig:
    slug: str


@dataclass
class EntityConfig:
    id: str
    name: str
    source_key: str
    kind: str
    ons_code: str | None = None
    # LG Inform comparison group (default works for unitary authorities)
    lginform_group: str = "AllUnitaryLaInCountry_England"
    news: NewsConfig | None = None
    minutes: MinutesConfig | None = None
    jsna: JsnaConfig | None = None
    wdtk: WdtkConfig | None = None


@dataclass
class PoliceArea:
    name: str
    entity_id: str
    lat: float
    lng: float


@dataclass
class PoliceConfig:
    force: str
    areas: list[PoliceArea] = field(default_factory=list)


@dataclass
class CityConfig:
    name: str
    slug: str
    db: str
    entities: list[EntityConfig] = field(default_factory=list)
    police: PoliceConfig | None = None
    rate_limits: dict[str, float] = field(default_factory=dict)
    chat_examples: list[str] = field(default_factory=list)

    # Derived lookups (populated in __post_init__)
    _entity_by_id: dict[str, EntityConfig] = field(default_factory=dict, repr=False)
    _entity_by_source_key: dict[str, EntityConfig] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        self._entity_by_id = {e.id: e for e in self.entities}
        self._entity_by_source_key = {e.source_key: e for e in self.entities}

    def entity(self, id: str) -> EntityConfig | None:
        return self._entity_by_id.get(id)

    def entity_by_source(self, source_key: str) -> EntityConfig | None:
        return self._entity_by_source_key.get(source_key)

    @property
    def source_keys(self) -> list[str]:
        """All valid source keys for this city."""
        return [e.source_key for e in self.entities]

    @property
    def entity_map(self) -> dict[str, str]:
        """Map source_key -> entity_id (replaces the old hardcoded ENTITY_MAP)."""
        return {e.source_key: e.id for e in self.entities}

    @property
    def ons_codes(self) -> dict[str, str]:
        """Map source_key -> ONS code."""
        return {e.source_key: e.ons_code for e in self.entities if e.ons_code}

    @property
    def has_jsna(self) -> bool:
        return any(e.jsna and e.jsna.sections for e in self.entities)

    @property
    def has_ons(self) -> bool:
        """True if any entity has an ONS code (needed for NOMIS/Fingertips/LG Inform)."""
        return any(e.ons_code for e in self.entities)

    @property
    def has_police(self) -> bool:
        return bool(self.police and self.police.areas)

    @property
    def has_wdtk(self) -> bool:
        return any(e.wdtk for e in self.entities)

    @property
    def valid_sources(self) -> list[str]:
        """All valid --source choices for the CLI, based on what's configured."""
        sources = list(self.source_keys)

        def _add(key: str) -> None:
            if key not in sources:
                sources.append(key)

        if self.has_jsna:
            _add("jsna")
        if self.has_ons:
            for s in ["nomis", "fingertips", "lginform", "oflog", "cqc",
                       "housing", "land_registry"]:
                _add(s)
        if self.has_wdtk:
            _add("foi")
        if self.has_police:
            _add("police")
            _add("environment")
        # Always available (no special config needed)
        _add("planning")
        _add("parliamentary")
        return sources

    @property
    def db_path(self) -> Path:
        return Path(self.db)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _parse_entity(raw: dict) -> EntityConfig:
    """Parse a single [[entities]] table."""
    news = None
    if "news" in raw:
        n = raw["news"]
        news = NewsConfig(
            url=n["url"],
            base_url=n.get("base_url"),
            date_pattern=n.get("date_pattern"),
            article_pattern=n.get("article_pattern"),
            pagination=n.get("pagination", "path"),
        )

    minutes = None
    if "minutes" in raw:
        m = raw["minutes"]
        minutes = MinutesConfig(
            index_url=m["index_url"],
            base_url=m["base_url"],
            committees=m.get("committees", []),
            link_pattern=m.get("link_pattern", "/meetings/"),
        )

    jsna = None
    if "jsna" in raw:
        j = raw["jsna"]
        jsna = JsnaConfig(sections=j.get("sections", []))

    wdtk = None
    if "wdtk" in raw:
        wdtk = WdtkConfig(slug=raw["wdtk"]["slug"])

    return EntityConfig(
        id=raw["id"],
        name=raw["name"],
        source_key=raw["source_key"],
        kind=raw.get("kind", "local_authority"),
        ons_code=raw.get("ons_code"),
        lginform_group=raw.get("lginform_group", "AllUnitaryLaInCountry_England"),
        news=news,
        minutes=minutes,
        jsna=jsna,
        wdtk=wdtk,
    )


def load_city_config(path: str | Path) -> CityConfig:
    """Load a city configuration from a TOML file."""
    path = Path(path)
    with open(path, "rb") as f:
        raw = tomllib.load(f)

    city = raw.get("city", {})
    entities = [_parse_entity(e) for e in raw.get("entities", [])]

    police = None
    if "police" in raw:
        p = raw["police"]
        areas = [
            PoliceArea(
                name=a["name"],
                entity_id=a["entity_id"],
                lat=a["lat"],
                lng=a["lng"],
            )
            for a in p.get("areas", [])
        ]
        police = PoliceConfig(force=p["force"], areas=areas)

    chat = raw.get("chat", {})

    return CityConfig(
        name=city.get("name", path.stem),
        slug=city.get("slug", path.stem),
        db=city.get("db", f"data/{path.stem}.db"),
        entities=entities,
        police=police,
        rate_limits=raw.get("rate_limits", {}),
        chat_examples=chat.get("examples", []),
    )


def find_config(explicit: str | None = None) -> CityConfig:
    """Find and load a city config.

    Resolution order:
    1. Explicit path passed via --config
    2. INCOHERENCE_CONFIG env var
    3. cities/hull.toml (default)
    """
    if explicit:
        return load_city_config(explicit)

    env = os.environ.get("INCOHERENCE_CONFIG")
    if env:
        return load_city_config(env)

    # Default to hull
    default = Path("cities/hull.toml")
    if default.exists():
        return load_city_config(default)

    # Try relative to this file's project root
    project_root = Path(__file__).resolve().parent.parent.parent
    default = project_root / "cities" / "hull.toml"
    if default.exists():
        return load_city_config(default)

    raise FileNotFoundError(
        "No city config found. Pass --config or set INCOHERENCE_CONFIG."
    )
