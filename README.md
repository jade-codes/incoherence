# Incoherence Detector

Detects contradictions between what institutions say and what actually happens. Scrapes council press releases, meeting minutes, FOI responses, and public health/economic data, then uses geometric trust analysis to surface where stated commitments diverge from measured outcomes.

Built for Hull & East Riding, but config-driven so any English city can plug in their own sources.

## How it works

1. **Discover** — Crawls council news pages, committee minutes, JSNA health data, NOMIS labour stats, Fingertips public health indicators, Police UK crime data, LG Inform metrics, WhatDoTheyKnow FOI requests, and MHCLG housing statistics
2. **Scrape** — Downloads and parses HTML pages, PDFs, and API responses
3. **Extract** — Identifies claims (commitments, targets, pledges) and outcomes (statistics, measurements, trends) using rule-based patterns or LLM extraction
4. **Analyse** — Generates sentence embeddings, computes cosine similarity between claims and outcomes, validates contradictions with NLP classification, and scores institutional coherence per topic
5. **Serve** — Web dashboard with knowledge graph, timeline, coherence charts, contradiction panel, and chat search

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .

# Initialise database and run the pipeline
incoherence init
incoherence pipeline

# Extract candidates without an API key (rule-based)
incoherence candidates

# Run coherence analysis (requires sentence-transformers)
python scripts/analyse.py

# Start the web dashboard
uvicorn incoherence.server:app --port 8000
```

## Using a different city

Create a config file in `cities/`:

```toml
# cities/manchester.toml
[city]
name = "Manchester"
slug = "manchester"
db = "data/manchester.db"

[[entities]]
id = "manchester-cc"
name = "Manchester City Council"
source_key = "manchester_cc"
kind = "local_authority"
ons_code = "E08000003"

[entities.news]
url = "https://www.manchester.gov.uk/news"
base_url = "https://www.manchester.gov.uk"

[entities.wdtk]
slug = "manchester_city_council"

[police]
force = "greater-manchester"

[[police.areas]]
name = "Manchester"
entity_id = "manchester-cc"
lat = 53.4808
lng = -2.2426

[rate_limits]
"www.manchester.gov.uk" = 3.0

[chat]
examples = [
    "Worst housing contradictions",
    "Health outcomes for Manchester",
    "Climate claims",
]
```

Then run:

```bash
incoherence --config cities/manchester.toml init
incoherence --config cities/manchester.toml pipeline
```

National data sources (Fingertips, NOMIS, LG Inform, Police UK, WhatDoTheyKnow, MHCLG housing) work automatically for any English local authority — they just need the ONS geography code in the config.

## CLI commands

| Command | Description |
|---|---|
| `incoherence init` | Create the SQLite database |
| `incoherence discover` | Find new URLs from configured sources |
| `incoherence scrape` | Download pending discovered pages |
| `incoherence extract` | Extract claims/outcomes using LLM (needs API key) |
| `incoherence candidates` | Extract candidates using regex patterns (no API key) |
| `incoherence review` | Interactively review extracted candidates |
| `incoherence promote` | Move accepted candidates into the knowledge graph |
| `incoherence pipeline` | Run discover + scrape + extract in sequence |
| `incoherence status` | Show pipeline progress |
| `incoherence contradictions` | List worst contradictions found |
| `incoherence summary <entity>` | Show stats for an entity |

All commands accept `--config <path>` to target a different city. Defaults to `cities/hull.toml`.

## Data sources

| Source | Type | API key needed |
|---|---|---|
| Council news/press releases | Claims | No |
| Council meeting minutes | Claims | No |
| WhatDoTheyKnow FOI requests | Claims | No |
| Hull/East Riding JSNA | Outcomes | No |
| OHID Fingertips | Outcomes | No |
| NOMIS labour market | Outcomes | No |
| LG Inform metrics | Outcomes | No (optional key for higher limits) |
| Police UK crime data | Outcomes | No |
| MHCLG housing statistics | Outcomes | No |

## Project structure

```
cities/              City config files (TOML)
python/incoherence/  Python package
  config.py          City config loader
  cli.py             CLI entry point
  server.py          FastAPI dashboard server
  orchestrator.py    Pipeline coordinator
  discovery/         URL discovery modules
  extraction/        Claim/outcome extraction
  scraper/           Page downloading/parsing
  graph/             Knowledge graph model + storage
scripts/             Analysis and seeding scripts
web/                 Static dashboard (HTML/CSS/JS)
data/                SQLite databases and scraped documents
```

## Deploying to Firebase

```bash
# Export API data as static JSON
uvicorn incoherence.server:app --port 8000 &
for ep in config graph contradictions coherence-history timeline; do
  curl -s localhost:8000/api/$ep > web/api/$ep.json
done

# Deploy
firebase deploy --only hosting
```

## License

MIT
