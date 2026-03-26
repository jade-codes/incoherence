"""Ingestion for FOI responses and open data from data.gov.uk."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from pathlib import Path

import httpx


@dataclass
class OpenDataset:
    """A dataset from data.gov.uk or council open data portals."""

    id: str
    title: str
    description: str
    url: str
    format: str  # "csv", "json", "xlsx"
    records: list[dict]


class FoiIngester:
    """Fetches and parses FOI responses and open data."""

    DATA_GOV_API = "https://data.gov.uk/api/action"

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.client = httpx.Client(timeout=30.0, follow_redirects=True)

    def search_data_gov(self, query: str, n_results: int = 10) -> list[dict]:
        """Search data.gov.uk for datasets related to Hull."""
        resp = self.client.get(
            f"{self.DATA_GOV_API}/package_search",
            params={"q": query, "rows": n_results},
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("result", {}).get("results", [])

    def fetch_csv(self, url: str) -> list[dict]:
        """Download and parse a CSV dataset."""
        resp = self.client.get(url)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        return list(reader)

    def fetch_json(self, url: str) -> list[dict]:
        """Download and parse a JSON dataset."""
        resp = self.client.get(url)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return [data]

    def save_dataset(self, dataset: OpenDataset) -> Path:
        out_path = self.output_dir / f"opendata_{dataset.id}.json"
        out_path.write_text(
            json.dumps(
                {
                    "id": dataset.id,
                    "title": dataset.title,
                    "description": dataset.description,
                    "url": dataset.url,
                    "format": dataset.format,
                    "n_records": len(dataset.records),
                    "records": dataset.records[:100],  # Cap for storage
                },
                indent=2,
            )
        )
        return out_path

    def close(self):
        self.client.close()
