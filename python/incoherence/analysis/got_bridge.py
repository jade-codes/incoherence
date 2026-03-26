"""Bridge to the Geometry of Trust web API for coherence analysis."""

from __future__ import annotations

from dataclasses import dataclass

import httpx


@dataclass
class Contradiction:
    term_a: str
    term_b: str
    severity: float
    causal_cosine: float
    angle_degrees: float


@dataclass
class CoherenceResult:
    coherence_score: float
    contradictions: list[Contradiction]
    num_terms: int


class GotBridge:
    """Calls the GoT web API for coherence and conversation analysis."""

    def __init__(self, base_url: str = "http://localhost:3000"):
        self.base_url = base_url
        self.client = httpx.Client(timeout=30.0)

    def check_coherence(self, embeddings: dict[str, list[float]]) -> CoherenceResult:
        """POST embeddings to /api/coherence and parse the result."""
        resp = self.client.post(
            f"{self.base_url}/api/coherence",
            json={"embeddings": embeddings},
        )
        resp.raise_for_status()
        data = resp.json()

        contradictions = [
            Contradiction(
                term_a=c["term_a"],
                term_b=c["term_b"],
                severity=c["severity"],
                causal_cosine=c["causal_cosine"],
                angle_degrees=c["angle_degrees"],
            )
            for c in data.get("contradictions", [])
        ]

        return CoherenceResult(
            coherence_score=data["coherence_score"],
            contradictions=contradictions,
            num_terms=data.get("num_terms", len(embeddings)),
        )

    def analyse_conversation(self, messages: list[dict]) -> dict:
        """POST a chronological sequence of institutional claims as a 'conversation'.

        Each message should have: speaker, text, embedding (list[float]).
        Returns the full GoT conversation analysis response.
        """
        resp = self.client.post(
            f"{self.base_url}/api/conversation/analyse",
            json={"messages": messages},
        )
        resp.raise_for_status()
        return resp.json()

    def check_collapse(self, embeddings: dict[str, list[float]]) -> dict:
        """Check for manifold collapse — all rhetoric collapsing to one direction."""
        resp = self.client.post(
            f"{self.base_url}/api/collapse",
            json={"embeddings": embeddings},
        )
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self.client.close()
