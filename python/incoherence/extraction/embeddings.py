"""Embedding generation for claims and outcomes using sentence-transformers."""

from __future__ import annotations

import struct
from pathlib import Path

from sentence_transformers import SentenceTransformer


class EmbeddingModel:
    """Wraps sentence-transformers for generating claim/outcome embeddings."""

    def __init__(self, model_name: str = "all-mpnet-base-v2", device: str = "cpu"):
        self.model = SentenceTransformer(model_name, device=device)
        self.dim = self.model.get_sentence_embedding_dimension()

    def embed(self, text: str) -> list[float]:
        """Embed a single text string."""
        vec = self.model.encode(text, normalize_embeddings=True)
        return vec.tolist()

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts."""
        vecs = self.model.encode(texts, normalize_embeddings=True, batch_size=32)
        return [v.tolist() for v in vecs]

    def embed_claims_and_outcomes(
        self, claims: list[str], outcomes: list[str]
    ) -> dict[str, list[float]]:
        """Embed all claims and outcomes, returning a dict suitable for GoT's PrecomputedEmbeddings.

        Keys are the text strings, values are the embedding vectors.
        """
        all_texts = claims + outcomes
        all_embeddings = self.embed_batch(all_texts)
        return dict(zip(all_texts, all_embeddings))
