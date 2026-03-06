"""Embedding generation using sentence-transformers."""

from __future__ import annotations

from sentence_transformers import SentenceTransformer


class ReviewEmbedder:
    def __init__(self, model_name: str) -> None:
        self.model = SentenceTransformer(model_name)

    def encode(self, texts: list[str]) -> list[list[float]]:
        vectors = self.model.encode(texts, normalize_embeddings=True)
        return [v.tolist() for v in vectors]
