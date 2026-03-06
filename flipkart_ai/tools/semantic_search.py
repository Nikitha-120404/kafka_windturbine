"""Semantic search CLI for review embeddings."""

from __future__ import annotations

import argparse

from src.config.settings import settings
from src.embeddings.embedder import ReviewEmbedder
from src.embeddings.vector_store import PgVectorStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product_id", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--top_k", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    embedder = ReviewEmbedder(settings.embedding_model)
    query_vec = embedder.encode([args.query])[0]

    store = PgVectorStore(
        settings.postgres_host,
        settings.postgres_port,
        settings.postgres_db,
        settings.postgres_user,
        settings.postgres_password,
    )
    rows = store.semantic_search(args.product_id, query_vec, args.top_k)
    for i, row in enumerate(rows, start=1):
        snippet = (row["cleaned_text"] or "")[:180]
        print(
            f"{i}. score={row['score']:.4f} rating={row['rating']} sentiment={row['sentiment_label']} "
            f"review_id={row['review_id']}\n   {snippet}"
        )


if __name__ == "__main__":
    main()
