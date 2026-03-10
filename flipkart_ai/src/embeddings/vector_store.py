"""Pgvector storage and similarity search."""

from __future__ import annotations

from typing import Any

import psycopg2
import psycopg2.extras


def _to_vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in vector) + "]"


class PgVectorStore:
    def __init__(self, host: str, port: int, db: str, user: str, password: str) -> None:
        self.conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)

    def upsert_embeddings(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO gold_review_embeddings
            (review_id, product_id, cleaned_text, rating, sentiment_label, embedding, updated_at_utc)
        VALUES
            (%(review_id)s, %(product_id)s, %(cleaned_text)s, %(rating)s, %(sentiment_label)s, %(embedding)s::vector, NOW())
        ON CONFLICT (review_id) DO UPDATE SET
            embedding = EXCLUDED.embedding,
            sentiment_label = EXCLUDED.sentiment_label,
            updated_at_utc = NOW();
        """
        payload = []
        for row in rows:
            payload.append(
                {
                    **row,
                    "embedding": _to_vector_literal(row["embedding"]),
                }
            )
        with self.conn, self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, payload, page_size=256)
        return len(rows)

    def fetch_unembedded_reviews(self, product_id: str | None = None) -> list[dict[str, Any]]:
        sql = """
        SELECT f.review_id, f.product_id, f.cleaned_text, f.rating, f.sentiment_label
        FROM gold_ml_features f
        LEFT JOIN gold_review_embeddings e ON e.review_id = f.review_id
        WHERE e.review_id IS NULL
        """
        params: list[Any] = []
        if product_id:
            sql += " AND f.product_id = %s"
            params.append(product_id)
        with self.conn, self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())

    def semantic_search(self, product_id: str, query_vector: list[float], top_k: int) -> list[dict[str, Any]]:
        sql = """
        SELECT review_id, cleaned_text, rating, sentiment_label,
               1 - (embedding <=> %s::vector) AS score
        FROM gold_review_embeddings
        WHERE product_id = %s
        ORDER BY embedding <=> %s::vector
        LIMIT %s;
        """
        v = _to_vector_literal(query_vector)
        with self.conn, self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (v, product_id, v, top_k))
            return list(cur.fetchall())
