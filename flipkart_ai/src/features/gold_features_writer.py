"""Persist gold ML features to PostgreSQL."""

from __future__ import annotations

from sqlalchemy import create_engine, text


class GoldFeaturesWriter:
    def __init__(self, dsn: str) -> None:
        self.engine = create_engine(dsn)

    def upsert(self, records: list[dict]) -> int:
        if not records:
            return 0
        stmt = text(
            """
            INSERT INTO gold_ml_features (
                review_id, product_id, processed_date_utc, rating, cleaned_text,
                review_length_chars, word_count, sentiment_score, sentiment_label,
                mentions_camera, mentions_battery, mentions_display, mentions_performance,
                mentions_heat, mentions_price, mentions_delivery, updated_at_utc
            ) VALUES (
                :review_id, :product_id, :processed_date_utc, :rating, :cleaned_text,
                :review_length_chars, :word_count, :sentiment_score, :sentiment_label,
                :mentions_camera, :mentions_battery, :mentions_display, :mentions_performance,
                :mentions_heat, :mentions_price, :mentions_delivery, NOW()
            ) ON CONFLICT (review_id) DO UPDATE SET
                sentiment_score = EXCLUDED.sentiment_score,
                sentiment_label = EXCLUDED.sentiment_label,
                updated_at_utc = NOW();
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt, records)
        return len(records)
