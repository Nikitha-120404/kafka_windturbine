"""Persist analytics gold tables in PostgreSQL."""

from __future__ import annotations

from sqlalchemy import create_engine, text


class GoldAnalyticsWriter:
    def __init__(self, dsn: str) -> None:
        self.engine = create_engine(dsn)

    def upsert_daily_metrics(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        stmt = text(
            """
            INSERT INTO gold_daily_metrics
                (product_id, date, review_count, avg_rating, pos_count, neu_count, neg_count)
            VALUES
                (:product_id, :date, :review_count, :avg_rating, :pos_count, :neu_count, :neg_count)
            ON CONFLICT (product_id, date) DO UPDATE SET
                review_count = EXCLUDED.review_count,
                avg_rating = EXCLUDED.avg_rating,
                pos_count = EXCLUDED.pos_count,
                neu_count = EXCLUDED.neu_count,
                neg_count = EXCLUDED.neg_count;
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt, rows)
        return len(rows)

    def upsert_feature_sentiment(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        stmt = text(
            """
            INSERT INTO gold_feature_sentiment
                (product_id, feature, pos_count, neu_count, neg_count)
            VALUES
                (:product_id, :feature, :pos_count, :neu_count, :neg_count)
            ON CONFLICT (product_id, feature) DO UPDATE SET
                pos_count = EXCLUDED.pos_count,
                neu_count = EXCLUDED.neu_count,
                neg_count = EXCLUDED.neg_count;
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt, rows)
        return len(rows)

    def upsert_top_issues(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        stmt = text(
            """
            INSERT INTO gold_top_issues
                (product_id, issue, neg_mentions_count, example_review_ids)
            VALUES
                (:product_id, :issue, :neg_mentions_count, :example_review_ids)
            ON CONFLICT (product_id, issue) DO UPDATE SET
                neg_mentions_count = EXCLUDED.neg_mentions_count,
                example_review_ids = EXCLUDED.example_review_ids;
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt, rows)
        return len(rows)
