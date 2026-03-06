"""Write silver datasets to parquet and PostgreSQL."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text


def write_silver_parquet(records: list[dict[str, Any]], data_dir: str, product_id: str, run_date: str) -> Path:
    out_dir = Path(data_dir) / "silver" / product_id / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "reviews.parquet"
    pd.DataFrame(records).to_parquet(out_path, index=False)
    return out_path


class SilverPostgresWriter:
    def __init__(self, dsn: str) -> None:
        self.engine = create_engine(dsn)

    def upsert(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        stmt = text(
            """
            INSERT INTO flipkart_reviews_silver (
                review_id, product_id, product_slug, rating, title, review_text_raw, cleaned_text,
                reviewer_name, location_raw, buyer_status, city, date_text_raw, processed_date_utc,
                source_url, page_number, scrape_timestamp_utc, ingestion_run_id, is_valid, invalid_reason,
                updated_at_utc
            ) VALUES (
                :review_id, :product_id, :product_slug, :rating, :title, :review_text_raw, :cleaned_text,
                :reviewer_name, :location_raw, :buyer_status, :city, :date_text_raw, :processed_date_utc,
                :source_url, :page_number, :scrape_timestamp_utc, :ingestion_run_id, :is_valid, :invalid_reason,
                NOW()
            )
            ON CONFLICT (review_id) DO UPDATE SET
                product_id = EXCLUDED.product_id,
                rating = EXCLUDED.rating,
                cleaned_text = EXCLUDED.cleaned_text,
                processed_date_utc = EXCLUDED.processed_date_utc,
                is_valid = EXCLUDED.is_valid,
                invalid_reason = EXCLUDED.invalid_reason,
                updated_at_utc = NOW();
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt, records)
        return len(records)
