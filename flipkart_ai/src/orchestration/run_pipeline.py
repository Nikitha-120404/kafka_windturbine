"""CLI entrypoint for batch flipkart pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from src.analytics.aggregations import (
    aggregate_daily_metrics,
    aggregate_feature_sentiment,
    aggregate_top_issues,
)
from src.analytics.gold_analytics_writer import GoldAnalyticsWriter
from src.bronze.mongo_writer import BronzeMongoWriter
from src.config.settings import settings
from src.embeddings.embedder import ReviewEmbedder
from src.embeddings.vector_store import PgVectorStore
from src.extract.flipkart_scraper import scrape_reviews
from src.extract.parser import parse_product_metadata
from src.features.feature_engineering import build_ml_features
from src.features.gold_features_writer import GoldFeaturesWriter
from src.quality.dq_report import build_dq_report
from src.quality.validators import validate_record
from src.transform.cleaner import clean_review_text
from src.transform.date_normalizer import normalize_review_date
from src.transform.location_normalizer import normalize_location
from src.transform.silver_writer import SilverPostgresWriter, write_silver_parquet
from src.utils.http import HttpClient
from src.utils.logging import configure_logging

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flipkart batch pipeline")
    parser.add_argument("--url", required=True)
    parser.add_argument("--max_pages", type=int, default=200)
    parser.add_argument(
        "--mode",
        choices=["full", "extract_only", "transform_only", "embed_only", "analytics_only"],
        default="full",
    )
    parser.add_argument("--since", default=None)
    return parser.parse_args()


def _create_pipeline_run(engine, run_id: str, mode: str, url: str, product_id: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO pipeline_runs(run_id, start_time_utc, status, mode, source_url, product_id)
                VALUES (:run_id, NOW(), 'RUNNING', :mode, :url, :product_id)
                """
            ),
            {"run_id": run_id, "mode": mode, "url": url, "product_id": product_id},
        )


def _finalize_pipeline_run(engine, run_id: str, status: str, metrics: dict) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE pipeline_runs
                SET end_time_utc = NOW(), status = :status,
                    extracted_count = :extracted_count,
                    bronze_upserts = :bronze_upserts,
                    silver_valid_count = :silver_valid_count,
                    silver_invalid_count = :silver_invalid_count,
                    embedded_count = :embedded_count,
                    notes = :notes::jsonb
                WHERE run_id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "extracted_count": metrics.get("extracted_count", 0),
                "bronze_upserts": metrics.get("bronze_upserts", 0),
                "silver_valid_count": metrics.get("silver_valid_count", 0),
                "silver_invalid_count": metrics.get("silver_invalid_count", 0),
                "embedded_count": metrics.get("embedded_count", 0),
                "notes": json.dumps(metrics.get("dq_report", {})),
            },
        )


def main() -> None:
    args = parse_args()
    configure_logging(settings.log_level)

    run_id = str(uuid.uuid4())
    metadata = parse_product_metadata(args.url)
    product_id = metadata["product_id"]

    engine = create_engine(settings.postgres_dsn)
    _create_pipeline_run(engine, run_id, args.mode, args.url, product_id)

    metrics: dict[str, object] = {}

    try:
        bronze = BronzeMongoWriter(settings.mongo_uri, settings.mongo_db, settings.mongo_bronze_collection)
        silver_writer = SilverPostgresWriter(settings.postgres_dsn)

        records: list[dict] = []
        if args.mode in {"full", "extract_only"}:
            http_client = HttpClient(
                timeout_seconds=settings.request_timeout_seconds,
                max_retries=settings.request_max_retries,
                backoff_base=settings.request_backoff_base,
                sleep_seconds=settings.request_sleep_seconds,
            )
            records = scrape_reviews(args.url, args.max_pages, http_client)
            metrics["extracted_count"] = len(records)
            metrics["bronze_upserts"] = bronze.upsert_reviews(records, run_id)

        if args.mode in {"full", "transform_only", "analytics_only", "embed_only"}:
            bronze_docs = bronze.fetch_reviews(product_id=product_id, since_iso=args.since)
            silver_records: list[dict] = []
            valid_rows: list[dict] = []
            invalid_count = 0

            for rec in bronze_docs:
                cleaned = clean_review_text(rec.get("review_text_raw", ""))
                processed_date = normalize_review_date(rec.get("date_text_raw", ""), rec["scrape_timestamp_utc"])
                buyer_status, city = normalize_location(rec.get("location_raw", ""))

                silver = {
                    **rec,
                    "cleaned_text": cleaned,
                    "processed_date_utc": processed_date,
                    "buyer_status": buyer_status,
                    "city": city,
                }
                is_valid, reason = validate_record(silver)
                silver["is_valid"] = is_valid
                silver["invalid_reason"] = reason
                silver_records.append(silver)
                if is_valid:
                    valid_rows.append(silver)
                else:
                    invalid_count += 1

            write_silver_parquet(
                silver_records,
                settings.data_dir,
                product_id,
                datetime.now(timezone.utc).date().isoformat(),
            )
            silver_writer.upsert(silver_records)
            metrics["silver_valid_count"] = len(valid_rows)
            metrics["silver_invalid_count"] = invalid_count
            metrics["dq_report"] = build_dq_report(silver_records)

        feature_rows: list[dict] = []
        if args.mode in {"full", "analytics_only", "embed_only"}:
            feature_rows = build_ml_features(valid_rows)
            feat_writer = GoldFeaturesWriter(settings.postgres_dsn)
            feat_writer.upsert(feature_rows)

            analytics_writer = GoldAnalyticsWriter(settings.postgres_dsn)
            analytics_writer.upsert_daily_metrics(aggregate_daily_metrics(feature_rows))
            analytics_writer.upsert_feature_sentiment(aggregate_feature_sentiment(feature_rows))
            analytics_writer.upsert_top_issues(aggregate_top_issues(feature_rows))

        if args.mode in {"full", "embed_only"}:
            store = PgVectorStore(
                settings.postgres_host,
                settings.postgres_port,
                settings.postgres_db,
                settings.postgres_user,
                settings.postgres_password,
            )
            to_embed = store.fetch_unembedded_reviews(product_id=product_id)
            if to_embed:
                embedder = ReviewEmbedder(settings.embedding_model)
                vectors = embedder.encode([r["cleaned_text"] for r in to_embed])
                for row, vec in zip(to_embed, vectors, strict=True):
                    row["embedding"] = vec
                metrics["embedded_count"] = store.upsert_embeddings(to_embed)
            else:
                metrics["embedded_count"] = 0

        logger.info("Pipeline finished", extra={"event": "pipeline_summary", "extra": metrics})
        _finalize_pipeline_run(engine, run_id, "SUCCESS", metrics)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Pipeline failed", extra={"event": "pipeline_failure"})
        _finalize_pipeline_run(engine, run_id, "FAILED", metrics)
        raise exc


if __name__ == "__main__":
    main()
