"""Bronze layer writer to MongoDB."""

from __future__ import annotations

from typing import Any

from pymongo import MongoClient, UpdateOne


class BronzeMongoWriter:
    """Persist raw scraped reviews with upserts."""

    def __init__(self, mongo_uri: str, db_name: str, collection_name: str) -> None:
        self.client = MongoClient(mongo_uri)
        self.collection = self.client[db_name][collection_name]
        self.collection.create_index("review_id", unique=True)

    def upsert_reviews(self, records: list[dict[str, Any]], ingestion_run_id: str) -> int:
        if not records:
            return 0
        ops = []
        for rec in records:
            payload = {**rec, "ingestion_run_id": ingestion_run_id}
            ops.append(UpdateOne({"review_id": rec["review_id"]}, {"$set": payload}, upsert=True))
        result = self.collection.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    def fetch_reviews(self, product_id: str | None = None, since_iso: str | None = None) -> list[dict[str, Any]]:
        query: dict[str, Any] = {}
        if product_id:
            query["product_id"] = product_id
        if since_iso:
            query["scrape_timestamp_utc"] = {"$gte": since_iso}
        docs = list(self.collection.find(query, {"_id": 0}))
        return docs
