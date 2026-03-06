"""Data quality report builder."""

from __future__ import annotations

from collections import Counter
from typing import Any


def build_dq_report(records: list[dict[str, Any]]) -> dict[str, Any]:
    reason_counter: Counter[str] = Counter()
    null_rating = 0
    date_success = 0
    for rec in records:
        if rec.get("rating") is None:
            null_rating += 1
        if rec.get("processed_date_utc") is not None:
            date_success += 1
        if rec.get("invalid_reason"):
            reason_counter[rec["invalid_reason"]] += 1

    return {
        "total_rows": len(records),
        "null_rating_count": null_rating,
        "date_parse_success_count": date_success,
        "invalid_reasons": dict(reason_counter),
    }
