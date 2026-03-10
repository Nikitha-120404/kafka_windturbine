"""Gold analytics aggregations over feature records."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

ISSUE_MAP = {
    "battery": ["drain", "battery"],
    "heating": ["heat", "heating", "warm"],
    "performance": ["lag", "slow", "hang"],
    "delivery": ["delivery", "late", "damaged"],
}


def aggregate_daily_metrics(feature_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, date], list[dict[str, Any]]] = defaultdict(list)
    for row in feature_rows:
        dt = row["processed_date_utc"].date() if row.get("processed_date_utc") else None
        if dt:
            grouped[(row["product_id"], dt)].append(row)

    out = []
    for (pid, dt), rows in grouped.items():
        out.append(
            {
                "product_id": pid,
                "date": dt,
                "review_count": len(rows),
                "avg_rating": sum(r.get("rating", 0) for r in rows) / max(len(rows), 1),
                "pos_count": sum(1 for r in rows if r["sentiment_label"] == "pos"),
                "neu_count": sum(1 for r in rows if r["sentiment_label"] == "neu"),
                "neg_count": sum(1 for r in rows if r["sentiment_label"] == "neg"),
            }
        )
    return out


def aggregate_feature_sentiment(feature_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    features = ["camera", "battery", "display", "performance", "heat", "price", "delivery"]
    counter: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: {"pos": 0, "neu": 0, "neg": 0})
    for r in feature_rows:
        for feat in features:
            if r.get(f"mentions_{feat}"):
                counter[(r["product_id"], feat)][r["sentiment_label"]] += 1
    return [
        {
            "product_id": pid,
            "feature": feat,
            "pos_count": vals["pos"],
            "neu_count": vals["neu"],
            "neg_count": vals["neg"],
        }
        for (pid, feat), vals in counter.items()
    ]


def aggregate_top_issues(feature_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    issue_counts: dict[tuple[str, str], list[str]] = defaultdict(list)
    for r in feature_rows:
        if r["sentiment_label"] != "neg":
            continue
        text = r.get("cleaned_text", "")
        for issue, words in ISSUE_MAP.items():
            if any(w in text for w in words):
                issue_counts[(r["product_id"], issue)].append(r["review_id"])
    return [
        {
            "product_id": pid,
            "issue": issue,
            "neg_mentions_count": len(ids),
            "example_review_ids": ",".join(ids[:5]),
        }
        for (pid, issue), ids in issue_counts.items()
    ]
