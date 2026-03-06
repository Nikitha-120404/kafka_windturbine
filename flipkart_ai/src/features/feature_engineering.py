"""Gold ML feature engineering."""

from __future__ import annotations

from typing import Any

from src.features.sentiment import score_sentiment

FEATURE_KEYWORDS = {
    "camera": ["camera", "photo", "portrait"],
    "battery": ["battery", "charging", "backup"],
    "display": ["display", "screen", "brightness"],
    "performance": ["performance", "speed", "lag"],
    "heat": ["heat", "heating", "warm"],
    "price": ["price", "cost", "value"],
    "delivery": ["delivery", "courier", "shipping"],
}


def has_keywords(text: str, words: list[str]) -> bool:
    lowered = text.lower()
    return any(w in lowered for w in words)


def build_ml_features(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rec in records:
        text = rec.get("cleaned_text", "")
        score, label = score_sentiment(text)
        out.append(
            {
                "review_id": rec["review_id"],
                "product_id": rec["product_id"],
                "processed_date_utc": rec.get("processed_date_utc"),
                "rating": rec.get("rating"),
                "cleaned_text": text,
                "review_length_chars": len(text),
                "word_count": len(text.split()),
                "sentiment_score": score,
                "sentiment_label": label,
                "mentions_camera": has_keywords(text, FEATURE_KEYWORDS["camera"]),
                "mentions_battery": has_keywords(text, FEATURE_KEYWORDS["battery"]),
                "mentions_display": has_keywords(text, FEATURE_KEYWORDS["display"]),
                "mentions_performance": has_keywords(text, FEATURE_KEYWORDS["performance"]),
                "mentions_heat": has_keywords(text, FEATURE_KEYWORDS["heat"]),
                "mentions_price": has_keywords(text, FEATURE_KEYWORDS["price"]),
                "mentions_delivery": has_keywords(text, FEATURE_KEYWORDS["delivery"]),
            }
        )
    return out
