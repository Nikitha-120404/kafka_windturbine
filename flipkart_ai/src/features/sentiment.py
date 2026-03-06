"""Deterministic sentiment scoring using VADER."""

from __future__ import annotations

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()


def score_sentiment(text: str) -> tuple[float, str]:
    score = analyzer.polarity_scores(text).get("compound", 0.0)
    if score >= 0.05:
        label = "pos"
    elif score <= -0.05:
        label = "neg"
    else:
        label = "neu"
    return score, label
