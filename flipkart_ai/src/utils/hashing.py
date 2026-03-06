"""Hashing helpers for deterministic identifiers."""

from __future__ import annotations

import hashlib


def stable_review_id(product_id: str, reviewer_name: str, title: str, review_text_raw: str) -> str:
    """Generate deterministic hash for a review record."""
    payload = "||".join(
        [product_id.strip(), reviewer_name.strip(), title.strip(), review_text_raw.strip()]
    ).lower()
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
