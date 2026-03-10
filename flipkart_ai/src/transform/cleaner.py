"""Text cleaning utilities."""

from __future__ import annotations

import re


def clean_review_text(text: str) -> str:
    cleaned = text.replace("READ MORE", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip().lower()
