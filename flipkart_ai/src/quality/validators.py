"""Silver data validators."""

from __future__ import annotations

from typing import Any


def validate_record(record: dict[str, Any]) -> tuple[bool, str | None]:
    if record.get("rating") not in {1, 2, 3, 4, 5}:
        return False, "invalid_rating"
    if not record.get("cleaned_text"):
        return False, "empty_cleaned_text"
    if record.get("processed_date_utc") is None:
        return False, "invalid_date"
    return True, None
