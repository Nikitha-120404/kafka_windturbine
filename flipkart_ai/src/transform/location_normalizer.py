"""Location normalizer for buyer metadata."""

from __future__ import annotations


def normalize_location(location_raw: str) -> tuple[str | None, str | None]:
    if not location_raw:
        return None, None
    parts = [p.strip() for p in location_raw.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return parts[0], None
