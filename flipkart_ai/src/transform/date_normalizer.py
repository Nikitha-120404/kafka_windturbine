"""Normalize Flipkart date strings into UTC timestamp."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from dateutil import parser as dt_parser


RELATIVE_PATTERN = re.compile(r"(?P<n>\d+)\s+(?P<unit>day|days|month|months|year|years)\s+ago", re.I)


def normalize_review_date(date_text_raw: str, scrape_timestamp_utc: str) -> datetime | None:
    if not date_text_raw:
        return None

    ref = datetime.fromisoformat(scrape_timestamp_utc.replace("Z", "+00:00")).astimezone(timezone.utc)
    match = RELATIVE_PATTERN.search(date_text_raw)
    if match:
        n = int(match.group("n"))
        unit = match.group("unit").lower()
        if "day" in unit:
            return ref - timedelta(days=n)
        if "month" in unit:
            return ref - timedelta(days=30 * n)
        if "year" in unit:
            return ref - timedelta(days=365 * n)

    try:
        parsed = dt_parser.parse(date_text_raw, fuzzy=True)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None
