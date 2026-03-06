"""Flipkart reviews scraper with safety limits and retries."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

from src.extract.parser import parse_reviews_from_html
from src.utils.http import BlockedByWebsiteError, HttpClient

logger = logging.getLogger(__name__)


def _url_with_page(url: str, page: int) -> str:
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    q["page"] = [str(page)]
    query = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment))


def scrape_reviews(url: str, max_pages: int, http_client: HttpClient) -> list[dict[str, Any]]:
    all_reviews: list[dict[str, Any]] = []
    empty_streak = 0

    for page in range(1, max_pages + 1):
        page_url = _url_with_page(url, page)
        try:
            response = http_client.get(page_url)
        except BlockedByWebsiteError:
            logger.error("Blocked by website; stopping scrape", extra={"event": "scrape_blocked"})
            break

        page_reviews = parse_reviews_from_html(response.text, url, page)
        if not page_reviews:
            empty_streak += 1
            if empty_streak >= 2:
                logger.info("No reviews in consecutive pages; stopping", extra={"event": "scrape_stop"})
                break
            continue

        empty_streak = 0
        all_reviews.extend(page_reviews)
        logger.info(
            "Scraped page",
            extra={"event": "scrape_page", "extra": {"page": page, "count": len(page_reviews)}},
        )

    return all_reviews
