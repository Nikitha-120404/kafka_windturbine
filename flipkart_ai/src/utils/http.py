"""HTTP utilities with retries/backoff for polite scraping."""

from __future__ import annotations

import time
from typing import Optional

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class BlockedByWebsiteError(RuntimeError):
    """Raised when website likely blocked request."""


class HttpClient:
    """Resilient client with retry, timeout and throttling."""

    def __init__(
        self,
        timeout_seconds: int,
        max_retries: int,
        backoff_base: float,
        sleep_seconds: float,
        user_agent: Optional[str] = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.sleep_seconds = sleep_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent
                or "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    @retry(
        retry=retry_if_exception_type((requests.RequestException, BlockedByWebsiteError)),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get(self, url: str, params: Optional[dict[str, str]] = None) -> requests.Response:
        response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        if response.status_code in (403, 429):
            raise BlockedByWebsiteError(f"Blocked with status={response.status_code} for {url}")
        response.raise_for_status()
        time.sleep(self.sleep_seconds)
        return response
