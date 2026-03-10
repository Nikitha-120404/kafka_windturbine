"""Backfill utility wrapper around main pipeline."""

from __future__ import annotations

import subprocess


def main() -> None:
    cmd = (
        "python -m src.orchestration.run_pipeline "
        "--url \"<flipkart_review_url>\" --max_pages 200 --mode full"
    )
    print("Run this command for backfill:")
    print(cmd)


if __name__ == "__main__":
    main()
