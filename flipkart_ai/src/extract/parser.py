"""HTML parser for Flipkart reviews pages."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from src.utils.hashing import stable_review_id


def parse_product_metadata(url: str) -> dict[str, str]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    product_id = query.get("pid", ["unknown"])[0]
    path_parts = [p for p in parsed.path.split("/") if p]
    product_slug = path_parts[0] if path_parts else "unknown"
    return {"product_id": product_id, "product_slug": product_slug}


def parse_reviews_from_html(html: str, source_url: str, page_number: int) -> list[dict[str, Any]]:
    """Parse reviews from page html. Raises ValueError when structure changed."""
    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.select("div.col.EPCmJX") or soup.select("div._27M-vq")

    if not blocks:
        return []

    meta = parse_product_metadata(source_url)
    scrape_time = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for block in blocks:
        rating_txt = (block.select_one("div.XQDdHH") or block.select_one("div._3LWZlK"))
        title_tag = block.select_one("p.z9E0IG") or block.select_one("p._2-N8zT")
        body_tag = block.select_one("div.ZmyHeo") or block.select_one("div.t-ZTKy")
        user_tag = block.select_one("p._2NsDsF")
        meta_tag = block.select_one("p.MztJPv") or block.select_one("p._2sc7ZR + p")
        date_tag = block.select_one("p._2NsDsF + p") or block.select_one("p._2sc7ZR ~ p")

        review_text_raw = body_tag.get_text(" ", strip=True) if body_tag else ""
        title = title_tag.get_text(" ", strip=True) if title_tag else ""
        reviewer_name = user_tag.get_text(" ", strip=True) if user_tag else "anonymous"

        record = {
            "product_id": meta["product_id"],
            "product_slug": meta["product_slug"],
            "rating": int(float(rating_txt.get_text(strip=True))) if rating_txt else None,
            "title": title,
            "review_text_raw": review_text_raw,
            "reviewer_name": reviewer_name,
            "location_raw": meta_tag.get_text(" ", strip=True) if meta_tag else "",
            "date_text_raw": date_tag.get_text(" ", strip=True) if date_tag else "",
            "source_url": source_url,
            "page_number": page_number,
            "scrape_timestamp_utc": scrape_time,
        }
        record["review_id"] = stable_review_id(
            record["product_id"], reviewer_name, title, review_text_raw
        )
        records.append(record)

    return records
