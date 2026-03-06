# Flipkart AI Batch Review Pipeline

Production-grade batch data platform for Flipkart product reviews with Bronze/Silver/Gold layers, ML feature engineering, pgvector embeddings, and semantic search.

## Architecture Overview

### Batch-only medallion design
1. **Bronze (MongoDB):** raw immutable capture from Flipkart review pages, upserted by deterministic `review_id`.
2. **Silver (Parquet + Postgres):** cleaned/normalized rows with parsed dates, structured location fields, and quality flags.
3. **Gold analytics (Postgres):**
   - `gold_daily_metrics`
   - `gold_feature_sentiment`
   - `gold_top_issues`
4. **Gold ML features (Postgres):** `gold_ml_features` for model-ready features and sentiment.
5. **Gold embeddings/vector DB (Postgres + pgvector):** `gold_review_embeddings` for semantic retrieval.
6. **Semantic search CLI:** cosine similarity top-k lookup by product.

## Repository Structure

```text
flipkart_ai/
  README.md
  docker-compose.yml
  .env.example
  requirements.txt
  sql/init.sql
  src/
    config/settings.py
    orchestration/run_pipeline.py
    extract/{flipkart_scraper.py,parser.py}
    bronze/mongo_writer.py
    transform/{cleaner.py,date_normalizer.py,location_normalizer.py,silver_writer.py}
    quality/{validators.py,dq_report.py}
    features/{feature_engineering.py,sentiment.py,gold_features_writer.py}
    embeddings/{embedder.py,vector_store.py}
    analytics/{aggregations.py,gold_analytics_writer.py}
    utils/{logging.py,hashing.py,http.py}
  tools/{semantic_search.py,backfill.py}
  tests/{test_hashing.py,test_date_normalizer.py,test_cleaner.py,test_scraper_parser.py}
```

## Setup

### 1) Start databases
```bash
docker compose up -d
```

### 2) Python env
```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env
```

### 3) Run tests
```bash
PYTHONPATH=. pytest -q
```

## Run Pipeline

```bash
cd flipkart_ai
PYTHONPATH=. python -m src.orchestration.run_pipeline \
  --url "https://www.flipkart.com/apple-iphone-16-pro-max-natural-titanium-256-gb/product-reviews/itm05ad8e674782a?pid=MOBH4DQFRST2BQQ8&lid=LSTMOBH4DQFRST2BQQ8UII8UK&marketplace=FLIPKART" \
  --max_pages 200 \
  --mode full
```

Modes:
- `full`
- `extract_only`
- `transform_only`
- `embed_only`
- `analytics_only`

Optional incremental:
```bash
PYTHONPATH=. python -m src.orchestration.run_pipeline --url "<url>" --mode full --since "2024-01-01T00:00:00+00:00"
```

## Semantic Search

```bash
cd flipkart_ai
PYTHONPATH=. python tools/semantic_search.py --product_id MOBH4DQFRST2BQQ8 --query "camera quality" --top_k 5
```

## Example Run Output

```text
{"event":"scrape_page","extra":{"page":1,"count":10}}
{"event":"scrape_page","extra":{"page":2,"count":10}}
{"event":"pipeline_summary","extra":{"extracted_count":20,"bronze_upserts":20,"silver_valid_count":19,"silver_invalid_count":1,"embedded_count":19}}
```

## Example Semantic Search Output

```text
1. score=0.8421 rating=5 sentiment=pos review_id=ab12...
   camera is excellent in daylight and portrait mode is sharp.
2. score=0.8010 rating=4 sentiment=pos review_id=cd34...
   good camera but battery could be better.
```

## Data Quality

DQ report includes:
- total row count
- null ratings
- date parse success count
- invalid reason distribution

Stored in `pipeline_runs.notes` JSON at end of each run.

## Observability

- JSON structured logs with event names
- End-of-run summary metrics in logs + `pipeline_runs`

## Troubleshooting

- **403/429 from Flipkart:** pipeline stops safely; reduce request rate (`REQUEST_SLEEP_SECONDS`), retry later.
- **Model download issues:** verify internet access for sentence-transformers download.
- **Postgres pgvector errors:** ensure `docker compose` service uses `pgvector/pgvector` image.

## Compliance

- No secrets in code; use `.env` only.
- Respects polite scraping with throttling and stop conditions.
