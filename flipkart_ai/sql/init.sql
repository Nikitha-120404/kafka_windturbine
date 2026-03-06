CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    start_time_utc TIMESTAMPTZ NOT NULL,
    end_time_utc TIMESTAMPTZ,
    status TEXT NOT NULL,
    mode TEXT NOT NULL,
    source_url TEXT NOT NULL,
    product_id TEXT,
    extracted_count INT DEFAULT 0,
    bronze_upserts INT DEFAULT 0,
    silver_valid_count INT DEFAULT 0,
    silver_invalid_count INT DEFAULT 0,
    embedded_count INT DEFAULT 0,
    notes JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS flipkart_reviews_silver (
    review_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    product_slug TEXT,
    rating INT,
    title TEXT,
    review_text_raw TEXT,
    cleaned_text TEXT,
    reviewer_name TEXT,
    location_raw TEXT,
    buyer_status TEXT,
    city TEXT,
    date_text_raw TEXT,
    processed_date_utc TIMESTAMPTZ,
    source_url TEXT,
    page_number INT,
    scrape_timestamp_utc TIMESTAMPTZ,
    ingestion_run_id TEXT,
    is_valid BOOLEAN,
    invalid_reason TEXT,
    created_at_utc TIMESTAMPTZ DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold_ml_features (
    review_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    processed_date_utc TIMESTAMPTZ,
    rating INT,
    cleaned_text TEXT,
    review_length_chars INT,
    word_count INT,
    sentiment_score DOUBLE PRECISION,
    sentiment_label TEXT,
    mentions_camera BOOLEAN,
    mentions_battery BOOLEAN,
    mentions_display BOOLEAN,
    mentions_performance BOOLEAN,
    mentions_heat BOOLEAN,
    mentions_price BOOLEAN,
    mentions_delivery BOOLEAN,
    created_at_utc TIMESTAMPTZ DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold_daily_metrics (
    product_id TEXT NOT NULL,
    date DATE NOT NULL,
    review_count INT,
    avg_rating DOUBLE PRECISION,
    pos_count INT,
    neu_count INT,
    neg_count INT,
    PRIMARY KEY (product_id, date)
);

CREATE TABLE IF NOT EXISTS gold_feature_sentiment (
    product_id TEXT NOT NULL,
    feature TEXT NOT NULL,
    pos_count INT,
    neu_count INT,
    neg_count INT,
    PRIMARY KEY (product_id, feature)
);

CREATE TABLE IF NOT EXISTS gold_top_issues (
    product_id TEXT NOT NULL,
    issue TEXT NOT NULL,
    neg_mentions_count INT,
    example_review_ids TEXT,
    PRIMARY KEY (product_id, issue)
);

CREATE TABLE IF NOT EXISTS gold_review_embeddings (
    review_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    cleaned_text TEXT,
    rating INT,
    sentiment_label TEXT,
    embedding VECTOR(384),
    created_at_utc TIMESTAMPTZ DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_product_date ON flipkart_reviews_silver(product_id, processed_date_utc);
CREATE INDEX IF NOT EXISTS idx_features_product_date ON gold_ml_features(product_id, processed_date_utc);
CREATE INDEX IF NOT EXISTS idx_embeddings_product ON gold_review_embeddings(product_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_embeddings_ivfflat'
    ) THEN
        CREATE INDEX idx_embeddings_ivfflat ON gold_review_embeddings USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
    END IF;
END
$$;
