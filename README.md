# Real-Time Wind Turbine IoT Streaming Pipeline (Kafka + TimescaleDB + Airflow)

Production-style local data engineering pipeline that streams simulated wind turbine telemetry into Kafka, validates/transforms events, stores time-series data in TimescaleDB, and orchestrates health checks with Airflow.

## Architecture

`Producer -> Kafka -> Consumer (validation + enrichment + DLQ) -> TimescaleDB -> Airflow`

## Tech Stack

| Tool | Purpose |
|---|---|
| Python | Producer, consumer, transformer, DB writer |
| Apache Kafka | Streaming backbone + DLQ |
| TimescaleDB/Postgres | Time-series storage + continuous aggregates |
| Airflow | Pipeline health orchestration |
| Docker Compose | Local reproducible environment |
| pytest | Unit testing |

## Project Structure

```text
.
├── config/
├── dags/
├── docker/
├── docs/
├── logs/
├── sql/
├── src/
│   ├── consumer/
│   ├── db/
│   ├── producer/
│   ├── transformer/
│   └── utils/
├── tests/
├── Makefile
└── README.md
```

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config/.env.example config/.env
make up
make sql-init
```

Run producer and consumer in separate shells:

```bash
make producer
make consumer
```

Optional Airflow:

```bash
make airflow-webserver
make airflow-scheduler
```

UI: http://localhost:8080 (`admin/admin`)

## Pipeline Features

- Live event producer (no log-file dependency)
- Environment-driven configuration with dotenv
- Structured file + console logging
- JSON schema validation and quality checks
- Dead-letter Kafka topic for invalid events
- Enrichment (`health_status`, `power_efficiency`, `is_anomaly`)
- Timescale hypertable, indexes, continuous aggregate, compression, retention
- Airflow DAG for health/freshness/quality/aggregate refresh

## Example SQL Analytics

```sql
SELECT *
FROM turbine_5min_kpis
WHERE bucket > NOW() - INTERVAL '1 hour'
ORDER BY turbine_id, bucket DESC;
```

## Testing

```bash
make test
make lint
```

## Resume Bullets

- Built a real-time Kafka ingestion pipeline for 12 simulated wind turbines with partition-keyed ordering and idempotent producer settings.
- Implemented schema validation + transformation layer with dead-letter topic handling to prevent malformed IoT records from polluting downstream storage.
- Designed TimescaleDB hypertable and continuous aggregate strategy for low-latency KPI queries, plus lifecycle policies for compression and retention.
- Orchestrated pipeline health checks and aggregate refresh via Airflow DAG scheduled every 30 minutes.
