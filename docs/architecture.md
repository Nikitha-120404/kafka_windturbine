# Architecture

```text
Python Simulator/Producer -> Kafka Topic -> Consumer + Validator + Enricher -> TimescaleDB Hypertable -> Airflow DAG checks
                                        \-> Dead-letter topic
```

- Kafka is keyed by `Turbine_ID` for deterministic partition ordering.
- Consumer commits offsets only after successful DB write.
- Invalid events are routed to DLQ with reason headers.
