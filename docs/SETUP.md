# Setup Guide

1. Copy env vars:
   ```bash
   cp config/.env.example config/.env
   ```
2. Start services:
   ```bash
   make up
   ```
3. Initialize TimescaleDB schema:
   ```bash
   make sql-init
   ```
4. Run producer and consumer in separate terminals:
   ```bash
   make producer
   make consumer
   ```
5. Optional: start Airflow webserver/scheduler:
   ```bash
   make airflow-webserver
   make airflow-scheduler
   ```
