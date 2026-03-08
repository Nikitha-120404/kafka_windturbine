.PHONY: up down build producer consumer dlq test lint sql-init airflow-webserver airflow-scheduler logs

up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

build:
	docker compose -f docker/docker-compose.yml build

producer:
	python -m src.producer.live_producer

consumer:
	python -m src.consumer.consumer

dlq:
	python -m src.consumer.dead_letter_consumer

test:
	pytest tests -v

lint:
	python -m compileall src dags tests

sql-init:
	for f in sql/01_create_table.sql sql/03_continuous_aggregate.sql sql/04_compression.sql sql/05_retention_policy.sql; do \
		docker exec -i wind_timescaledb psql -U postgres -d wind_turbine_db < $$f; \
	done

airflow-webserver:
	docker compose -f docker/docker-compose.yml up -d airflow-webserver

airflow-scheduler:
	docker compose -f docker/docker-compose.yml up -d airflow-scheduler

logs:
	tail -f logs/pipeline.log
