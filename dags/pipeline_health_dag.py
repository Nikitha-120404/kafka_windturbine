from datetime import datetime, timedelta
import os

import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def check_db_health() -> None:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'timescaledb'),
        dbname=os.getenv('DB_NAME', 'wind_turbine_db'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres'),
        port=int(os.getenv('DB_PORT', '5432')),
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM wind_turbine_readings
                WHERE time > NOW() - INTERVAL '30 minutes'
                """
            )
            count = cur.fetchone()[0]
            if count == 0:
                raise ValueError('No rows found in last 30 minutes.')


def run_quality_check() -> None:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'timescaledb'),
        dbname=os.getenv('DB_NAME', 'wind_turbine_db'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres'),
        port=int(os.getenv('DB_PORT', '5432')),
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT turbine_id, COUNT(*)
                FROM wind_turbine_readings
                WHERE health_status='CRITICAL'
                  AND time > NOW() - INTERVAL '1 hour'
                GROUP BY turbine_id
                ORDER BY COUNT(*) DESC
                """
            )
            cur.fetchall()


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='wind_turbine_pipeline_health',
    description='Health checks and aggregate refresh for wind turbine pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='*/30 * * * *',
    catchup=False,
    tags=['wind-turbine', 'monitoring'],
) as dag:
    check_kafka = BashOperator(
        task_id='check_kafka_health',
        bash_command='kafka-topics --bootstrap-server ${KAFKA_BROKER} --list',
    )

    check_db = PythonOperator(
        task_id='check_db_health',
        python_callable=check_db_health,
    )

    quality = PythonOperator(
        task_id='run_data_quality_check',
        python_callable=run_quality_check,
    )

    refresh_aggregates = PostgresOperator(
        task_id='refresh_aggregates',
        postgres_conn_id='timescaledb_default',
        sql="CALL refresh_continuous_aggregate('turbine_5min_kpis', NULL, NULL);",
    )

    check_kafka >> check_db >> quality >> refresh_aggregates
