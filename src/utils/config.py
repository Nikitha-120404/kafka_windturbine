import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path('config/.env'))


@dataclass(frozen=True)
class KafkaConfig:
    broker: str = os.getenv('KAFKA_BROKER', 'localhost:9092')
    topic: str = os.getenv('KAFKA_TOPIC', 'windturbine-data')
    dlq_topic: str = os.getenv('KAFKA_DLQ_TOPIC', 'windturbine-deadletter')
    group_id: str = os.getenv('KAFKA_GROUP_ID', 'wind_turbine_consumer_group')
    partitions: int = int(os.getenv('KAFKA_NUM_PARTITIONS', '12'))


@dataclass(frozen=True)
class DbConfig:
    host: str = os.getenv('DB_HOST', 'localhost')
    port: int = int(os.getenv('DB_PORT', '5432'))
    name: str = os.getenv('DB_NAME', 'wind_turbine_db')
    user: str = os.getenv('DB_USER', 'postgres')
    password: str = os.getenv('DB_PASSWORD', 'postgres')
    table: str = os.getenv('DB_TABLE', 'wind_turbine_readings')


@dataclass(frozen=True)
class PipelineConfig:
    num_turbines: int = int(os.getenv('NUM_TURBINES', '12'))
    sensor_interval_sec: float = float(os.getenv('SENSOR_INTERVAL_SEC', '1.0'))
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    log_file: str = os.getenv('LOG_FILE', 'logs/pipeline.log')


KAFKA = KafkaConfig()
DB = DbConfig()
PIPELINE = PipelineConfig()
