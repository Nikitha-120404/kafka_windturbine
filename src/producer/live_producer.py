import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.utils.config import KAFKA, PIPELINE
from src.utils.logger import get_logger

logger = get_logger('producer')

RANGES = {
    'Nacelle_Position': (0.0, 360.0),
    'Wind_direction': (0.0, 360.0),
    'Ambient_Air_temp': (-30.0, 45.0),
    'Bearing_Temp': (10.0, 70.0),
    'BladePitchAngle': (0.0, 90.0),
    'GearBoxSumpTemp': (20.0, 130.0),
    'Generator_Speed': (40.0, 60.0),
    'Hub_Speed': (1, 5),
    'Power': (50.0, 1500.0),
    'Wind_Speed': (2.0, 25.0),
    'GearTemp': (50.0, 350.0),
    'GeneratorTemp': (25.0, 150.0),
}


def ensure_topics_exist() -> None:
    admin = AdminClient({'bootstrap.servers': KAFKA.broker})
    existing_topics = admin.list_topics(timeout=10).topics

    to_create = []
    for topic_name in (KAFKA.topic, KAFKA.dlq_topic):
        if topic_name not in existing_topics:
            to_create.append(
                NewTopic(topic_name, num_partitions=KAFKA.partitions, replication_factor=1)
            )

    if to_create:
        admin.create_topics(to_create)
        logger.info('Created topics: %s', ', '.join(t.topic for t in to_create))


def generate_event(turbine_id: int) -> dict:
    record = {}
    for key, bounds in RANGES.items():
        if isinstance(bounds[0], int):
            record[key] = random.randint(bounds[0], bounds[1])
        else:
            record[key] = round(random.uniform(bounds[0], bounds[1]), 2)

    record['Turbine_ID'] = f'Turbine_{turbine_id:02d}'
    record['Timestamp'] = datetime.now(timezone.utc).isoformat()
    return record


def delivery_report(err, msg):
    if err:
        logger.error('Delivery failed: %s', err)


def run() -> None:
    ensure_topics_exist()
    producer = Producer(
        {
            'bootstrap.servers': KAFKA.broker,
            'client.id': 'wind_turbine_live_producer',
            'acks': 'all',
            'enable.idempotence': True,
        }
    )
    logger.info('Producer started. Publishing to topic=%s', KAFKA.topic)

    try:
        while True:
            for turbine_id in range(1, PIPELINE.num_turbines + 1):
                event = generate_event(turbine_id)
                producer.produce(
                    topic=KAFKA.topic,
                    key=str(turbine_id),
                    value=json.dumps(event).encode('utf-8'),
                    callback=delivery_report,
                )
                producer.poll(0)
            time.sleep(PIPELINE.sensor_interval_sec)
    except KeyboardInterrupt:
        logger.info('Producer shutting down...')
    finally:
        producer.flush()
        logger.info('Producer stopped.')


if __name__ == '__main__':
    run()
