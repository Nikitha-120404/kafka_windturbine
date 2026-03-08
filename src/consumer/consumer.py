import json

from confluent_kafka import Consumer, Producer

from src.db.writer import insert_record
from src.transformer.enricher import enrich
from src.transformer.validator import validate_message
from src.utils.config import KAFKA
from src.utils.logger import get_logger

logger = get_logger('consumer')


def run() -> None:
    consumer = Consumer(
        {
            'bootstrap.servers': KAFKA.broker,
            'group.id': KAFKA.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
    )
    dlq_producer = Producer({'bootstrap.servers': KAFKA.broker})
    consumer.subscribe([KAFKA.topic])

    logger.info('Consumer subscribed to topic=%s', KAFKA.topic)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error('Kafka error: %s', msg.error())
                continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                is_valid, reason = validate_message(payload)
                if not is_valid:
                    dlq_producer.produce(
                        KAFKA.dlq_topic,
                        value=msg.value(),
                        headers={'reason': reason.encode('utf-8')},
                    )
                    dlq_producer.poll(0)
                    consumer.commit(msg)
                    logger.warning('Message sent to DLQ: %s', reason)
                    continue

                enriched = enrich(payload)
                insert_record(enriched)
                consumer.commit(msg)
                logger.info(
                    'Processed %s | health=%s | anomaly=%s',
                    enriched['Turbine_ID'],
                    enriched['health_status'],
                    enriched['is_anomaly'],
                )
            except json.JSONDecodeError:
                logger.exception('Invalid JSON payload, committing offset')
                consumer.commit(msg)
            except Exception:
                logger.exception('Processing failure, offset not committed')
    except KeyboardInterrupt:
        logger.info('Consumer shutting down...')
    finally:
        consumer.close()
        dlq_producer.flush()


if __name__ == '__main__':
    run()
