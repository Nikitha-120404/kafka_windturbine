from confluent_kafka import Consumer

from src.utils.config import KAFKA
from src.utils.logger import get_logger

logger = get_logger('dead_letter_consumer')


def run() -> None:
    consumer = Consumer(
        {
            'bootstrap.servers': KAFKA.broker,
            'group.id': f"{KAFKA.group_id}_dlq",
            'auto.offset.reset': 'earliest',
        }
    )
    consumer.subscribe([KAFKA.dlq_topic])
    logger.info('DLQ consumer subscribed to %s', KAFKA.dlq_topic)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error('Kafka error: %s', msg.error())
                continue
            logger.warning('DLQ message: %s', msg.value().decode('utf-8'))
    except KeyboardInterrupt:
        logger.info('DLQ consumer shutting down...')
    finally:
        consumer.close()


if __name__ == '__main__':
    run()
