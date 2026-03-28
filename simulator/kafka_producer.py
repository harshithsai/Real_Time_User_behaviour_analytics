# simulator/kafka_producer.py
# Sends events to Kafka topics: user-clicks, user-sessions, user-purchases

import json
import random
import time
import uuid
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
from event_generator import (
    generate_click_event,
    generate_session_event,
    generate_purchase_event,
)
from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVERS   = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_CLICKS    = os.getenv('KAFKA_TOPIC_CLICKS',    'user-clicks')
TOPIC_SESSIONS  = os.getenv('KAFKA_TOPIC_SESSIONS',  'user-sessions')
TOPIC_PURCHASES = os.getenv('KAFKA_TOPIC_PURCHASES', 'user-purchases')


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=5,
        compression_type='gzip',
        request_timeout_ms=30000,
    )


def on_send_error(exc):
    print(f'[ERROR] Failed to deliver message: {exc}')


def run_simulator(events_per_second: int = 100, duration_minutes: int = 60):
    try:
        producer = create_producer()
    except KafkaError as e:
        print(f'[FATAL] Could not connect to Kafka at {KAFKA_SERVERS}: {e}')
        return

    print(f'✅ Connected to Kafka at {KAFKA_SERVERS}')
    print(f'🚀 Simulator: {events_per_second} events/sec for {duration_minutes} min')
    print('Press Ctrl+C to stop\n')

    user_pool = [str(uuid.uuid4()) for _ in range(10_000)]

    total_clicks    = 0
    total_sessions  = 0
    total_purchases = 0
    start_time      = time.time()
    end_time        = start_time + (duration_minutes * 60)

    try:
        while time.time() < end_time:
            batch_start = time.time()

            for _ in range(events_per_second):
                user_id    = random.choice(user_pool)
                session_id = str(uuid.uuid4())

                # Always send a click
                click = generate_click_event(user_id, session_id)
                producer.send(TOPIC_CLICKS, key=user_id, value=click).add_errback(on_send_error)
                total_clicks += 1

                # 20% chance: session event
                if random.random() < 0.2:
                    session = generate_session_event(user_id)
                    producer.send(TOPIC_SESSIONS, key=user_id, value=session).add_errback(on_send_error)
                    total_sessions += 1

                # 3% chance: purchase event
                if random.random() < 0.03:
                    purchase = generate_purchase_event(user_id, session_id)
                    producer.send(TOPIC_PURCHASES, key=user_id, value=purchase).add_errback(on_send_error)
                    total_purchases += 1

            producer.flush()

            elapsed = time.time() - start_time
            total   = total_clicks + total_sessions + total_purchases
            print(
                f'[{elapsed:6.0f}s] '
                f'clicks={total_clicks:,} | '
                f'sessions={total_sessions:,} | '
                f'purchases={total_purchases:,} | '
                f'rate={total / elapsed:.0f}/sec'
            )

            sleep_time = 1.0 - (time.time() - batch_start)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print('\n⏹ Stopped by user.')
    finally:
        producer.flush()
        producer.close()
        total = total_clicks + total_sessions + total_purchases
        print(f'\n✅ Done. clicks={total_clicks:,} | sessions={total_sessions:,} | purchases={total_purchases:,} | total={total:,}')


if __name__ == '__main__':
    run_simulator(events_per_second=100, duration_minutes=60)