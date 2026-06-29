from confluent_kafka import Consumer
import json
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv


# ── Load environment variables ──
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('RDS_HOST'),
    'port': os.getenv('RDS_PORT', '5432'),
    'dbname': os.getenv('RDS_DB'),
    'user': os.getenv('RDS_USER'),
    'password': os.getenv('RDS_PASSWORD')
}


# ── Consumer config ──
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'raw-consumer-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False
})


# ── Database setup ──
# Tables are created once by setup_database.py, not here. This script only inserts.
# A single connection is reused across messages instead of opening one per message,
# since reconnecting on every insert was slow enough to cause Kafka session timeouts.
db_conn = None


def get_db_connection():
    global db_conn
    if db_conn is None or db_conn.closed:
        db_conn = psycopg2.connect(**DB_CONFIG)
    return db_conn


def insert_telemetry(telemetry):
    global db_conn
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute('''
            INSERT INTO fleet_telemetry_raw
                (vehicle_id, timestamp, speed_kmh, battery_pct, motor_temp_c,
                 lat, lon, charge_state, odometer_km, cumulative_charge_pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            telemetry['vehicle_id'],
            telemetry['timestamp'],
            telemetry['speed_kmh'],
            telemetry['battery_pct'],
            telemetry['motor_temp_c'],
            telemetry['lat'],
            telemetry['lon'],
            telemetry['charge_state'],
            telemetry['odometer_km'],
            telemetry['cumulative_charge_pct']
        ))

        conn.commit()
        cur.close()

    except Exception as e:
        print(f'  ✗ DB write failed for {telemetry.get("vehicle_id")}: {e}')
        # Connection may be in a bad state, force a fresh one on the next message
        if db_conn is not None:
            try:
                db_conn.close()
            except Exception:
                pass
            db_conn = None


# ── Main loop ──
if __name__ == '__main__':
    topic = 'fleet-telemetry-raw'
    consumer.subscribe([topic])

    print(f'Raw telemetry consumer started')
    print(f'  Reading from: {topic}')
    print(f'  Writing to:   fleet_telemetry_raw on AWS RDS ({DB_CONFIG["host"]})')
    print(f'  Press Ctrl+C to stop\n')

    messages_processed = 0

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f'Consumer error: {msg.error()}')
                continue

            telemetry = json.loads(msg.value().decode('utf-8'))
            insert_telemetry(telemetry)
            messages_processed += 1

            try:
                consumer.commit(asynchronous=False)
            except Exception as e:
                print(f'  ✗ Kafka commit failed, will retry on next message: {e}')

            if messages_processed % 100 == 0:
                print(f'  [{datetime.now().strftime("%H:%M:%S")}] '
                      f'Processed {messages_processed} messages')

    except KeyboardInterrupt:
        print(f'\nTotal messages processed: {messages_processed}')

    finally:
        consumer.close()
        if db_conn is not None and not db_conn.closed:
            db_conn.close()
        print('Consumer closed.')
