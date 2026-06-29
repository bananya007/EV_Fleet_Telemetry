from confluent_kafka import Consumer, Producer
import json
import psycopg2
import os
from datetime import datetime, timezone
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


# ── Consumer config — reads raw telemetry ──
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'anomaly-detector-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False
})

# ── Producer config — writes anomaly alerts ──
alert_producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,
    'acks': 'all'
})

# ── Anomaly thresholds ──
SPEED_THRESHOLD = 200        # km/h
MOTOR_TEMP_THRESHOLD = 85    # °C
BATTERY_DRAIN_THRESHOLD = 5  # % per minute

# ── Per-vehicle state tracking ──
vehicle_state = {}


# ── Database setup ──
# Table is created once by setup_database.py, not here. This script only inserts.
# A single connection is reused across alerts instead of opening one per alert,
# since reconnecting on every insert was slow enough to cause Kafka session timeouts.
db_conn = None


def get_db_connection():
    global db_conn
    if db_conn is None or db_conn.closed:
        db_conn = psycopg2.connect(**DB_CONFIG)
    return db_conn


def insert_alert(alert):
    global db_conn
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute('''
            INSERT INTO fleet_anomaly_alerts
                (vehicle_id, timestamp, alert_type, triggered_value, threshold, severity)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            alert['vehicle_id'],
            alert['timestamp'],
            alert['alert_type'],
            alert['triggered_value'],
            alert['threshold'],
            alert['severity']
        ))

        conn.commit()
        cur.close()

    except Exception as e:
        print(f'  ✗ DB write failed for alert on {alert.get("vehicle_id")}: {e}')
        # Connection may be in a bad state, force a fresh one on the next alert
        if db_conn is not None:
            try:
                db_conn.close()
            except Exception:
                pass
            db_conn = None


def detect_anomalies(telemetry):
    """Check one telemetry event against all anomaly rules.
    Returns a list of alert dicts (0, 1, or multiple)."""

    alerts = []
    vid = telemetry['vehicle_id']
    ts = telemetry['timestamp']

    # Check 1: Overspeed
    if telemetry['speed_kmh'] > SPEED_THRESHOLD:
        alerts.append({
            'vehicle_id': vid,
            'timestamp': ts,
            'alert_type': 'overspeed',
            'triggered_value': telemetry['speed_kmh'],
            'threshold': SPEED_THRESHOLD,
            'severity': 'critical'
        })

    # Check 2: Motor overheating
    if telemetry['motor_temp_c'] > MOTOR_TEMP_THRESHOLD:
        severity = 'critical' if telemetry['motor_temp_c'] > 100 else 'warning'
        alerts.append({
            'vehicle_id': vid,
            'timestamp': ts,
            'alert_type': 'motor_overheating',
            'triggered_value': telemetry['motor_temp_c'],
            'threshold': MOTOR_TEMP_THRESHOLD,
            'severity': severity
        })

    # Check 3: Battery drain rate
    if vid in vehicle_state:
        prev = vehicle_state[vid]
        prev_ts = datetime.fromisoformat(prev['timestamp'])
        curr_ts = datetime.fromisoformat(ts)
        elapsed_min = (curr_ts - prev_ts).total_seconds() / 60

        if elapsed_min > 0:
            drain_rate = (prev['battery_pct'] - telemetry['battery_pct']) / elapsed_min

            if drain_rate > BATTERY_DRAIN_THRESHOLD:
                alerts.append({
                    'vehicle_id': vid,
                    'timestamp': ts,
                    'alert_type': 'battery_drain_spike',
                    'triggered_value': round(drain_rate, 2),
                    'threshold': BATTERY_DRAIN_THRESHOLD,
                    'severity': 'critical'
                })

    # Update state with current reading
    vehicle_state[vid] = {
        'battery_pct': telemetry['battery_pct'],
        'timestamp': ts
    }

    return alerts


def alert_delivery_report(err, msg):
    if err is not None:
        print(f'  ✗ Alert delivery failed: {err}')
    else:
        print(f'  → Alert delivered to partition [{msg.partition()}]')


# ── Main loop ──
if __name__ == '__main__':
    input_topic = 'fleet-telemetry-raw'
    output_topic = 'fleet-anomaly-alerts'

    consumer.subscribe([input_topic])

    print(f'Anomaly detector started')
    print(f'  Reading from:  {input_topic}')
    print(f'  Writing to:    {output_topic}')
    print(f'  Thresholds:    speed>{SPEED_THRESHOLD}km/h  '
          f'temp>{MOTOR_TEMP_THRESHOLD}°C  '
          f'drain>{BATTERY_DRAIN_THRESHOLD}%/min')
    print(f'  Press Ctrl+C to stop\n')

    messages_processed = 0
    alerts_fired = 0

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f'Consumer error: {msg.error()}')
                continue

            telemetry = json.loads(msg.value().decode('utf-8'))
            messages_processed += 1

            alerts = detect_anomalies(telemetry)

            for alert in alerts:
                alert_producer.produce(
                    output_topic,
                    key=alert['vehicle_id'],
                    value=json.dumps(alert),
                    callback=alert_delivery_report
                )
                insert_alert(alert)
                alerts_fired += 1

                print(f'  🚨 [{alert["severity"].upper()}] '
                      f'{alert["vehicle_id"]} — {alert["alert_type"]} '
                      f'(value: {alert["triggered_value"]}, '
                      f'threshold: {alert["threshold"]})')

            alert_producer.poll(0)

            # Commit offset AFTER processing + producing
            try:
                consumer.commit(asynchronous=False)
            except Exception as e:
                print(f'  ✗ Kafka commit failed, will retry on next message: {e}')

            if messages_processed % 100 == 0:
                print(f'  [{datetime.now().strftime("%H:%M:%S")}] '
                      f'Processed {messages_processed} messages, '
                      f'{alerts_fired} alerts fired')

    except KeyboardInterrupt:
        print(f'\nStopping anomaly detector.')
        print(f'  Messages processed: {messages_processed}')
        print(f'  Alerts fired: {alerts_fired}')

    finally:
        consumer.close()
        alert_producer.flush()
        if db_conn is not None and not db_conn.closed:
            db_conn.close()
        print('Consumer closed, producer flushed.')