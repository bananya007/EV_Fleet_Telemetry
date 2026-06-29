"""
One-time schema setup for the fleet telemetry pipeline.

Run this once before starting the simulator, consumer, anomaly detector, or
Airflow DAG. None of those scripts create tables themselves, they all assume
this schema already exists. Re-run this script any time the schema changes
during development.
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('RDS_HOST'),
    'port': os.getenv('RDS_PORT', '5432'),
    'dbname': os.getenv('RDS_DB'),
    'user': os.getenv('RDS_USER'),
    'password': os.getenv('RDS_PASSWORD')
}


def setup_database():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ── Raw telemetry, written continuously by consumer.py ──
    cur.execute('''
        CREATE TABLE IF NOT EXISTS fleet_telemetry_raw (
            id                      SERIAL PRIMARY KEY,
            vehicle_id              VARCHAR(10)   NOT NULL,
            timestamp               TIMESTAMP     NOT NULL,
            speed_kmh               FLOAT,
            battery_pct             FLOAT,
            motor_temp_c            FLOAT,
            lat                     FLOAT,
            lon                     FLOAT,
            charge_state            VARCHAR(20),
            odometer_km             FLOAT,
            cumulative_charge_pct   FLOAT,
            created_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print('  ✓ fleet_telemetry_raw')

    # ── Anomaly alerts, written by anomly_detector.py ──
    cur.execute('''
        CREATE TABLE IF NOT EXISTS fleet_anomaly_alerts (
            alert_id        SERIAL PRIMARY KEY,
            vehicle_id      VARCHAR(10)   NOT NULL,
            timestamp       TIMESTAMP     NOT NULL,
            alert_type      VARCHAR(30),
            triggered_value FLOAT,
            threshold       FLOAT,
            severity        VARCHAR(10),
            created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print('  ✓ fleet_anomaly_alerts')

    # ── Daily summary, written once a day by the Airflow DAG ──
    cur.execute('''
        CREATE TABLE IF NOT EXISTS fleet_daily_summary (
            summary_date            DATE          NOT NULL,
            vehicle_id              VARCHAR(10)   NOT NULL,
            avg_speed_kmh           FLOAT,
            avg_battery_pct         FLOAT,
            avg_motor_temp_c        FLOAT,
            total_events            INTEGER,
            is_at_risk              BOOLEAN       DEFAULT FALSE,
            risk_reasons            TEXT,
            distance_traveled_today FLOAT,
            charge_cycles_today     FLOAT,
            created_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (summary_date, vehicle_id)
        )
    ''')
    print('  ✓ fleet_daily_summary')

    # ── Reliability metrics, written once a day by the Airflow DAG ──
    cur.execute('''
        CREATE TABLE IF NOT EXISTS fleet_reliability_metrics (
            analysis_date           DATE         NOT NULL,
            vehicle_id              VARCHAR(10)  NOT NULL,
            at_risk_days_7d         INTEGER,
            distance_traveled_7d    FLOAT,
            charge_cycles_7d        FLOAT,
            failure_rate_per_100km  FLOAT,
            battery_risk_per_cycle  FLOAT,
            dominant_fault_type     VARCHAR(30),
            reliability_score       FLOAT,
            created_at              TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (analysis_date, vehicle_id)
        )
    ''')
    print('  ✓ fleet_reliability_metrics')

    conn.commit()
    cur.close()
    conn.close()
    print('\nSchema setup complete.')


if __name__ == '__main__':
    setup_database()
