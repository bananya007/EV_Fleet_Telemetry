from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── RDS Connection ──
DB_CONFIG = {
    'host': os.getenv('RDS_HOST'),
    'port': os.getenv('RDS_PORT', '5432'),
    'dbname': os.getenv('RDS_DB', 'fleetdb'),
    'user': os.getenv('RDS_USER', 'postgres'),
    'password': os.getenv('RDS_PASSWORD')
}


# ── DAG config ──
default_args = {
    'owner': 'fleet-pipeline',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fleet_daily_summary',
    default_args=default_args,
    description='Daily summary of fleet metrics and at-risk vehicle flagging',
    schedule_interval='@daily',
    start_date=datetime(2026, 6, 26),
    catchup=True,
    tags=['fleet', 'kafka', 'streaming']
)


# ── Task 1: Generate daily summary ──
# Tables already exist, created once by setup_database.py. This task only reads and writes data.
def generate_daily_summary(**context):
    execution_date = context['ds']

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Daily averages and event count, straight from raw telemetry
    cur.execute('''
        SELECT
            vehicle_id,
            AVG(speed_kmh)    AS daily_avg_speed,
            AVG(battery_pct)  AS daily_avg_battery,
            AVG(motor_temp_c) AS daily_avg_temp,
            COUNT(*)          AS total_events
        FROM fleet_telemetry_raw
        WHERE timestamp::date = %s
        GROUP BY vehicle_id
        ORDER BY vehicle_id
    ''', (execution_date,))

    rows = cur.fetchall()

    if not rows:
        print(f'No data found for {execution_date}. Skipping.')
        cur.close()
        conn.close()
        return

    # First reading of the day per vehicle, the starting point for distance and charge cycles
    cur.execute('''
        SELECT DISTINCT ON (vehicle_id) vehicle_id, odometer_km, cumulative_charge_pct
        FROM fleet_telemetry_raw
        WHERE timestamp::date = %s
        ORDER BY vehicle_id, timestamp ASC
    ''', (execution_date,))
    first_readings = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    # Last reading of the day per vehicle, the ending point for distance and charge cycles
    cur.execute('''
        SELECT DISTINCT ON (vehicle_id) vehicle_id, odometer_km, cumulative_charge_pct
        FROM fleet_telemetry_raw
        WHERE timestamp::date = %s
        ORDER BY vehicle_id, timestamp DESC
    ''', (execution_date,))
    last_readings = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    print(f'Processing {len(rows)} vehicles for {execution_date}')

    for row in rows:
        vid, avg_speed, avg_battery, avg_temp, total_events = row

        first_odometer, first_charge = first_readings.get(vid, (0, 0))
        last_odometer, last_charge = last_readings.get(vid, (0, 0))

        distance_traveled_today = round(last_odometer - first_odometer, 2)
        charge_cycles_today = round((last_charge - first_charge) / 100, 4)

        risk_reasons = []
        if avg_temp and avg_temp > 75:
            risk_reasons.append(f'high avg motor temp: {avg_temp:.1f}°C')
        if avg_battery and avg_battery < 30:
            risk_reasons.append(f'low avg battery: {avg_battery:.1f}%')
        if avg_speed and avg_speed > 150:
            risk_reasons.append(f'high avg speed: {avg_speed:.1f}km/h')

        is_at_risk = len(risk_reasons) > 0
        risk_text = '; '.join(risk_reasons) if risk_reasons else None

        cur.execute('''
            INSERT INTO fleet_daily_summary
                (summary_date, vehicle_id, avg_speed_kmh, avg_battery_pct,
                 avg_motor_temp_c, total_events, is_at_risk, risk_reasons,
                 distance_traveled_today, charge_cycles_today)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (summary_date, vehicle_id)
            DO UPDATE SET
                avg_speed_kmh = EXCLUDED.avg_speed_kmh,
                avg_battery_pct = EXCLUDED.avg_battery_pct,
                avg_motor_temp_c = EXCLUDED.avg_motor_temp_c,
                total_events = EXCLUDED.total_events,
                is_at_risk = EXCLUDED.is_at_risk,
                risk_reasons = EXCLUDED.risk_reasons,
                distance_traveled_today = EXCLUDED.distance_traveled_today,
                charge_cycles_today = EXCLUDED.charge_cycles_today,
                created_at = CURRENT_TIMESTAMP
        ''', (
            execution_date, vid,
            round(avg_speed, 2) if avg_speed else None,
            round(avg_battery, 2) if avg_battery else None,
            round(avg_temp, 2) if avg_temp else None,
            total_events,
            is_at_risk, risk_text,
            distance_traveled_today, charge_cycles_today
        ))

        status = 'AT RISK' if is_at_risk else 'OK'
        print(f'  {status} {vid} — speed: {avg_speed:.1f}, '
              f'battery: {avg_battery:.1f}%, temp: {avg_temp:.1f}°C, '
              f'distance: {distance_traveled_today}km, cycles: {charge_cycles_today}')
        if risk_text:
            print(f'         Reason: {risk_text}')

    conn.commit()
    cur.close()
    conn.close()
    print(f'\nDaily summary written for {execution_date}.')


# ── Task 2: Compute reliability metrics ──
def compute_reliability_metrics_task(**context):
    from reliability_analysis import compute_reliability_metrics
    execution_date = context['ds']
    compute_reliability_metrics(analysis_date=execution_date)


# ── Wire tasks ──
t2 = PythonOperator(
    task_id='generate_daily_summary',
    python_callable=generate_daily_summary,
    dag=dag
)

t3 = PythonOperator(
    task_id='compute_reliability_metrics',
    python_callable=compute_reliability_metrics_task,
    dag=dag
)

t2 >> t3