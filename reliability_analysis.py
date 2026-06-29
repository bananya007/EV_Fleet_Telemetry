import psycopg2
import os
from datetime import date, timedelta
from dotenv import load_dotenv
from collections import Counter

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('RDS_HOST'),
    'port': os.getenv('RDS_PORT', '5432'),
    'dbname': os.getenv('RDS_DB'),
    'user': os.getenv('RDS_USER'),
    'password': os.getenv('RDS_PASSWORD')
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def _dominant_fault_type(risk_reasons_list):
    """Look at risk_reasons text across at-risk days and find the most common fault."""
    fault_counts = Counter()
    for reasons in risk_reasons_list:
        if not reasons:
            continue
        if 'motor temp' in reasons:
            fault_counts['motor_overheating'] += 1
        if 'battery' in reasons:
            fault_counts['battery_degradation'] += 1
        if 'speed' in reasons:
            fault_counts['overspeed'] += 1
    return fault_counts.most_common(1)[0][0] if fault_counts else None


def _reliability_score(failure_rate_per_100km, battery_risk_per_cycle):
    score = 100.0
    score -= (failure_rate_per_100km or 0) * 20
    score -= (battery_risk_per_cycle or 0) * 20
    return max(0.0, round(score, 1))


def compute_reliability_metrics(analysis_date=None):
    if analysis_date is None:
        analysis_date = date.today()
    if isinstance(analysis_date, str):
        analysis_date = date.fromisoformat(analysis_date)

    # 7 day window, inclusive of analysis_date
    since_7d = analysis_date - timedelta(days=6)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT vehicle_id FROM fleet_daily_summary ORDER BY vehicle_id")
    vehicles = [r[0] for r in cur.fetchall()]

    if not vehicles:
        print('No vehicle data found in fleet_daily_summary. Run the DAG first.')
        cur.close()
        conn.close()
        return

    print(f'Computing reliability metrics for {len(vehicles)} vehicles on {analysis_date}...\n')

    for vid in vehicles:
        cur.execute('''
            SELECT is_at_risk, risk_reasons, distance_traveled_today, charge_cycles_today
            FROM fleet_daily_summary
            WHERE vehicle_id = %s
              AND summary_date >= %s AND summary_date <= %s
            ORDER BY summary_date ASC
        ''', (vid, since_7d, analysis_date))
        daily_rows = cur.fetchall()

        at_risk_rows = [r for r in daily_rows if r[0]]
        at_risk_days_7d = len(at_risk_rows)

        distance_traveled_7d = sum(r[2] or 0 for r in daily_rows)
        charge_cycles_7d = sum(r[3] or 0 for r in daily_rows)

        failure_rate_per_100km = round((at_risk_days_7d / distance_traveled_7d) * 100, 4) \
            if distance_traveled_7d > 0 else None

        battery_related_days = sum(1 for r in at_risk_rows if r[1] and 'battery' in r[1])
        battery_risk_per_cycle = round(battery_related_days / charge_cycles_7d, 4) \
            if charge_cycles_7d > 0 else None

        dominant_fault = _dominant_fault_type([r[1] for r in at_risk_rows])

        score = _reliability_score(failure_rate_per_100km, battery_risk_per_cycle)

        cur.execute('''
            INSERT INTO fleet_reliability_metrics
                (analysis_date, vehicle_id, at_risk_days_7d, distance_traveled_7d,
                 charge_cycles_7d, failure_rate_per_100km, battery_risk_per_cycle,
                 dominant_fault_type, reliability_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (analysis_date, vehicle_id) DO UPDATE SET
                at_risk_days_7d        = EXCLUDED.at_risk_days_7d,
                distance_traveled_7d   = EXCLUDED.distance_traveled_7d,
                charge_cycles_7d       = EXCLUDED.charge_cycles_7d,
                failure_rate_per_100km = EXCLUDED.failure_rate_per_100km,
                battery_risk_per_cycle = EXCLUDED.battery_risk_per_cycle,
                dominant_fault_type    = EXCLUDED.dominant_fault_type,
                reliability_score      = EXCLUDED.reliability_score,
                created_at             = CURRENT_TIMESTAMP
        ''', (
            analysis_date, vid, at_risk_days_7d, round(distance_traveled_7d, 2),
            round(charge_cycles_7d, 4), failure_rate_per_100km, battery_risk_per_cycle,
            dominant_fault, score
        ))

        print(f'  {vid} | score: {score:5.1f} | '
              f'at_risk_days_7d: {at_risk_days_7d} | '
              f'distance_7d: {round(distance_traveled_7d, 1)}km | '
              f'cycles_7d: {round(charge_cycles_7d, 2)} | '
              f'dominant_fault: {dominant_fault or "none"}')

    conn.commit()
    cur.close()
    conn.close()
    print(f'\nDone. Reliability metrics written for {len(vehicles)} vehicles.')


if __name__ == '__main__':
    compute_reliability_metrics()
