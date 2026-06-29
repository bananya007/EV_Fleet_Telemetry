# Fleet Telemetry Reliability Pipeline

## Tech Stack

- **Apache Kafka** — streams telemetry events between the producer and consumers
- **Python** — simulator, consumer, anomaly detector, and Airflow DAG logic
- **AWS RDS (PostgreSQL)** — persists raw telemetry, daily summaries, reliability metrics, and alerts
- **Apache Airflow** (Dockerized) — orchestrates the daily batch rollup
- **Docker / Docker Compose** — runs Kafka and Airflow as isolated services
- **SQL** — root cause analysis queries against the data

## Problem Statement

A fleet of vehicles is constantly generating sensor data: speed, battery level, motor temperature, and location. Buried in that constant stream are early warning signs of vehicles that are degrading or at risk of failure, such as motors running hot, batteries draining abnormally fast, or speeds spiking. A reliability engineer can't manually watch raw second-by-second data for every vehicle, since there's too much of it and most of it is normal.

Even once the raw data is summarized, a simple day-by-day count of problems is misleading. Two vehicles can show the same number of bad days while one has driven far more than the other, or one battery has gone through more charge cycles than the other. Comparing vehicles fairly means measuring problems against how much each vehicle was actually used, not just how many days have passed.

**The problem: how do you take a continuous flood of raw vehicle sensor data and turn it into a small, prioritized list of vehicles to look at, in a way that fairly reflects how much each vehicle has actually been driven and charged, not just how many calendar days have passed?**

## Deliverable

A pipeline that takes raw vehicle telemetry and produces one end result: a dashboard that ranks every vehicle in the fleet by a reliability score that accounts for distance traveled and charge cycles, shows what's wrong with the unhealthy ones (overheating, speed, or battery), and lets someone glance at it once a day and know exactly where to focus.

## Pipeline Architecture

```
simulator.py  (producer)
      │
      ▼
Kafka topic: fleet-telemetry-raw
      │
      ├─────────────────────────────────┐
      ▼                                 ▼
consumer.py                      anomly_detector.py
      │                                 │
      ▼                                 ├───────────────────────────┐
fleet_telemetry_raw (RDS)                ▼                           ▼
      │                       Kafka topic:                 fleet_anomaly_alerts (RDS)
      │                       fleet-anomaly-alerts
      ▼
Airflow DAG: fleet_daily_summary  (runs once a day)
      │
      ├─ create_summary_table
      │
      ├─ generate_daily_summary ─────────► fleet_daily_summary (RDS)
      │
      └─ compute_reliability_metrics ────► fleet_reliability_metrics (RDS)
```

There are two independent paths once data leaves Kafka. The top path is real time: `anomly_detector.py` reacts to every single reading the instant it arrives, publishing alerts to Kafka for any live consumer, while also writing the same alert into `fleet_anomaly_alerts` for later analysis. The bottom path is batch: `consumer.py` persists every raw reading into RDS continuously, and once a day the Airflow DAG rolls that raw data up into a daily summary, then rolls 7 days of daily summaries up into a reliability score per vehicle.

## Data Lineage

### Table: raw_telemetry
One row per second, per vehicle.

| Column | Description | Formula |
|---|---|---|
| `vehicle_id` | Identifies the vehicle | - |
| `timestamp` | When the reading was taken | - |
| `speed_kmh` | Current speed | - |
| `battery_pct` | Current battery level | - |
| `motor_temp_c` | Current motor temperature | - |
| `lat`, `lon` | GPS coordinates, kept as location metadata | - |
| `charge_state` | Charging, discharging, or idle | - |
| `odometer_km` | Cumulative distance driven over the vehicle's life | Increases each tick by speed_kmh / 3600 |
| `cumulative_charge_pct` | Cumulative percentage of charge that has flowed into the battery over the vehicle's life | Increases each tick by max(0, battery_pct now minus battery_pct previous) |

### Table: daily_summary
One row per vehicle, per day. Built by summarizing raw_telemetry.

| Column | Description | Formula |
|---|---|---|
| `summary_date` | The day this row covers | - |
| `vehicle_id` | Identifies the vehicle | - |
| `avg_speed_kmh` | Average speed for the day | Average of speed_kmh across the day |
| `avg_battery_pct` | Average battery level for the day | Average of battery_pct across the day |
| `avg_motor_temp_c` | Average motor temperature for the day | Average of motor_temp_c across the day |
| `is_at_risk` | Whether the vehicle showed a risk signal that day | TRUE if avg_motor_temp_c > 75, avg_battery_pct < 30, or avg_speed_kmh > 150 |
| `distance_traveled_today` | Distance driven that day | odometer_km at end of day minus odometer_km at start of day |
| `charge_cycles_today` | Equivalent full charge cycles that day | cumulative_charge_pct change that day, divided by 100 |

### Table: reliability_metrics
One row per vehicle, computed over a rolling 7 day window. Built by summarizing daily_summary.

| Column | Description | Formula |
|---|---|---|
| `vehicle_id` | Identifies the vehicle | - |
| `at_risk_days_7d` | Count of at-risk days in the last 7 days | Count of is_at_risk = TRUE in the last 7 daily_summary rows |
| `distance_traveled_7d` | Total distance driven in the last 7 days | Sum of distance_traveled_today over the last 7 days |
| `charge_cycles_7d` | Total charge cycles in the last 7 days | Sum of charge_cycles_today over the last 7 days |
| `failure_rate_per_100km` | At-risk days normalized by distance driven | (at_risk_days_7d / distance_traveled_7d) x 100 |
| `battery_risk_per_cycle` | Battery-related at-risk days normalized by charge cycles | Battery-related at-risk days / charge_cycles_7d |
| `dominant_fault_type` | Most common reason behind the at-risk days | Most frequent risk reason in the last 7 days |
| `reliability_score` | Composite 0 to 100 reliability score | 100 minus (failure_rate_per_100km x 20) minus (battery_risk_per_cycle x 20) |

## Getting Started

**Prerequisites:** Docker, Python 3.x, a PostgreSQL database (e.g. AWS RDS).

1. Copy `.env.example` to `.env` and fill in your database credentials and an `AIRFLOW_SECRET_KEY` (generate one with `openssl rand -hex 16`).
2. Start Kafka: `docker compose -f docker-compose.yml up -d`
3. Create the Kafka topics: `python setup_topics.py`
4. Create the database schema: `python setup_database.py`
5. Start the streaming layer, each in its own terminal: `python simulator.py`, `python consumer.py`, `python anomly_detector.py`
6. Start Airflow: `docker compose -f docker-compose.airflow.yml up -d`
7. Open the Airflow UI at `http://localhost:8085` (`admin` / `admin`), unpause `fleet_daily_summary`, and trigger it
8. Explore the results with the queries in `root_cause_queries.sql`

