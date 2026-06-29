-- Root cause analysis queries for the fleet telemetry pipeline.
-- Meant to be run one at a time, top to bottom, going from "which vehicle"
-- to "why" to "show me the exact moment it happened."
-- Replace the vehicle_id and timestamp placeholders as you drill down.


-- 1. Fleet ranking: which vehicles currently have the worst reliability score.
-- Start here to decide which vehicle is worth investigating.
SELECT vehicle_id, reliability_score, failure_rate_per_100km,
       battery_risk_per_cycle, dominant_fault_type
FROM fleet_reliability_metrics
WHERE analysis_date = (SELECT MAX(analysis_date) FROM fleet_reliability_metrics)
ORDER BY reliability_score ASC;


-- 2. Why is this vehicle flagged: its daily risk history over time.
-- Shows whether it's a one-off bad day or a repeating pattern.
SELECT summary_date, is_at_risk, risk_reasons,
       avg_speed_kmh, avg_battery_pct, avg_motor_temp_c,
       distance_traveled_today, charge_cycles_today
FROM fleet_daily_summary
WHERE vehicle_id = 'V003'   -- replace with the vehicle from query 1
ORDER BY summary_date DESC;


-- 3. What actually triggered it: every individual anomaly alert for this vehicle.
-- Unlike the daily average above, this shows the exact moments thresholds were crossed.
SELECT timestamp, alert_type, triggered_value, threshold, severity
FROM fleet_anomaly_alerts
WHERE vehicle_id = 'V003'   -- replace with the vehicle from query 1
ORDER BY timestamp DESC;


-- 4. The raw signal around a specific alert.
-- Take a timestamp from query 3 and see what the sensor was doing just
-- before and after it, this is the actual root cause view.
SELECT timestamp, speed_kmh, battery_pct, motor_temp_c, charge_state
FROM fleet_telemetry_raw
WHERE vehicle_id = 'V003'   -- replace with the vehicle from query 1
  AND timestamp BETWEEN '2026-06-26 18:00:00'::timestamp - INTERVAL '5 minutes'
                     AND '2026-06-26 18:00:00'::timestamp + INTERVAL '5 minutes'
ORDER BY timestamp ASC;


-- 5. Dominant fault across the fleet: is this a single vehicle's problem,
-- or a fault type showing up everywhere.
SELECT alert_type, severity, COUNT(*) AS alert_count
FROM fleet_anomaly_alerts
GROUP BY alert_type, severity
ORDER BY alert_count DESC;
