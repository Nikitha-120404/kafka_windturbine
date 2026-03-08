DROP MATERIALIZED VIEW IF EXISTS turbine_5min_kpis;

CREATE MATERIALIZED VIEW turbine_5min_kpis
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS bucket,
    turbine_id,
    AVG(wind_speed) AS avg_wind_speed,
    AVG(power_kw) AS avg_power_kw,
    MAX(generator_temp) AS max_generator_temp,
    MAX(gear_temp) AS max_gear_temp,
    AVG(power_efficiency) AS avg_efficiency,
    COUNT(*) AS reading_count,
    COUNT(*) FILTER (WHERE health_status = 'WARNING') AS warning_count,
    COUNT(*) FILTER (WHERE health_status = 'CRITICAL') AS critical_count,
    COUNT(*) FILTER (WHERE is_anomaly = TRUE) AS anomaly_count
FROM wind_turbine_readings
GROUP BY bucket, turbine_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'turbine_5min_kpis',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes'
);
