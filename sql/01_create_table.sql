CREATE EXTENSION IF NOT EXISTS timescaledb;

DROP TABLE IF EXISTS wind_turbine_readings CASCADE;

CREATE TABLE wind_turbine_readings (
    time TIMESTAMPTZ NOT NULL,
    turbine_id TEXT NOT NULL,
    wind_speed DOUBLE PRECISION,
    power_kw DOUBLE PRECISION,
    generator_temp DOUBLE PRECISION,
    bearing_temp DOUBLE PRECISION,
    gear_temp DOUBLE PRECISION,
    blade_pitch_angle DOUBLE PRECISION,
    nacelle_position DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION,
    ambient_air_temp DOUBLE PRECISION,
    gearbox_sump_temp DOUBLE PRECISION,
    generator_speed DOUBLE PRECISION,
    hub_speed INTEGER,
    health_status TEXT,
    power_efficiency DOUBLE PRECISION,
    is_anomaly BOOLEAN DEFAULT FALSE
);

SELECT create_hypertable('wind_turbine_readings', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_wind_turbine_time ON wind_turbine_readings (turbine_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_wind_health_time ON wind_turbine_readings (health_status, time DESC);
CREATE INDEX IF NOT EXISTS idx_wind_anomaly_time ON wind_turbine_readings (is_anomaly, time DESC) WHERE is_anomaly = TRUE;
