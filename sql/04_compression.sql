ALTER TABLE wind_turbine_readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'turbine_id',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('wind_turbine_readings', INTERVAL '7 days');
