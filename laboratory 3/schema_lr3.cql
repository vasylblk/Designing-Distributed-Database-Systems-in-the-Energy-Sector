CREATE KEYSPACE IF NOT EXISTS hydro_analytics
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE hydro_analytics;

CREATE TABLE IF NOT EXISTS telemetry_simple (
    station_id TEXT,
    recorded_at TIMESTAMP,
    turbine_type TEXT,
    water_flow DOUBLE,
    water_level DOUBLE,
    power_output DOUBLE,
    PRIMARY KEY (station_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC)
AND comment = 'Схема 1: Simple Wide Row';
CREATE TABLE IF NOT EXISTS telemetry_hourly (
    station_id TEXT,
    bucket_hour TIMESTAMP,
    recorded_at TIMESTAMP,
    turbine_type TEXT,
    water_flow DOUBLE,
    water_level DOUBLE,
    power_output DOUBLE,
    PRIMARY KEY ((station_id, bucket_hour), recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC)
AND comment = 'Схема 2: Hourly Bucketing';
CREATE TABLE IF NOT EXISTS telemetry_daily (
    station_id TEXT,
    bucket_date DATE,
    recorded_at TIMESTAMP,
    turbine_type TEXT,
    water_flow DOUBLE,
    water_level DOUBLE,
    power_output DOUBLE,
    PRIMARY KEY ((station_id, bucket_date), recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC)
AND default_time_to_live = 7776000 -- TTL 90 днів для сирих даних
AND comment = 'Схема 3: Daily Bucketing з TTL';
CREATE TABLE IF NOT EXISTS hydro_hourly_aggregates (
    station_id TEXT,
    hour TIMESTAMP,
    avg_water_flow DOUBLE,
    max_water_flow DOUBLE,
    avg_water_level DOUBLE,
    total_power DOUBLE,
    samples_count INT,
    PRIMARY KEY (station_id, hour)
) WITH CLUSTERING ORDER BY (hour DESC)
AND comment = 'Схема 3: Hourly Aggregates';
CREATE TABLE IF NOT EXISTS telemetry_daily_deflate (
    station_id TEXT,
    bucket_date DATE,
    recorded_at TIMESTAMP,
    turbine_type TEXT,
    water_flow DOUBLE,
    water_level DOUBLE,
    power_output DOUBLE,
    PRIMARY KEY ((station_id, bucket_date), recorded_at)
) WITH compression = {'class': 'DeflateCompressor', 'chunk_length_in_kb': 64};
