USE hydro_analytics;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_critical_high_flow AS
    SELECT station_id, bucket_date, recorded_at, water_flow, power_output
    FROM telemetry_daily
    WHERE station_id IS NOT NULL 
      AND bucket_date IS NOT NULL 
      AND recorded_at IS NOT NULL 
      AND water_flow IS NOT NULL
    PRIMARY KEY ((station_id, bucket_date), water_flow, recorded_at)
    WITH CLUSTERING ORDER BY (water_flow DESC, recorded_at DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_high_water_level AS
    SELECT station_id, bucket_date, recorded_at, water_level, water_flow
    FROM telemetry_daily
    WHERE station_id IS NOT NULL 
      AND bucket_date IS NOT NULL 
      AND recorded_at IS NOT NULL 
      AND water_level IS NOT NULL
    PRIMARY KEY ((station_id, bucket_date), water_level, recorded_at)
    WITH CLUSTERING ORDER BY (water_level DESC, recorded_at DESC);
