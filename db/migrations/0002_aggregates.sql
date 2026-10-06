-- WITH NO DATA lets continuous aggregates be created inside the migration transaction.

CREATE MATERIALIZED VIEW health_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT device_id,
       time_bucket(INTERVAL '1 hour', ts) AS bucket,
       avg(cpu_usage_percent)   AS cpu_avg,
       max(cpu_usage_percent)   AS cpu_max,
       avg(cpu_temperature_c)   AS temp_avg,
       max(cpu_temperature_c)   AS temp_max,
       avg(ram_usage_percent)   AS ram_avg,
       max(disk_usage_percent)  AS disk_max,
       avg(packet_loss_percent) AS packet_loss_avg,
       count(*)                 AS reading_count
FROM health_readings
GROUP BY device_id, bucket
WITH NO DATA;

CREATE MATERIALIZED VIEW health_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT device_id,
       time_bucket(INTERVAL '1 day', ts) AS bucket,
       avg(cpu_usage_percent)   AS cpu_avg,
       max(cpu_usage_percent)   AS cpu_max,
       avg(cpu_temperature_c)   AS temp_avg,
       max(cpu_temperature_c)   AS temp_max,
       avg(ram_usage_percent)   AS ram_avg,
       max(disk_usage_percent)  AS disk_max,
       avg(packet_loss_percent) AS packet_loss_avg,
       count(*)                 AS reading_count
FROM health_readings
GROUP BY device_id, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('health_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes');

SELECT add_continuous_aggregate_policy('health_daily',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour');

SELECT add_retention_policy('health_readings', INTERVAL '30 days');
SELECT add_retention_policy('health_hourly', INTERVAL '1 year');
