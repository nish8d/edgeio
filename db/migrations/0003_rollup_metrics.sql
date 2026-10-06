-- Recreate both rollups with avg + max for every metric the dashboard charts,
-- including network rates. Dropping a continuous aggregate also drops its policies,
-- so they are re-added below.

DROP MATERIALIZED VIEW health_daily;
DROP MATERIALIZED VIEW health_hourly;

CREATE MATERIALIZED VIEW health_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT device_id,
       time_bucket(INTERVAL '1 hour', ts) AS bucket,
       avg(cpu_usage_percent)   AS cpu_avg,
       max(cpu_usage_percent)   AS cpu_max,
       avg(cpu_temperature_c)   AS temp_avg,
       max(cpu_temperature_c)   AS temp_max,
       avg(ram_usage_percent)   AS ram_avg,
       max(ram_usage_percent)   AS ram_max,
       avg(disk_usage_percent)  AS disk_avg,
       max(disk_usage_percent)  AS disk_max,
       avg(packet_loss_percent) AS packet_loss_avg,
       max(packet_loss_percent) AS packet_loss_max,
       avg(rx_rate_bps)         AS rx_rate_avg,
       max(rx_rate_bps)         AS rx_rate_max,
       avg(tx_rate_bps)         AS tx_rate_avg,
       max(tx_rate_bps)         AS tx_rate_max,
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
       max(ram_usage_percent)   AS ram_max,
       avg(disk_usage_percent)  AS disk_avg,
       max(disk_usage_percent)  AS disk_max,
       avg(packet_loss_percent) AS packet_loss_avg,
       max(packet_loss_percent) AS packet_loss_max,
       avg(rx_rate_bps)         AS rx_rate_avg,
       max(rx_rate_bps)         AS rx_rate_max,
       avg(tx_rate_bps)         AS tx_rate_avg,
       max(tx_rate_bps)         AS tx_rate_max,
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

SELECT add_retention_policy('health_hourly', INTERVAL '1 year');
