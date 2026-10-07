-- Real agents spool readings while Kafka is unreachable and deliver up to 7 days late.
-- A refresh policy only re-materializes buckets inside [now - start_offset, now - end_offset],
-- so widen both windows past the spool's 7-day cap; otherwise a drained backlog never reaches
-- the rollups and long-range charts keep a permanent hole. Both stay well inside raw retention
-- (30 days) so a refresh never sees raw data already dropped.
SELECT remove_continuous_aggregate_policy('health_hourly');
SELECT add_continuous_aggregate_policy('health_hourly',
    start_offset => INTERVAL '8 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes');

SELECT remove_continuous_aggregate_policy('health_daily');
SELECT add_continuous_aggregate_policy('health_daily',
    start_offset => INTERVAL '10 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour');
