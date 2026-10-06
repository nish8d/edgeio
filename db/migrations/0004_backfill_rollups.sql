-- migrate: no-transaction
-- 0003 recreated the rollups empty. Their refresh policies only look back 3 hours (hourly)
-- and 3 days (daily), so once a policy moves the watermark, older buckets would vanish from
-- the views. Materialize everything up to the policies' end offsets now. Hourly buckets older
-- than the 30-day raw retention could not be rebuilt and are gone.
CALL refresh_continuous_aggregate('health_hourly', NULL, now() - INTERVAL '1 hour');
CALL refresh_continuous_aggregate('health_daily', NULL, now() - INTERVAL '1 day');
