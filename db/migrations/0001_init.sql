CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE devices (
    device_id           inet PRIMARY KEY,
    hostname            text NOT NULL,
    os                  text NOT NULL,
    first_seen          timestamptz NOT NULL,
    last_seen           timestamptz NOT NULL,
    status              text NOT NULL DEFAULT 'healthy'
                        CHECK (status IN ('healthy', 'warning', 'critical', 'offline')),
    uptime_seconds      bigint,
    cpu_usage_percent   double precision,
    cpu_temperature_c   double precision,
    ram_usage_percent   double precision,
    disk_usage_percent  double precision,
    packet_loss_percent double precision,
    last_rx_bytes       bigint NOT NULL,
    last_tx_bytes       bigint NOT NULL
);

CREATE TABLE health_readings (
    device_id           inet NOT NULL,
    ts                  timestamptz NOT NULL,
    hostname            text NOT NULL,
    uptime_seconds      bigint NOT NULL,
    cpu_usage_percent   double precision NOT NULL,
    cpu_temperature_c   double precision NOT NULL,
    load_1m             double precision NOT NULL,
    ram_total_mb        integer NOT NULL,
    ram_used_mb         integer NOT NULL,
    ram_usage_percent   double precision NOT NULL,
    disk_total_gb       double precision NOT NULL,
    disk_used_gb        double precision NOT NULL,
    disk_free_gb        double precision NOT NULL,
    disk_usage_percent  double precision NOT NULL,
    disk_free_percent   double precision NOT NULL,
    net_interface       text NOT NULL,
    rx_bytes            bigint NOT NULL,
    tx_bytes            bigint NOT NULL,
    rx_rate_bps         double precision,
    tx_rate_bps         double precision,
    packet_loss_percent double precision NOT NULL,
    containers_running  integer NOT NULL,
    containers_stopped  integer NOT NULL,
    raw                 jsonb NOT NULL,
    PRIMARY KEY (device_id, ts)
);

SELECT create_hypertable('health_readings', by_range('ts', INTERVAL '1 day'));

CREATE TABLE service_status (
    device_id  inet NOT NULL,
    service    text NOT NULL,
    state      text NOT NULL,
    changed_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (device_id, service)
);

CREATE TABLE alerts (
    id          bigserial PRIMARY KEY,
    device_id   inet NOT NULL,
    rule        text NOT NULL,
    severity    text NOT NULL CHECK (severity IN ('warning', 'critical')),
    opened_at   timestamptz NOT NULL,
    resolved_at timestamptz,
    last_value  double precision,
    message     text NOT NULL
);

CREATE UNIQUE INDEX alerts_one_open_per_rule ON alerts (device_id, rule) WHERE resolved_at IS NULL;
CREATE INDEX alerts_opened_at_idx ON alerts (opened_at DESC);
