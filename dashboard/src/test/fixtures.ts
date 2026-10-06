import type {
  Alert,
  DeviceDetail,
  DeviceSummary,
  FleetSummary,
  MetricName,
  MetricSeries,
} from "../api/types";

export function device(overrides: Partial<DeviceSummary> = {}): DeviceSummary {
  return {
    device_id: "100.64.0.1",
    hostname: "edge-001",
    os: "Ubuntu 24.04",
    status: "healthy",
    last_seen: "2026-10-06T09:45:00Z",
    uptime_seconds: 382941,
    cpu_usage_percent: 43.7,
    cpu_temperature_c: 57.2,
    ram_usage_percent: 56.6,
    disk_usage_percent: 61.1,
    packet_loss_percent: 0,
    open_alert_count: 0,
    ...overrides,
  };
}

export function deviceDetail(overrides: Partial<DeviceDetail> = {}): DeviceDetail {
  return {
    ...device(),
    first_seen: "2026-10-01T00:00:00Z",
    services: [
      { name: "docker", state: "running", changed_at: "2026-10-01T00:00:00Z" },
      { name: "edge_streamer", state: "failed", changed_at: "2026-10-06T09:40:00Z" },
    ],
    latest: {
      ts: "2026-10-06T09:45:00Z",
      load_1m: 1.42,
      ram_used_mb: 9271,
      ram_total_mb: 16384,
      disk_used_gb: 291,
      disk_total_gb: 476,
      net_interface: "eth0",
      rx_rate_bps: 80000,
      tx_rate_bps: 8000,
      containers_running: 5,
      containers_stopped: 0,
    },
    open_alerts: [],
    ...overrides,
  };
}

export function alert(overrides: Partial<Alert> = {}): Alert {
  return {
    id: 1,
    device_id: "100.64.0.2",
    hostname: "edge-002",
    rule: "cpu_temp_high",
    severity: "critical",
    opened_at: "2026-10-06T09:00:00Z",
    resolved_at: null,
    last_value: 90,
    message: "CPU temperature at 90°C",
    ...overrides,
  };
}

export function fleetSummary(overrides: Partial<FleetSummary> = {}): FleetSummary {
  return {
    devices: { healthy: 47, warning: 1, critical: 2, offline: 0, total: 50 },
    open_alerts: { warning: 1, critical: 2 },
    hottest: [],
    fullest_disks: [],
    ...overrides,
  };
}

export function metricSeries(
  metric: MetricName,
  values: (number | null)[] = [40, 42],
): MetricSeries {
  return {
    device_id: "100.64.0.1",
    metric,
    unit: "%",
    bucket: "raw",
    start: "2026-10-06T09:00:00Z",
    end: "2026-10-06T10:00:00Z",
    points: values.map((value, i) => ({
      ts: new Date(Date.parse("2026-10-06T09:00:00Z") + i * 300_000).toISOString(),
      value,
      max: null,
    })),
  };
}
