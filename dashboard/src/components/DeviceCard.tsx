import { Link } from "react-router";
import type { DeviceSummary } from "../api/types";
import { formatCelsius, formatPercent, formatRelative } from "../lib/format";
import { StatusBadge } from "./StatusBadge";

export function DeviceCard({ device, now }: { device: DeviceSummary; now?: number }) {
  const alerts = device.open_alert_count;
  return (
    <Link to={`/devices/${device.device_id}`} className={`device-card badge-${device.status}`}>
      <div className="device-card-head">
        <span className="device-name">{device.hostname}</span>
        <StatusBadge status={device.status} />
      </div>
      <div className="muted mono">{device.device_id}</div>
      <dl className="device-metrics">
        <div>
          <dt>CPU</dt>
          <dd>{formatPercent(device.cpu_usage_percent)}</dd>
        </div>
        <div>
          <dt>Temp</dt>
          <dd>{formatCelsius(device.cpu_temperature_c)}</dd>
        </div>
        <div>
          <dt>Disk</dt>
          <dd>{formatPercent(device.disk_usage_percent)}</dd>
        </div>
      </dl>
      <div className="muted small">
        Last seen {formatRelative(device.last_seen, now)}
        {alerts > 0 && ` · ${alerts} open alert${alerts === 1 ? "" : "s"}`}
      </div>
    </Link>
  );
}
