import { useState } from "react";
import { useParams } from "react-router";
import { ApiError } from "../api/client";
import { useAlerts, useDevice } from "../api/hooks";
import { AlertsTable } from "../components/AlertsTable";
import { MetricChart } from "../components/MetricChart";
import { RangePicker } from "../components/RangePicker";
import { ServicesPanel } from "../components/ServicesPanel";
import { StatusBadge } from "../components/StatusBadge";
import { formatRelative, formatUptime } from "../lib/format";
import { CHARTS } from "../lib/metrics";
import type { RangeKey } from "../lib/range";

export function DeviceDetail() {
  const { deviceId = "" } = useParams();
  const [range, setRange] = useState<RangeKey>("24h");
  const device = useDevice(deviceId);
  const alerts = useAlerts({ state: "all", deviceId });

  if (device.isError) {
    const missing = device.error instanceof ApiError && [404, 422].includes(device.error.status);
    return <p className="error">{missing ? "Device not found." : "Couldn't load this device."}</p>;
  }
  if (!device.data) return <p className="muted">Loading device…</p>;
  const d = device.data;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>{d.hostname}</h1>
          <p className="muted">
            <span className="mono">{d.device_id}</span> · {d.os} · up{" "}
            {formatUptime(d.uptime_seconds)} · last seen {formatRelative(d.last_seen)}
          </p>
        </div>
        <StatusBadge status={d.status} />
      </div>
      <div className="filters">
        <RangePicker value={range} onChange={setRange} />
      </div>
      <div className="chart-grid">
        {CHARTS.map((spec) => (
          <MetricChart key={spec.metric} deviceId={d.device_id} spec={spec} range={range} />
        ))}
      </div>
      <div className="detail-grid">
        <ServicesPanel services={d.services} latest={d.latest} />
        <section className="panel">
          <h2>Alert history</h2>
          {alerts.isError ? (
            <p className="error">Couldn't load alerts.</p>
          ) : alerts.data ? (
            <AlertsTable alerts={alerts.data.items} showDevice={false} />
          ) : (
            <p className="muted">Loading alerts…</p>
          )}
        </section>
      </div>
    </>
  );
}
