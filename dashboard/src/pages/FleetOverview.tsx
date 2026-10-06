import { Link, useSearchParams } from "react-router";
import { useDevices, useFleetSummary } from "../api/hooks";
import type { DeviceStatus } from "../api/types";
import { DeviceCard } from "../components/DeviceCard";
import { StatusTiles } from "../components/StatusTiles";
import { STATUS_META, isDeviceStatus } from "../lib/status";

export function FleetOverview() {
  const [params, setParams] = useSearchParams();
  const raw = params.get("status");
  const status = isDeviceStatus(raw) ? raw : undefined;
  const summary = useFleetSummary();
  const devices = useDevices(status);
  const select = (next: DeviceStatus | undefined) => setParams(next ? { status: next } : {});

  return (
    <>
      <div className="page-head">
        <h1>Fleet</h1>
        {summary.data && (
          <Link to="/alerts" className="muted">
            {summary.data.open_alerts.critical} critical · {summary.data.open_alerts.warning}{" "}
            warning alerts open
          </Link>
        )}
      </div>
      {summary.isError ? (
        <p className="error">Couldn't load the fleet summary.</p>
      ) : summary.data ? (
        <StatusTiles counts={summary.data.devices} selected={status} onSelect={select} />
      ) : (
        <div className="tiles tiles-placeholder" />
      )}
      <h2 className="section-title">
        {status ? `${STATUS_META[status].label} devices` : "All devices"}
        {devices.data && <span className="muted"> ({devices.data.total})</span>}
      </h2>
      {devices.isError ? (
        <p className="error">Couldn't load devices.</p>
      ) : !devices.data ? (
        <p className="muted">Loading devices…</p>
      ) : devices.data.items.length === 0 ? (
        <p className="empty">No devices match this filter.</p>
      ) : (
        <div className="device-grid">
          {devices.data.items.map((d) => (
            <DeviceCard key={d.device_id} device={d} />
          ))}
        </div>
      )}
    </>
  );
}
