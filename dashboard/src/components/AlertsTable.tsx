import { Link } from "react-router";
import type { Alert } from "../api/types";
import { formatTimestamp } from "../lib/format";
import { StatusBadge } from "./StatusBadge";

export function AlertsTable({
  alerts,
  showDevice = true,
}: {
  alerts: Alert[];
  showDevice?: boolean;
}) {
  if (alerts.length === 0) return <p className="empty">No alerts.</p>;
  return (
    <div className="table-wrap">
      <table className="table">
        <thead>
          <tr>
            <th>Severity</th>
            {showDevice && <th>Device</th>}
            <th>Rule</th>
            <th>Details</th>
            <th>Opened</th>
            <th>Resolved</th>
          </tr>
        </thead>
        <tbody>
          {alerts.map((a) => (
            <tr key={a.id}>
              <td>
                <StatusBadge status={a.severity} />
              </td>
              {showDevice && (
                <td>
                  <Link to={`/devices/${a.device_id}`}>{a.hostname ?? a.device_id}</Link>
                </td>
              )}
              <td className="mono">{a.rule}</td>
              <td>{a.message}</td>
              <td>{formatTimestamp(a.opened_at)}</td>
              <td>{a.resolved_at ? formatTimestamp(a.resolved_at) : <strong>Open</strong>}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
