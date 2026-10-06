import { useSearchParams } from "react-router";
import { useAlerts } from "../api/hooks";
import type { AlertState, Severity } from "../api/types";
import { AlertsTable } from "../components/AlertsTable";

const STATES: AlertState[] = ["open", "resolved", "all"];

function parseState(value: string | null): AlertState {
  return STATES.find((s) => s === value) ?? "open";
}

function parseSeverity(value: string | null): Severity | undefined {
  return value === "warning" || value === "critical" ? value : undefined;
}

export function AlertsPage() {
  const [params, setParams] = useSearchParams();
  const state = parseState(params.get("state"));
  const severity = parseSeverity(params.get("severity"));
  const alerts = useAlerts({ state, severity });

  const update = (next: { state?: AlertState; severity?: Severity | undefined }) => {
    const merged = { state, severity, ...next };
    const query: Record<string, string> = { state: merged.state };
    if (merged.severity) query.severity = merged.severity;
    setParams(query);
  };

  return (
    <>
      <div className="page-head">
        <h1>Alerts</h1>
        {alerts.data && <span className="muted">{alerts.data.total} total</span>}
      </div>
      <div className="filters">
        <div className="segmented" role="group" aria-label="Alert state">
          {STATES.map((s) => (
            <button
              key={s}
              type="button"
              aria-pressed={state === s}
              onClick={() => update({ state: s })}
            >
              {s}
            </button>
          ))}
        </div>
        <label className="select">
          Severity
          <select
            value={severity ?? ""}
            onChange={(e) => update({ severity: parseSeverity(e.target.value) })}
          >
            <option value="">All</option>
            <option value="warning">Warning</option>
            <option value="critical">Critical</option>
          </select>
        </label>
      </div>
      {alerts.isError ? (
        <p className="error">Couldn't load alerts.</p>
      ) : alerts.data ? (
        <AlertsTable alerts={alerts.data.items} />
      ) : (
        <p className="muted">Loading alerts…</p>
      )}
    </>
  );
}
