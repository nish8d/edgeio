import type { DeviceStatus } from "../api/types";
import { STATUS_META } from "../lib/status";

export function StatusBadge({ status }: { status: DeviceStatus }) {
  const { label, icon } = STATUS_META[status];
  return (
    <span className={`badge badge-${status}`}>
      <span aria-hidden="true" className="badge-icon">
        {icon}
      </span>
      {label}
    </span>
  );
}
