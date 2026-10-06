import type { DeviceStatus, StatusCounts } from "../api/types";
import { STATUS_META, STATUS_ORDER } from "../lib/status";

interface Props {
  counts: StatusCounts;
  selected?: DeviceStatus;
  onSelect: (status: DeviceStatus | undefined) => void;
}

export function StatusTiles({ counts, selected, onSelect }: Props) {
  return (
    <div className="tiles" role="group" aria-label="Filter devices by status">
      {STATUS_ORDER.map((status) => {
        const { label, icon } = STATUS_META[status];
        const active = selected === status;
        return (
          <button
            key={status}
            type="button"
            className={`tile badge-${status}`}
            aria-pressed={active}
            onClick={() => onSelect(active ? undefined : status)}
          >
            <span className="tile-value">{counts[status]}</span>
            <span className="tile-label">
              <span aria-hidden="true" className="badge-icon">
                {icon}
              </span>
              {label}
            </span>
          </button>
        );
      })}
    </div>
  );
}
