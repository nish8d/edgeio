import { RANGE_KEYS, type RangeKey } from "../lib/range";

export function RangePicker({
  value,
  onChange,
}: {
  value: RangeKey;
  onChange: (range: RangeKey) => void;
}) {
  return (
    <div className="segmented" role="group" aria-label="Time range">
      {RANGE_KEYS.map((key) => (
        <button key={key} type="button" aria-pressed={value === key} onClick={() => onChange(key)}>
          {key}
        </button>
      ))}
    </div>
  );
}
