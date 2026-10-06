import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useMetric } from "../api/hooks";
import type { MetricPoint } from "../api/types";
import { formatAxisTime, formatTimestamp } from "../lib/format";
import type { ChartSpec } from "../lib/metrics";
import type { RangeKey } from "../lib/range";

interface Props {
  deviceId: string;
  spec: ChartSpec;
  range: RangeKey;
}

const TICK = { fill: "var(--text-muted)", fontSize: 12 };

export function MetricChart({ deviceId, spec, range }: Props) {
  const { data, isPending, isError } = useMetric(deviceId, spec.metric, range);
  const points = data?.points ?? [];
  const latest = [...points].reverse().find((p) => p.value != null)?.value ?? null;
  const rollup = data !== undefined && data.bucket !== "raw";

  return (
    <section className="panel chart-card" aria-label={spec.title}>
      <header className="chart-head">
        <h3>{spec.title}</h3>
        <span className="chart-latest">{latest == null ? "—" : spec.format(latest)}</span>
      </header>
      {isError ? (
        <p className="error">Couldn't load this metric.</p>
      ) : isPending ? (
        <div className="chart-placeholder" />
      ) : points.length === 0 ? (
        <p className="empty">No data in this range.</p>
      ) : (
        <>
          <div className="chart-plot">
            <ResponsiveContainer width="100%" height={180}>
              <LineChart data={points} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                <CartesianGrid vertical={false} stroke="var(--grid)" />
                <XAxis
                  dataKey="ts"
                  tickFormatter={(ts: string) => formatAxisTime(ts, range)}
                  stroke="var(--axis)"
                  tick={TICK}
                  tickLine={false}
                  minTickGap={32}
                />
                <YAxis
                  domain={spec.domain ?? ["auto", "auto"]}
                  tickFormatter={(v: number) => spec.format(v)}
                  tick={TICK}
                  tickLine={false}
                  axisLine={false}
                  width={76}
                />
                <Tooltip
                  cursor={{ stroke: "var(--axis)", strokeWidth: 1 }}
                  content={<ChartTooltip format={spec.format} />}
                />
                <Line
                  type="monotone"
                  dataKey="value"
                  stroke="var(--series-1)"
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface)" }}
                  isAnimationActive={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <details className="chart-table">
            <summary>Data table ({rollup ? `${data.bucket} averages` : "raw readings"})</summary>
            <table className="table compact">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Value</th>
                  {rollup && <th>Max</th>}
                </tr>
              </thead>
              <tbody>
                {points.map((p) => (
                  <tr key={p.ts}>
                    <td>{formatTimestamp(p.ts)}</td>
                    <td>{p.value == null ? "—" : spec.format(p.value)}</td>
                    {rollup && <td>{p.max == null ? "—" : spec.format(p.max)}</td>}
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        </>
      )}
    </section>
  );
}

interface TooltipContent {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: unknown }>;
  format: (value: number) => string;
}

function ChartTooltip({ active, payload, format }: TooltipContent) {
  const point = payload?.[0]?.payload as MetricPoint | undefined;
  if (!active || !point) return null;
  return (
    <div className="chart-tooltip">
      <strong>{point.value == null ? "—" : format(point.value)}</strong>
      {point.max != null && <span>max {format(point.max)}</span>}
      <span className="muted">{formatTimestamp(point.ts)}</span>
    </div>
  );
}
