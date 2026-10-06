import type { MetricPoint } from "../api/types";

export interface ChartPoint {
  t: number; // epoch milliseconds, for a true time axis
  value: number | null;
  max: number | null;
}

/**
 * Convert API points for a time-scaled chart. Where the spacing jumps past twice the usual
 * interval (device offline, missing buckets), insert a null so the line breaks instead of
 * drawing a straight bridge across the outage.
 */
export function toChartPoints(points: MetricPoint[]): ChartPoint[] {
  const converted = points.map((p) => ({
    t: Date.parse(p.ts),
    value: p.value,
    max: p.max ?? null,
  }));
  if (converted.length < 3) return converted;

  const steps = converted
    .slice(1)
    .map((p, i) => p.t - converted[i].t)
    .sort((a, b) => a - b);
  const usual = steps[Math.floor(steps.length / 2)];

  const result: ChartPoint[] = [converted[0]];
  for (let i = 1; i < converted.length; i++) {
    const previous = converted[i - 1];
    if (converted[i].t - previous.t > 2 * usual) {
      result.push({ t: previous.t + usual, value: null, max: null });
    }
    result.push(converted[i]);
  }
  return result;
}
