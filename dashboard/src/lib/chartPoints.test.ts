import { describe, expect, it } from "vitest";
import { toChartPoints } from "./chartPoints";

const at = (minutes: number) => new Date(Date.UTC(2026, 9, 6, 9, minutes)).toISOString();
const point = (minutes: number, value: number | null = 1) => ({
  ts: at(minutes),
  value,
  max: null,
});
const t = (minutes: number) => Date.parse(at(minutes));

describe("toChartPoints", () => {
  it("converts timestamps to epoch milliseconds", () => {
    expect(toChartPoints([point(0, 5)])).toEqual([{ t: t(0), value: 5, max: null }]);
  });

  it("keeps evenly spaced series unchanged", () => {
    expect(toChartPoints([point(0), point(5), point(10), point(15)])).toHaveLength(4);
  });

  it("breaks the line across a gap instead of bridging it", () => {
    // 5-minute readings, then the device is silent for over an hour.
    const result = toChartPoints([point(0), point(5), point(10), point(80), point(85)]);
    expect(result.map((p) => p.value)).toEqual([1, 1, 1, null, 1, 1]);
    expect(result[3].t).toBe(t(15));
  });

  it("leaves very short series alone", () => {
    expect(toChartPoints([point(0), point(60)])).toHaveLength(2);
  });
});
