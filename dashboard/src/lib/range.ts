const HOUR = 60 * 60 * 1000;

export const RANGES = { "1h": HOUR, "24h": 24 * HOUR, "7d": 7 * 24 * HOUR, "30d": 30 * 24 * HOUR };
export type RangeKey = keyof typeof RANGES;
export const RANGE_KEYS = Object.keys(RANGES) as RangeKey[];

export function rangeBounds(range: RangeKey, now: Date = new Date()): { from: string; to: string } {
  return { from: new Date(now.getTime() - RANGES[range]).toISOString(), to: now.toISOString() };
}
