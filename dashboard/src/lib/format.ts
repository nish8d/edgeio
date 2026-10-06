import type { RangeKey } from "./range";

const DASH = "—";

export function formatUptime(seconds: number | null | undefined): string {
  if (seconds == null) return DASH;
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

export function formatBitsPerSecond(bps: number | null | undefined): string {
  if (bps == null) return DASH;
  const units = ["bps", "kbps", "Mbps", "Gbps"];
  let value = bps;
  let unit = 0;
  while (value >= 1000 && unit < units.length - 1) {
    value /= 1000;
    unit += 1;
  }
  return unit === 0 ? `${Math.round(value)} bps` : `${value.toFixed(1)} ${units[unit]}`;
}

export function formatPercent(value: number | null | undefined): string {
  return value == null ? DASH : `${value.toFixed(1)}%`;
}

export function formatCelsius(value: number | null | undefined): string {
  return value == null ? DASH : `${value.toFixed(1)} °C`;
}

export function formatRelative(iso: string, now: number = Date.now()): string {
  const seconds = Math.max(0, Math.round((now - Date.parse(iso)) / 1000));
  if (seconds < 60) return "just now";
  if (seconds < 3_600) return `${Math.floor(seconds / 60)} min ago`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3_600)} h ago`;
  return `${Math.floor(seconds / 86_400)} d ago`;
}

export function formatTimestamp(iso: string): string {
  return new Date(iso).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

export function formatAxisTime(iso: string, range: RangeKey): string {
  const date = new Date(iso);
  return range === "1h" || range === "24h"
    ? date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })
    : date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}
