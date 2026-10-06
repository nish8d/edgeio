import type { MetricName } from "../api/types";
import { formatBitsPerSecond, formatCelsius, formatPercent } from "./format";

export interface ChartSpec {
  metric: MetricName;
  title: string;
  format: (value: number) => string;
  domain?: [number, number];
}

// One metric per chart: never two scales on one plot.
export const CHARTS: ChartSpec[] = [
  { metric: "cpu", title: "CPU usage", format: formatPercent, domain: [0, 100] },
  { metric: "temperature", title: "CPU temperature", format: formatCelsius },
  { metric: "ram", title: "RAM usage", format: formatPercent, domain: [0, 100] },
  { metric: "disk", title: "Disk usage", format: formatPercent, domain: [0, 100] },
  { metric: "rx_rate", title: "Network receive", format: formatBitsPerSecond },
  { metric: "tx_rate", title: "Network transmit", format: formatBitsPerSecond },
  { metric: "packet_loss", title: "Packet loss", format: formatPercent },
];
