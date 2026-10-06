import type { DeviceStatus } from "../api/types";

export const STATUS_ORDER: DeviceStatus[] = ["healthy", "warning", "critical", "offline"];

// Status is never color alone: every badge/tile pairs the color with this icon and label.
export const STATUS_META: Record<DeviceStatus, { label: string; icon: string }> = {
  healthy: { label: "Healthy", icon: "✓" },
  warning: { label: "Warning", icon: "!" },
  critical: { label: "Critical", icon: "✕" },
  offline: { label: "Offline", icon: "○" },
};

export function isDeviceStatus(value: string | null): value is DeviceStatus {
  return value !== null && (STATUS_ORDER as string[]).includes(value);
}
