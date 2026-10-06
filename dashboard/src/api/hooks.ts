import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { type RangeKey, rangeBounds } from "../lib/range";
import { api, unwrap } from "./client";
import type { AlertState, DeviceStatus, MetricName, Severity } from "./types";

export function useFleetSummary() {
  return useQuery({
    queryKey: ["fleet-summary"],
    queryFn: () => unwrap(api.GET("/api/v1/fleet/summary")),
  });
}

export function useDevices(status: DeviceStatus | undefined) {
  return useQuery({
    queryKey: ["devices", status ?? "all"],
    queryFn: () =>
      unwrap(api.GET("/api/v1/devices", { params: { query: { status, limit: 500 } } })),
    placeholderData: keepPreviousData,
  });
}

export function useDevice(deviceId: string) {
  return useQuery({
    queryKey: ["device", deviceId],
    queryFn: () =>
      unwrap(api.GET("/api/v1/devices/{device_id}", { params: { path: { device_id: deviceId } } })),
  });
}

export function useMetric(deviceId: string, metric: MetricName, range: RangeKey) {
  return useQuery({
    queryKey: ["metric", deviceId, metric, range],
    queryFn: () => {
      const { from, to } = rangeBounds(range);
      return unwrap(
        api.GET("/api/v1/devices/{device_id}/metrics", {
          params: { path: { device_id: deviceId }, query: { metric, from, to } },
        }),
      );
    },
    placeholderData: keepPreviousData,
  });
}

export interface AlertFilters {
  state: AlertState;
  deviceId?: string;
  severity?: Severity;
}

export function useAlerts({ state, deviceId, severity }: AlertFilters) {
  return useQuery({
    queryKey: ["alerts", state, deviceId ?? null, severity ?? null],
    queryFn: () =>
      unwrap(
        api.GET("/api/v1/alerts", {
          params: { query: { state, device_id: deviceId, severity, limit: 200 } },
        }),
      ),
    placeholderData: keepPreviousData,
  });
}
