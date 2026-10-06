import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { MetricName } from "../api/types";
import { stubApi } from "../test/api";
import { alert, deviceDetail, metricSeries } from "../test/fixtures";
import { renderRoute } from "../test/render";
import { DeviceDetail } from "./DeviceDetail";

const ROUTE = { path: "/devices/:deviceId", route: "/devices/100.64.0.2" };

function stubDevice() {
  return stubApi({
    "/api/v1/devices/100.64.0.2": deviceDetail({
      device_id: "100.64.0.2",
      hostname: "edge-002",
      status: "critical",
    }),
    "/api/v1/devices/100.64.0.2/metrics": (url: URL) =>
      metricSeries(url.searchParams.get("metric") as MetricName),
    "/api/v1/alerts": { items: [alert()], total: 1, limit: 200, offset: 0 },
  });
}

const spanHours = (u: URL) =>
  (Date.parse(u.searchParams.get("to") ?? "") - Date.parse(u.searchParams.get("from") ?? "")) /
  3_600_000;

describe("DeviceDetail", () => {
  it("shows the device header, services, charts and alert history", async () => {
    stubDevice();
    renderRoute(<DeviceDetail />, ROUTE);
    expect(await screen.findByRole("heading", { name: "edge-002" })).toBeInTheDocument();
    expect(screen.getByText("edge_streamer")).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "CPU usage" })).toBeInTheDocument();
    expect(await screen.findByText("CPU temperature at 90°C")).toBeInTheDocument();
  });

  it("requests a wider window when the range changes", async () => {
    const requests = stubDevice();
    renderRoute(<DeviceDetail />, ROUTE);
    await userEvent.click(await screen.findByRole("button", { name: "7d" }));
    await waitFor(() =>
      expect(requests.some((u) => u.pathname.endsWith("/metrics") && spanHours(u) === 168)).toBe(
        true,
      ),
    );
  });

  it("says when the device does not exist", async () => {
    stubApi({});
    renderRoute(<DeviceDetail />, ROUTE);
    expect(await screen.findByText("Device not found.")).toBeInTheDocument();
  });
});
