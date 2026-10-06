import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { stubApi, stubApiDown } from "../test/api";
import { device, fleetSummary } from "../test/fixtures";
import { renderRoute } from "../test/render";
import { FleetOverview } from "./FleetOverview";

const deviceList = (items = [device()]) => ({ items, total: items.length, limit: 500, offset: 0 });

describe("FleetOverview", () => {
  it("filters devices by the status in the URL", async () => {
    const hot = device({ device_id: "100.64.0.7", hostname: "edge-007", status: "critical" });
    const requests = stubApi({
      "/api/v1/fleet/summary": fleetSummary(),
      "/api/v1/devices": deviceList([hot]),
    });
    renderRoute(<FleetOverview />, { route: "/?status=critical" });
    expect(await screen.findByText("edge-007")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: /2\s*Critical/ })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    const call = requests.find((u) => u.pathname === "/api/v1/devices");
    expect(call?.searchParams.get("status")).toBe("critical");
  });

  it("clicking a tile requests that status", async () => {
    const requests = stubApi({
      "/api/v1/fleet/summary": fleetSummary(),
      "/api/v1/devices": deviceList(),
    });
    renderRoute(<FleetOverview />);
    await userEvent.click(await screen.findByRole("button", { name: /Warning/ }));
    await waitFor(() =>
      expect(
        requests.some(
          (u) => u.pathname === "/api/v1/devices" && u.searchParams.get("status") === "warning",
        ),
      ).toBe(true),
    );
  });

  it("shows an empty state when no devices match", async () => {
    stubApi({ "/api/v1/fleet/summary": fleetSummary(), "/api/v1/devices": deviceList([]) });
    renderRoute(<FleetOverview />);
    expect(await screen.findByText("No devices match this filter.")).toBeInTheDocument();
  });

  it("test_fleet_overview_shows_error_when_api_fails", async () => {
    stubApiDown();
    renderRoute(<FleetOverview />);
    expect(await screen.findByText("Couldn't load devices.")).toBeInTheDocument();
  });
});
