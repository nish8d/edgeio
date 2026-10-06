import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { renderRoute } from "../test/render";
import { device } from "../test/fixtures";
import { DeviceCard } from "./DeviceCard";

describe("DeviceCard", () => {
  it("links to the device and shows its key metrics", () => {
    const now = Date.parse("2026-10-06T09:50:00Z");
    renderRoute(
      <DeviceCard device={device({ status: "critical", open_alert_count: 2 })} now={now} />,
    );
    const link = screen.getByRole("link");
    expect(link).toHaveAttribute("href", "/devices/100.64.0.1");
    expect(screen.getByText("edge-001")).toBeInTheDocument();
    expect(screen.getByText("100.64.0.1")).toBeInTheDocument();
    expect(screen.getByText("Critical")).toBeInTheDocument();
    expect(screen.getByText("57.2 °C")).toBeInTheDocument();
    expect(screen.getByText(/5 min ago · 2 open alerts/)).toBeInTheDocument();
  });
});
