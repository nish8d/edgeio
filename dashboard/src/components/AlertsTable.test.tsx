import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { alert } from "../test/fixtures";
import { renderRoute } from "../test/render";
import { AlertsTable } from "./AlertsTable";

describe("AlertsTable", () => {
  it("lists alerts with a device link and open/resolved state", () => {
    renderRoute(
      <AlertsTable
        alerts={[
          alert(),
          alert({ id: 2, severity: "warning", resolved_at: "2026-10-06T09:30:00Z" }),
        ]}
      />,
    );
    const rows = screen.getAllByRole("row").slice(1);
    expect(rows).toHaveLength(2);
    expect(within(rows[0]).getByText("Critical")).toBeInTheDocument();
    expect(within(rows[0]).getByText("Open")).toBeInTheDocument();
    expect(within(rows[0]).getByRole("link", { name: "edge-002" })).toHaveAttribute(
      "href",
      "/devices/100.64.0.2",
    );
    expect(within(rows[1]).getByText("Warning")).toBeInTheDocument();
    expect(within(rows[1]).queryByText("Open")).not.toBeInTheDocument();
  });

  it("can hide the device column", () => {
    renderRoute(<AlertsTable alerts={[alert()]} showDevice={false} />);
    expect(screen.queryByRole("columnheader", { name: "Device" })).not.toBeInTheDocument();
  });

  it("shows an empty state", () => {
    renderRoute(<AlertsTable alerts={[]} />);
    expect(screen.getByText("No alerts.")).toBeInTheDocument();
  });
});
