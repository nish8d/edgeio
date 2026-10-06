import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { CHARTS } from "../lib/metrics";
import { stubApi } from "../test/api";
import { metricSeries } from "../test/fixtures";
import { renderRoute } from "../test/render";
import { MetricChart } from "./MetricChart";

const cpu = CHARTS[0];

describe("MetricChart", () => {
  it("shows the latest value and a data table", async () => {
    stubApi({ "/api/v1/devices/100.64.0.1/metrics": metricSeries("cpu", [40, 42.25]) });
    renderRoute(<MetricChart deviceId="100.64.0.1" spec={cpu} range="24h" />);
    expect(await screen.findAllByText("42.3%")).not.toHaveLength(0);
    expect(screen.getByText(/Data table/)).toBeInTheDocument();
  });

  it("test_metric_chart_empty_state", async () => {
    stubApi({ "/api/v1/devices/100.64.0.1/metrics": metricSeries("cpu", []) });
    renderRoute(<MetricChart deviceId="100.64.0.1" spec={cpu} range="24h" />);
    expect(await screen.findByText("No data in this range.")).toBeInTheDocument();
  });
});
