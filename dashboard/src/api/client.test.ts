import { describe, expect, it } from "vitest";
import { stubApi } from "../test/api";
import { fleetSummary } from "../test/fixtures";
import { ApiError, api, unwrap } from "./client";

describe("unwrap", () => {
  it("returns the parsed body on success", async () => {
    stubApi({ "/api/v1/fleet/summary": fleetSummary() });
    const body = await unwrap(api.GET("/api/v1/fleet/summary"));
    expect(body.devices.total).toBe(50);
  });

  it("throws ApiError with the status and detail on failure", async () => {
    stubApi({});
    const request = unwrap(
      api.GET("/api/v1/devices/{device_id}", { params: { path: { device_id: "100.64.0.9" } } }),
    );
    await expect(request).rejects.toEqual(new ApiError(404, "Not Found"));
  });
});
