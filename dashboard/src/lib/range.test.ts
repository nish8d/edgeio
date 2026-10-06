import { describe, expect, it } from "vitest";
import { RANGE_KEYS, rangeBounds } from "./range";

describe("rangeBounds", () => {
  it("ends now and starts one range earlier", () => {
    expect(rangeBounds("24h", new Date("2026-10-06T12:00:00Z"))).toEqual({
      from: "2026-10-05T12:00:00.000Z",
      to: "2026-10-06T12:00:00.000Z",
    });
  });
  it("offers the presets in order", () => expect(RANGE_KEYS).toEqual(["1h", "24h", "7d", "30d"]));
});
