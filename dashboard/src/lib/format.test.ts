import { describe, expect, it } from "vitest";
import {
  formatBitsPerSecond,
  formatCelsius,
  formatPercent,
  formatRelative,
  formatUptime,
} from "./format";

describe("formatUptime", () => {
  it("shows days and hours for long uptimes", () => expect(formatUptime(382941)).toBe("4d 10h"));
  it("shows hours and minutes under a day", () =>
    expect(formatUptime(3 * 3600 + 300)).toBe("3h 5m"));
  it("shows minutes under an hour", () => expect(formatUptime(42 * 60 + 5)).toBe("42m"));
  it("shows a dash when unknown", () => expect(formatUptime(null)).toBe("—"));
});

describe("formatBitsPerSecond", () => {
  it("keeps small rates in bps", () => expect(formatBitsPerSecond(950)).toBe("950 bps"));
  it("scales to kbps", () => expect(formatBitsPerSecond(80_000)).toBe("80.0 kbps"));
  it("scales to Mbps", () => expect(formatBitsPerSecond(1_234_567)).toBe("1.2 Mbps"));
  it("shows a dash for missing rates", () => expect(formatBitsPerSecond(null)).toBe("—"));
});

describe("percent and temperature", () => {
  it("formats percent with one decimal", () => expect(formatPercent(61.14)).toBe("61.1%"));
  it("formats celsius", () => expect(formatCelsius(57.2)).toBe("57.2 °C"));
  it("dashes missing values", () => expect(formatPercent(undefined)).toBe("—"));
});

describe("formatRelative", () => {
  const now = Date.parse("2026-10-06T10:00:00Z");
  it("says just now under a minute", () =>
    expect(formatRelative("2026-10-06T09:59:30Z", now)).toBe("just now"));
  it("uses minutes", () => expect(formatRelative("2026-10-06T09:55:00Z", now)).toBe("5 min ago"));
  it("uses hours", () => expect(formatRelative("2026-10-06T07:00:00Z", now)).toBe("3 h ago"));
  it("uses days", () => expect(formatRelative("2026-10-04T10:00:00Z", now)).toBe("2 d ago"));
});
