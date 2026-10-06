import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { stubApi } from "../test/api";
import { alert } from "../test/fixtures";
import { renderRoute } from "../test/render";
import { AlertsPage } from "./AlertsPage";

describe("AlertsPage", () => {
  it("loads open alerts, then filters by state and severity", async () => {
    const requests = stubApi({
      "/api/v1/alerts": { items: [alert()], total: 1, limit: 200, offset: 0 },
    });
    renderRoute(<AlertsPage />);
    expect(await screen.findByText("CPU temperature at 90°C")).toBeInTheDocument();
    expect(requests[0].searchParams.get("state")).toBe("open");

    await userEvent.click(screen.getByRole("button", { name: "resolved" }));
    await userEvent.selectOptions(screen.getByLabelText("Severity"), "critical");
    await waitFor(() => {
      const last = requests[requests.length - 1];
      expect(last.searchParams.get("state")).toBe("resolved");
      expect(last.searchParams.get("severity")).toBe("critical");
    });
  });
});
