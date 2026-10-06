import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { deviceDetail } from "../test/fixtures";
import { ServicesPanel } from "./ServicesPanel";

describe("ServicesPanel", () => {
  it("shows each service state and container counts", () => {
    const detail = deviceDetail();
    render(<ServicesPanel services={detail.services} latest={detail.latest} />);
    expect(screen.getByText("docker")).toBeInTheDocument();
    expect(screen.getByText("running")).toBeInTheDocument();
    expect(screen.getByText("failed")).toBeInTheDocument();
    expect(screen.getByText(/Containers:/)).toHaveTextContent("Containers: 5 running, 0 stopped");
  });
});
