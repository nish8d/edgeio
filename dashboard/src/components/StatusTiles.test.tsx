import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { StatusTiles } from "./StatusTiles";

const counts = { healthy: 47, warning: 1, critical: 2, offline: 0, total: 50 };

describe("StatusTiles", () => {
  it("shows a count, icon and label for every status", () => {
    render(<StatusTiles counts={counts} onSelect={() => {}} />);
    expect(screen.getByRole("button", { name: /47\s*Healthy/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /2\s*Critical/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /0\s*Offline/ })).toBeInTheDocument();
  });

  it("selects a status and clears it on a second click", async () => {
    const onSelect = vi.fn();
    const { rerender } = render(<StatusTiles counts={counts} onSelect={onSelect} />);
    await userEvent.click(screen.getByRole("button", { name: /Critical/ }));
    expect(onSelect).toHaveBeenLastCalledWith("critical");

    rerender(<StatusTiles counts={counts} selected="critical" onSelect={onSelect} />);
    const tile = screen.getByRole("button", { name: /Critical/ });
    expect(tile).toHaveAttribute("aria-pressed", "true");
    await userEvent.click(tile);
    expect(onSelect).toHaveBeenLastCalledWith(undefined);
  });
});
