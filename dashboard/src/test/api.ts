import { vi } from "vitest";

type RouteBody = unknown | ((url: URL) => unknown);

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/** Stub global fetch with JSON bodies keyed by URL pathname. Returns the requested URLs. */
export function stubApi(routes: Record<string, RouteBody>): URL[] {
  const requests: URL[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: Request) => {
      const url = new URL(input.url);
      requests.push(url);
      const route = routes[url.pathname];
      if (route === undefined) return json({ detail: "Not Found" }, 404);
      return json(typeof route === "function" ? (route as (u: URL) => unknown)(url) : route);
    }),
  );
  return requests;
}

/** Stub global fetch so every request fails like an API whose database is down. */
export function stubApiDown(): void {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => json({ detail: "database unavailable" }, 503)),
  );
}
