import createClient from "openapi-fetch";
import type { paths } from "./schema";

export const api = createClient<paths>({
  baseUrl: import.meta.env.VITE_API_BASE_URL || window.location.origin,
  // Look fetch up per call so tests can stub it.
  fetch: (request: Request) => globalThis.fetch(request),
});

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

/** Resolve an openapi-fetch call to its data, or throw ApiError for a non-2xx response. */
export async function unwrap<T>(
  request: Promise<{ data?: T; error?: unknown; response: Response }>,
): Promise<T> {
  const { data, error, response } = await request;
  if (data === undefined) {
    const detail =
      typeof error === "object" && error !== null && "detail" in error
        ? String(error.detail)
        : response.statusText;
    throw new ApiError(response.status, detail);
  }
  return data;
}
