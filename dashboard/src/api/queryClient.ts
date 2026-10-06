import { QueryClient } from "@tanstack/react-query";
import { ApiError } from "./client";

export const REFRESH_INTERVAL_MS = 30_000;

export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        refetchInterval: REFRESH_INTERVAL_MS,
        staleTime: 10_000,
        // Client errors (404/422) won't fix themselves; retry only server/network failures.
        retry: (failureCount, error) =>
          !(error instanceof ApiError && error.status < 500) && failureCount < 2,
      },
    },
  });
}
