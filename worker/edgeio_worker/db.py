"""Postgres access with retry: a transient outage blocks and retries, never drops data."""

import logging
import threading
from collections.abc import Callable
from typing import Any

import psycopg

log = logging.getLogger(__name__)

Connect = Callable[[], psycopg.Connection[Any]]


class Database:
    def __init__(
        self,
        connect: Connect,
        stop: threading.Event,
        *,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 30.0,
    ) -> None:
        self._connect = connect
        self._stop = stop
        self._initial_backoff = initial_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._conn: psycopg.Connection[Any] | None = None

    @classmethod
    def from_url(cls, url: str, stop: threading.Event) -> "Database":
        return cls(lambda: psycopg.connect(url, autocommit=True), stop)

    def run_in_transaction[T](self, fn: Callable[[psycopg.Connection[Any]], T]) -> T:
        backoff = self._initial_backoff
        while True:
            try:
                conn = self._connection()
                with conn.transaction():
                    return fn(conn)
            except psycopg.OperationalError:
                log.warning(
                    "database unavailable, retrying",
                    exc_info=True,
                    extra={"backoff_seconds": backoff},
                )
                self.close()
                if self._stop.wait(backoff):
                    raise
                backoff = min(backoff * 2, self._max_backoff)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _connection(self) -> psycopg.Connection[Any]:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn
