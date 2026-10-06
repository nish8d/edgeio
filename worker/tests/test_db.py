import threading
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
import pytest

from edgeio_worker.db import Database


class FakeConn:
    def __init__(self) -> None:
        self.closed = False

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield

    def close(self) -> None:
        self.closed = True


def test_retries_after_database_outage() -> None:
    connections: list[FakeConn] = []

    def connect() -> FakeConn:
        connections.append(FakeConn())
        return connections[-1]

    calls = {"n": 0}

    def work(conn: FakeConn) -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        return "stored"

    db = Database(connect, threading.Event(), initial_backoff_seconds=0.01)  # type: ignore[arg-type]
    assert db.run_in_transaction(work) == "stored"
    assert len(connections) == 2
    assert connections[0].closed


def test_reuses_healthy_connection() -> None:
    connections: list[FakeConn] = []

    def connect() -> FakeConn:
        connections.append(FakeConn())
        return connections[-1]

    db = Database(connect, threading.Event())  # type: ignore[arg-type]
    db.run_in_transaction(lambda c: None)
    db.run_in_transaction(lambda c: None)
    assert len(connections) == 1


def test_gives_up_when_shutting_down() -> None:
    stop = threading.Event()
    stop.set()

    def always_down(conn: FakeConn) -> None:
        raise psycopg.OperationalError("down")

    db = Database(FakeConn, stop, initial_backoff_seconds=0.01)  # type: ignore[arg-type]
    with pytest.raises(psycopg.OperationalError):
        db.run_in_transaction(always_down)


def test_non_transient_errors_propagate_immediately() -> None:
    def broken(conn: FakeConn) -> None:
        raise ValueError("bug")

    db = Database(FakeConn, threading.Event())  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        db.run_in_transaction(broken)
