"""Connection pool and the per-request connection dependency."""

from collections.abc import Iterator
from typing import Any

from fastapi import Request
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import ApiSettings

DbConn = Connection[dict[str, Any]]


def create_pool(settings: ApiSettings) -> ConnectionPool[Any]:
    return ConnectionPool(
        settings.database_url,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        timeout=settings.pool_timeout_seconds,
        open=False,
        check=ConnectionPool.check_connection,  # drop connections broken by a DB restart
        kwargs={"autocommit": True, "row_factory": dict_row},
    )


def get_conn(request: Request) -> Iterator[DbConn]:
    pool: ConnectionPool[Any] = request.app.state.pool
    with pool.connection() as conn:
        yield conn
