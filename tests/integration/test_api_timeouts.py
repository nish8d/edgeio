import psycopg
import pytest
from fastapi.testclient import TestClient

from edgeio_api.app import create_app
from edgeio_api.config import ApiSettings


def test_slow_queries_are_cancelled_instead_of_hanging(database_url: str) -> None:
    app = create_app(ApiSettings(database_url=database_url, statement_timeout_ms=200))
    with (
        TestClient(app),
        app.state.pool.connection() as conn,
        pytest.raises(psycopg.errors.QueryCanceled),
    ):
        conn.execute("SELECT pg_sleep(2)")
