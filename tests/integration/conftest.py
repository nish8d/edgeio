from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from edgeio_api.app import create_app
from edgeio_api.config import ApiSettings
from edgeio_contracts.models import HealthReport
from edgeio_contracts.samples import sample_report
from edgeio_worker.migrate import apply_migrations
from edgeio_worker.store import process_report

TIMESCALE_IMAGE = "timescale/timescaledb:2.17.2-pg16"
BASE_TIME = datetime(2026, 10, 6, 9, 0, tzinfo=UTC)


@pytest.fixture(scope="session")
def migrations_dir() -> Path:
    return Path(__file__).parents[2] / "db" / "migrations"


@pytest.fixture(scope="session")
def database_url(migrations_dir: Path) -> Iterator[str]:
    with PostgresContainer(
        TIMESCALE_IMAGE, username="edgeio", password="edgeio", dbname="edgeio", driver=None
    ) as pg:
        url = pg.get_connection_url()
        apply_migrations(url, migrations_dir)
        yield url


ROLLUPS = ("health_hourly", "health_daily")


def _refresh(conn: psycopg.Connection[Any]) -> None:
    for view in ROLLUPS:
        conn.execute(
            sql.SQL("CALL refresh_continuous_aggregate({}, NULL, NULL)").format(sql.Literal(view))
        )


@pytest.fixture
def conn(database_url: str) -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(database_url, autocommit=True) as c:
        c.execute("TRUNCATE health_readings, devices, service_status, alerts RESTART IDENTITY")
        _refresh(c)  # drop rollup rows materialized by earlier tests
        yield c


@pytest.fixture
def refresh_rollups(conn: psycopg.Connection[Any]) -> Callable[[], None]:
    return lambda: _refresh(conn)


@pytest.fixture
def report_at() -> Callable[..., HealthReport]:
    def build(minutes: int, changes: dict[str, Any] | None = None) -> HealthReport:
        ts = (BASE_TIME + timedelta(minutes=minutes)).isoformat()
        return sample_report({"timestamp": ts, "containers.stopped": 0, **(changes or {})})

    return build


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    with KafkaContainer().with_kraft() as kafka:
        yield kafka.get_bootstrap_server()


@pytest.fixture
def api(database_url: str) -> Iterator[TestClient]:
    with TestClient(create_app(ApiSettings(database_url=database_url))) as client:
        yield client


@pytest.fixture
def seed(
    conn: psycopg.Connection[Any], report_at: Callable[..., HealthReport]
) -> Callable[..., HealthReport]:
    def store(n: int, minutes: int = 0, changes: dict[str, Any] | None = None) -> HealthReport:
        identity = {"device_id": f"100.64.0.{n}", "system.hostname": f"edge-{n:03d}"}
        report = report_at(minutes, {**identity, **(changes or {})})
        process_report(conn, report)
        return report

    return store
