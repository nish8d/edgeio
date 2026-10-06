from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

from edgeio_contracts.models import HealthReport
from edgeio_contracts.samples import sample_report
from edgeio_worker.migrate import apply_migrations

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


@pytest.fixture
def conn(database_url: str) -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(database_url, autocommit=True) as c:
        c.execute("TRUNCATE health_readings, devices, service_status, alerts RESTART IDENTITY")
        yield c


@pytest.fixture
def report_at() -> Callable[..., HealthReport]:
    def build(minutes: int, changes: dict[str, Any] | None = None) -> HealthReport:
        ts = (BASE_TIME + timedelta(minutes=minutes)).isoformat()
        return sample_report({"timestamp": ts, "containers.stopped": 0, **(changes or {})})

    return build
