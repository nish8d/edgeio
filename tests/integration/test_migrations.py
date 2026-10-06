from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from edgeio_worker.migrate import apply_migrations


def test_tables_exist(conn: psycopg.Connection) -> None:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    ).fetchall()
    names = {r[0] for r in rows}
    assert {"devices", "health_readings", "service_status", "alerts", "schema_migrations"} <= names


def test_health_readings_is_a_hypertable(conn: psycopg.Connection) -> None:
    rows = conn.execute(
        "SELECT hypertable_name FROM timescaledb_information.hypertables"
    ).fetchall()
    assert ("health_readings",) in rows


def test_continuous_aggregates_exist(conn: psycopg.Connection) -> None:
    rows = conn.execute(
        "SELECT view_name FROM timescaledb_information.continuous_aggregates"
    ).fetchall()
    assert {r[0] for r in rows} == {"health_hourly", "health_daily"}


def test_reapplying_is_a_no_op(database_url: str, migrations_dir: Path) -> None:
    assert apply_migrations(database_url, migrations_dir) == []


def test_only_one_open_alert_per_device_and_rule(conn: psycopg.Connection) -> None:
    now = datetime(2026, 10, 6, tzinfo=UTC)
    insert = (
        "INSERT INTO alerts (device_id, rule, severity, opened_at, resolved_at, message) "
        "VALUES ('100.64.0.1', 'cpu_temp_high', 'warning', %s, %s, 'm')"
    )
    conn.execute(insert, (now, now))  # resolved
    conn.execute(insert, (now, None))  # open
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(insert, (now, None))  # second open
