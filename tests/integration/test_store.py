from typing import Any

import psycopg

from edgeio_worker.store import process_report

DEVICE = "100.101.12.7"


def scalar(conn: psycopg.Connection, query: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(query, params).fetchone()
    assert row is not None
    return row[0]


def open_alerts(conn: psycopg.Connection) -> dict[str, str]:
    rows = conn.execute(
        "SELECT rule, severity FROM alerts WHERE resolved_at IS NULL ORDER BY rule"
    ).fetchall()
    return dict(rows)


def test_first_report_creates_device_reading_and_services(conn, report_at) -> None:
    process_report(conn, report_at(0))
    assert scalar(conn, "SELECT count(*) FROM health_readings") == 1
    device = conn.execute(
        "SELECT host(device_id), hostname, status, last_rx_bytes FROM devices"
    ).fetchone()
    assert device == (DEVICE, "edge-001", "healthy", 482938192)
    assert scalar(conn, "SELECT count(*) FROM service_status WHERE state = 'running'") == 3
    assert scalar(conn, "SELECT rx_rate_bps FROM health_readings") is None
    assert open_alerts(conn) == {}


def test_duplicate_report_is_ignored(conn, report_at) -> None:
    hot = report_at(0, {"system.cpu_temperature_c": 90})
    process_report(conn, hot)
    process_report(conn, hot)
    assert scalar(conn, "SELECT count(*) FROM health_readings") == 1
    assert scalar(conn, "SELECT count(*) FROM alerts") == 1


def test_second_report_derives_rates_from_device_row(conn, report_at) -> None:
    process_report(conn, report_at(0, {"network.rx_bytes": 1_000_000}))
    process_report(conn, report_at(5, {"network.rx_bytes": 4_000_000}))
    rate = scalar(conn, "SELECT rx_rate_bps FROM health_readings ORDER BY ts DESC LIMIT 1")
    assert rate == 80_000.0


def test_overheat_opens_holds_and_resolves_alert(conn, report_at) -> None:
    process_report(conn, report_at(0, {"system.cpu_temperature_c": 90}))
    assert open_alerts(conn) == {"cpu_temp_high": "critical"}
    assert scalar(conn, "SELECT status FROM devices") == "critical"

    process_report(conn, report_at(5, {"system.cpu_temperature_c": 72}))  # hysteresis band
    assert open_alerts(conn) == {"cpu_temp_high": "critical"}
    assert scalar(conn, "SELECT last_value FROM alerts") == 72

    process_report(conn, report_at(10, {"system.cpu_temperature_c": 65}))
    assert open_alerts(conn) == {}
    assert scalar(conn, "SELECT status FROM devices") == "healthy"
    resolved = conn.execute("SELECT opened_at, resolved_at FROM alerts").fetchone()
    assert resolved is not None and resolved[1] > resolved[0]


def test_late_reading_is_stored_but_does_not_rewind_state(conn, report_at) -> None:
    newer = report_at(10)
    process_report(conn, newer)
    process_report(conn, report_at(5, {"system.cpu_temperature_c": 95}))
    assert scalar(conn, "SELECT count(*) FROM health_readings") == 2
    assert scalar(conn, "SELECT last_seen FROM devices") == newer.timestamp
    assert scalar(conn, "SELECT status FROM devices") == "healthy"
    assert open_alerts(conn) == {}


def test_service_failure_opens_alert_and_tracks_change_time(conn, report_at) -> None:
    process_report(conn, report_at(0))
    failed = report_at(5, {"services.edge_streamer": "failed"})
    process_report(conn, failed)
    assert open_alerts(conn) == {"service_down": "critical"}
    row = conn.execute(
        "SELECT state, changed_at FROM service_status WHERE service = 'edge_streamer'"
    ).fetchone()
    assert row == ("failed", failed.timestamp)
    unchanged = scalar(conn, "SELECT changed_at FROM service_status WHERE service = 'docker'")
    assert unchanged < failed.timestamp
