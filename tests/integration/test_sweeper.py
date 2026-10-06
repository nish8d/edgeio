from datetime import timedelta

from edgeio_worker.store import process_report
from edgeio_worker.sweeper import sweep_offline

WINDOW = timedelta(minutes=15)


def test_silent_device_goes_offline_once(conn, report_at) -> None:
    last = report_at(0)
    process_report(conn, last)
    now = last.timestamp + timedelta(minutes=20)

    assert sweep_offline(conn, now, WINDOW) == ["100.101.12.7"]
    assert sweep_offline(conn, now, WINDOW) == []  # no duplicate alert

    alert = conn.execute(
        "SELECT severity, last_value, message FROM alerts WHERE rule = 'offline'"
    ).fetchone()
    assert alert == ("critical", 1200.0, "No health report for over 15 minutes")
    assert conn.execute("SELECT status FROM devices").fetchone() == ("offline",)


def test_recent_device_is_left_alone(conn, report_at) -> None:
    last = report_at(0)
    process_report(conn, last)
    assert sweep_offline(conn, last.timestamp + timedelta(minutes=5), WINDOW) == []
    assert conn.execute("SELECT status FROM devices").fetchone() == ("healthy",)


def test_next_report_brings_device_back(conn, report_at) -> None:
    process_report(conn, report_at(0))
    sweep_offline(conn, report_at(20).timestamp, WINDOW)
    process_report(conn, report_at(25))
    open_count = conn.execute("SELECT count(*) FROM alerts WHERE resolved_at IS NULL").fetchone()
    assert open_count == (0,)
    assert conn.execute("SELECT status FROM devices").fetchone() == ("healthy",)
