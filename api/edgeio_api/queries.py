"""All SQL the API runs. Read-only; identifiers come only from fixed allow-lists."""

from ipaddress import IPv4Address
from typing import Any

from .db import DbConn
from .schemas import AlertState, DeviceStatus, Severity

Row = dict[str, Any]

_SUMMARY_COLUMNS = """
    host(d.device_id) AS device_id, d.hostname, d.os, d.status, d.last_seen, d.uptime_seconds,
    d.cpu_usage_percent, d.cpu_temperature_c, d.ram_usage_percent, d.disk_usage_percent,
    d.packet_loss_percent,
    (SELECT count(*) FROM alerts a
      WHERE a.device_id = d.device_id AND a.resolved_at IS NULL) AS open_alert_count
"""
_DEVICE_FILTER = "(%(status)s::text IS NULL OR d.status = %(status)s::text)"

_LIST_DEVICES = f"""
SELECT {_SUMMARY_COLUMNS} FROM devices d WHERE {_DEVICE_FILTER}
ORDER BY d.hostname LIMIT %(limit)s OFFSET %(offset)s
"""
_COUNT_DEVICES = f"SELECT count(*) AS n FROM devices d WHERE {_DEVICE_FILTER}"
_GET_DEVICE = f"SELECT {_SUMMARY_COLUMNS}, d.first_seen FROM devices d WHERE d.device_id = %s"

_ALERT_COLUMNS = """
    a.id, host(a.device_id) AS device_id, d.hostname, a.rule, a.severity, a.opened_at,
    a.resolved_at, a.last_value, a.message
"""
_ALERT_FILTER = """
    (%(state)s::text = 'all' OR (%(state)s::text = 'open') = (a.resolved_at IS NULL))
    AND (%(device_id)s::inet IS NULL OR a.device_id = %(device_id)s::inet)
    AND (%(severity)s::text IS NULL OR a.severity = %(severity)s::text)
"""
_LIST_ALERTS = f"""
SELECT {_ALERT_COLUMNS} FROM alerts a LEFT JOIN devices d ON d.device_id = a.device_id
WHERE {_ALERT_FILTER}
ORDER BY a.opened_at DESC, a.id DESC LIMIT %(limit)s OFFSET %(offset)s
"""
_COUNT_ALERTS = f"SELECT count(*) AS n FROM alerts a WHERE {_ALERT_FILTER}"


def _count(conn: DbConn, query: str, params: dict[str, Any]) -> int:
    row = conn.execute(query, params).fetchone()
    return int(row["n"]) if row else 0


def list_devices(
    conn: DbConn, status: DeviceStatus | None, limit: int, offset: int
) -> tuple[list[Row], int]:
    params = {"status": status, "limit": limit, "offset": offset}
    return conn.execute(_LIST_DEVICES, params).fetchall(), _count(conn, _COUNT_DEVICES, params)


def get_device(conn: DbConn, device_id: IPv4Address) -> Row | None:
    return conn.execute(_GET_DEVICE, (device_id,)).fetchone()


def device_exists(conn: DbConn, device_id: IPv4Address) -> bool:
    row = conn.execute("SELECT 1 FROM devices WHERE device_id = %s", (device_id,)).fetchone()
    return row is not None


def get_services(conn: DbConn, device_id: IPv4Address) -> list[Row]:
    return conn.execute(
        "SELECT service AS name, state, changed_at FROM service_status "
        "WHERE device_id = %s ORDER BY service",
        (device_id,),
    ).fetchall()


def get_latest_reading(conn: DbConn, device_id: IPv4Address) -> Row | None:
    return conn.execute(
        "SELECT ts, load_1m, ram_used_mb, ram_total_mb, disk_used_gb, disk_total_gb, "
        "net_interface, rx_rate_bps, tx_rate_bps, containers_running, containers_stopped "
        "FROM health_readings WHERE device_id = %s ORDER BY ts DESC LIMIT 1",
        (device_id,),
    ).fetchone()


def list_alerts(
    conn: DbConn,
    *,
    state: AlertState,
    device_id: IPv4Address | None,
    severity: Severity | None,
    limit: int,
    offset: int,
) -> tuple[list[Row], int]:
    params = {
        "state": state,
        "device_id": device_id,
        "severity": severity,
        "limit": limit,
        "offset": offset,
    }
    return conn.execute(_LIST_ALERTS, params).fetchall(), _count(conn, _COUNT_ALERTS, params)
