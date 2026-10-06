"""All SQL the API runs. Read-only; identifiers come only from fixed allow-lists."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from ipaddress import IPv4Address
from typing import Any

from psycopg import sql

from .db import DbConn
from .schemas import AlertState, Bucket, DeviceStatus, MetricName, Severity

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


@dataclass(frozen=True)
class MetricSource:
    raw: str  # column in health_readings
    avg: str  # column in the rollup views
    max: str
    unit: str


METRICS: dict[str, MetricSource] = {
    "cpu": MetricSource("cpu_usage_percent", "cpu_avg", "cpu_max", "%"),
    "temperature": MetricSource("cpu_temperature_c", "temp_avg", "temp_max", "°C"),
    "ram": MetricSource("ram_usage_percent", "ram_avg", "ram_max", "%"),
    "disk": MetricSource("disk_usage_percent", "disk_avg", "disk_max", "%"),
    "packet_loss": MetricSource("packet_loss_percent", "packet_loss_avg", "packet_loss_max", "%"),
    "rx_rate": MetricSource("rx_rate_bps", "rx_rate_avg", "rx_rate_max", "bps"),
    "tx_rate": MetricSource("tx_rate_bps", "tx_rate_avg", "tx_rate_max", "bps"),
}

_ROLLUPS: dict[str, tuple[str, str]] = {
    "1h": ("health_hourly", "1 hour"),
    "1d": ("health_daily", "1 day"),
}
RAW_MAX_SPAN = timedelta(hours=24)
HOURLY_MAX_SPAN = timedelta(days=30)


def choose_bucket(start: datetime, end: datetime) -> Bucket:
    span = end - start
    if span <= RAW_MAX_SPAN:
        return "raw"
    if span <= HOURLY_MAX_SPAN:
        return "1h"
    return "1d"


def metric_points(
    conn: DbConn,
    device_id: IPv4Address,
    metric: MetricName,
    bucket: Bucket,
    start: datetime,
    end: datetime,
) -> list[Row]:
    source = METRICS[metric]
    if bucket == "raw":
        query = sql.SQL(
            "SELECT ts, {value} AS value, NULL::double precision AS max FROM health_readings "
            "WHERE device_id = %s AND ts >= %s AND ts < %s ORDER BY ts"
        ).format(value=sql.Identifier(source.raw))
        return conn.execute(query, (device_id, start, end)).fetchall()
    view, width = _ROLLUPS[bucket]
    query = sql.SQL(
        "SELECT bucket AS ts, {avg} AS value, {max} AS max FROM {view} "
        "WHERE device_id = %s AND bucket >= time_bucket(%s::interval, %s::timestamptz) "
        "AND bucket < %s ORDER BY bucket"
    ).format(
        avg=sql.Identifier(source.avg),
        max=sql.Identifier(source.max),
        view=sql.Identifier(view),
    )
    return conn.execute(query, (device_id, width, start, end)).fetchall()
