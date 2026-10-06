"""Persist validated reports and their alert consequences. Caller owns the transaction."""

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime
from typing import Any, cast

from psycopg import Connection, sql
from psycopg.types.json import Jsonb

from edgeio_contracts.models import HealthReport

from .rules import AlertAction, DeviceStatus, Severity, apply_actions, device_status, evaluate
from .transform import PreviousCounters, ReadingRow, to_row

_READING_COLUMNS = (
    "device_id", "ts", "hostname", "uptime_seconds", "cpu_usage_percent", "cpu_temperature_c",
    "load_1m", "ram_total_mb", "ram_used_mb", "ram_usage_percent", "disk_total_gb",
    "disk_used_gb", "disk_free_gb", "disk_usage_percent", "disk_free_percent", "net_interface",
    "rx_bytes", "tx_bytes", "rx_rate_bps", "tx_rate_bps", "packet_loss_percent",
    "containers_running", "containers_stopped", "raw",
)  # fmt: skip

_INSERT_READING = sql.SQL(
    "INSERT INTO health_readings ({columns}) VALUES ({values}) "
    "ON CONFLICT (device_id, ts) DO NOTHING"
).format(
    columns=sql.SQL(", ").join(map(sql.Identifier, _READING_COLUMNS)),
    values=sql.SQL(", ").join(map(sql.Placeholder, _READING_COLUMNS)),
)

_UPSERT_DEVICE = """
INSERT INTO devices (
    device_id, hostname, os, first_seen, last_seen, uptime_seconds, cpu_usage_percent,
    cpu_temperature_c, ram_usage_percent, disk_usage_percent, packet_loss_percent,
    last_rx_bytes, last_tx_bytes
) VALUES (
    %(device_id)s, %(hostname)s, %(os)s, %(ts)s, %(ts)s, %(uptime_seconds)s,
    %(cpu_usage_percent)s, %(cpu_temperature_c)s, %(ram_usage_percent)s,
    %(disk_usage_percent)s, %(packet_loss_percent)s, %(rx_bytes)s, %(tx_bytes)s
)
ON CONFLICT (device_id) DO UPDATE SET
    hostname = EXCLUDED.hostname,
    os = EXCLUDED.os,
    last_seen = EXCLUDED.last_seen,
    uptime_seconds = EXCLUDED.uptime_seconds,
    cpu_usage_percent = EXCLUDED.cpu_usage_percent,
    cpu_temperature_c = EXCLUDED.cpu_temperature_c,
    ram_usage_percent = EXCLUDED.ram_usage_percent,
    disk_usage_percent = EXCLUDED.disk_usage_percent,
    packet_loss_percent = EXCLUDED.packet_loss_percent,
    last_rx_bytes = EXCLUDED.last_rx_bytes,
    last_tx_bytes = EXCLUDED.last_tx_bytes
"""

_UPSERT_SERVICE = """
INSERT INTO service_status (device_id, service, state, changed_at, updated_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (device_id, service) DO UPDATE SET
    changed_at = CASE WHEN service_status.state = EXCLUDED.state
                      THEN service_status.changed_at ELSE EXCLUDED.updated_at END,
    state = EXCLUDED.state,
    updated_at = EXCLUDED.updated_at
"""

_UPSERT_ALERT = """
INSERT INTO alerts (device_id, rule, severity, opened_at, last_value, message)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (device_id, rule) WHERE resolved_at IS NULL DO UPDATE SET
    severity = EXCLUDED.severity,
    last_value = EXCLUDED.last_value,
    message = EXCLUDED.message
"""

_RESOLVE_ALERT = """
UPDATE alerts SET resolved_at = %s, last_value = COALESCE(%s, last_value)
WHERE device_id = %s AND rule = %s AND resolved_at IS NULL
"""


def store_batch(conn: Connection[Any], reports: Sequence[HealthReport]) -> None:
    for report in reports:
        process_report(conn, report)


def process_report(conn: Connection[Any], report: HealthReport) -> None:
    device_id = str(report.device_id)
    snapshot = conn.execute(
        "SELECT last_seen, last_rx_bytes, last_tx_bytes FROM devices "
        "WHERE device_id = %s FOR UPDATE",
        (device_id,),
    ).fetchone()
    previous = PreviousCounters(*snapshot) if snapshot is not None else None
    row = to_row(report, previous)

    if not _insert_reading(conn, row):
        return  # duplicate delivery: already processed
    if previous is not None and row.ts <= previous.ts:
        return  # late arrival: keep the history, don't rewind current state

    params = asdict(row)
    conn.execute(_UPSERT_DEVICE, params)
    with conn.cursor() as cur:
        cur.executemany(
            _UPSERT_SERVICE,
            [(device_id, name, state, row.ts, row.ts) for name, state in report.services.items()],
        )

    open_alerts = load_open_alerts(conn, device_id)
    actions = evaluate(row, report.services, open_alerts)
    apply_alert_actions(conn, device_id, actions, row.ts)
    set_status(conn, device_id, device_status(apply_actions(open_alerts, actions)))


def load_open_alerts(conn: Connection[Any], device_id: str) -> dict[str, Severity]:
    rows = conn.execute(
        "SELECT rule, severity FROM alerts WHERE device_id = %s AND resolved_at IS NULL",
        (device_id,),
    ).fetchall()
    return {rule: cast(Severity, severity) for rule, severity in rows}


def apply_alert_actions(
    conn: Connection[Any], device_id: str, actions: Sequence[AlertAction], at: datetime
) -> None:
    for action in actions:
        if action.kind == "upsert":
            conn.execute(
                _UPSERT_ALERT,
                (device_id, action.rule, action.severity, at, action.value, action.message),
            )
        else:
            conn.execute(_RESOLVE_ALERT, (at, action.value, device_id, action.rule))


def set_status(conn: Connection[Any], device_id: str, status: DeviceStatus) -> None:
    conn.execute("UPDATE devices SET status = %s WHERE device_id = %s", (status, device_id))


def _insert_reading(conn: Connection[Any], row: ReadingRow) -> bool:
    params: Mapping[str, Any] = {**asdict(row), "raw": Jsonb(row.raw)}
    return conn.execute(_INSERT_READING, params).rowcount == 1
