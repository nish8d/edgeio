"""Flatten a validated HealthReport into a DB row and derive computed fields."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from edgeio_contracts.models import HealthReport


@dataclass(frozen=True)
class PreviousCounters:
    ts: datetime
    rx_bytes: int
    tx_bytes: int


@dataclass(frozen=True)
class ReadingRow:
    device_id: str
    ts: datetime
    hostname: str
    os: str
    uptime_seconds: int
    cpu_usage_percent: float
    cpu_temperature_c: float
    load_1m: float
    ram_total_mb: int
    ram_used_mb: int
    ram_usage_percent: float
    disk_total_gb: float
    disk_used_gb: float
    disk_free_gb: float
    disk_usage_percent: float
    disk_free_percent: float
    net_interface: str
    rx_bytes: int
    tx_bytes: int
    rx_rate_bps: float | None
    tx_rate_bps: float | None
    packet_loss_percent: float
    containers_running: int
    containers_stopped: int
    raw: dict[str, Any]


def to_row(report: HealthReport, previous: PreviousCounters | None) -> ReadingRow:
    rx_rate, tx_rate = _rates(report, previous)
    system, disk, network = report.system, report.disk, report.network
    return ReadingRow(
        device_id=str(report.device_id),
        ts=report.timestamp,
        hostname=system.hostname,
        os=system.os,
        uptime_seconds=system.uptime_seconds,
        cpu_usage_percent=system.cpu_usage_percent,
        cpu_temperature_c=system.cpu_temperature_c,
        load_1m=system.load_1m,
        ram_total_mb=system.ram_total_mb,
        ram_used_mb=system.ram_used_mb,
        ram_usage_percent=system.ram_usage_percent,
        disk_total_gb=disk.root_total_gb,
        disk_used_gb=disk.root_used_gb,
        disk_free_gb=disk.root_free_gb,
        disk_usage_percent=disk.root_usage_percent,
        disk_free_percent=round(disk.root_free_gb / disk.root_total_gb * 100, 2),
        net_interface=network.interface,
        rx_bytes=network.rx_bytes,
        tx_bytes=network.tx_bytes,
        rx_rate_bps=rx_rate,
        tx_rate_bps=tx_rate,
        packet_loss_percent=network.packet_loss_percent,
        containers_running=report.containers.running,
        containers_stopped=report.containers.stopped,
        raw=report.model_dump(mode="json"),
    )


def _rates(
    report: HealthReport, previous: PreviousCounters | None
) -> tuple[float | None, float | None]:
    """Bits per second since the previous reading; None after a counter reset or reorder."""
    if previous is None:
        return None, None
    elapsed = (report.timestamp - previous.ts).total_seconds()
    rx, tx = report.network.rx_bytes, report.network.tx_bytes
    if elapsed <= 0 or rx < previous.rx_bytes or tx < previous.tx_bytes:
        return None, None
    return (rx - previous.rx_bytes) * 8 / elapsed, (tx - previous.tx_bytes) * 8 / elapsed
