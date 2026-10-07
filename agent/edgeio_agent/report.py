"""Assemble one validated HealthReport from a snapshot of host facts."""

from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv4Address

from edgeio_contracts.models import CURRENT_SCHEMA_VERSION, HealthReport, ServiceState

from .host import DiskUsage

MIB = 2**20


@dataclass(frozen=True)
class HostSnapshot:
    device_id: IPv4Address
    hostname: str
    os: str
    uptime_seconds: int
    cpu_usage_percent: float
    cpu_temperature_c: float
    load_1m: float
    ram_total_bytes: int
    ram_available_bytes: int
    disk: DiskUsage
    interface: str
    rx_bytes: int
    tx_bytes: int
    packet_loss_percent: float
    services: dict[str, ServiceState]
    containers_running: int
    containers_stopped: int


def build_report(snapshot: HostSnapshot, now: datetime) -> HealthReport:
    """Raises pydantic.ValidationError if the snapshot cannot form a valid v1 report."""
    ram_total_mb = snapshot.ram_total_bytes // MIB
    ram_used_mb = min(
        ram_total_mb, (snapshot.ram_total_bytes - snapshot.ram_available_bytes) // MIB
    )
    return HealthReport.model_validate(
        {
            "schema_version": CURRENT_SCHEMA_VERSION,
            "device_id": snapshot.device_id,
            "timestamp": now,
            "system": {
                "hostname": snapshot.hostname,
                "os": snapshot.os,
                "uptime_seconds": snapshot.uptime_seconds,
                "cpu_usage_percent": round(float(snapshot.cpu_usage_percent), 1),
                "cpu_temperature_c": round(min(125.0, max(-40.0, snapshot.cpu_temperature_c)), 1),
                "load_1m": round(float(snapshot.load_1m), 2),
                "ram_total_mb": ram_total_mb,
                "ram_used_mb": ram_used_mb,
                "ram_usage_percent": round(ram_used_mb / ram_total_mb * 100, 1),
            },
            "disk": {
                "root_total_gb": snapshot.disk.total_gb,
                "root_used_gb": snapshot.disk.used_gb,
                "root_free_gb": snapshot.disk.free_gb,
                "root_usage_percent": snapshot.disk.usage_percent,
            },
            "network": {
                "interface": snapshot.interface,
                "rx_bytes": snapshot.rx_bytes,
                "tx_bytes": snapshot.tx_bytes,
                "packet_loss_percent": float(snapshot.packet_loss_percent),
            },
            "services": dict(snapshot.services),
            "containers": {
                "running": snapshot.containers_running,
                "stopped": snapshot.containers_stopped,
            },
        }
    )
