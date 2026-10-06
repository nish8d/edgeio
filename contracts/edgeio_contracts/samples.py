"""Canonical example payload and helpers for building variations in tests."""

import copy
from collections.abc import Mapping
from typing import Any, Final

from .models import HealthReport

DELETE: Final = object()

_SAMPLE: Final[dict[str, Any]] = {
    "schema_version": 1,
    "device_id": "100.101.12.7",
    "timestamp": "2026-10-06T09:45:00Z",
    "system": {
        "hostname": "edge-001",
        "os": "Ubuntu 24.04",
        "uptime_seconds": 382941,
        "cpu_usage_percent": 43.7,
        "cpu_temperature_c": 57.2,
        "load_1m": 1.42,
        "ram_total_mb": 16384,
        "ram_used_mb": 9271,
        "ram_usage_percent": 56.6,
    },
    "disk": {
        "root_total_gb": 476,
        "root_used_gb": 291,
        "root_free_gb": 185,
        "root_usage_percent": 61.1,
    },
    "network": {
        "interface": "eth0",
        "rx_bytes": 482938192,
        "tx_bytes": 182938291,
        "packet_loss_percent": 0.0,
    },
    "services": {"docker": "running", "postgresql": "running", "edge_streamer": "running"},
    "containers": {"running": 5, "stopped": 1},
}


def sample_payload() -> dict[str, Any]:
    """A fresh deep copy of the canonical example payload."""
    return copy.deepcopy(_SAMPLE)


def set_path(payload: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    """Set (or with DELETE, remove) a dotted path like 'system.cpu_usage_percent'."""
    *parents, leaf = path.split(".")
    node = payload
    for key in parents:
        node = node[key]
    if value is DELETE:
        del node[leaf]
    else:
        node[leaf] = value
    return payload


def sample_report(changes: Mapping[str, Any] | None = None) -> HealthReport:
    """Validated HealthReport from the sample with dotted-path changes applied."""
    payload = sample_payload()
    for path, value in (changes or {}).items():
        set_path(payload, path, value)
    return HealthReport.model_validate(payload)
