"""Deliberately invalid payloads that exercise the worker's dead-letter path."""

import json

from edgeio_contracts.models import HealthReport

MALFORMATIONS: tuple[str, ...] = (
    "cpu_out_of_range",
    "non_tailscale_device_id",
    "missing_disk",
    "wrong_type",
    "truncated_json",
)


def malform(report: HealthReport, kind: str) -> bytes:
    if kind == "truncated_json":
        return report.model_dump_json().encode()[:40]
    payload = report.model_dump(mode="json")
    if kind == "cpu_out_of_range":
        payload["system"]["cpu_usage_percent"] = 143.7
    elif kind == "non_tailscale_device_id":
        payload["device_id"] = "192.168.1.50"
    elif kind == "missing_disk":
        del payload["disk"]
    elif kind == "wrong_type":
        payload["system"]["uptime_seconds"] = "a long time"
    else:
        raise ValueError(f"unknown malformation {kind!r}")
    return json.dumps(payload).encode()
