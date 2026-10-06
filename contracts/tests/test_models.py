import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from edgeio_contracts.models import HealthReport
from edgeio_contracts.samples import DELETE, sample_payload, sample_report, set_path

FIXTURES = Path(__file__).parents[1] / "fixtures"


def test_sample_payload_matches_fixture_file() -> None:
    assert json.loads((FIXTURES / "valid" / "basic.json").read_text()) == sample_payload()


@pytest.mark.parametrize("path", sorted((FIXTURES / "valid").glob("*.json")), ids=lambda p: p.stem)
def test_valid_fixtures_parse(path: Path) -> None:
    HealthReport.model_validate_json(path.read_bytes())


def test_sample_report_fields() -> None:
    report = sample_report()
    assert str(report.device_id) == "100.101.12.7"
    assert report.system.hostname == "edge-001"
    assert report.services["edge_streamer"] == "running"
    assert report.timestamp == datetime(2026, 10, 6, 9, 45, tzinfo=UTC)


def test_timestamp_with_offset_is_normalized_to_utc() -> None:
    report = sample_report({"timestamp": "2026-10-06T15:15:00+05:30"})
    assert report.timestamp == datetime(2026, 10, 6, 9, 45, tzinfo=UTC)
    assert report.timestamp.utcoffset() == timedelta(0)


def test_extra_service_keys_are_allowed() -> None:
    report = sample_report({"services.nginx": "stopped"})
    assert report.services["nginx"] == "stopped"


def test_round_trip_serialization() -> None:
    dumped = json.loads(sample_report().model_dump_json())
    assert dumped == sample_payload()
    assert dumped["timestamp"] == "2026-10-06T09:45:00Z"


INVALID_MUTATIONS: list[tuple[str, str, Any]] = [
    ("device_id_outside_tailscale", "device_id", "192.168.1.10"),
    ("device_id_not_ip", "device_id", "edge-001"),
    ("cpu_percent_over_100", "system.cpu_usage_percent", 143.7),
    ("negative_uptime", "system.uptime_seconds", -1),
    ("fractional_uptime", "system.uptime_seconds", 1.5),
    ("temperature_out_of_range", "system.cpu_temperature_c", 150.0),
    ("ram_used_exceeds_total", "system.ram_used_mb", 20000),
    ("disk_sizes_inconsistent", "disk.root_free_gb", 100),
    ("disk_used_exceeds_total", "disk.root_used_gb", 500),
    ("negative_rx_bytes", "network.rx_bytes", -5),
    ("packet_loss_over_100", "network.packet_loss_percent", 101.0),
    ("unknown_service_state", "services.docker", "crashed"),
    ("negative_containers", "containers.running", -1),
    ("naive_timestamp", "timestamp", "2026-10-06T09:45:00"),
    ("unsupported_schema_version", "schema_version", 2),
    ("unknown_top_level_field", "gpu", {"usage": 3}),
    ("unknown_nested_field", "system.kernel", "6.8"),
    ("missing_section", "disk", DELETE),
    ("missing_field", "system.hostname", DELETE),
    ("empty_hostname", "system.hostname", ""),
    # Values that would overflow the DB columns or that Postgres text/jsonb rejects.
    ("ram_total_overflows_int32", "system.ram_total_mb", 3_000_000_000),
    ("stopped_containers_overflow_int32", "containers.stopped", 3_000_000_000),
    ("rx_bytes_overflows_int64", "network.rx_bytes", 2**64),
    ("uptime_overflows_int64", "system.uptime_seconds", 2**63),
    ("nul_in_hostname", "system.hostname", "edge\u0000001"),
    ("nul_in_service_name", "services.dock\u0000er", "running"),
    ("empty_service_name", "services.", "running"),
    # Strict typing: no coercion that the JSON Schema would reject.
    ("int_as_string", "system.ram_total_mb", "16384"),
    ("version_as_string", "schema_version", "1"),
    ("version_as_bool", "schema_version", True),
    ("version_as_float", "schema_version", 1.0),
    ("epoch_timestamp", "timestamp", 1790000000),
    ("infinite_load", "system.load_1m", float("inf")),
    ("nan_temperature", "system.cpu_temperature_c", float("nan")),
]


@pytest.mark.parametrize(
    ("path", "value"),
    [(p, v) for _, p, v in INVALID_MUTATIONS],
    ids=[name for name, _, _ in INVALID_MUTATIONS],
)
def test_invalid_payloads_are_rejected(path: str, value: Any) -> None:
    payload = set_path(sample_payload(), path, value)
    with pytest.raises(ValidationError):
        HealthReport.model_validate_json(json.dumps(payload))
