from collections.abc import Callable
from datetime import UTC, datetime

from edgeio_agent.report import HostSnapshot, build_report
from edgeio_contracts.validation import validate_report

NOW = datetime(2026, 10, 7, 9, 0, tzinfo=UTC)


def test_reference_device_report_passes_the_contract(
    make_snapshot: Callable[..., HostSnapshot],
) -> None:
    report = build_report(make_snapshot(), NOW)
    assert validate_report(report.model_dump_json(), NOW) == report
    assert report.timestamp == NOW
    assert (report.system.ram_total_mb, report.system.ram_used_mb) == (6869, 2260)
    assert report.system.ram_usage_percent == 32.9
    assert report.system.cpu_usage_percent == 22.5
    assert report.system.load_1m == 0.88
    assert report.services["app-1"] == "failed"
    assert (report.containers.running, report.containers.stopped) == (6, 1)


def test_temperature_is_clamped_to_the_contract_range(
    make_snapshot: Callable[..., HostSnapshot],
) -> None:
    assert build_report(make_snapshot(cpu_temperature_c=130.0), NOW).system.cpu_temperature_c == 125
    assert build_report(make_snapshot(cpu_temperature_c=-55.0), NOW).system.cpu_temperature_c == -40
