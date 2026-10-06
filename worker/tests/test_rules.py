from typing import Any

from edgeio_contracts.samples import sample_report
from edgeio_worker.rules import (
    OFFLINE_RULE,
    SERVICE_DOWN_RULE,
    AlertAction,
    apply_actions,
    device_status,
    evaluate,
)
from edgeio_worker.transform import ReadingRow, to_row

RUNNING = {"docker": "running", "postgresql": "running", "edge_streamer": "running"}


def row_with(changes: dict[str, Any] | None = None) -> ReadingRow:
    """A healthy reading (no stopped containers) with dotted-path changes."""
    return to_row(sample_report({"containers.stopped": 0, **(changes or {})}), None)


def kinds(actions: list[AlertAction]) -> dict[str, tuple[str, str | None]]:
    return {a.rule: (a.kind, a.severity) for a in actions}


def test_healthy_reading_produces_no_actions() -> None:
    assert evaluate(row_with(), RUNNING, {}) == []


def test_temperature_opens_warning_then_critical() -> None:
    assert kinds(evaluate(row_with({"system.cpu_temperature_c": 80}), RUNNING, {})) == {
        "cpu_temp_high": ("upsert", "warning")
    }
    assert kinds(evaluate(row_with({"system.cpu_temperature_c": 90}), RUNNING, {})) == {
        "cpu_temp_high": ("upsert", "critical")
    }


def test_hysteresis_band_keeps_open_alert_at_its_severity() -> None:
    actions = evaluate(
        row_with({"system.cpu_temperature_c": 72}), RUNNING, {"cpu_temp_high": "critical"}
    )
    assert kinds(actions) == {"cpu_temp_high": ("upsert", "critical")}
    assert actions[0].value == 72


def test_hysteresis_band_does_not_open_a_new_alert() -> None:
    assert evaluate(row_with({"system.cpu_temperature_c": 72}), RUNNING, {}) == []


def test_alert_resolves_at_clear_threshold() -> None:
    actions = evaluate(
        row_with({"system.cpu_temperature_c": 70}), RUNNING, {"cpu_temp_high": "warning"}
    )
    assert kinds(actions) == {"cpu_temp_high": ("resolve", None)}


def test_critical_deescalates_to_warning() -> None:
    actions = evaluate(
        row_with({"system.cpu_temperature_c": 80}), RUNNING, {"cpu_temp_high": "critical"}
    )
    assert kinds(actions) == {"cpu_temp_high": ("upsert", "warning")}


def test_ram_rule_has_no_critical_level() -> None:
    row = row_with({"system.ram_used_mb": 16000, "system.ram_usage_percent": 97.7})
    assert kinds(evaluate(row, RUNNING, {})) == {"ram_usage_high": ("upsert", "warning")}


def test_disk_and_packet_loss_thresholds() -> None:
    row = row_with(
        {
            "disk.root_used_gb": 460,
            "disk.root_free_gb": 16,
            "disk.root_usage_percent": 96.6,
            "network.packet_loss_percent": 3.0,
        }
    )
    assert kinds(evaluate(row, RUNNING, {})) == {
        "disk_usage_high": ("upsert", "critical"),
        "packet_loss_high": ("upsert", "warning"),
    }


def test_stopped_containers_warn_and_resolve_at_zero() -> None:
    assert kinds(evaluate(row_with({"containers.stopped": 1}), RUNNING, {})) == {
        "containers_stopped": ("upsert", "warning")
    }
    assert kinds(evaluate(row_with(), RUNNING, {"containers_stopped": "warning"})) == {
        "containers_stopped": ("resolve", None)
    }


def test_service_down_is_critical_and_names_services() -> None:
    services = {**RUNNING, "edge_streamer": "failed", "docker": "stopped"}
    actions = evaluate(row_with(), services, {})
    assert kinds(actions) == {SERVICE_DOWN_RULE: ("upsert", "critical")}
    assert actions[0].message == "Services not running: docker, edge_streamer"
    assert actions[0].value == 2


def test_service_down_resolves_when_all_running() -> None:
    assert kinds(evaluate(row_with(), RUNNING, {SERVICE_DOWN_RULE: "critical"})) == {
        SERVICE_DOWN_RULE: ("resolve", None)
    }


def test_any_reading_resolves_offline() -> None:
    assert kinds(evaluate(row_with(), RUNNING, {OFFLINE_RULE: "critical"})) == {
        OFFLINE_RULE: ("resolve", None)
    }


def test_apply_actions_updates_open_set() -> None:
    actions = [
        AlertAction("upsert", "cpu_temp_high", "critical", 90.0),
        AlertAction("resolve", "disk_usage_high"),
    ]
    assert apply_actions({"disk_usage_high": "warning"}, actions) == {"cpu_temp_high": "critical"}


def test_device_status_precedence() -> None:
    assert device_status({}) == "healthy"
    assert device_status({"ram_usage_high": "warning"}) == "warning"
    assert device_status({"ram_usage_high": "warning", "cpu_temp_high": "critical"}) == "critical"
    assert device_status({OFFLINE_RULE: "critical", "ram_usage_high": "warning"}) == "offline"
