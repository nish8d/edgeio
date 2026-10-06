"""Alert rules as data. Pure functions: no I/O."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from .transform import ReadingRow

Severity = Literal["warning", "critical"]
DeviceStatus = Literal["healthy", "warning", "critical", "offline"]

OFFLINE_RULE = "offline"
SERVICE_DOWN_RULE = "service_down"


@dataclass(frozen=True)
class ThresholdRule:
    name: str
    label: str
    unit: str
    metric: Callable[[ReadingRow], float]
    warning: float | None  # opens when value > warning
    critical: float | None  # opens/escalates when value > critical
    clear_at: float  # an open alert resolves when value <= clear_at (hysteresis)

    def severity_for(self, value: float) -> Severity | None:
        if self.critical is not None and value > self.critical:
            return "critical"
        if self.warning is not None and value > self.warning:
            return "warning"
        return None


THRESHOLD_RULES: tuple[ThresholdRule, ...] = (
    ThresholdRule(
        "cpu_temp_high", "CPU temperature", "°C", lambda r: r.cpu_temperature_c, 75, 85, 70
    ),
    ThresholdRule("disk_usage_high", "Disk usage", "%", lambda r: r.disk_usage_percent, 85, 95, 80),
    ThresholdRule("ram_usage_high", "RAM usage", "%", lambda r: r.ram_usage_percent, 90, None, 85),
    ThresholdRule(
        "packet_loss_high", "Packet loss", "%", lambda r: r.packet_loss_percent, 2, 10, 1
    ),
    ThresholdRule(
        "containers_stopped", "Stopped containers", "", lambda r: r.containers_stopped, 0, None, 0
    ),
)


@dataclass(frozen=True)
class AlertAction:
    kind: Literal["upsert", "resolve"]
    rule: str
    severity: Severity | None = None
    value: float | None = None
    message: str = ""


def evaluate(
    row: ReadingRow, services: Mapping[str, str], open_alerts: Mapping[str, Severity]
) -> list[AlertAction]:
    actions: list[AlertAction] = []
    if OFFLINE_RULE in open_alerts:
        actions.append(AlertAction("resolve", OFFLINE_RULE, message="Device reporting again"))

    for rule in THRESHOLD_RULES:
        value = rule.metric(row)
        message = f"{rule.label} at {value:g}{rule.unit}"
        severity = rule.severity_for(value)
        if severity is not None:
            actions.append(AlertAction("upsert", rule.name, severity, value, message))
        elif rule.name in open_alerts:
            if value <= rule.clear_at:
                actions.append(AlertAction("resolve", rule.name, value=value, message=message))
            else:  # inside the hysteresis band: stay open, refresh the value
                actions.append(
                    AlertAction("upsert", rule.name, open_alerts[rule.name], value, message)
                )

    down = sorted(name for name, state in services.items() if state != "running")
    if down:
        actions.append(
            AlertAction(
                "upsert",
                SERVICE_DOWN_RULE,
                "critical",
                float(len(down)),
                f"Services not running: {', '.join(down)}",
            )
        )
    elif SERVICE_DOWN_RULE in open_alerts:
        actions.append(AlertAction("resolve", SERVICE_DOWN_RULE, value=0.0))
    return actions


def apply_actions(
    open_alerts: Mapping[str, Severity], actions: Sequence[AlertAction]
) -> dict[str, Severity]:
    result = dict(open_alerts)
    for action in actions:
        if action.kind == "resolve":
            result.pop(action.rule, None)
        elif action.severity is not None:
            result[action.rule] = action.severity
    return result


def device_status(open_alerts: Mapping[str, Severity]) -> DeviceStatus:
    if OFFLINE_RULE in open_alerts:
        return "offline"
    if "critical" in open_alerts.values():
        return "critical"
    if open_alerts:
        return "warning"
    return "healthy"
