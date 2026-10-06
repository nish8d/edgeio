"""Response models. These are the API's public contract; the dashboard's types are
generated from the OpenAPI document built from them."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

DeviceStatus = Literal["healthy", "warning", "critical", "offline"]
Severity = Literal["warning", "critical"]
AlertState = Literal["open", "resolved", "all"]
MetricName = Literal["cpu", "temperature", "ram", "disk", "packet_loss", "rx_rate", "tx_rate"]
Bucket = Literal["raw", "1h", "1d"]


class Health(BaseModel):
    status: Literal["ok", "degraded"]
    database: Literal["ok", "unavailable"]


class DeviceSummary(BaseModel):
    device_id: str
    hostname: str
    os: str
    status: DeviceStatus
    last_seen: datetime
    uptime_seconds: int | None
    cpu_usage_percent: float | None
    cpu_temperature_c: float | None
    ram_usage_percent: float | None
    disk_usage_percent: float | None
    packet_loss_percent: float | None
    open_alert_count: int


class DeviceList(BaseModel):
    items: list[DeviceSummary]
    total: int
    limit: int
    offset: int


class ServiceStatus(BaseModel):
    name: str
    state: str
    changed_at: datetime


class LatestReading(BaseModel):
    ts: datetime
    load_1m: float
    ram_used_mb: int
    ram_total_mb: int
    disk_used_gb: float
    disk_total_gb: float
    net_interface: str
    rx_rate_bps: float | None
    tx_rate_bps: float | None
    containers_running: int
    containers_stopped: int


class Alert(BaseModel):
    id: int
    device_id: str
    hostname: str | None
    rule: str
    severity: Severity
    opened_at: datetime
    resolved_at: datetime | None
    last_value: float | None
    message: str


class DeviceDetail(DeviceSummary):
    first_seen: datetime
    services: list[ServiceStatus]
    latest: LatestReading | None
    open_alerts: list[Alert]


class MetricPoint(BaseModel):
    ts: datetime
    value: float | None
    max: float | None = None


class MetricSeries(BaseModel):
    device_id: str
    metric: MetricName
    unit: str
    bucket: Bucket
    start: datetime
    end: datetime
    points: list[MetricPoint]
