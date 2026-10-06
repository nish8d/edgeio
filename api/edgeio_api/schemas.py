"""Response models. These are the API's public contract; the dashboard's types are
generated from the OpenAPI document built from them."""

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
