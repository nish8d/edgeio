"""Wire-format models for the device.health topic (schema_version 1)."""

from datetime import UTC, datetime
from ipaddress import IPv4Address, IPv4Network
from typing import Annotated, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

TAILSCALE_NETWORK = IPv4Network("100.64.0.0/10")
CURRENT_SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = frozenset({1})
DISK_SUM_TOLERANCE_GB = 1.0

# Upper bounds match the Postgres columns the worker writes (integer / bigint).
INT32_MAX = 2**31 - 1
INT64_MAX = 2**63 - 1

ServiceState = Literal["running", "stopped", "failed", "unknown"]
Percent = Annotated[float, Field(ge=0, le=100)]
Int32 = Annotated[int, Field(ge=0, le=INT32_MAX)]
Int64 = Annotated[int, Field(ge=0, le=INT64_MAX)]
NonNegativeFloat = Annotated[float, Field(ge=0)]
# Postgres text/jsonb cannot store NUL characters.
Text = Annotated[str, StringConstraints(min_length=1, pattern=r"^[^\x00]+$")]


class _ContractModel(BaseModel):
    # Strict: no type coercion ("1" is not 1, epochs are not timestamps), no inf/nan.
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)


class SystemInfo(_ContractModel):
    hostname: Annotated[Text, Field(max_length=253)]
    os: Text
    uptime_seconds: Int64
    cpu_usage_percent: Percent
    cpu_temperature_c: float = Field(ge=-40, le=125)
    load_1m: NonNegativeFloat
    ram_total_mb: Annotated[Int32, Field(gt=0)]
    ram_used_mb: Int32
    ram_usage_percent: Percent

    @model_validator(mode="after")
    def _ram_used_within_total(self) -> Self:
        if self.ram_used_mb > self.ram_total_mb:
            raise ValueError("ram_used_mb exceeds ram_total_mb")
        return self


class DiskInfo(_ContractModel):
    root_total_gb: float = Field(gt=0)
    root_used_gb: NonNegativeFloat
    root_free_gb: NonNegativeFloat
    root_usage_percent: Percent

    @model_validator(mode="after")
    def _consistent_sizes(self) -> Self:
        if self.root_used_gb > self.root_total_gb:
            raise ValueError("root_used_gb exceeds root_total_gb")
        if abs(self.root_used_gb + self.root_free_gb - self.root_total_gb) > DISK_SUM_TOLERANCE_GB:
            raise ValueError("root_used_gb + root_free_gb must equal root_total_gb (within 1 GB)")
        return self


class NetworkInfo(_ContractModel):
    interface: Text
    rx_bytes: Int64
    tx_bytes: Int64
    packet_loss_percent: Percent


class ContainerCounts(_ContractModel):
    running: Int32
    stopped: Int32


class HealthReport(_ContractModel):
    """Health report published by an edge device to the device.health topic."""

    schema_version: int
    device_id: IPv4Address
    timestamp: AwareDatetime
    system: SystemInfo
    disk: DiskInfo
    network: NetworkInfo
    services: dict[Annotated[Text, Field(max_length=64)], ServiceState]
    containers: ContainerCounts

    @field_validator("schema_version")
    @classmethod
    def _supported_version(cls, v: int) -> int:
        if v not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(f"unsupported schema_version {v}")
        return v

    @field_validator("device_id")
    @classmethod
    def _tailscale_address(cls, v: IPv4Address) -> IPv4Address:
        if v not in TAILSCALE_NETWORK:
            raise ValueError(f"device_id {v} is not a Tailscale address (100.64.0.0/10)")
        return v

    @field_validator("timestamp")
    @classmethod
    def _to_utc(cls, v: datetime) -> datetime:
        return v.astimezone(UTC)
