"""Wire-format models for the device.health topic (schema_version 1)."""

from datetime import UTC, datetime
from ipaddress import IPv4Address, IPv4Network
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

TAILSCALE_NETWORK = IPv4Network("100.64.0.0/10")
CURRENT_SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = frozenset({1})
DISK_SUM_TOLERANCE_GB = 1.0

ServiceState = Literal["running", "stopped", "failed", "unknown"]
Percent = Annotated[float, Field(ge=0, le=100)]
NonNegativeInt = Annotated[int, Field(ge=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]


class _ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SystemInfo(_ContractModel):
    hostname: str = Field(min_length=1, max_length=253)
    os: str = Field(min_length=1)
    uptime_seconds: NonNegativeInt
    cpu_usage_percent: Percent
    cpu_temperature_c: float = Field(ge=-40, le=125)
    load_1m: NonNegativeFloat
    ram_total_mb: int = Field(gt=0)
    ram_used_mb: NonNegativeInt
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
    interface: str = Field(min_length=1)
    rx_bytes: NonNegativeInt
    tx_bytes: NonNegativeInt
    packet_loss_percent: Percent


class ContainerCounts(_ContractModel):
    running: NonNegativeInt
    stopped: NonNegativeInt


class HealthReport(_ContractModel):
    """Health report published by an edge device to the device.health topic."""

    schema_version: int
    device_id: IPv4Address
    timestamp: AwareDatetime
    system: SystemInfo
    disk: DiskInfo
    network: NetworkInfo
    services: dict[str, ServiceState]
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
