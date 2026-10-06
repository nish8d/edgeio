from pydantic import Field
from pydantic_settings import BaseSettings


class SimulatorSettings(BaseSettings):
    """Read from environment variables named after each field, upper-cased."""

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "device.health"
    sim_device_count: int = Field(default=50, ge=1)
    sim_device_offset: int = Field(default=0, ge=0)
    sim_seed: int = 42
    sim_interval_seconds: float = Field(default=300.0, gt=0)
    sim_malformed_rate: float = Field(default=0.005, ge=0, le=1)
    sim_fault_rate: float = Field(default=0.005, ge=0, le=1)
    log_level: str = "INFO"
