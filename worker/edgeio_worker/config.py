from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class WorkerSettings(BaseSettings):
    """Read from environment variables named after each field, upper-cased."""

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "device.health"
    kafka_dlq_topic: str = "device.health.dlq"
    kafka_group_id: str = "edgeio-worker"
    database_url: str = "postgresql://edgeio:edgeio@localhost:5432/edgeio"
    migrations_dir: Path = Path("db/migrations")
    batch_size: int = Field(default=500, ge=1)
    poll_timeout_seconds: float = Field(default=1.0, gt=0)
    offline_after_seconds: int = Field(default=900, gt=0)
    sweep_interval_seconds: float = Field(default=60.0, gt=0)
    log_level: str = "INFO"
