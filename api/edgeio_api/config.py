from pydantic import Field
from pydantic_settings import BaseSettings


class ApiSettings(BaseSettings):
    """Read from environment variables named after each field, upper-cased."""

    database_url: str = "postgresql://edgeio:edgeio@localhost:5433/edgeio"
    cors_origins: list[str] = ["http://localhost:5173"]
    pool_min_size: int = Field(default=1, ge=1)
    pool_max_size: int = Field(default=5, ge=1)
    pool_timeout_seconds: float = Field(default=5.0, gt=0)
    statement_timeout_ms: int = Field(default=5000, gt=0)
    connect_timeout_seconds: int = Field(default=5, gt=0)
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
