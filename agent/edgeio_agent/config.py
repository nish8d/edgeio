from pydantic import Field
from pydantic_settings import BaseSettings


def split_list(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


class AgentSettings(BaseSettings):
    """Read from environment variables named after each field, upper-cased."""

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "device.health"
    agent_interval_seconds: float = Field(default=300.0, gt=0)
    agent_device_id: str | None = None
    agent_host_root: str = "/host"
    agent_cgroup_root: str = "/host-cgroup"
    agent_hwmon_root: str = "/sys/class/hwmon"
    agent_route_file: str = "/proc/net/route"
    agent_docker_socket: str = "/var/run/docker.sock"
    agent_host_services: str = "docker,tailscaled"
    agent_service_renames: str = ""
    agent_ping_targets: str = Field(default="1.1.1.1,8.8.8.8", min_length=1)
    agent_ping_count: int = Field(default=5, ge=1, le=20)
    agent_spool_dir: str = "/spool"
    agent_spool_max_files: int = Field(default=2016, ge=1)  # 7 days at 300 s
    agent_send_timeout_seconds: float = Field(default=30.0, gt=0)
    log_level: str = "INFO"

    @property
    def host_services(self) -> list[str]:
        return split_list(self.agent_host_services)

    @property
    def ping_targets(self) -> list[str]:
        return split_list(self.agent_ping_targets)

    @property
    def service_renames(self) -> dict[str, str]:
        renames: dict[str, str] = {}
        for item in split_list(self.agent_service_renames):
            source, sep, target = item.partition("=")
            if not sep or not source.strip() or not target.strip():
                raise ValueError(f"AGENT_SERVICE_RENAMES entry {item!r} is not name=new_name")
            renames[source.strip()] = target.strip()
        return renames
