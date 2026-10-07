"""Impure shell around the host readers: one HostSnapshot from the live machine."""

import logging
import os
import socket
import time
from ipaddress import IPv4Address
from pathlib import Path

import psutil

from .config import AgentSettings
from .containers import DockerUnavailable, container_services, list_containers
from .host import (
    cpu_temperature,
    default_interface,
    disk_usage,
    host_service_state,
    read_hostname,
    read_os,
    tailscale_ipv4,
)
from .ping import PingRunner, internet_packet_loss, run_ping
from .report import HostSnapshot

log = logging.getLogger(__name__)


class CollectError(Exception):
    """A required field could not be read; the tick is skipped."""


def collect(settings: AgentSettings, ping: PingRunner = run_ping) -> HostSnapshot:
    host_root = Path(settings.agent_host_root)
    device_id = _device_id(settings)

    temperature = cpu_temperature(Path(settings.agent_hwmon_root))
    if temperature is None:
        raise CollectError(f"no temperature sensors under {settings.agent_hwmon_root}")

    try:
        route_table = Path(settings.agent_route_file).read_text()
    except OSError as exc:
        raise CollectError(f"cannot read {settings.agent_route_file}: {exc}") from exc
    interface = default_interface(route_table)
    if interface is None:
        raise CollectError("no default route")
    counters = psutil.net_io_counters(pernic=True).get(interface)
    if counters is None:
        raise CollectError(f"no traffic counters for interface {interface}")

    cgroup_root = Path(settings.agent_cgroup_root)
    services = {unit: host_service_state(cgroup_root, unit) for unit in settings.host_services}
    running = stopped = 0
    try:
        containers = list_containers(settings.agent_docker_socket)
    except DockerUnavailable as exc:
        log.warning("docker API unavailable", extra={"error": str(exc)})
        services["docker"] = "unknown"
    else:
        services.update(container_services(containers, settings.service_renames))
        running = sum(1 for c in containers if c.state == "running")
        stopped = len(containers) - running

    stat = os.statvfs(host_root)
    memory = psutil.virtual_memory()
    return HostSnapshot(
        device_id=device_id,
        hostname=read_hostname(host_root),
        os=read_os(host_root),
        uptime_seconds=max(0, int(time.time() - psutil.boot_time())),
        cpu_usage_percent=psutil.cpu_percent(interval=1),
        cpu_temperature_c=temperature,
        load_1m=os.getloadavg()[0],
        ram_total_bytes=memory.total,
        ram_available_bytes=memory.available,
        disk=disk_usage(stat.f_blocks, stat.f_bavail, stat.f_frsize),
        interface=interface,
        rx_bytes=counters.bytes_recv,
        tx_bytes=counters.bytes_sent,
        packet_loss_percent=internet_packet_loss(
            settings.ping_targets, settings.agent_ping_count, ping
        ),
        services=services,
        containers_running=running,
        containers_stopped=stopped,
    )


def _device_id(settings: AgentSettings) -> IPv4Address:
    if settings.agent_device_id:
        return IPv4Address(settings.agent_device_id)
    interfaces = {
        name: [a.address for a in addresses if a.family == socket.AF_INET]
        for name, addresses in psutil.net_if_addrs().items()
    }
    found = tailscale_ipv4(interfaces)
    if found is None:
        raise CollectError("no Tailscale IPv4 address (100.64.0.0/10) on any interface")
    return found
