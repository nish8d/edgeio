"""Pure readers for host facts; each takes the path or text it reads, so tests use fake trees."""

import socket
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from ipaddress import IPv4Address
from pathlib import Path

from edgeio_contracts.models import TAILSCALE_NETWORK, ServiceState

GIB = 2**30
CPU_SENSOR_CHIPS = ("k10temp", "coretemp", "zenpower", "cpu_thermal")
CPU_SENSOR_LABELS = ("Tctl", "Tdie", "Package id 0")


def _read(path: Path) -> str:
    try:
        return path.read_text().strip()
    except OSError:
        return ""


def read_hostname(host_root: Path) -> str:
    return _read(host_root / "etc/hostname") or socket.gethostname()


def read_os(host_root: Path) -> str:
    fields: dict[str, str] = {}
    for line in _read(host_root / "etc/os-release").splitlines():
        key, sep, value = line.partition("=")
        if sep:
            fields[key.strip()] = value.strip().strip('"')
    if fields.get("NAME") and fields.get("VERSION_ID"):
        return f"{fields['NAME']} {fields['VERSION_ID']}"
    return fields.get("PRETTY_NAME") or "Linux"


def default_interface(route_table: str) -> str | None:
    """Interface of the lowest-metric default route, from /proc/net/route text."""
    best: tuple[int, str] | None = None
    for line in route_table.splitlines()[1:]:
        cols = line.split()
        if len(cols) < 7 or cols[1] != "00000000":
            continue
        metric = int(cols[6])
        if best is None or metric < best[0]:
            best = (metric, cols[0])
    return best[1] if best else None


def tailscale_ipv4(interfaces: Mapping[str, Sequence[str]]) -> IPv4Address | None:
    """First IPv4 address in 100.64.0.0/10, looking at tailscale0 before other interfaces."""
    ordered = sorted(interfaces.items(), key=lambda item: item[0] != "tailscale0")
    for _name, addresses in ordered:
        for address in addresses:
            try:
                ip = IPv4Address(address)
            except ValueError:
                continue
            if ip in TAILSCALE_NETWORK:
                return ip
    return None


def host_service_state(cgroup_root: Path, unit: str) -> ServiceState:
    """cgroup v2: an active unit has a cgroup with processes; an inactive one has none."""
    if not cgroup_root.is_dir():
        return "unknown"
    procs = _read(cgroup_root / "system.slice" / f"{unit}.service" / "cgroup.procs")
    return "running" if procs else "stopped"


def cpu_temperature(hwmon_root: Path) -> float | None:
    chips: dict[str, list[tuple[str, float]]] = {}
    for chip in sorted(hwmon_root.glob("hwmon*")):
        readings: list[tuple[str, float]] = []
        for temp_input in sorted(chip.glob("temp*_input")):
            try:
                value = int(_read(temp_input)) / 1000
            except ValueError:
                continue
            label = _read(chip / temp_input.name.replace("_input", "_label"))
            readings.append((label, value))
        name = _read(chip / "name")
        if name and readings:
            chips.setdefault(name, []).extend(readings)
    for name in CPU_SENSOR_CHIPS:
        if name in chips:
            labelled = [value for label, value in chips[name] if label in CPU_SENSOR_LABELS]
            return labelled[0] if labelled else chips[name][0][1]
    values = [value for readings in chips.values() for _label, value in readings]
    return max(values) if values else None


@dataclass(frozen=True)
class DiskUsage:
    total_gb: float
    used_gb: float
    free_gb: float
    usage_percent: float


def disk_usage(blocks: int, available: int, fragment_size: int) -> DiskUsage:
    """Reserved blocks count as used: free is what unprivileged processes can still write."""
    total = blocks * fragment_size
    free = available * fragment_size
    used = total - free
    return DiskUsage(
        total_gb=round(total / GIB, 1),
        used_gb=round(used / GIB, 1),
        free_gb=round(free / GIB, 1),
        usage_percent=round(used / total * 100, 1) if total else 0.0,
    )
