from ipaddress import IPv4Address
from pathlib import Path

import pytest

from edgeio_agent.host import (
    cpu_temperature,
    default_interface,
    disk_usage,
    host_service_state,
    read_hostname,
    read_os,
    tailscale_ipv4,
)

ROUTE_TABLE = (
    "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n"
    "wlan0\t00000000\t0101A8C0\t0003\t0\t0\t600\t00000000\t0\t0\t0\n"
    "eno1\t00000000\t0101A8C0\t0003\t0\t0\t100\t00000000\t0\t0\t0\n"
    "eno1\t0001A8C0\t00000000\t0001\t0\t0\t100\t00FFFFFF\t0\t0\t0\n"
)


def make_chip(root: Path, index: int, name: str, temps: dict[int, tuple[str | None, int]]) -> None:
    chip = root / f"hwmon{index}"
    chip.mkdir(parents=True)
    (chip / "name").write_text(f"{name}\n")
    for number, (label, millidegrees) in temps.items():
        (chip / f"temp{number}_input").write_text(f"{millidegrees}\n")
        if label is not None:
            (chip / f"temp{number}_label").write_text(f"{label}\n")


def test_default_interface_picks_lowest_metric_default_route() -> None:
    assert default_interface(ROUTE_TABLE) == "eno1"


def test_default_interface_none_without_default_route() -> None:
    assert default_interface(ROUTE_TABLE.splitlines()[0] + "\n") is None


def test_tailscale_ipv4_prefers_tailscale0() -> None:
    interfaces = {
        "eno1": ["192.168.1.20"],
        "other": ["100.80.0.9"],
        "tailscale0": ["100.70.1.2"],
    }
    assert tailscale_ipv4(interfaces) == IPv4Address("100.70.1.2")


def test_tailscale_ipv4_ignores_non_cgnat_and_garbage() -> None:
    assert tailscale_ipv4({"eno1": ["192.168.1.20", "fe80::1", "not-an-ip"]}) is None


def test_read_os_uses_name_and_version_id(tmp_path: Path) -> None:
    (tmp_path / "etc").mkdir()
    (tmp_path / "etc/os-release").write_text(
        'PRETTY_NAME="Ubuntu 24.04.3 LTS"\nNAME="Ubuntu"\nVERSION_ID="24.04"\n'
    )
    assert read_os(tmp_path) == "Ubuntu 24.04"


def test_read_os_falls_back_to_pretty_name_then_linux(tmp_path: Path) -> None:
    assert read_os(tmp_path) == "Linux"
    (tmp_path / "etc").mkdir()
    (tmp_path / "etc/os-release").write_text('PRETTY_NAME="Debian GNU/Linux 12 (bookworm)"\n')
    assert read_os(tmp_path) == "Debian GNU/Linux 12 (bookworm)"


def test_read_hostname_strips_newline(tmp_path: Path) -> None:
    (tmp_path / "etc").mkdir()
    (tmp_path / "etc/hostname").write_text("edge-box\n")
    assert read_hostname(tmp_path) == "edge-box"


def test_host_service_state_from_cgroup(tmp_path: Path) -> None:
    running = tmp_path / "system.slice/docker.service"
    running.mkdir(parents=True)
    (running / "cgroup.procs").write_text("1234\n")
    idle = tmp_path / "system.slice/ssh.service"
    idle.mkdir(parents=True)
    (idle / "cgroup.procs").write_text("")
    assert host_service_state(tmp_path, "docker") == "running"
    assert host_service_state(tmp_path, "ssh") == "stopped"
    assert host_service_state(tmp_path, "tailscaled") == "stopped"


def test_host_service_state_unknown_without_cgroup_mount(tmp_path: Path) -> None:
    assert host_service_state(tmp_path / "missing", "docker") == "unknown"


def test_cpu_temperature_prefers_cpu_chip_over_hotter_sensors(tmp_path: Path) -> None:
    make_chip(tmp_path, 0, "nvme", {1: ("Composite", 21850), 2: ("Sensor 1", 35850)})
    make_chip(tmp_path, 1, "k10temp", {1: ("Tctl", 36250)})
    make_chip(tmp_path, 2, "amdgpu", {1: ("edge", 36000)})
    assert cpu_temperature(tmp_path) == 36.25


def test_cpu_temperature_uses_package_label_on_intel(tmp_path: Path) -> None:
    make_chip(tmp_path, 0, "coretemp", {1: ("Core 0", 50000), 2: ("Package id 0", 55000)})
    assert cpu_temperature(tmp_path) == 55.0


def test_cpu_temperature_falls_back_to_hottest_sensor(tmp_path: Path) -> None:
    make_chip(tmp_path, 0, "acpitz", {1: (None, 40000)})
    make_chip(tmp_path, 1, "nvme", {1: ("Composite", 45500)})
    assert cpu_temperature(tmp_path) == 45.5


def test_cpu_temperature_none_without_sensors(tmp_path: Path) -> None:
    assert cpu_temperature(tmp_path) is None


def test_disk_counts_reserved_blocks_as_used() -> None:
    # ext4 root of the reference device: ~124 GB with ~5 % reserved for root.
    usage = disk_usage(blocks=30_346_679, available=10_571_289, fragment_size=4096)
    assert usage.free_gb == round(10_571_289 * 4096 / 2**30, 1)
    assert abs(usage.used_gb + usage.free_gb - usage.total_gb) <= 0.1
    assert usage.usage_percent == pytest.approx(65.2, abs=0.1)
