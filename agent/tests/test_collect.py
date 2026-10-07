from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from edgeio_agent.collect import CollectError, collect
from edgeio_agent.config import AgentSettings
from edgeio_agent.report import build_report


def fake_ping(target: str, count: int) -> str:
    return "5 packets transmitted, 5 received, 0% packet loss\n"


def settings(tmp_path: Path, **overrides: Any) -> AgentSettings:
    chip = tmp_path / "hwmon/hwmon0"
    chip.mkdir(parents=True)
    (chip / "name").write_text("k10temp\n")
    (chip / "temp1_input").write_text("41000\n")
    (chip / "temp1_label").write_text("Tctl\n")
    unit = tmp_path / "cgroup/system.slice/docker.service"
    unit.mkdir(parents=True)
    (unit / "cgroup.procs").write_text("1\n")
    values: dict[str, Any] = {
        "agent_device_id": "100.70.1.2",
        "agent_host_root": "/",
        "agent_hwmon_root": str(tmp_path / "hwmon"),
        "agent_cgroup_root": str(tmp_path / "cgroup"),
        "agent_docker_socket": str(tmp_path / "missing.sock"),
    }
    return AgentSettings.model_validate(values | overrides)


def test_collect_reads_this_machine_into_a_valid_report(tmp_path: Path) -> None:
    snapshot = collect(settings(tmp_path), ping=fake_ping)
    assert snapshot.cpu_temperature_c == 41.0
    assert snapshot.services["tailscaled"] == "stopped"  # no cgroup for it in the fake tree
    assert snapshot.services["docker"] == "unknown"  # Docker API unreachable
    assert (snapshot.containers_running, snapshot.containers_stopped) == (0, 0)
    build_report(snapshot, datetime.now(UTC))  # raises if the contract rejects it


def test_collect_maps_containers_from_the_docker_api(tmp_path: Path, docker_socket: str) -> None:
    snapshot = collect(
        settings(
            tmp_path,
            agent_docker_socket=docker_socket,
            agent_service_renames="streamer-app=edge_streamer",
        ),
        ping=fake_ping,
    )
    assert snapshot.services["docker"] == "running"
    assert snapshot.services["edge_streamer"] == "running"
    assert snapshot.services["app-1"] == "failed"
    assert (snapshot.containers_running, snapshot.containers_stopped) == (1, 1)


def test_collect_without_temperature_sensors_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(CollectError, match="temperature"):
        collect(settings(tmp_path, agent_hwmon_root=str(tmp_path / "none")), ping=fake_ping)


def test_collect_without_default_route_is_an_error(tmp_path: Path) -> None:
    route = tmp_path / "route"
    route.write_text("Iface\tDestination\tGateway\n")
    with pytest.raises(CollectError, match="default route"):
        collect(settings(tmp_path, agent_route_file=str(route)), ping=fake_ping)
