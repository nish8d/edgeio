from pathlib import Path

import pytest

from edgeio_agent.containers import (
    Container,
    DockerUnavailable,
    container_services,
    container_state,
    list_containers,
)


@pytest.mark.parametrize(
    ("state", "status", "expected"),
    [
        ("running", "Up 2 days", "running"),
        ("exited", "Exited (0) 3 hours ago", "stopped"),
        ("exited", "Exited (137) 5 minutes ago", "failed"),
        ("restarting", "Restarting (1) 4 seconds ago", "failed"),
        ("dead", "Dead", "failed"),
        ("created", "Created", "stopped"),
        ("paused", "Up 1 hour (Paused)", "stopped"),
        ("removing", "Removal In Progress", "unknown"),
    ],
)
def test_container_state(state: str, status: str, expected: str) -> None:
    assert container_state(Container("app-1", state, status)) == expected


def test_container_services_applies_renames_and_truncates() -> None:
    containers = [
        Container("streamer-app", "running", "Up 1 hour"),
        Container("x" * 80, "exited", "Exited (1) now"),
    ]
    services = container_services(containers, {"streamer-app": "edge_streamer"})
    assert services == {"edge_streamer": "running", "x" * 64: "failed"}


def test_list_containers_reads_docker_api(docker_socket: str) -> None:
    assert list_containers(docker_socket) == [
        Container("streamer-app", "running", "Up 2 days"),
        Container("app-1", "exited", "Exited (1) 1 hour ago"),
    ]


def test_list_containers_raises_when_socket_missing(tmp_path: Path) -> None:
    with pytest.raises(DockerUnavailable):
        list_containers(str(tmp_path / "missing.sock"))
