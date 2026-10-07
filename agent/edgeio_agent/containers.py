"""Container states from the Docker Engine API, mapped onto contract service states."""

import http.client
import json
import re
import socket
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from edgeio_contracts.models import ServiceState

MAX_SERVICE_NAME = 64  # contract limit on service keys
_EXIT_CODE = re.compile(r"Exited \((-?\d+)\)")


@dataclass(frozen=True)
class Container:
    name: str
    state: str  # Docker State: created|running|paused|restarting|removing|exited|dead
    status: str  # Docker's human Status, e.g. "Exited (1) 2 hours ago"


class DockerUnavailable(Exception):
    """The Docker Engine API could not be queried."""


def container_state(container: Container) -> ServiceState:
    if container.state == "running":
        return "running"
    if container.state in ("created", "paused"):
        return "stopped"
    if container.state == "exited":
        match = _EXIT_CODE.search(container.status)
        return "stopped" if match and int(match.group(1)) == 0 else "failed"
    if container.state in ("restarting", "dead"):
        return "failed"
    return "unknown"


def container_services(
    containers: Iterable[Container], renames: Mapping[str, str]
) -> dict[str, ServiceState]:
    return {renames.get(c.name, c.name)[:MAX_SERVICE_NAME]: container_state(c) for c in containers}


def parse_containers(body: bytes) -> list[Container]:
    entries: list[dict[str, Any]] = json.loads(body)
    return [
        Container(
            name=str((entry.get("Names") or ["/unnamed"])[0]).lstrip("/"),
            state=str(entry.get("State", "")),
            status=str(entry.get("Status", "")),
        )
        for entry in entries
    ]


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self._socket_path)
        self.sock = sock


def list_containers(socket_path: str, timeout: float = 5.0) -> list[Container]:
    conn = _UnixHTTPConnection(socket_path, timeout)
    try:
        conn.request("GET", "/containers/json?all=1")
        response = conn.getresponse()
        body = response.read()
        if response.status != 200:
            raise DockerUnavailable(f"docker API returned HTTP {response.status}")
        return parse_containers(body)
    except (OSError, http.client.HTTPException, ValueError) as exc:
        raise DockerUnavailable(str(exc)) from exc
    finally:
        conn.close()
