import json
import socketserver
import tempfile
import threading
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from ipaddress import IPv4Address
from pathlib import Path
from typing import Any

import pytest

from edgeio_agent.host import disk_usage
from edgeio_agent.report import HostSnapshot
from edgeio_contracts.samples import sample_report

DOCKER_ENTRIES = [
    {"Names": ["/streamer-app"], "State": "running", "Status": "Up 2 days"},
    {"Names": ["/app-1"], "State": "exited", "Status": "Exited (1) 1 hour ago"},
]


class _DockerHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/containers/json?all=1":
            self.send_error(404)
            return
        body = json.dumps(DOCKER_ENTRIES).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


class _UnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


@pytest.fixture
def docker_socket() -> Iterator[str]:
    # Short /tmp path: Unix socket paths are limited to ~108 bytes.
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / "docker.sock")
        server = _UnixServer(path, _DockerHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            yield path
        finally:
            server.shutdown()
            server.server_close()


# Generalised from the surveyed reference device (no customer identifiers).
REFERENCE = HostSnapshot(
    device_id=IPv4Address("100.70.1.2"),
    hostname="edge-box",
    os="Ubuntu 24.04",
    uptime_seconds=257_000,
    cpu_usage_percent=22.46,
    cpu_temperature_c=36.25,
    load_1m=0.8823,
    ram_total_bytes=6869 * 2**20,
    ram_available_bytes=4609 * 2**20,
    disk=disk_usage(30_346_679, 10_571_289, 4096),
    interface="eno1",
    rx_bytes=631_669_472_517,
    tx_bytes=135_417_761_841,
    packet_loss_percent=0.0,
    services={
        "docker": "running",
        "tailscaled": "running",
        "edge_streamer": "running",
        "app-1": "failed",
    },
    containers_running=6,
    containers_stopped=1,
)


@pytest.fixture
def make_snapshot() -> Callable[..., HostSnapshot]:
    def build(**changes: Any) -> HostSnapshot:
        return replace(REFERENCE, **changes)

    return build


class FakeProducer:
    """Delivers on flush; payloads in `fail` get a delivery error instead."""

    def __init__(self, fail: frozenset[bytes] = frozenset()) -> None:
        self.fail = fail
        self.sent: list[tuple[str, bytes, bytes]] = []
        self._queued: list[tuple[str, bytes, bytes, Callable[[Any, Any], None]]] = []

    def produce(
        self, topic: str, *, key: bytes, value: bytes, on_delivery: Callable[[Any, Any], None]
    ) -> None:
        self._queued.append((topic, key, value, on_delivery))

    def flush(self, timeout: float) -> int:
        for topic, key, value, on_delivery in self._queued:
            if value in self.fail:
                on_delivery("broker unavailable", None)
            else:
                self.sent.append((topic, key, value))
                on_delivery(None, None)
        self._queued.clear()
        return 0


@pytest.fixture
def fake_producer() -> type[FakeProducer]:
    # Tests can't import from conftest under --import-mode=importlib, so expose the class.
    return FakeProducer


@pytest.fixture
def report_bytes() -> Callable[[datetime], bytes]:
    def build(ts: datetime) -> bytes:
        changes = {"device_id": "100.70.1.2", "timestamp": ts.isoformat()}
        return sample_report(changes).model_dump_json().encode()

    return build
