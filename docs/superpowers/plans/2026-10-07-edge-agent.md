# Edge Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `edgeio_agent`, a container that runs on a real edge device. Every 300 s it reads the device's health, validates it against the v1 `device.health` contract, spools it on disk and delivers it to Kafka over Tailscale. Also add an opt-in Kafka listener on the server's Tailscale address.

**Architecture:** This is a new uv workspace member, `agent/`, with a functional core and an imperative shell, the same split as the simulator:
- Pure readers (`host.py`, `containers.py` mapping, `ping.py` parsing) turn host files and raw outputs into values.
- `report.py` turns a `HostSnapshot` into a validated `HealthReport`.
- `collect.py` is the only module that touches the live machine (psutil, statvfs, Docker socket, ping).
- `spool.py` and `publisher.py` give at-least-once delivery: a file is deleted only after the broker acks it.
- `runtime.py` runs the fixed-cadence loop.

**Tech Stack:** Python 3.12, psutil, confluent-kafka, pydantic-settings, python-json-logger, stdlib `http.client` over a Unix socket for the Docker Engine API, iputils `ping`, Docker, Docker Compose override file.

**Spec:** `docs/superpowers/specs/2026-10-07-edge-agent-design.md` (plus `CLAUDE.md`, the project spec).

## Global Constraints

- Python 3.12, type hints everywhere, `mypy --strict` clean, `ruff` clean (line length 100). `edgeio_agent` is added to ruff's `known-first-party`.
- Config only via env vars (pydantic-settings). Structured JSON logging. All times are UTC.
- Payloads are the **v1 contract unchanged**. Shared models are imported from `edgeio_contracts` and never duplicated.
- **No customer identifiers in the repo:** no container names, image registry, hostnames, IPs of the reference device. Fixtures use generic names (`eno1`, `app-1`, `edge-box`, `100.70.1.2`, `streamer-app`).
- **Few commits** (user preference). Tasks do **not** commit. Task 7 makes one feature commit and folds this plan into the existing spec commit. All commits are authored by Nishad with **no** Co-Authored-By or "Generated with" trailers.
- Kafka producer settings match the simulator: `enable.idempotence=True`, `acks=all`, key = `device_id`.

## Review Focus

1. **Kafka unreachable for hours or days.** Ticks keep running. The spool grows to `AGENT_SPOOL_MAX_FILES` and then drops the oldest files. Nothing crashes, and the backlog drains when the broker returns (`test_spool`, `test_publisher::test_unreachable_broker_keeps_spool`, `test_agent` integration).
2. **Missing mounts.** No Docker socket, no cgroup mount: the agent reports `unknown`/`stopped` states instead of crashing. Missing temperature sensors, default route or Tailscale address: the tick is skipped with a clear log and the backlog **still drains** (`test_collect`, `test_runtime::test_failed_collection_still_drains_backlog`).
3. **Corrupt or partial spool file** (e.g. power loss mid-write): discarded with a warning; it never blocks the queue (`test_publisher::test_corrupt_spool_entry_is_discarded`).
4. **Internet loss semantics.** One provider dropping ICMP isn't an outage (minimum loss across targets). No route or no reply from any target means 100 %. A hung `ping` is bounded by a timeout (`test_ping`).
5. **ext4 reserved blocks.** The disk triple always satisfies the contract's sum rule, and RAM used never exceeds total (`test_host::test_disk_counts_reserved_blocks_as_used`, `test_report`).

---

### Task 1: Package scaffold, settings and host readers

**Files:**
- Create: `agent/pyproject.toml`, `agent/edgeio_agent/__init__.py`, `agent/edgeio_agent/config.py`, `agent/edgeio_agent/host.py`
- Test: `agent/tests/test_host.py`, `agent/tests/test_config.py`
- Modify: `pyproject.toml` (workspace member, dependency, source, testpaths, first-party, dev dep `types-psutil`)

**Interfaces:**
- Produces:
  - `AgentSettings` (fields below) with properties `host_services: list[str]`, `ping_targets: list[str]` and `service_renames: dict[str, str]`.
  - `host.py`:
    - `read_hostname(host_root: Path) -> str`
    - `read_os(host_root: Path) -> str`
    - `default_interface(route_table: str) -> str | None`
    - `tailscale_ipv4(interfaces: Mapping[str, Sequence[str]]) -> IPv4Address | None`
    - `host_service_state(cgroup_root: Path, unit: str) -> ServiceState`
    - `cpu_temperature(hwmon_root: Path) -> float | None`
    - `DiskUsage(total_gb, used_gb, free_gb, usage_percent)`
    - `disk_usage(blocks: int, available: int, fragment_size: int) -> DiskUsage`

- [ ] **Step 1: Scaffold the package**

`agent/pyproject.toml`:

```toml
[project]
name = "edgeio-agent"
version = "0.1.0"
description = "Health agent for real edge devices: reads the host, publishes device.health over Tailscale"
requires-python = ">=3.12"
dependencies = [
    "edgeio-contracts",
    "confluent-kafka>=2.5",
    "psutil>=5.9",
    "pydantic-settings>=2.4",
    "python-json-logger>=3.1",
]

[tool.uv.sources]
edgeio-contracts = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

`agent/edgeio_agent/__init__.py`:

```python
"""Health agent that runs on a real edge device."""
```

Root `pyproject.toml` changes:
- `dependencies` gains `"edgeio-agent"`.
- `[tool.uv.workspace] members` gains `"agent"`.
- `[tool.uv.sources]` gains `edgeio-agent = { workspace = true }`.
- `testpaths` gains `"agent/tests"`.
- `known-first-party` gains `"edgeio_agent"`.
- The `dev` group gains `"types-psutil>=5.9"`.

Then run `uv sync`.

- [ ] **Step 2: Write the failing tests**

`agent/tests/test_config.py`:

```python
import pytest

from edgeio_agent.config import AgentSettings


def test_list_settings_split_on_commas() -> None:
    settings = AgentSettings.model_validate(
        {"agent_host_services": "docker, tailscaled,", "agent_ping_targets": "1.1.1.1"}
    )
    assert settings.host_services == ["docker", "tailscaled"]
    assert settings.ping_targets == ["1.1.1.1"]


def test_service_renames_parse_pairs() -> None:
    settings = AgentSettings.model_validate(
        {"agent_service_renames": "streamer-app=edge_streamer, db-1=postgresql"}
    )
    assert settings.service_renames == {"streamer-app": "edge_streamer", "db-1": "postgresql"}


def test_malformed_rename_is_rejected() -> None:
    settings = AgentSettings.model_validate({"agent_service_renames": "streamer-app"})
    with pytest.raises(ValueError, match="name=new_name"):
        _ = settings.service_renames
```

`agent/tests/test_host.py`:

```python
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest agent/tests -q`
Expected: collection errors, `ModuleNotFoundError: No module named 'edgeio_agent.config'` / `'edgeio_agent.host'`.

- [ ] **Step 4: Implement**

`agent/edgeio_agent/config.py`:

```python
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
```

`agent/edgeio_agent/host.py`:

```python
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest agent/tests -q && uv run mypy agent/edgeio_agent && uv run ruff check agent`
Expected: all pass, mypy `Success`, ruff `All checks passed!`.

---

### Task 2: Container states from the Docker Engine API

**Files:**
- Create: `agent/edgeio_agent/containers.py`, `agent/tests/conftest.py`
- Test: `agent/tests/test_containers.py`

**Interfaces:**
- Consumes: `ServiceState` from `edgeio_contracts.models`.
- Produces:
  - `Container(name: str, state: str, status: str)` (frozen dataclass)
  - `container_state(Container) -> ServiceState`
  - `container_services(Iterable[Container], Mapping[str, str]) -> dict[str, ServiceState]`
  - `list_containers(socket_path: str, timeout: float = 5.0) -> list[Container]`
  - `DockerUnavailable(Exception)`
  - The conftest fixture `docker_socket -> str` (the path of a fake Docker API serving two containers).

- [ ] **Step 1: Write the failing tests**

`agent/tests/conftest.py`:

```python
import json
import socketserver
import tempfile
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

import pytest

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
```

`agent/tests/test_containers.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest agent/tests/test_containers.py -q`
Expected: `ModuleNotFoundError: No module named 'edgeio_agent.containers'`.

- [ ] **Step 3: Implement**

`agent/edgeio_agent/containers.py`:

```python
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
    return {
        renames.get(c.name, c.name)[:MAX_SERVICE_NAME]: container_state(c) for c in containers
    }


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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest agent/tests -q && uv run mypy agent/edgeio_agent && uv run ruff check agent`
Expected: all pass.

---

### Task 3: Internet packet loss

**Files:**
- Create: `agent/edgeio_agent/ping.py`
- Test: `agent/tests/test_ping.py`

**Interfaces:**
- Produces:
  - `PingRunner = Callable[[str, int], str]`
  - `parse_loss(output: str) -> float`
  - `run_ping(target: str, count: int) -> str`
  - `internet_packet_loss(targets: Sequence[str], count: int, runner: PingRunner = run_ping) -> float`

- [ ] **Step 1: Write the failing tests**

`agent/tests/test_ping.py`:

```python
import pytest

from edgeio_agent.ping import internet_packet_loss, parse_loss

OK = "5 packets transmitted, 5 received, 0% packet loss, time 804ms\n"
LOSSY = "5 packets transmitted, 3 received, 40% packet loss, time 812ms\n"
DROPPED = "5 packets transmitted, 0 received, 100% packet loss, time 4005ms\n"
UNREACHABLE = "ping: connect: Network is unreachable\n"


def test_parse_loss() -> None:
    assert parse_loss(OK) == 0.0
    assert parse_loss(LOSSY) == 40.0
    assert parse_loss(DROPPED) == 100.0
    assert parse_loss("5 packets transmitted, 0 received, +5 errors, 100% packet loss") == 100.0


def test_unparseable_output_counts_as_total_loss() -> None:
    assert parse_loss(UNREACHABLE) == 100.0
    assert parse_loss("") == 100.0


def test_loss_is_the_best_target_so_one_provider_dropping_icmp_is_not_an_outage() -> None:
    outputs = {"1.1.1.1": DROPPED, "8.8.8.8": LOSSY}

    def runner(target: str, count: int) -> str:
        return outputs[target]

    assert internet_packet_loss(["1.1.1.1", "8.8.8.8"], 5, runner) == 40.0


def test_loss_is_100_when_every_target_fails() -> None:
    def runner(target: str, count: int) -> str:
        return UNREACHABLE

    assert internet_packet_loss(["1.1.1.1", "8.8.8.8"], 5, runner) == 100.0


def test_runner_receives_target_and_count() -> None:
    seen: list[tuple[str, int]] = []

    def runner(target: str, count: int) -> str:
        seen.append((target, count))
        return OK

    internet_packet_loss(["9.9.9.9"], 3, runner)
    assert seen == [("9.9.9.9", 3)]


def test_no_targets_is_a_configuration_error() -> None:
    with pytest.raises(ValueError, match="ping target"):
        internet_packet_loss([], 5)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest agent/tests/test_ping.py -q`
Expected: `ModuleNotFoundError: No module named 'edgeio_agent.ping'`.

- [ ] **Step 3: Implement**

`agent/edgeio_agent/ping.py`:

```python
"""Internet packet loss: ping several public targets and keep the best result."""

import re
import subprocess
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor

PingRunner = Callable[[str, int], str]
_LOSS = re.compile(r"([\d.]+)% packet loss")


def parse_loss(output: str) -> float:
    """Loss % from iputils ping output; anything unparseable (no route, no DNS) is 100."""
    match = _LOSS.search(output)
    return min(100.0, float(match.group(1))) if match else 100.0


def run_ping(target: str, count: int) -> str:
    try:
        result = subprocess.run(
            ["ping", "-n", "-q", "-c", str(count), "-i", "0.2", "-W", "1", target],
            capture_output=True,
            text=True,
            timeout=count + 10,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return ""
    return result.stdout + result.stderr


def internet_packet_loss(
    targets: Sequence[str], count: int, runner: PingRunner = run_ping
) -> float:
    """Minimum loss across targets: one provider dropping ICMP isn't an internet outage."""
    if not targets:
        raise ValueError("at least one ping target is required")

    def probe(target: str) -> float:
        return parse_loss(runner(target, count))

    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        return min(pool.map(probe, targets))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest agent/tests -q && uv run mypy agent/edgeio_agent && uv run ruff check agent`
Expected: all pass.

---

### Task 4: Snapshot → validated report, and collecting from the live host

**Files:**
- Create: `agent/edgeio_agent/report.py`, `agent/edgeio_agent/collect.py`
- Modify: `agent/tests/conftest.py` (add the `make_snapshot` fixture)
- Test: `agent/tests/test_report.py`, `agent/tests/test_collect.py`

**Interfaces:**
- Consumes: Task 1 readers and `AgentSettings`, Task 2 `list_containers`/`container_services`/`DockerUnavailable`, Task 3 `internet_packet_loss`/`PingRunner`/`run_ping`.
- Produces:
  - `HostSnapshot` (frozen dataclass, fields below)
  - `build_report(snapshot: HostSnapshot, now: datetime) -> HealthReport`
  - `CollectError(Exception)`
  - `collect(settings: AgentSettings, ping: PingRunner = run_ping) -> HostSnapshot`
  - The conftest fixture `make_snapshot -> Callable[..., HostSnapshot]` (keyword overrides via `dataclasses.replace`).

- [ ] **Step 1: Write the failing tests**

Append to `agent/tests/conftest.py`:

```python
from collections.abc import Callable
from dataclasses import replace
from ipaddress import IPv4Address

from edgeio_agent.host import disk_usage
from edgeio_agent.report import HostSnapshot

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
```

(Move the new imports to the top of the file with the existing ones.)

`agent/tests/test_report.py`:

```python
from collections.abc import Callable
from datetime import UTC, datetime

from edgeio_agent.report import HostSnapshot, build_report
from edgeio_contracts.validation import validate_report

NOW = datetime(2026, 10, 7, 9, 0, tzinfo=UTC)


def test_reference_device_report_passes_the_contract(
    make_snapshot: Callable[..., HostSnapshot],
) -> None:
    report = build_report(make_snapshot(), NOW)
    assert validate_report(report.model_dump_json(), NOW) == report
    assert report.timestamp == NOW
    assert (report.system.ram_total_mb, report.system.ram_used_mb) == (6869, 2260)
    assert report.system.ram_usage_percent == 32.9
    assert report.system.cpu_usage_percent == 22.5
    assert report.system.load_1m == 0.88
    assert report.services["app-1"] == "failed"
    assert (report.containers.running, report.containers.stopped) == (6, 1)


def test_temperature_is_clamped_to_the_contract_range(
    make_snapshot: Callable[..., HostSnapshot],
) -> None:
    assert build_report(make_snapshot(cpu_temperature_c=130.0), NOW).system.cpu_temperature_c == 125
    assert build_report(make_snapshot(cpu_temperature_c=-55.0), NOW).system.cpu_temperature_c == -40
```

`agent/tests/test_collect.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest agent/tests -q`
Expected: collection errors, `No module named 'edgeio_agent.report'` / `'edgeio_agent.collect'`.

- [ ] **Step 3: Implement**

`agent/edgeio_agent/report.py`:

```python
"""Assemble one validated HealthReport from a snapshot of host facts."""

from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv4Address

from edgeio_contracts.models import CURRENT_SCHEMA_VERSION, HealthReport, ServiceState

from .host import DiskUsage

MIB = 2**20


@dataclass(frozen=True)
class HostSnapshot:
    device_id: IPv4Address
    hostname: str
    os: str
    uptime_seconds: int
    cpu_usage_percent: float
    cpu_temperature_c: float
    load_1m: float
    ram_total_bytes: int
    ram_available_bytes: int
    disk: DiskUsage
    interface: str
    rx_bytes: int
    tx_bytes: int
    packet_loss_percent: float
    services: dict[str, ServiceState]
    containers_running: int
    containers_stopped: int


def build_report(snapshot: HostSnapshot, now: datetime) -> HealthReport:
    """Raises pydantic.ValidationError if the snapshot cannot form a valid v1 report."""
    ram_total_mb = snapshot.ram_total_bytes // MIB
    ram_used_mb = min(ram_total_mb, (snapshot.ram_total_bytes - snapshot.ram_available_bytes) // MIB)
    return HealthReport.model_validate(
        {
            "schema_version": CURRENT_SCHEMA_VERSION,
            "device_id": snapshot.device_id,
            "timestamp": now,
            "system": {
                "hostname": snapshot.hostname,
                "os": snapshot.os,
                "uptime_seconds": snapshot.uptime_seconds,
                "cpu_usage_percent": round(float(snapshot.cpu_usage_percent), 1),
                "cpu_temperature_c": round(min(125.0, max(-40.0, snapshot.cpu_temperature_c)), 1),
                "load_1m": round(float(snapshot.load_1m), 2),
                "ram_total_mb": ram_total_mb,
                "ram_used_mb": ram_used_mb,
                "ram_usage_percent": round(ram_used_mb / ram_total_mb * 100, 1),
            },
            "disk": {
                "root_total_gb": snapshot.disk.total_gb,
                "root_used_gb": snapshot.disk.used_gb,
                "root_free_gb": snapshot.disk.free_gb,
                "root_usage_percent": snapshot.disk.usage_percent,
            },
            "network": {
                "interface": snapshot.interface,
                "rx_bytes": snapshot.rx_bytes,
                "tx_bytes": snapshot.tx_bytes,
                "packet_loss_percent": float(snapshot.packet_loss_percent),
            },
            "services": dict(snapshot.services),
            "containers": {
                "running": snapshot.containers_running,
                "stopped": snapshot.containers_stopped,
            },
        }
    )
```

`agent/edgeio_agent/collect.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest agent/tests -q && uv run mypy agent/edgeio_agent && uv run ruff check agent`
Expected: all pass. (`test_collect` reads this machine's real `/proc/net/route`, `statvfs("/")` and psutil, so it needs a default route. Dev machines and GitHub runners have one.)

---

### Task 5: Spool and publisher (at-least-once delivery)

**Files:**
- Create: `agent/edgeio_agent/spool.py`, `agent/edgeio_agent/publisher.py`
- Modify: `agent/tests/conftest.py` (add the `FakeProducer` class with its `fake_producer` fixture, and the `report_bytes` fixture)
- Test: `agent/tests/test_spool.py`, `agent/tests/test_publisher.py`, `tests/integration/test_agent.py`

**Interfaces:**
- Produces:
  - `Spool(directory: Path, max_files: int)` with `.add(timestamp: datetime, payload: bytes) -> int` (the number of oldest files dropped) and `.pending() -> list[Path]` (oldest first).
  - `Producer` Protocol (`produce(topic, *, key, value, on_delivery)`, `flush(timeout) -> int`).
  - `send_pending(spool: Spool, producer: Producer, topic: str, timeout: float) -> int` (the count delivered and deleted).

- [ ] **Step 1: Write the failing tests**

Append to `agent/tests/conftest.py` (imports to the top):

```python
from datetime import datetime

from edgeio_contracts.samples import sample_report


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
```

`agent/tests/test_spool.py`:

```python
from datetime import UTC, datetime
from pathlib import Path

from edgeio_agent.spool import Spool


def at(minute: int) -> datetime:
    return datetime(2026, 10, 7, 9, minute, tzinfo=UTC)


def test_add_lists_oldest_first_and_leaves_no_temp_files(tmp_path: Path) -> None:
    spool = Spool(tmp_path, max_files=10)
    spool.add(at(5), b"second")
    spool.add(at(0), b"first")
    assert [path.read_bytes() for path in spool.pending()] == [b"first", b"second"]
    assert not list(tmp_path.glob(".*"))


def test_add_drops_the_oldest_beyond_the_cap(tmp_path: Path) -> None:
    spool = Spool(tmp_path, max_files=2)
    dropped = [spool.add(at(minute), str(minute).encode()) for minute in (0, 5, 10)]
    assert dropped == [0, 0, 1]
    assert [path.read_bytes() for path in spool.pending()] == [b"5", b"10"]


def test_spool_creates_its_directory(tmp_path: Path) -> None:
    Spool(tmp_path / "a/b", max_files=1).add(at(0), b"x")
    assert (tmp_path / "a/b").is_dir()
```

`agent/tests/test_publisher.py`:

```python
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import Producer

from typing import Any

from edgeio_agent.publisher import send_pending
from edgeio_agent.spool import Spool

BASE = datetime(2026, 10, 7, 9, 0, tzinfo=UTC)


def test_delivers_oldest_first_keyed_by_device_and_deletes_acked(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes], fake_producer: type[Any]
) -> None:
    spool = Spool(tmp_path, max_files=10)
    payloads = [report_bytes(BASE + timedelta(minutes=5 * i)) for i in range(3)]
    for i, payload in enumerate(payloads):
        spool.add(BASE + timedelta(minutes=5 * i), payload)
    producer = fake_producer()
    assert send_pending(spool, producer, "device.health", timeout=5) == 3
    assert producer.sent == [("device.health", b"100.70.1.2", p) for p in payloads]
    assert spool.pending() == []


def test_failed_delivery_stays_in_the_spool(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes], fake_producer: type[Any]
) -> None:
    spool = Spool(tmp_path, max_files=10)
    ok, bad = report_bytes(BASE), report_bytes(BASE + timedelta(minutes=5))
    spool.add(BASE, ok)
    spool.add(BASE + timedelta(minutes=5), bad)
    assert send_pending(spool, fake_producer(fail=frozenset({bad})), "t", timeout=5) == 1
    assert [path.read_bytes() for path in spool.pending()] == [bad]


def test_corrupt_spool_entry_is_discarded(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes], fake_producer: type[Any]
) -> None:
    spool = Spool(tmp_path, max_files=10)
    spool.add(BASE, b'{"truncated": ')
    spool.add(BASE + timedelta(minutes=5), report_bytes(BASE + timedelta(minutes=5)))
    producer = fake_producer()
    assert send_pending(spool, producer, "t", timeout=5) == 1
    assert spool.pending() == []


def test_unreachable_broker_keeps_spool(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes]
) -> None:
    spool = Spool(tmp_path, max_files=10)
    spool.add(BASE, report_bytes(BASE))
    producer = Producer({"bootstrap.servers": "127.0.0.1:1", "message.timeout.ms": 1000})
    assert send_pending(spool, producer, "device.health", timeout=10) == 0
    assert len(spool.pending()) == 1
```

`tests/integration/test_agent.py`:

```python
import json
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from edgeio_agent.publisher import send_pending
from edgeio_agent.spool import Spool
from edgeio_contracts.samples import sample_report


def test_spooled_backlog_reaches_kafka_in_order_keyed_by_device(
    kafka_bootstrap: str, tmp_path: Path
) -> None:
    topic = f"device.health.agent.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    admin.create_topics([NewTopic(topic, 3, 1)])[topic].result(timeout=30)

    spool = Spool(tmp_path, max_files=100)
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=15)
    stamps = [base + timedelta(minutes=5 * i) for i in range(3)]
    for ts in stamps:
        changes = {"device_id": "100.70.1.2", "timestamp": ts.isoformat()}
        spool.add(ts, sample_report(changes).model_dump_json().encode())

    producer = Producer(
        {"bootstrap.servers": kafka_bootstrap, "enable.idempotence": True, "acks": "all"}
    )
    assert send_pending(spool, producer, topic, timeout=30) == 3
    assert spool.pending() == []

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": f"reader-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    messages = []
    deadline = time.monotonic() + 30
    while len(messages) < 3 and time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is not None and msg.error() is None:
            messages.append(msg)
    consumer.close()

    assert {msg.key() for msg in messages} == {b"100.70.1.2"}
    received = [datetime.fromisoformat(json.loads(msg.value())["timestamp"]) for msg in messages]
    assert received == stamps
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest agent/tests -q`
Expected: `No module named 'edgeio_agent.spool'` / `'edgeio_agent.publisher'`.

- [ ] **Step 3: Implement**

`agent/edgeio_agent/spool.py`:

```python
"""On-disk queue of encoded reports awaiting delivery, oldest first."""

import os
from datetime import datetime
from pathlib import Path


class Spool:
    def __init__(self, directory: Path, max_files: int) -> None:
        self.directory = directory
        self.max_files = max_files
        directory.mkdir(parents=True, exist_ok=True)

    def add(self, timestamp: datetime, payload: bytes) -> int:
        """Write atomically (temp file + rename); return how many oldest entries were dropped."""
        name = f"{timestamp:%Y%m%dT%H%M%S}Z.json"
        tmp = self.directory / f".{name}.tmp"
        with tmp.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.directory / name)
        pending = self.pending()
        excess = pending[: max(0, len(pending) - self.max_files)]
        for path in excess:
            path.unlink(missing_ok=True)
        return len(excess)

    def pending(self) -> list[Path]:
        return sorted(self.directory.glob("*.json"))
```

`agent/edgeio_agent/publisher.py`:

```python
"""Deliver spooled reports to Kafka; a file is deleted only after the broker acknowledges it."""

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from pydantic import ValidationError

from edgeio_contracts.models import HealthReport

from .spool import Spool

log = logging.getLogger(__name__)


class Producer(Protocol):
    def produce(
        self, topic: str, *, key: bytes, value: bytes, on_delivery: Callable[[Any, Any], None]
    ) -> None: ...

    def flush(self, timeout: float) -> int: ...


def send_pending(spool: Spool, producer: Producer, topic: str, timeout: float) -> int:
    delivered: list[Path] = []
    for path in spool.pending():
        try:
            payload = path.read_bytes()
            report = HealthReport.model_validate_json(payload)
        except (OSError, ValidationError) as exc:
            log.warning(
                "discarding unreadable spool entry", extra={"file": path.name, "error": str(exc)}
            )
            path.unlink(missing_ok=True)
            continue
        producer.produce(
            topic,
            key=str(report.device_id).encode(),
            value=payload,
            on_delivery=_record(path, delivered),
        )
    remaining = producer.flush(timeout)
    if remaining:
        log.warning("delivery still pending at flush timeout", extra={"pending": remaining})
    for path in delivered:
        path.unlink(missing_ok=True)
    return len(delivered)


def _record(path: Path, delivered: list[Path]) -> Callable[[Any, Any], None]:
    def on_delivery(err: Any, _msg: Any) -> None:
        if err is None:
            delivered.append(path)
        else:
            log.warning("delivery failed; kept in spool", extra={"file": path.name, "error": str(err)})

    return on_delivery
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest agent/tests -q && uv run pytest tests/integration/test_agent.py -q && uv run mypy agent/edgeio_agent && uv run ruff check agent tests`
Expected: all pass. The integration test needs Docker.

---

### Task 6: Loop, entry point, image, Tailscale listener, CI

**Files:**
- Create:
  - `agent/edgeio_agent/logs.py`, `agent/edgeio_agent/runtime.py`, `agent/edgeio_agent/__main__.py`
  - `docker/agent.Dockerfile`, `docker-compose.tailscale.yml`
  - `deploy/agent/README.md`, `deploy/agent/agent.env.example`
- Modify: `Makefile` (mypy path, `up-tailnet`, `agent-image`), `.github/workflows/ci.yml` (mypy path, agent image build)
- Test: `agent/tests/test_runtime.py`

**Interfaces:**
- Consumes: `build_report`, `HostSnapshot`, `CollectError`, `collect`, `Spool`, `send_pending`, `Producer`, `AgentSettings`, `validate_report`, `ContractError`.
- Produces:
  - `tick(snapshot_source: Callable[[], HostSnapshot], spool: Spool, producer: Producer, topic: str, flush_timeout: float, now: datetime) -> None`
  - `run(snapshot_source, spool, producer, topic, flush_timeout, interval: float, stop: threading.Event) -> None`
  - `python -m edgeio_agent`

- [ ] **Step 1: Write the failing tests**

`agent/tests/test_runtime.py`:

```python
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from edgeio_agent.collect import CollectError
from edgeio_agent.report import HostSnapshot
from edgeio_agent.runtime import run, tick
from edgeio_agent.spool import Spool
from typing import Any

from edgeio_contracts.validation import validate_report

NOW = datetime(2026, 10, 7, 9, 0, tzinfo=UTC)


def test_tick_spools_a_valid_report_and_sends_it(
    tmp_path: Path, make_snapshot: Callable[..., HostSnapshot], fake_producer: type[Any]
) -> None:
    spool, producer = Spool(tmp_path, 10), fake_producer()
    tick(make_snapshot, spool, producer, "device.health", 5.0, NOW)
    assert len(producer.sent) == 1
    topic, key, value = producer.sent[0]
    assert (topic, key) == ("device.health", b"100.70.1.2")
    assert validate_report(value, NOW).timestamp == NOW
    assert spool.pending() == []


def test_failed_collection_still_drains_backlog(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes], fake_producer: type[Any]
) -> None:
    spool, producer = Spool(tmp_path, 10), fake_producer()
    earlier = NOW - timedelta(minutes=5)
    spool.add(earlier, report_bytes(earlier))

    def broken() -> HostSnapshot:
        raise CollectError("no default route")

    tick(broken, spool, producer, "device.health", 5.0, NOW)
    assert len(producer.sent) == 1
    assert spool.pending() == []


def test_invalid_snapshot_is_skipped_not_spooled(
    tmp_path: Path, make_snapshot: Callable[..., HostSnapshot], fake_producer: type[Any]
) -> None:
    spool, producer = Spool(tmp_path, 10), fake_producer()
    tick(lambda: make_snapshot(interface=""), spool, producer, "t", 5.0, NOW)  # contract: min 1
    assert producer.sent == [] and spool.pending() == []


def test_run_ticks_until_stopped(
    tmp_path: Path, make_snapshot: Callable[..., HostSnapshot], fake_producer: type[Any]
) -> None:
    stop = threading.Event()
    calls: list[int] = []

    def source() -> HostSnapshot:
        calls.append(1)
        if len(calls) == 3:
            stop.set()
        return make_snapshot()

    run(source, Spool(tmp_path, 10), fake_producer(), "t", 5.0, interval=0.01, stop=stop)
    assert len(calls) == 3
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest agent/tests/test_runtime.py -q`
Expected: `No module named 'edgeio_agent.runtime'`.

- [ ] **Step 3: Implement the loop and entry point**

`agent/edgeio_agent/logs.py`:

```python
import logging

from pythonjsonlogger.json import JsonFormatter


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.basicConfig(level=level, handlers=[handler], force=True)
```

`agent/edgeio_agent/runtime.py`:

```python
"""The agent loop: collect → validate → spool → deliver, on a fixed cadence."""

import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime

from edgeio_contracts.validation import ContractError, validate_report

from .collect import CollectError
from .publisher import Producer, send_pending
from .report import HostSnapshot, build_report
from .spool import Spool

log = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def tick(
    snapshot_source: Callable[[], HostSnapshot],
    spool: Spool,
    producer: Producer,
    topic: str,
    flush_timeout: float,
    now: datetime,
) -> None:
    try:
        payload = build_report(snapshot_source(), now).model_dump_json().encode()
        validate_report(payload, now)  # the exact bytes the worker will see
    except (CollectError, ContractError, ValueError) as exc:  # ValidationError is a ValueError
        log.error("skipping reading: could not build a valid report", extra={"error": str(exc)})
    else:
        dropped = spool.add(now, payload)
        if dropped:
            log.warning("spool full; dropped oldest readings", extra={"dropped": dropped})
    sent = send_pending(spool, producer, topic, flush_timeout)
    log.info("tick", extra={"sent": sent, "backlog": len(spool.pending())})


def run(
    snapshot_source: Callable[[], HostSnapshot],
    spool: Spool,
    producer: Producer,
    topic: str,
    flush_timeout: float,
    interval: float,
    stop: threading.Event,
) -> None:
    next_at = time.monotonic()
    while not stop.is_set():
        tick(snapshot_source, spool, producer, topic, flush_timeout, utc_now())
        next_at = max(next_at + interval, time.monotonic())
        stop.wait(next_at - time.monotonic())
```

`agent/edgeio_agent/__main__.py`:

```python
"""Entry point: python -m edgeio_agent"""

import functools
import logging
import shutil
import signal
import threading
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from .collect import collect
from .config import AgentSettings
from .logs import configure_logging
from .runtime import run
from .spool import Spool

log = logging.getLogger("edgeio_agent")


def main() -> None:
    settings = AgentSettings()
    configure_logging(settings.log_level)
    _ = settings.service_renames  # fail fast on a malformed AGENT_SERVICE_RENAMES
    if shutil.which("ping") is None:
        raise SystemExit("ping is not installed; packet loss cannot be measured")

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            # Give up on a message within one send window so flush() reports every outcome.
            "message.timeout.ms": int(settings.agent_send_timeout_seconds * 1000),
        }
    )
    spool = Spool(Path(settings.agent_spool_dir), settings.agent_spool_max_files)
    stop = threading.Event()

    def request_stop(signum: int, _frame: Any) -> None:
        log.info("stopping", extra={"signal": signum})
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, request_stop)

    log.info("agent starting", extra={"bootstrap": settings.kafka_bootstrap})
    run(
        functools.partial(collect, settings),
        spool,
        producer,
        settings.kafka_topic,
        settings.agent_send_timeout_seconds + 5,
        settings.agent_interval_seconds,
        stop,
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest agent/tests -q && uv run mypy agent/edgeio_agent && uv run ruff check agent`
Expected: all pass.

- [ ] **Step 5: Image, Tailscale listener, Make targets, CI, deploy docs**

`docker/agent.Dockerfile`:

```dockerfile
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends iputils-ping \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY . .
RUN uv sync --frozen --no-dev --no-editable --package edgeio-agent

CMD ["python", "-m", "edgeio_agent"]
```

`docker-compose.tailscale.yml`:

```yaml
# Opt-in: also accept producers (real edge agents) on this machine's Tailscale address.
# Use `make up-tailnet`, which sets TAILSCALE_IP from `tailscale ip -4`.
services:
  kafka:
    ports:
      - "${TAILSCALE_IP:?set TAILSCALE_IP}:9094:9094"
    environment:
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,TAILNET://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092,TAILNET://${TAILSCALE_IP:?set TAILSCALE_IP}:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,TAILNET:PLAINTEXT,CONTROLLER:PLAINTEXT
```

`Makefile` changes:
- Add `up-tailnet agent-image` to `.PHONY`.
- Add `agent/edgeio_agent` to the `mypy` line in `lint`.
- Add:

```make
up-tailnet:
	TAILSCALE_IP=$$(tailscale ip -4) docker compose -f docker-compose.yml -f docker-compose.tailscale.yml up -d --build

agent-image:
	docker build -f docker/agent.Dockerfile -t edgeio/agent:dev .
```

`.github/workflows/ci.yml` changes:
- Add `agent/edgeio_agent` to the Mypy step.
- In the `images` job, after "Build images", add:

```yaml
      - name: Build agent image
        run: docker build -f docker/agent.Dockerfile -t edgeio/agent:ci .
```

`deploy/agent/agent.env.example`:

```sh
# Copy to ~/edgeio-agent.env on the device and edit. Never commit the real file.
KAFKA_BOOTSTRAP=100.x.y.z:9094          # the server's Tailscale IP (make up-tailnet)
AGENT_SERVICE_RENAMES=streamer-app=edge_streamer   # container=service_key, comma-separated
# AGENT_HOST_SERVICES=docker,tailscaled
# AGENT_PING_TARGETS=1.1.1.1,8.8.8.8
# AGENT_INTERVAL_SECONDS=300
```

`deploy/agent/README.md`:

````markdown
# Running the edge agent on a device

The agent reads the host it runs on and publishes a `device.health` v1 report every 300 s to Kafka over Tailscale. If Kafka can't be reached, readings are spooled on disk for up to 7 days and sent oldest first when it comes back.

**Requirements on the device:**
- Linux with cgroup v2 (e.g. Ubuntu 22.04+)
- Docker
- Tailscale
- A user in the `docker` group. No sudo needed.

## 1. Start the stack with the Tailscale listener (server)

```sh
make up-tailnet        # Kafka also listens on <server tailscale ip>:9094
```

## 2. Build and ship the image (server → device)

```sh
make agent-image
docker save edgeio/agent:dev | gzip | ssh USER@DEVICE 'gunzip | docker load'
```

## 3. Configure (device)

Copy `deploy/agent/agent.env.example` to `~/edgeio-agent.env` on the device. Then set:
- `KAFKA_BOOTSTRAP` to the server's Tailscale IP with port `:9094`;
- `AGENT_SERVICE_RENAMES` for the container that is the edge streamer.

## 4. Run (device)

```sh
docker run -d --name edgeio-agent --restart=always \
  --network host --pid host --uts host \
  -v /:/host:ro \
  -v /sys/fs/cgroup:/host-cgroup:ro \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -v edgeio-agent-spool:/spool \
  --env-file ~/edgeio-agent.env \
  edgeio/agent:dev
docker logs -f edgeio-agent      # one JSON "tick" line per reading
```

The device appears on the dashboard under its Tailscale IP within one interval.

**Security note:** Docker socket access is root-equivalent, even when mounted read-only. The agent only issues GET requests to the Docker API.

## Upgrade / remove

```sh
docker rm -f edgeio-agent        # the spool volume is kept; re-run step 4 with the new image
docker volume rm edgeio-agent-spool   # only to discard unsent readings
```
````

- [ ] **Step 6: Verify the image builds and the compose override renders**

Run: `make agent-image && TAILSCALE_IP=100.64.0.1 docker compose -f docker-compose.yml -f docker-compose.tailscale.yml config --quiet && docker run --rm --entrypoint python edgeio/agent:dev -c "import edgeio_agent.collect; import shutil; print(shutil.which('ping'))"`
Expected: the image builds, compose config is valid (no output), and it prints `/usr/bin/ping`.

---

### Task 7: Docs, full verification, commit

**Files:**
- Modify: `CLAUDE.md` (§3 layout, §4 disk convention note, §5 Tailscale listener, new §6b Edge agent, §12 commands, §15 status), `README.md` ("Running on a real device" section + layout line)
- Modify: `docs/superpowers/specs/2026-10-07-edge-agent-design.md` (env file path `~/edgeio-agent.env` instead of `/etc/edgeio-agent.env`; publisher sends all pending and deletes only acked files, rather than stopping at the first failure; a missing temperature sensor skips the tick)

- [ ] **Step 1: Update docs**

- **CLAUDE.md §3:** add `agent/` (health agent for real devices) and `deploy/agent/` (device run instructions).
- **CLAUDE.md §4:** add a note: "Disk convention: `root_free_gb` is space available to unprivileged processes; reserved blocks count as used (`used = total − free`)."
- **CLAUDE.md §5:** add "Opt-in `TAILNET` listener on `<tailscale ip>:9094` via `docker-compose.tailscale.yml` (`make up-tailnet`) for real agents."
- **CLAUDE.md, new §6b "Edge agent (`agent/`)":** summarize spec §3–§6:
  - the field sources;
  - spool + at-least-once;
  - container/cgroup state mapping;
  - min-loss ping;
  - the `docker run` flags;
  - "never commit customer identifiers".
- **CLAUDE.md §12:** add `make up-tailnet` and `make agent-image`.
- **CLAUDE.md §15:** add "Plan 3 (edge agent) implemented."
- **README:** a "Running on a real device" section pointing to `deploy/agent/README.md`, plus `agent/` and `deploy/` in the layout block.
- **Spec:** the three corrections listed under Files.

- [ ] **Step 2: Full verification**

Run: `make lint && make test && make test-int && make agent-image`
Expected: all green.

Then check no customer identifiers slipped in:
```sh
git diff main --name-only | xargs grep -n -i -E -f ~/.edgeio-private-patterns || echo clean   # patterns kept outside the repo
```
Expected: `clean`.

- [ ] **Step 3: Commit (few commits)**

- Fold this plan into the spec commit, which is `HEAD` before any feature commit on `feat/edge-agent`. Do it **before** staging the feature: run `git add docs/superpowers/plans/2026-10-07-edge-agent.md`, then `git commit --amend -m "docs: edge agent design spec and implementation plan"`.
- Then make one feature commit:

```bash
git add -A
git commit -m "feat(agent): health agent for real edge devices over Tailscale

Reads the host (psutil, hwmon, statvfs, cgroup v2, Docker API, ping), validates
a v1 device.health report locally, spools it on disk and delivers it to Kafka
at-least-once. Adds an opt-in Tailscale Kafka listener, the agent image, deploy
instructions and CI coverage."
```

Both commits are authored by Nishad, with no trailers.

---

### Task 8 (manual, with the user): live run on the reference device

Not part of automated execution. It needs the user's SSH session to the device and their go-ahead to start a container on a customer machine.

1. On the laptop: `make down && make up-tailnet`. Check `docker compose ps` shows Kafka publishing `<tailscale ip>:9094`.
2. Ship the image: `docker save edgeio/agent:dev | gzip | ssh DEVICE 'gunzip | docker load'`.
3. Write `~/edgeio-agent.env` on the device: `KAFKA_BOOTSTRAP=<laptop tailscale ip>:9094` and `AGENT_SERVICE_RENAMES=<streamer container>=edge_streamer`.
4. Run the `docker run` command from `deploy/agent/README.md`. Check `docker logs edgeio-agent` shows `"sent": 1`.
5. On the dashboard: a 51st device appears with real metrics and per-container services.
6. Spool check: `docker compose stop kafka` for about 10 minutes and confirm the backlog grows in the agent logs. Start Kafka again and confirm the backlog drains and the chart has no gap.
