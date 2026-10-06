# edgeio Pipeline (Plan 1 of 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the data pipeline end to end: shared message contract → 50-device simulator → Kafka → validating stream worker → TimescaleDB (readings, rollups, alerts), all runnable with `make up`.

**Architecture:** A `uv` workspace with three Python packages: `contracts` (Pydantic wire models, the single source of truth), `simulator` (pure device state model wrapped in a thin asyncio/Kafka shell), and `worker` (pure transform/rule logic wrapped in a psycopg store layer and a Kafka consumer loop). The worker commits offsets only after the DB transaction commits; inserts are idempotent. Plain SQL migrations create a Timescale hypertable, continuous aggregates and retention policies.

**Tech Stack:** Python 3.12, uv workspace, Pydantic v2, pydantic-settings, confluent-kafka, psycopg 3, python-json-logger, pytest, testcontainers, ruff, mypy; Kafka `apache/kafka:3.8.0` (KRaft); `timescale/timescaledb:2.17.2-pg16`; Docker Compose.

**Spec:** `CLAUDE.md` (repo root) — sections 1–9, 12–15. Plan 2 (API + dashboard, spec §10–11) is written after this plan lands.

## Global Constraints

- Python 3.12; every package type-checks under `mypy --strict` and is `ruff` clean (line length 100).
- Config only via environment variables (pydantic-settings); no hard-coded hosts in code.
- All times are UTC; DB columns are `timestamptz`.
- The payload schema lives only in `contracts/`; no other package re-declares it.
- No ORM: plain SQL with parameterized psycopg 3 queries.
- Topics: `device.health` (6 partitions, key = `device_id`), `device.health.dlq` (1 partition). Broker auto-create disabled.
- Simulator default: 50 devices, `SIM_INTERVAL_SECONDS=300`, real time only (no acceleration, no backfill).
- Offsets are committed only after the DB transaction commits (at-least-once + idempotent inserts).
- Worker DB tests run against a real Timescale container — never mock the database.
- Images: `apache/kafka:3.8.0`, `timescale/timescaledb:2.17.2-pg16`.

## Review Focus

1. **Redelivered message** (worker crashed after DB commit, before offset commit) → no duplicate reading and no duplicate alert. Tests: Task 10 `test_duplicate_report_is_ignored`, Task 12 pipeline test.
2. **Late / out-of-order reading** (older than the device's `last_seen`) → stored in history, but doesn't rewind `devices` state or touch alerts. Test: Task 10 `test_late_reading_is_stored_but_does_not_rewind_state`.
3. **Device reboot resets rx/tx counters** → rates are null, never negative. Test: Task 7 `test_counter_reset_after_reboot_gives_no_rates`.
4. **Timestamp with a non-UTC offset** (`+05:30`) → accepted and normalized to UTC. Test: Task 1 `test_timestamp_with_offset_is_normalized_to_utc`.
5. **Database outage mid-batch** → worker retries with backoff, never drops the batch or commits offsets; gives up only on shutdown. Tests: Task 12 `test_db.py`.

---

## File Structure

```
pyproject.toml                    # uv workspace root (virtual), tool config
.python-version  .gitignore  .dockerignore  Makefile  docker-compose.yml
docker/python.Dockerfile          # one image recipe, ARG PACKAGE selects the service
contracts/
  pyproject.toml
  health.schema.json              # generated from the Pydantic model
  fixtures/valid/basic.json       # canonical example payload
  edgeio_contracts/
    __init__.py  py.typed
    models.py                     # HealthReport & nested models, constants
    samples.py                    # sample_payload/sample_report/set_path for tests
    validation.py                 # validate_report(), ContractError
    export_schema.py              # JSON Schema export CLI
  tests/test_models.py  tests/test_validation.py  tests/test_schema.py
simulator/
  pyproject.toml
  edgeio_simulator/
    __init__.py  py.typed  __main__.py
    config.py                     # SimulatorSettings
    logs.py                       # JSON logging setup
    fleet.py                      # DeviceProfile, generate_fleet, tailscale_ip
    faults.py                     # Fault enum, ActiveFault, next_fault
    device.py                     # DeviceState, initial_state, step
    malformed.py                  # deliberately invalid payloads
    runtime.py                    # asyncio fleet runner, MessageSink protocol
  tests/test_fleet.py  tests/test_faults.py  tests/test_device.py
  tests/test_malformed.py  tests/test_runtime.py
worker/
  pyproject.toml
  edgeio_worker/
    __init__.py  py.typed  __main__.py
    config.py                     # WorkerSettings
    logs.py                       # JSON logging setup
    transform.py                  # ReadingRow, PreviousCounters, to_row
    rules.py                      # alert rules, evaluate, apply_actions, device_status
    migrate.py                    # apply_migrations + CLI
    store.py                      # process_report, store_batch
    sweeper.py                    # sweep_offline
    dlq.py                        # build_dlq_record
    db.py                         # Database (transaction + retry)
    consumer.py                   # run(), handle_batch()
  tests/test_transform.py  tests/test_rules.py  tests/test_dlq.py  tests/test_db.py
db/migrations/0001_init.sql  db/migrations/0002_aggregates.sql
tests/integration/conftest.py  test_migrations.py  test_store.py  test_sweeper.py  test_pipeline.py
```

---

### Task 1: Workspace scaffold and contract models

**Files:**
- Create: `pyproject.toml`, `.python-version`, `.gitignore`
- Create: `contracts/pyproject.toml`, `contracts/edgeio_contracts/__init__.py`, `contracts/edgeio_contracts/py.typed`
- Create: `contracts/edgeio_contracts/models.py`, `contracts/edgeio_contracts/samples.py`
- Create: `contracts/fixtures/valid/basic.json`
- Test: `contracts/tests/test_models.py`
- Modify: `CLAUDE.md` §13 (invalid cases live in a mutation table)

**Interfaces:**
- Produces (`edgeio_contracts.models`): `HealthReport`, `SystemInfo`, `DiskInfo`, `NetworkInfo`, `ContainerCounts`, `ServiceState = Literal["running","stopped","failed","unknown"]`, `TAILSCALE_NETWORK: IPv4Network`, `CURRENT_SCHEMA_VERSION = 1`, `SUPPORTED_SCHEMA_VERSIONS`.
- Produces (`edgeio_contracts.samples`): `sample_payload() -> dict[str, Any]`, `set_path(payload, path: str, value) -> dict` (dotted path; `DELETE` sentinel removes the key), `sample_report(changes: Mapping[str, Any] | None = None) -> HealthReport`.

- [ ] **Step 1: Create the workspace files**

`pyproject.toml`:
```toml
[project]
name = "edgeio"
version = "0.1.0"
description = "Simulated edge device health monitoring platform"
requires-python = ">=3.12"
dependencies = ["edgeio-contracts"]

[dependency-groups]
dev = [
    "pytest>=8.3",
    "ruff>=0.6",
    "mypy>=1.12",
    "jsonschema>=4.23",
    "testcontainers[kafka,postgres]>=4.8",
]

[tool.uv]
package = false

[tool.uv.workspace]
members = ["contracts"]

[tool.uv.sources]
edgeio-contracts = { workspace = true }

[tool.pytest.ini_options]
addopts = "--import-mode=importlib"
testpaths = ["contracts/tests"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]

[tool.mypy]
strict = true
plugins = ["pydantic.mypy"]

[[tool.mypy.overrides]]
module = ["confluent_kafka.*", "testcontainers.*"]
ignore_missing_imports = true
```

`.python-version`:
```
3.12
```

`.gitignore`:
```
.venv/
__pycache__/
*.pyc
.mypy_cache/
.ruff_cache/
.pytest_cache/
node_modules/
dist/
```

`contracts/pyproject.toml`:
```toml
[project]
name = "edgeio-contracts"
version = "0.1.0"
description = "device.health wire-format models (single source of truth)"
requires-python = ">=3.12"
dependencies = ["pydantic>=2.8"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

`contracts/edgeio_contracts/__init__.py`:
```python
"""Wire-format contract for the device.health Kafka topic."""
```

`contracts/edgeio_contracts/py.typed`: empty file.

`contracts/fixtures/valid/basic.json`:
```json
{
  "schema_version": 1,
  "device_id": "100.101.12.7",
  "timestamp": "2026-10-06T09:45:00Z",
  "system": {
    "hostname": "edge-001",
    "os": "Ubuntu 24.04",
    "uptime_seconds": 382941,
    "cpu_usage_percent": 43.7,
    "cpu_temperature_c": 57.2,
    "load_1m": 1.42,
    "ram_total_mb": 16384,
    "ram_used_mb": 9271,
    "ram_usage_percent": 56.6
  },
  "disk": {
    "root_total_gb": 476,
    "root_used_gb": 291,
    "root_free_gb": 185,
    "root_usage_percent": 61.1
  },
  "network": {
    "interface": "eth0",
    "rx_bytes": 482938192,
    "tx_bytes": 182938291,
    "packet_loss_percent": 0.0
  },
  "services": {
    "docker": "running",
    "postgresql": "running",
    "edge_streamer": "running"
  },
  "containers": { "running": 5, "stopped": 1 }
}
```

- [ ] **Step 2: Write the failing tests**

`contracts/tests/test_models.py`:
```python
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from edgeio_contracts.models import HealthReport
from edgeio_contracts.samples import DELETE, sample_payload, sample_report, set_path

FIXTURES = Path(__file__).parents[1] / "fixtures"


def test_sample_payload_matches_fixture_file() -> None:
    assert json.loads((FIXTURES / "valid" / "basic.json").read_text()) == sample_payload()


@pytest.mark.parametrize(
    "path", sorted((FIXTURES / "valid").glob("*.json")), ids=lambda p: p.stem
)
def test_valid_fixtures_parse(path: Path) -> None:
    HealthReport.model_validate_json(path.read_bytes())


def test_sample_report_fields() -> None:
    report = sample_report()
    assert str(report.device_id) == "100.101.12.7"
    assert report.system.hostname == "edge-001"
    assert report.services["edge_streamer"] == "running"
    assert report.timestamp == datetime(2026, 10, 6, 9, 45, tzinfo=UTC)


def test_timestamp_with_offset_is_normalized_to_utc() -> None:
    report = sample_report({"timestamp": "2026-10-06T15:15:00+05:30"})
    assert report.timestamp == datetime(2026, 10, 6, 9, 45, tzinfo=UTC)
    assert report.timestamp.utcoffset() == timedelta(0)


def test_extra_service_keys_are_allowed() -> None:
    report = sample_report({"services.nginx": "stopped"})
    assert report.services["nginx"] == "stopped"


def test_round_trip_serialization() -> None:
    dumped = json.loads(sample_report().model_dump_json())
    assert dumped == sample_payload()
    assert dumped["timestamp"] == "2026-10-06T09:45:00Z"


INVALID_MUTATIONS: list[tuple[str, str, Any]] = [
    ("device_id_outside_tailscale", "device_id", "192.168.1.10"),
    ("device_id_not_ip", "device_id", "edge-001"),
    ("cpu_percent_over_100", "system.cpu_usage_percent", 143.7),
    ("negative_uptime", "system.uptime_seconds", -1),
    ("fractional_uptime", "system.uptime_seconds", 1.5),
    ("temperature_out_of_range", "system.cpu_temperature_c", 150.0),
    ("ram_used_exceeds_total", "system.ram_used_mb", 20000),
    ("disk_sizes_inconsistent", "disk.root_free_gb", 100),
    ("disk_used_exceeds_total", "disk.root_used_gb", 500),
    ("negative_rx_bytes", "network.rx_bytes", -5),
    ("packet_loss_over_100", "network.packet_loss_percent", 101.0),
    ("unknown_service_state", "services.docker", "crashed"),
    ("negative_containers", "containers.running", -1),
    ("naive_timestamp", "timestamp", "2026-10-06T09:45:00"),
    ("unsupported_schema_version", "schema_version", 2),
    ("unknown_top_level_field", "gpu", {"usage": 3}),
    ("unknown_nested_field", "system.kernel", "6.8"),
    ("missing_section", "disk", DELETE),
    ("missing_field", "system.hostname", DELETE),
    ("empty_hostname", "system.hostname", ""),
]


@pytest.mark.parametrize(
    ("path", "value"),
    [(p, v) for _, p, v in INVALID_MUTATIONS],
    ids=[name for name, _, _ in INVALID_MUTATIONS],
)
def test_invalid_payloads_are_rejected(path: str, value: Any) -> None:
    payload = set_path(sample_payload(), path, value)
    with pytest.raises(ValidationError):
        HealthReport.model_validate(payload)
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv sync && uv run pytest contracts/tests/test_models.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_contracts.models'`

- [ ] **Step 4: Implement the models**

`contracts/edgeio_contracts/models.py`:
```python
"""Wire-format models for the device.health topic (schema_version 1)."""

from datetime import UTC, datetime
from ipaddress import IPv4Address, IPv4Network
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

TAILSCALE_NETWORK = IPv4Network("100.64.0.0/10")
CURRENT_SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = frozenset({1})
DISK_SUM_TOLERANCE_GB = 1.0

ServiceState = Literal["running", "stopped", "failed", "unknown"]
Percent = Annotated[float, Field(ge=0, le=100)]
NonNegativeInt = Annotated[int, Field(ge=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]


class _ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SystemInfo(_ContractModel):
    hostname: str = Field(min_length=1, max_length=253)
    os: str = Field(min_length=1)
    uptime_seconds: NonNegativeInt
    cpu_usage_percent: Percent
    cpu_temperature_c: float = Field(ge=-40, le=125)
    load_1m: NonNegativeFloat
    ram_total_mb: int = Field(gt=0)
    ram_used_mb: NonNegativeInt
    ram_usage_percent: Percent

    @model_validator(mode="after")
    def _ram_used_within_total(self) -> Self:
        if self.ram_used_mb > self.ram_total_mb:
            raise ValueError("ram_used_mb exceeds ram_total_mb")
        return self


class DiskInfo(_ContractModel):
    root_total_gb: float = Field(gt=0)
    root_used_gb: NonNegativeFloat
    root_free_gb: NonNegativeFloat
    root_usage_percent: Percent

    @model_validator(mode="after")
    def _consistent_sizes(self) -> Self:
        if self.root_used_gb > self.root_total_gb:
            raise ValueError("root_used_gb exceeds root_total_gb")
        if abs(self.root_used_gb + self.root_free_gb - self.root_total_gb) > DISK_SUM_TOLERANCE_GB:
            raise ValueError("root_used_gb + root_free_gb must equal root_total_gb (within 1 GB)")
        return self


class NetworkInfo(_ContractModel):
    interface: str = Field(min_length=1)
    rx_bytes: NonNegativeInt
    tx_bytes: NonNegativeInt
    packet_loss_percent: Percent


class ContainerCounts(_ContractModel):
    running: NonNegativeInt
    stopped: NonNegativeInt


class HealthReport(_ContractModel):
    """Health report published by an edge device to the device.health topic."""

    schema_version: int
    device_id: IPv4Address
    timestamp: AwareDatetime
    system: SystemInfo
    disk: DiskInfo
    network: NetworkInfo
    services: dict[str, ServiceState]
    containers: ContainerCounts

    @field_validator("schema_version")
    @classmethod
    def _supported_version(cls, v: int) -> int:
        if v not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(f"unsupported schema_version {v}")
        return v

    @field_validator("device_id")
    @classmethod
    def _tailscale_address(cls, v: IPv4Address) -> IPv4Address:
        if v not in TAILSCALE_NETWORK:
            raise ValueError(f"device_id {v} is not a Tailscale address (100.64.0.0/10)")
        return v

    @field_validator("timestamp")
    @classmethod
    def _to_utc(cls, v: datetime) -> datetime:
        return v.astimezone(UTC)
```

`contracts/edgeio_contracts/samples.py`:
```python
"""Canonical example payload and helpers for building variations in tests."""

import copy
from collections.abc import Mapping
from typing import Any, Final

from .models import HealthReport

DELETE: Final = object()

_SAMPLE: Final[dict[str, Any]] = {
    "schema_version": 1,
    "device_id": "100.101.12.7",
    "timestamp": "2026-10-06T09:45:00Z",
    "system": {
        "hostname": "edge-001",
        "os": "Ubuntu 24.04",
        "uptime_seconds": 382941,
        "cpu_usage_percent": 43.7,
        "cpu_temperature_c": 57.2,
        "load_1m": 1.42,
        "ram_total_mb": 16384,
        "ram_used_mb": 9271,
        "ram_usage_percent": 56.6,
    },
    "disk": {
        "root_total_gb": 476,
        "root_used_gb": 291,
        "root_free_gb": 185,
        "root_usage_percent": 61.1,
    },
    "network": {
        "interface": "eth0",
        "rx_bytes": 482938192,
        "tx_bytes": 182938291,
        "packet_loss_percent": 0.0,
    },
    "services": {"docker": "running", "postgresql": "running", "edge_streamer": "running"},
    "containers": {"running": 5, "stopped": 1},
}


def sample_payload() -> dict[str, Any]:
    """A fresh deep copy of the canonical example payload."""
    return copy.deepcopy(_SAMPLE)


def set_path(payload: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    """Set (or with DELETE, remove) a dotted path like 'system.cpu_usage_percent'."""
    *parents, leaf = path.split(".")
    node = payload
    for key in parents:
        node = node[key]
    if value is DELETE:
        del node[leaf]
    else:
        node[leaf] = value
    return payload


def sample_report(changes: Mapping[str, Any] | None = None) -> HealthReport:
    """Validated HealthReport from the sample with dotted-path changes applied."""
    payload = sample_payload()
    for path, value in (changes or {}).items():
        set_path(payload, path, value)
    return HealthReport.model_validate(payload)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest contracts/tests/test_models.py -v`
Expected: PASS (all tests, including 20 parametrized invalid cases)

- [ ] **Step 6: Update CLAUDE.md §13**

In `CLAUDE.md`, replace:
```
- **Unit (pytest):** simulator state transitions (monotonic counters, uptime, fault lifecycles, payload always valid unless malformed injected); contract validation using fixture files in `contracts/fixtures/{valid,invalid}/`; transform + rate derivation (incl. counter reset); alert rule evaluation incl. hysteresis and open/resolve transitions.
```
with:
```
- **Unit (pytest):** simulator state transitions (monotonic counters, uptime, fault lifecycles, payload always valid unless malformed injected); contract validation — valid example payloads in `contracts/fixtures/valid/`, invalid cases as a mutation table over `edgeio_contracts.samples.sample_payload()`; transform + rate derivation (incl. counter reset); alert rule evaluation incl. hysteresis and open/resolve transitions.
```
Also in §3, replace `    fixtures/           #   valid/ and invalid/ example payloads for tests` with `    fixtures/valid/     #   example payloads (invalid cases are generated in tests)`.

- [ ] **Step 7: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy contracts/edgeio_contracts`
Expected: no errors

```bash
git add pyproject.toml uv.lock .python-version .gitignore contracts CLAUDE.md
git commit -m "feat(contracts): add health report wire models and samples"
```

---

### Task 2: Contract validation entry point and JSON Schema export

**Files:**
- Create: `contracts/edgeio_contracts/validation.py`, `contracts/edgeio_contracts/export_schema.py`
- Create (generated): `contracts/health.schema.json`
- Test: `contracts/tests/test_validation.py`, `contracts/tests/test_schema.py`

**Interfaces:**
- Consumes: `HealthReport`, `sample_payload`, `set_path` (Task 1).
- Produces (`edgeio_contracts.validation`): `ErrorStage = Literal["decode","schema","semantic"]`; `class ContractError(Exception)` with attributes `.stage: ErrorStage`, `.message: str`; `validate_report(raw: bytes | str | None, now: datetime) -> HealthReport`; `MAX_CLOCK_SKEW = timedelta(minutes=10)`.
- Produces (`edgeio_contracts.export_schema`): `health_json_schema() -> dict[str, Any]`.

- [ ] **Step 1: Write the failing tests**

`contracts/tests/test_validation.py`:
```python
import json
from datetime import UTC, datetime, timedelta

import pytest

from edgeio_contracts.samples import sample_payload, set_path
from edgeio_contracts.validation import ContractError, validate_report

NOW = datetime(2026, 10, 6, 9, 50, tzinfo=UTC)


def encode(payload: dict) -> bytes:
    return json.dumps(payload).encode()


def test_valid_payload_returns_report() -> None:
    report = validate_report(encode(sample_payload()), NOW)
    assert report.system.hostname == "edge-001"


def test_garbage_bytes_fail_at_decode_stage() -> None:
    with pytest.raises(ContractError) as exc:
        validate_report(b"{not json", NOW)
    assert exc.value.stage == "decode"


def test_missing_value_fails_at_decode_stage() -> None:
    with pytest.raises(ContractError) as exc:
        validate_report(None, NOW)
    assert exc.value.stage == "decode"


def test_schema_violation_names_the_field() -> None:
    payload = set_path(sample_payload(), "system.cpu_usage_percent", 143.7)
    with pytest.raises(ContractError) as exc:
        validate_report(encode(payload), NOW)
    assert exc.value.stage == "schema"
    assert "system.cpu_usage_percent" in exc.value.message


def test_json_that_is_not_an_object_fails_schema() -> None:
    with pytest.raises(ContractError) as exc:
        validate_report(b"[]", NOW)
    assert exc.value.stage == "schema"


def test_timestamp_more_than_ten_minutes_ahead_is_rejected() -> None:
    future = (NOW + timedelta(minutes=11)).isoformat()
    payload = set_path(sample_payload(), "timestamp", future)
    with pytest.raises(ContractError) as exc:
        validate_report(encode(payload), NOW)
    assert exc.value.stage == "semantic"


def test_small_clock_skew_is_accepted() -> None:
    ahead = (NOW + timedelta(minutes=9)).isoformat()
    payload = set_path(sample_payload(), "timestamp", ahead)
    assert validate_report(encode(payload), NOW).timestamp == NOW + timedelta(minutes=9)
```

`contracts/tests/test_schema.py`:
```python
import json
from pathlib import Path

import jsonschema
import pytest

from edgeio_contracts.export_schema import health_json_schema
from edgeio_contracts.samples import sample_payload, set_path

SCHEMA_FILE = Path(__file__).parents[1] / "health.schema.json"


def test_committed_schema_is_up_to_date() -> None:
    committed = json.loads(SCHEMA_FILE.read_text())
    assert committed == health_json_schema(), "run `make schema` to regenerate"


def test_schema_accepts_sample() -> None:
    jsonschema.Draft202012Validator(health_json_schema()).validate(sample_payload())


def test_schema_rejects_out_of_range_percent() -> None:
    payload = set_path(sample_payload(), "system.cpu_usage_percent", 143.7)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(health_json_schema()).validate(payload)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest contracts/tests/test_validation.py contracts/tests/test_schema.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_contracts.validation'`

- [ ] **Step 3: Implement validation and schema export**

`contracts/edgeio_contracts/validation.py`:
```python
"""Single entry point consumers use to accept or reject a raw message."""

from datetime import datetime, timedelta
from typing import Literal

from pydantic import ValidationError

from .models import HealthReport

ErrorStage = Literal["decode", "schema", "semantic"]
MAX_CLOCK_SKEW = timedelta(minutes=10)


class ContractError(Exception):
    """A payload that violates the device.health contract."""

    def __init__(self, stage: ErrorStage, message: str) -> None:
        super().__init__(message)
        self.stage: ErrorStage = stage
        self.message = message


def validate_report(raw: bytes | str | None, now: datetime) -> HealthReport:
    if raw is None:
        raise ContractError("decode", "message has no value")
    try:
        report = HealthReport.model_validate_json(raw)
    except ValidationError as exc:
        stage: ErrorStage = (
            "decode" if any(err["type"] == "json_invalid" for err in exc.errors()) else "schema"
        )
        raise ContractError(stage, _summarize(exc)) from exc
    if report.timestamp > now + MAX_CLOCK_SKEW:
        raise ContractError(
            "semantic",
            f"timestamp {report.timestamp.isoformat()} is more than 10 minutes in the future",
        )
    return report


def _summarize(exc: ValidationError, limit: int = 5) -> str:
    parts = []
    for err in exc.errors()[:limit]:
        location = ".".join(str(part) for part in err["loc"]) or "<root>"
        parts.append(f"{location}: {err['msg']}")
    return "; ".join(parts)
```

`contracts/edgeio_contracts/export_schema.py`:
```python
"""Export the HealthReport JSON Schema for non-Python producers (e.g. a real health.py)."""

import json
from typing import Any

from .models import HealthReport


def health_json_schema() -> dict[str, Any]:
    schema = HealthReport.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://edgeio.local/schemas/health.v1.json"
    schema["description"] = (
        "Health report published by an edge device to the device.health Kafka topic. "
        "Cross-field rules (Tailscale address range, RAM/disk consistency, clock skew) "
        "are enforced by the worker, not by this schema."
    )
    return schema


def main() -> None:
    print(json.dumps(health_json_schema(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Generate the schema file**

Run: `uv run python -m edgeio_contracts.export_schema > contracts/health.schema.json`
Expected: file created; `head -5 contracts/health.schema.json` shows JSON with `"$defs"`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest contracts -v`
Expected: PASS

- [ ] **Step 6: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy contracts/edgeio_contracts`

```bash
git add contracts
git commit -m "feat(contracts): add validate_report and JSON Schema export"
```

---

### Task 3: Simulator package and fleet generation

**Files:**
- Create: `simulator/pyproject.toml`, `simulator/edgeio_simulator/__init__.py`, `simulator/edgeio_simulator/py.typed`
- Create: `simulator/edgeio_simulator/fleet.py`
- Modify: `pyproject.toml` (workspace member, dependency, source, testpaths)
- Test: `simulator/tests/test_fleet.py`

**Interfaces:**
- Consumes: `TAILSCALE_NETWORK` (Task 1).
- Produces (`edgeio_simulator.fleet`): `SERVICES: tuple[str, ...] = ("docker", "postgresql", "edge_streamer")`; frozen dataclass `DeviceProfile(index, device_id: IPv4Address, hostname, os, cpu_cores, ram_total_mb, disk_total_gb, interface, base_cpu_percent, base_ram_fraction, initial_disk_fraction, container_count)`; `tailscale_ip(index: int, seed: int) -> IPv4Address`; `make_profile(index: int, seed: int) -> DeviceProfile`; `generate_fleet(seed: int, count: int, offset: int = 0) -> list[DeviceProfile]`.

- [ ] **Step 1: Create the package and register it**

`simulator/pyproject.toml`:
```toml
[project]
name = "edgeio-simulator"
version = "0.1.0"
description = "Virtual edge device fleet publishing health reports to Kafka"
requires-python = ">=3.12"
dependencies = [
    "edgeio-contracts",
    "confluent-kafka>=2.5",
    "pydantic-settings>=2.4",
    "python-json-logger>=3.1",
]

[tool.uv.sources]
edgeio-contracts = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

`simulator/edgeio_simulator/__init__.py`:
```python
"""Simulated edge devices standing in for health.py."""
```

`simulator/edgeio_simulator/py.typed`: empty file.

In root `pyproject.toml` change:
```toml
dependencies = ["edgeio-contracts"]
```
to
```toml
dependencies = ["edgeio-contracts", "edgeio-simulator"]
```
change `members = ["contracts"]` to `members = ["contracts", "simulator"]`, add under `[tool.uv.sources]`:
```toml
edgeio-simulator = { workspace = true }
```
and change `testpaths = ["contracts/tests"]` to `testpaths = ["contracts/tests", "simulator/tests"]`.

Run: `uv sync`
Expected: installs `edgeio-simulator`, `confluent-kafka`, `pydantic-settings`, `python-json-logger`.

- [ ] **Step 2: Write the failing tests**

`simulator/tests/test_fleet.py`:
```python
from edgeio_contracts.models import TAILSCALE_NETWORK
from edgeio_simulator.fleet import generate_fleet, tailscale_ip


def test_fleet_is_deterministic_for_a_seed() -> None:
    assert generate_fleet(seed=42, count=50) == generate_fleet(seed=42, count=50)


def test_different_seeds_give_different_addresses() -> None:
    a = {p.device_id for p in generate_fleet(seed=1, count=50)}
    b = {p.device_id for p in generate_fleet(seed=2, count=50)}
    assert a != b


def test_addresses_are_unique_tailscale_ips() -> None:
    ips = [tailscale_ip(i, seed=42) for i in range(2000)]
    assert len(set(ips)) == 2000
    assert all(ip in TAILSCALE_NETWORK for ip in ips)
    assert TAILSCALE_NETWORK.network_address not in ips
    assert TAILSCALE_NETWORK.broadcast_address not in ips


def test_hostnames_are_numbered_from_one() -> None:
    fleet = generate_fleet(seed=42, count=50)
    assert fleet[0].hostname == "edge-001"
    assert fleet[49].hostname == "edge-050"


def test_offset_shards_the_same_fleet() -> None:
    shard = generate_fleet(seed=42, count=3, offset=10)
    full = generate_fleet(seed=42, count=13)
    assert shard == full[10:13]
    assert shard[0].hostname == "edge-011"


def test_profiles_have_sane_hardware() -> None:
    for p in generate_fleet(seed=42, count=50):
        assert p.ram_total_mb in (4096, 8192, 16384, 32768)
        assert p.disk_total_gb in (128, 256, 476, 953)
        assert p.os in ("Ubuntu 24.04", "Ubuntu 22.04")
        assert 0.3 <= p.initial_disk_fraction <= 0.7
        assert 3 <= p.container_count <= 8
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest simulator/tests/test_fleet.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_simulator.fleet'`

- [ ] **Step 4: Implement fleet generation**

`simulator/edgeio_simulator/fleet.py`:
```python
"""Deterministic generation of the simulated device fleet."""

import random
from dataclasses import dataclass
from ipaddress import IPv4Address

from edgeio_contracts.models import TAILSCALE_NETWORK

SERVICES: tuple[str, ...] = ("docker", "postgresql", "edge_streamer")

# Knuth's multiplicative hash constant. It is prime, so it is coprime with the number of
# usable addresses and (index * multiplier) mod usable is a bijection: no IP collisions.
_HASH_MULTIPLIER = 2654435761


@dataclass(frozen=True)
class DeviceProfile:
    index: int
    device_id: IPv4Address
    hostname: str
    os: str
    cpu_cores: int
    ram_total_mb: int
    disk_total_gb: int
    interface: str
    base_cpu_percent: float
    base_ram_fraction: float
    initial_disk_fraction: float
    container_count: int


def tailscale_ip(index: int, seed: int) -> IPv4Address:
    """A random-looking but unique address in 100.64.0.0/10 for each device index."""
    usable = TAILSCALE_NETWORK.num_addresses - 2  # skip network and broadcast addresses
    offset = (index * _HASH_MULTIPLIER + seed) % usable
    return IPv4Address(int(TAILSCALE_NETWORK.network_address) + 1 + offset)


def make_profile(index: int, seed: int) -> DeviceProfile:
    rng = random.Random(f"{seed}:{index}")
    return DeviceProfile(
        index=index,
        device_id=tailscale_ip(index, seed),
        hostname=f"edge-{index + 1:03d}",
        os=rng.choices(["Ubuntu 24.04", "Ubuntu 22.04"], weights=[4, 1])[0],
        cpu_cores=rng.choice([4, 8]),
        ram_total_mb=rng.choice([4096, 8192, 16384, 32768]),
        disk_total_gb=rng.choice([128, 256, 476, 953]),
        interface=rng.choices(["eth0", "wlan0"], weights=[6, 1])[0],
        base_cpu_percent=round(rng.uniform(10, 45), 1),
        base_ram_fraction=rng.uniform(0.3, 0.6),
        initial_disk_fraction=rng.uniform(0.3, 0.7),
        container_count=rng.randint(3, 8),
    )


def generate_fleet(seed: int, count: int, offset: int = 0) -> list[DeviceProfile]:
    return [make_profile(offset + i, seed) for i in range(count)]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest simulator/tests/test_fleet.py -v`
Expected: PASS

- [ ] **Step 6: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy contracts/edgeio_contracts simulator/edgeio_simulator`

```bash
git add pyproject.toml uv.lock simulator
git commit -m "feat(simulator): deterministic fleet generation with unique Tailscale IPs"
```

---

### Task 4: Fault lifecycle

**Files:**
- Create: `simulator/edgeio_simulator/faults.py`
- Test: `simulator/tests/test_faults.py`

**Interfaces:**
- Consumes: `DeviceProfile`, `SERVICES` (Task 3).
- Produces (`edgeio_simulator.faults`): `class Fault(StrEnum)` with members `OVERHEAT, DISK_FILL, MEMORY_LEAK, PACKET_LOSS, SERVICE_CRASH, CONTAINER_CRASH, OFFLINE, REBOOT`; frozen dataclass `ActiveFault(kind: Fault, ticks_left: int, target: str | None = None, magnitude: int = 0)`; `start_fault(kind, rng, profile) -> ActiveFault`; `next_fault(current: ActiveFault | None, rng, fault_rate: float, profile) -> ActiveFault | None` — returns the fault in effect for the coming tick. A fault created with `ticks_left=N` applies to exactly N ticks.

- [ ] **Step 1: Write the failing tests**

`simulator/tests/test_faults.py`:
```python
import random

from edgeio_simulator.faults import ActiveFault, Fault, next_fault, start_fault
from edgeio_simulator.fleet import SERVICES, make_profile

PROFILE = make_profile(0, seed=7)


def test_no_fault_starts_when_rate_is_zero() -> None:
    rng = random.Random(1)
    assert all(next_fault(None, rng, 0.0, PROFILE) is None for _ in range(1000))


def test_fault_starts_when_rate_is_one() -> None:
    fault = next_fault(None, random.Random(1), 1.0, PROFILE)
    assert fault is not None
    assert fault.ticks_left >= 1


def test_fault_counts_down_then_ends() -> None:
    fault: ActiveFault | None = ActiveFault(Fault.OVERHEAT, ticks_left=3)
    rng = random.Random(1)
    seen = []
    for _ in range(3):
        fault = next_fault(fault, rng, 1.0, PROFILE)
        seen.append(fault.ticks_left if fault else None)
    assert seen == [2, 1, None]


def test_reboot_lasts_one_tick() -> None:
    assert start_fault(Fault.REBOOT, random.Random(1), PROFILE).ticks_left == 1


def test_service_crash_targets_a_known_service() -> None:
    for seed in range(20):
        fault = start_fault(Fault.SERVICE_CRASH, random.Random(seed), PROFILE)
        assert fault.target in SERVICES


def test_container_crash_magnitude_fits_the_device() -> None:
    for seed in range(20):
        fault = start_fault(Fault.CONTAINER_CRASH, random.Random(seed), PROFILE)
        assert 1 <= fault.magnitude <= min(2, PROFILE.container_count)


def test_offline_can_outlast_the_offline_alert_window() -> None:
    durations = {start_fault(Fault.OFFLINE, random.Random(s), PROFILE).ticks_left for s in range(200)}
    assert max(durations) >= 4  # > 15 min at 300 s per tick
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest simulator/tests/test_faults.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_simulator.faults'`

- [ ] **Step 3: Implement the fault lifecycle**

`simulator/edgeio_simulator/faults.py`:
```python
"""Fault scenarios a simulated device can fall into, and how long they last."""

import random
from dataclasses import dataclass, replace
from enum import StrEnum

from .fleet import SERVICES, DeviceProfile


class Fault(StrEnum):
    OVERHEAT = "overheat"
    DISK_FILL = "disk_fill"
    MEMORY_LEAK = "memory_leak"
    PACKET_LOSS = "packet_loss"
    SERVICE_CRASH = "service_crash"
    CONTAINER_CRASH = "container_crash"
    OFFLINE = "offline"
    REBOOT = "reboot"


# Inclusive (min, max) duration in ticks.
FAULT_DURATION_TICKS: dict[Fault, tuple[int, int]] = {
    Fault.OVERHEAT: (3, 8),
    Fault.DISK_FILL: (4, 10),
    Fault.MEMORY_LEAK: (4, 10),
    Fault.PACKET_LOSS: (2, 6),
    Fault.SERVICE_CRASH: (2, 6),
    Fault.CONTAINER_CRASH: (2, 6),
    Fault.OFFLINE: (2, 8),
    Fault.REBOOT: (1, 1),
}


@dataclass(frozen=True)
class ActiveFault:
    kind: Fault
    ticks_left: int  # ticks this fault still applies to, including the current one
    target: str | None = None  # crashed service name (SERVICE_CRASH)
    magnitude: int = 0  # number of crashed containers (CONTAINER_CRASH)


def start_fault(kind: Fault, rng: random.Random, profile: DeviceProfile) -> ActiveFault:
    low, high = FAULT_DURATION_TICKS[kind]
    target = rng.choice(SERVICES) if kind is Fault.SERVICE_CRASH else None
    magnitude = (
        rng.randint(1, min(2, profile.container_count)) if kind is Fault.CONTAINER_CRASH else 0
    )
    return ActiveFault(kind, rng.randint(low, high), target, magnitude)


def next_fault(
    current: ActiveFault | None, rng: random.Random, fault_rate: float, profile: DeviceProfile
) -> ActiveFault | None:
    """The fault in effect for the coming tick."""
    if current is not None:
        return replace(current, ticks_left=current.ticks_left - 1) if current.ticks_left > 1 else None
    if rng.random() < fault_rate:
        return start_fault(rng.choice(list(Fault)), rng, profile)
    return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest simulator/tests/test_faults.py -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy simulator/edgeio_simulator`

```bash
git add simulator
git commit -m "feat(simulator): fault scenarios with bounded durations"
```

---

### Task 5: Stateful device model and malformed payloads

**Files:**
- Create: `simulator/edgeio_simulator/device.py`, `simulator/edgeio_simulator/malformed.py`
- Test: `simulator/tests/test_device.py`, `simulator/tests/test_malformed.py`

**Interfaces:**
- Consumes: `DeviceProfile`, `SERVICES`, `make_profile` (Task 3); `Fault`, `ActiveFault`, `next_fault` (Task 4); `HealthReport` & nested models, `ServiceState`, `CURRENT_SCHEMA_VERSION` (Task 1); `validate_report`, `ContractError` (Task 2).
- Produces (`edgeio_simulator.device`): frozen dataclass `DeviceState(boot_time: datetime, rx_bytes: int, tx_bytes: int, disk_used_gb: float, leaked_ram_mb: float, fault: ActiveFault | None)`; `initial_state(profile, now, rng) -> DeviceState`; `step(profile, state, now: datetime, rng, fault_rate: float) -> tuple[DeviceState, HealthReport | None]` (`None` = device offline this tick).
- Produces (`edgeio_simulator.malformed`): `MALFORMATIONS: tuple[str, ...]`; `malform(report: HealthReport, kind: str) -> bytes`.

- [ ] **Step 1: Write the failing tests**

`simulator/tests/test_device.py`:
```python
import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from edgeio_contracts.models import HealthReport
from edgeio_contracts.validation import validate_report
from edgeio_simulator.device import DeviceState, initial_state, step
from edgeio_simulator.faults import ActiveFault, Fault
from edgeio_simulator.fleet import make_profile

PROFILE = make_profile(0, seed=7)
START = datetime(2026, 10, 6, 0, 0, tzinfo=UTC)
TICK = timedelta(seconds=300)


def fresh() -> tuple[DeviceState, random.Random]:
    rng = random.Random(1)
    return initial_state(PROFILE, START, rng), rng


def with_fault(state: DeviceState, kind: Fault, ticks: int = 2, **kw: object) -> DeviceState:
    # ticks_left is decremented before use, so ticks=2 applies the fault to the next step.
    return replace(state, fault=ActiveFault(kind, ticks_left=ticks, **kw))  # type: ignore[arg-type]


def run(state: DeviceState, rng: random.Random, ticks: int, start: datetime = START):
    reports: list[HealthReport] = []
    for i in range(1, ticks + 1):
        state, report = step(PROFILE, state, start + i * TICK, rng, fault_rate=0.0)
        assert report is not None
        reports.append(report)
    return state, reports


def test_a_day_of_normal_operation_is_valid_and_consistent() -> None:
    state, rng = fresh()
    _, reports = run(state, rng, ticks=288)
    for prev, cur in zip(reports, reports[1:], strict=False):
        validate_report(cur.model_dump_json(), now=cur.timestamp)
        assert cur.network.rx_bytes > prev.network.rx_bytes
        assert cur.network.tx_bytes > prev.network.tx_bytes
        assert cur.system.uptime_seconds - prev.system.uptime_seconds == 300
        assert cur.disk.root_used_gb >= prev.disk.root_used_gb
        assert all(state == "running" for state in cur.services.values())
    assert reports[0].system.hostname == "edge-001"
    assert str(reports[0].device_id) == str(PROFILE.device_id)


def test_overheat_pushes_temperature_past_critical() -> None:
    state, rng = fresh()
    _, report = step(PROFILE, with_fault(state, Fault.OVERHEAT), START + TICK, rng, 0.0)
    assert report is not None and report.system.cpu_temperature_c > 85


def test_packet_loss_fault_exceeds_warning_threshold() -> None:
    state, rng = fresh()
    _, report = step(PROFILE, with_fault(state, Fault.PACKET_LOSS), START + TICK, rng, 0.0)
    assert report is not None and report.network.packet_loss_percent > 2


def test_service_crash_marks_target_failed() -> None:
    state, rng = fresh()
    faulty = with_fault(state, Fault.SERVICE_CRASH, target="edge_streamer")
    _, report = step(PROFILE, faulty, START + TICK, rng, 0.0)
    assert report is not None
    assert report.services["edge_streamer"] == "failed"
    assert report.services["docker"] == "running"


def test_container_crash_moves_containers_to_stopped() -> None:
    state, rng = fresh()
    faulty = with_fault(state, Fault.CONTAINER_CRASH, magnitude=2)
    _, report = step(PROFILE, faulty, START + TICK, rng, 0.0)
    assert report is not None
    assert report.containers.stopped == 2
    assert report.containers.running == PROFILE.container_count - 2


def test_reboot_resets_uptime_and_counters() -> None:
    state, rng = fresh()
    state, before = run(state, rng, ticks=1)
    _, after = step(PROFILE, with_fault(state, Fault.REBOOT), START + 2 * TICK, rng, 0.0)
    assert after is not None
    assert after.system.uptime_seconds < 300
    assert after.network.rx_bytes < before[0].network.rx_bytes


def test_offline_device_publishes_nothing_but_keeps_running() -> None:
    state, rng = fresh()
    new_state, report = step(PROFILE, with_fault(state, Fault.OFFLINE), START + TICK, rng, 0.0)
    assert report is None
    assert new_state.rx_bytes > state.rx_bytes


def test_memory_leak_grows_ram_usage() -> None:
    state, rng = fresh()
    state = with_fault(state, Fault.MEMORY_LEAK, ticks=9)
    usages = []
    for i in range(1, 9):
        state, report = step(PROFILE, state, START + i * TICK, rng, 0.0)
        assert report is not None
        usages.append(report.system.ram_usage_percent)
    assert usages[-1] >= usages[0] + 15


def test_disk_fill_grows_then_cleans_up() -> None:
    state, rng = fresh()
    state, report = step(PROFILE, with_fault(state, Fault.DISK_FILL), START + TICK, rng, 0.0)
    assert report is not None
    baseline = PROFILE.disk_total_gb * PROFILE.initial_disk_fraction
    assert state.disk_used_gb >= baseline + PROFILE.disk_total_gb * 0.03
    state, _ = step(PROFILE, state, START + 2 * TICK, rng, 0.0)  # fault ends this tick
    assert state.fault is None
    assert abs(state.disk_used_gb - baseline) <= 0.05
```

`simulator/tests/test_malformed.py`:
```python
from datetime import UTC, datetime

import pytest

from edgeio_contracts.samples import sample_report
from edgeio_contracts.validation import ContractError, validate_report
from edgeio_simulator.malformed import MALFORMATIONS, malform


@pytest.mark.parametrize("kind", MALFORMATIONS)
def test_every_malformation_is_rejected_by_the_contract(kind: str) -> None:
    raw = malform(sample_report(), kind)
    with pytest.raises(ContractError):
        validate_report(raw, now=datetime(2026, 10, 6, 10, 0, tzinfo=UTC))


def test_unknown_malformation_is_an_error() -> None:
    with pytest.raises(ValueError):
        malform(sample_report(), "nope")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest simulator/tests/test_device.py simulator/tests/test_malformed.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_simulator.device'`

- [ ] **Step 3: Implement the device model**

`simulator/edgeio_simulator/device.py`:
```python
"""Pure, stateful model of one edge device: (state, time, rng) -> (new state, report)."""

import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from edgeio_contracts.models import (
    CURRENT_SCHEMA_VERSION,
    ContainerCounts,
    DiskInfo,
    HealthReport,
    NetworkInfo,
    ServiceState,
    SystemInfo,
)

from .faults import ActiveFault, Fault, next_fault
from .fleet import SERVICES, DeviceProfile


@dataclass(frozen=True)
class DeviceState:
    boot_time: datetime
    rx_bytes: int
    tx_bytes: int
    disk_used_gb: float
    leaked_ram_mb: float
    fault: ActiveFault | None


def initial_state(profile: DeviceProfile, now: datetime, rng: random.Random) -> DeviceState:
    return DeviceState(
        boot_time=now - timedelta(seconds=rng.randint(3600, 30 * 86400)),
        rx_bytes=rng.randint(10**8, 10**10),
        tx_bytes=rng.randint(10**7, 10**9),
        disk_used_gb=profile.disk_total_gb * profile.initial_disk_fraction,
        leaked_ram_mb=0.0,
        fault=None,
    )


def step(
    profile: DeviceProfile,
    state: DeviceState,
    now: datetime,
    rng: random.Random,
    fault_rate: float,
) -> tuple[DeviceState, HealthReport | None]:
    fault = next_fault(state.fault, rng, fault_rate, profile)
    kind = fault.kind if fault else None
    ended = state.fault.kind if state.fault is not None and fault is None else None

    boot_time, rx, tx = state.boot_time, state.rx_bytes, state.tx_bytes
    if kind is Fault.REBOOT:
        boot_time, rx, tx = now - timedelta(seconds=rng.randint(30, 120)), 0, 0
    rx += rng.randint(50_000, 5_000_000)
    tx += rng.randint(10_000, 2_000_000)

    disk_used = state.disk_used_gb + rng.uniform(0.0, 0.02)
    if kind is Fault.DISK_FILL:
        disk_used += profile.disk_total_gb * 0.03
    if ended is Fault.DISK_FILL:
        disk_used = profile.disk_total_gb * profile.initial_disk_fraction  # cleanup job ran
    disk_used = min(disk_used, float(profile.disk_total_gb))

    leaked = (
        state.leaked_ram_mb + profile.ram_total_mb * 0.04 if kind is Fault.MEMORY_LEAK else 0.0
    )

    new_state = DeviceState(boot_time, rx, tx, disk_used, leaked, fault)
    if kind is Fault.OFFLINE:
        return new_state, None
    return new_state, _build_report(profile, new_state, now, rng)


def _diurnal(now: datetime) -> float:
    """0..1 daily load curve peaking at 14:00 UTC."""
    hour = now.hour + now.minute / 60
    return 0.5 + 0.5 * math.cos(2 * math.pi * (hour - 14) / 24)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _build_report(
    profile: DeviceProfile, state: DeviceState, now: datetime, rng: random.Random
) -> HealthReport:
    fault = state.fault
    kind = fault.kind if fault else None

    cpu = profile.base_cpu_percent + 25 * _diurnal(now) + rng.gauss(0, 5)
    if kind is Fault.OVERHEAT:
        cpu += 35
    cpu = _clamp(cpu, 0.5, 100.0)
    temp = 35 + 0.45 * cpu + rng.gauss(0, 1.5)
    if kind is Fault.OVERHEAT:
        temp = max(temp, rng.uniform(86, 98))
    load = max(0.0, cpu / 100 * profile.cpu_cores + rng.gauss(0, 0.2))

    ram_total = profile.ram_total_mb
    ram_base = ram_total * profile.base_ram_fraction * (1 + rng.gauss(0, 0.05))
    ram_used = int(_clamp(ram_base + state.leaked_ram_mb, 0, ram_total * 0.99))

    disk_total = float(profile.disk_total_gb)
    disk_used = round(state.disk_used_gb, 1)

    if kind is Fault.PACKET_LOSS:
        packet_loss = round(rng.uniform(5, 25), 1)
    else:
        packet_loss = round(rng.uniform(0, 0.5), 1) if rng.random() < 0.1 else 0.0

    services: dict[str, ServiceState] = {name: "running" for name in SERVICES}
    if kind is Fault.SERVICE_CRASH and fault is not None and fault.target is not None:
        services[fault.target] = "failed"
    stopped = fault.magnitude if kind is Fault.CONTAINER_CRASH and fault is not None else 0

    return HealthReport(
        schema_version=CURRENT_SCHEMA_VERSION,
        device_id=profile.device_id,
        timestamp=now,
        system=SystemInfo(
            hostname=profile.hostname,
            os=profile.os,
            uptime_seconds=int((now - state.boot_time).total_seconds()),
            cpu_usage_percent=round(cpu, 1),
            cpu_temperature_c=round(temp, 1),
            load_1m=round(load, 2),
            ram_total_mb=ram_total,
            ram_used_mb=ram_used,
            ram_usage_percent=round(ram_used / ram_total * 100, 1),
        ),
        disk=DiskInfo(
            root_total_gb=disk_total,
            root_used_gb=disk_used,
            root_free_gb=round(disk_total - disk_used, 1),
            root_usage_percent=round(disk_used / disk_total * 100, 1),
        ),
        network=NetworkInfo(
            interface=profile.interface,
            rx_bytes=state.rx_bytes,
            tx_bytes=state.tx_bytes,
            packet_loss_percent=packet_loss,
        ),
        services=services,
        containers=ContainerCounts(running=profile.container_count - stopped, stopped=stopped),
    )
```

`simulator/edgeio_simulator/malformed.py`:
```python
"""Deliberately invalid payloads that exercise the worker's dead-letter path."""

import json

from edgeio_contracts.models import HealthReport

MALFORMATIONS: tuple[str, ...] = (
    "cpu_out_of_range",
    "non_tailscale_device_id",
    "missing_disk",
    "wrong_type",
    "truncated_json",
)


def malform(report: HealthReport, kind: str) -> bytes:
    if kind == "truncated_json":
        return report.model_dump_json().encode()[:40]
    payload = report.model_dump(mode="json")
    if kind == "cpu_out_of_range":
        payload["system"]["cpu_usage_percent"] = 143.7
    elif kind == "non_tailscale_device_id":
        payload["device_id"] = "192.168.1.50"
    elif kind == "missing_disk":
        del payload["disk"]
    elif kind == "wrong_type":
        payload["system"]["uptime_seconds"] = "a long time"
    else:
        raise ValueError(f"unknown malformation {kind!r}")
    return json.dumps(payload).encode()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest simulator -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy simulator/edgeio_simulator`

```bash
git add simulator
git commit -m "feat(simulator): stateful device model with faults and malformed payloads"
```

---

### Task 6: Simulator runtime (asyncio + Kafka)

**Files:**
- Create: `simulator/edgeio_simulator/config.py`, `simulator/edgeio_simulator/logs.py`, `simulator/edgeio_simulator/runtime.py`, `simulator/edgeio_simulator/__main__.py`
- Test: `simulator/tests/test_runtime.py`

**Interfaces:**
- Consumes: `generate_fleet` (Task 3); `initial_state`, `step`, `MALFORMATIONS`, `malform` (Task 5).
- Produces (`edgeio_simulator.config`): `SimulatorSettings` with fields `kafka_bootstrap`, `kafka_topic`, `sim_device_count`, `sim_device_offset`, `sim_seed`, `sim_interval_seconds`, `sim_malformed_rate`, `sim_fault_rate`, `log_level` (env vars = upper-cased field names).
- Produces (`edgeio_simulator.runtime`): `MessageSink` Protocol (`produce(topic, *, key: bytes, value: bytes)`, `poll(timeout) -> int`); `encode(report, rng, malformed_rate) -> bytes`; `async run_fleet(settings, sink, stop: asyncio.Event) -> None`.
- Produces: `python -m edgeio_simulator` entry point.

- [ ] **Step 1: Write the failing tests**

`simulator/tests/test_runtime.py`:
```python
import asyncio
import random
from collections import Counter
from datetime import UTC, datetime

import pytest

from edgeio_contracts.samples import sample_report
from edgeio_contracts.validation import ContractError, validate_report
from edgeio_simulator.config import SimulatorSettings
from edgeio_simulator.runtime import encode, run_fleet


class FakeSink:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.polls = 0

    def produce(self, topic: str, *, key: bytes, value: bytes) -> None:
        self.messages.append((topic, key, value))

    def poll(self, timeout: float) -> int:
        self.polls += 1
        return 0


def settings(**overrides: object) -> SimulatorSettings:
    base: dict[str, object] = {
        "kafka_topic": "device.health",
        "sim_device_count": 3,
        "sim_interval_seconds": 0.1,
        "sim_malformed_rate": 0.0,
        "sim_fault_rate": 0.0,
    }
    base.update(overrides)
    return SimulatorSettings(**base)  # type: ignore[arg-type]


async def run_for(seconds: float, cfg: SimulatorSettings, sink: FakeSink) -> None:
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(seconds, stop.set)
    await asyncio.wait_for(run_fleet(cfg, sink, stop), timeout=seconds + 5)


def test_every_device_publishes_valid_reports_keyed_by_device_id() -> None:
    sink = FakeSink()
    asyncio.run(run_for(0.6, settings(), sink))
    per_key = Counter(key for _, key, _ in sink.messages)
    assert len(per_key) == 3
    assert all(count >= 3 for count in per_key.values())
    for topic, key, value in sink.messages:
        assert topic == "device.health"
        report = validate_report(value, now=datetime.now(UTC))
        assert str(report.device_id).encode() == key
    assert sink.polls >= 1


def test_stop_ends_the_fleet_promptly() -> None:
    sink = FakeSink()
    asyncio.run(run_for(0.05, settings(sim_interval_seconds=300), sink))
    assert sink.messages == []  # stopped before any device's first jittered tick (≥ 0.05 s likely)


def test_encode_with_full_malformed_rate_produces_invalid_bytes() -> None:
    raw = encode(sample_report(), random.Random(1), malformed_rate=1.0)
    with pytest.raises(ContractError):
        validate_report(raw, now=datetime(2026, 10, 6, 10, 0, tzinfo=UTC))


def test_encode_with_zero_malformed_rate_is_valid_json_of_the_report() -> None:
    report = sample_report()
    assert encode(report, random.Random(1), malformed_rate=0.0) == report.model_dump_json().encode()
```

Note on `test_stop_ends_the_fleet_promptly`: with a 300 s interval, first-tick jitter is uniform in [0, 300) s, so the chance any of 3 devices fires inside 0.05 s is negligible; the real assertion is that `run_fleet` returns within the `wait_for` timeout.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest simulator/tests/test_runtime.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_simulator.config'`

- [ ] **Step 3: Implement config, logging, runtime and entry point**

`simulator/edgeio_simulator/config.py`:
```python
from pydantic import Field
from pydantic_settings import BaseSettings


class SimulatorSettings(BaseSettings):
    """Read from environment variables named after each field, upper-cased."""

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "device.health"
    sim_device_count: int = Field(default=50, ge=1)
    sim_device_offset: int = Field(default=0, ge=0)
    sim_seed: int = 42
    sim_interval_seconds: float = Field(default=300.0, gt=0)
    sim_malformed_rate: float = Field(default=0.005, ge=0, le=1)
    sim_fault_rate: float = Field(default=0.005, ge=0, le=1)
    log_level: str = "INFO"
```

`simulator/edgeio_simulator/logs.py`:
```python
import logging

from pythonjsonlogger.json import JsonFormatter


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.basicConfig(level=level, handlers=[handler], force=True)
```

`simulator/edgeio_simulator/runtime.py`:
```python
"""Asyncio shell: runs every virtual device on its own schedule and publishes reports."""

import asyncio
import logging
import random
from datetime import UTC, datetime
from typing import Protocol

from edgeio_contracts.models import HealthReport

from .config import SimulatorSettings
from .device import initial_state, step
from .fleet import DeviceProfile, generate_fleet
from .malformed import MALFORMATIONS, malform

log = logging.getLogger(__name__)


class MessageSink(Protocol):
    def produce(self, topic: str, *, key: bytes, value: bytes) -> None: ...

    def poll(self, timeout: float) -> int: ...


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def encode(report: HealthReport, rng: random.Random, malformed_rate: float) -> bytes:
    if rng.random() < malformed_rate:
        return malform(report, rng.choice(MALFORMATIONS))
    return report.model_dump_json().encode()


async def _stopped_within(stop: asyncio.Event, seconds: float) -> bool:
    """Wait up to `seconds`; True if stop was requested meanwhile."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=max(0.0, seconds))
    except TimeoutError:
        return False
    return True


async def run_device(
    profile: DeviceProfile,
    settings: SimulatorSettings,
    sink: MessageSink,
    stop: asyncio.Event,
    rng: random.Random,
) -> None:
    loop = asyncio.get_running_loop()
    state = initial_state(profile, utc_now(), rng)
    # Random first tick so the fleet doesn't publish in lockstep.
    next_at = loop.time() + rng.uniform(0, settings.sim_interval_seconds)
    while not await _stopped_within(stop, next_at - loop.time()):
        state, report = step(profile, state, utc_now(), rng, settings.sim_fault_rate)
        if report is not None:
            sink.produce(
                settings.kafka_topic,
                key=str(profile.device_id).encode(),
                value=encode(report, rng, settings.sim_malformed_rate),
            )
        next_at += settings.sim_interval_seconds


async def _serve_delivery_callbacks(sink: MessageSink, stop: asyncio.Event) -> None:
    while True:
        sink.poll(0)
        if await _stopped_within(stop, 0.5):
            return


async def run_fleet(settings: SimulatorSettings, sink: MessageSink, stop: asyncio.Event) -> None:
    profiles = generate_fleet(settings.sim_seed, settings.sim_device_count, settings.sim_device_offset)
    log.info(
        "starting fleet",
        extra={"devices": len(profiles), "interval_seconds": settings.sim_interval_seconds},
    )
    async with asyncio.TaskGroup() as group:
        for profile in profiles:
            rng = random.Random(f"{settings.sim_seed}:{profile.index}:runtime")
            group.create_task(run_device(profile, settings, sink, stop, rng))
        group.create_task(_serve_delivery_callbacks(sink, stop))
```

`simulator/edgeio_simulator/__main__.py`:
```python
"""Entry point: python -m edgeio_simulator"""

import asyncio
import logging
import signal
from typing import Any

from confluent_kafka import Producer

from .config import SimulatorSettings
from .logs import configure_logging
from .runtime import run_fleet

log = logging.getLogger("edgeio_simulator")


def _on_delivery(err: Any, msg: Any) -> None:
    if err is not None:
        key = msg.key().decode(errors="replace") if msg.key() else None
        log.error("delivery failed", extra={"error": str(err), "device_id": key})


async def _run(settings: SimulatorSettings, producer: Any) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await run_fleet(settings, producer, stop)
    finally:
        remaining = producer.flush(10)
        if remaining:
            log.warning("undelivered messages at shutdown", extra={"count": remaining})


def main() -> None:
    settings = SimulatorSettings()
    configure_logging(settings.log_level)
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "on_delivery": _on_delivery,
        }
    )
    asyncio.run(_run(settings, producer))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest simulator -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy simulator/edgeio_simulator`

```bash
git add simulator
git commit -m "feat(simulator): asyncio fleet runtime publishing to Kafka"
```

---

### Task 7: Worker package and reading transform

**Files:**
- Create: `worker/pyproject.toml`, `worker/edgeio_worker/__init__.py`, `worker/edgeio_worker/py.typed`, `worker/edgeio_worker/transform.py`
- Modify: `pyproject.toml` (workspace member, dependency, source, testpaths)
- Test: `worker/tests/test_transform.py`

**Interfaces:**
- Consumes: `HealthReport`, `sample_report` (Task 1).
- Produces (`edgeio_worker.transform`): frozen dataclass `PreviousCounters(ts: datetime, rx_bytes: int, tx_bytes: int)`; frozen dataclass `ReadingRow` with fields `device_id: str, ts, hostname, os, uptime_seconds, cpu_usage_percent, cpu_temperature_c, load_1m, ram_total_mb, ram_used_mb, ram_usage_percent, disk_total_gb, disk_used_gb, disk_free_gb, disk_usage_percent, disk_free_percent, net_interface, rx_bytes, tx_bytes, rx_rate_bps: float | None, tx_rate_bps: float | None, packet_loss_percent, containers_running, containers_stopped, raw: dict[str, Any]`; `to_row(report, previous: PreviousCounters | None) -> ReadingRow`. Rates are **bits** per second.

- [ ] **Step 1: Create the package and register it**

`worker/pyproject.toml`:
```toml
[project]
name = "edgeio-worker"
version = "0.1.0"
description = "Kafka stream worker: validate, transform, store, alert"
requires-python = ">=3.12"
dependencies = [
    "edgeio-contracts",
    "confluent-kafka>=2.5",
    "psycopg[binary]>=3.2",
    "pydantic-settings>=2.4",
    "python-json-logger>=3.1",
]

[tool.uv.sources]
edgeio-contracts = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

`worker/edgeio_worker/__init__.py`:
```python
"""Stream worker for the device.health topic."""
```

`worker/edgeio_worker/py.typed`: empty file.

In root `pyproject.toml`: `dependencies = ["edgeio-contracts", "edgeio-simulator", "edgeio-worker"]`, `members = ["contracts", "simulator", "worker"]`, add `edgeio-worker = { workspace = true }` under `[tool.uv.sources]`, and `testpaths = ["contracts/tests", "simulator/tests", "worker/tests"]`.

Run: `uv sync`
Expected: installs `edgeio-worker` and `psycopg`.

- [ ] **Step 2: Write the failing tests**

`worker/tests/test_transform.py`:
```python
from datetime import timedelta

from edgeio_contracts.samples import sample_report
from edgeio_worker.transform import PreviousCounters, to_row

RX, TX = 482938192, 182938291  # sample counters


def test_maps_report_fields() -> None:
    report = sample_report()
    row = to_row(report, previous=None)
    assert row.device_id == "100.101.12.7"
    assert row.ts == report.timestamp
    assert (row.hostname, row.os) == ("edge-001", "Ubuntu 24.04")
    assert row.cpu_temperature_c == 57.2
    assert row.disk_usage_percent == 61.1
    assert row.net_interface == "eth0"
    assert (row.containers_running, row.containers_stopped) == (5, 1)
    assert row.raw["system"]["hostname"] == "edge-001"
    assert row.raw["timestamp"] == "2026-10-06T09:45:00Z"


def test_disk_free_percent_is_derived() -> None:
    assert to_row(sample_report(), None).disk_free_percent == round(185 / 476 * 100, 2)


def test_no_previous_reading_gives_no_rates() -> None:
    row = to_row(sample_report(), None)
    assert row.rx_rate_bps is None and row.tx_rate_bps is None


def test_rates_are_bits_per_second() -> None:
    report = sample_report()
    previous = PreviousCounters(
        ts=report.timestamp - timedelta(seconds=300), rx_bytes=RX - 3_000_000, tx_bytes=TX - 300_000
    )
    row = to_row(report, previous)
    assert row.rx_rate_bps == 80_000.0
    assert row.tx_rate_bps == 8_000.0


def test_counter_reset_after_reboot_gives_no_rates() -> None:
    report = sample_report()
    previous = PreviousCounters(
        ts=report.timestamp - timedelta(seconds=300), rx_bytes=RX + 1, tx_bytes=TX - 1
    )
    row = to_row(report, previous)
    assert row.rx_rate_bps is None and row.tx_rate_bps is None


def test_non_increasing_timestamp_gives_no_rates() -> None:
    report = sample_report()
    previous = PreviousCounters(ts=report.timestamp, rx_bytes=RX - 10, tx_bytes=TX - 10)
    row = to_row(report, previous)
    assert row.rx_rate_bps is None and row.tx_rate_bps is None
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest worker/tests/test_transform.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_worker.transform'`

- [ ] **Step 4: Implement the transform**

`worker/edgeio_worker/transform.py`:
```python
"""Flatten a validated HealthReport into a DB row and derive computed fields."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from edgeio_contracts.models import HealthReport


@dataclass(frozen=True)
class PreviousCounters:
    ts: datetime
    rx_bytes: int
    tx_bytes: int


@dataclass(frozen=True)
class ReadingRow:
    device_id: str
    ts: datetime
    hostname: str
    os: str
    uptime_seconds: int
    cpu_usage_percent: float
    cpu_temperature_c: float
    load_1m: float
    ram_total_mb: int
    ram_used_mb: int
    ram_usage_percent: float
    disk_total_gb: float
    disk_used_gb: float
    disk_free_gb: float
    disk_usage_percent: float
    disk_free_percent: float
    net_interface: str
    rx_bytes: int
    tx_bytes: int
    rx_rate_bps: float | None
    tx_rate_bps: float | None
    packet_loss_percent: float
    containers_running: int
    containers_stopped: int
    raw: dict[str, Any]


def to_row(report: HealthReport, previous: PreviousCounters | None) -> ReadingRow:
    rx_rate, tx_rate = _rates(report, previous)
    system, disk, network = report.system, report.disk, report.network
    return ReadingRow(
        device_id=str(report.device_id),
        ts=report.timestamp,
        hostname=system.hostname,
        os=system.os,
        uptime_seconds=system.uptime_seconds,
        cpu_usage_percent=system.cpu_usage_percent,
        cpu_temperature_c=system.cpu_temperature_c,
        load_1m=system.load_1m,
        ram_total_mb=system.ram_total_mb,
        ram_used_mb=system.ram_used_mb,
        ram_usage_percent=system.ram_usage_percent,
        disk_total_gb=disk.root_total_gb,
        disk_used_gb=disk.root_used_gb,
        disk_free_gb=disk.root_free_gb,
        disk_usage_percent=disk.root_usage_percent,
        disk_free_percent=round(disk.root_free_gb / disk.root_total_gb * 100, 2),
        net_interface=network.interface,
        rx_bytes=network.rx_bytes,
        tx_bytes=network.tx_bytes,
        rx_rate_bps=rx_rate,
        tx_rate_bps=tx_rate,
        packet_loss_percent=network.packet_loss_percent,
        containers_running=report.containers.running,
        containers_stopped=report.containers.stopped,
        raw=report.model_dump(mode="json"),
    )


def _rates(
    report: HealthReport, previous: PreviousCounters | None
) -> tuple[float | None, float | None]:
    """Bits per second since the previous reading; None after a counter reset or reorder."""
    if previous is None:
        return None, None
    elapsed = (report.timestamp - previous.ts).total_seconds()
    rx, tx = report.network.rx_bytes, report.network.tx_bytes
    if elapsed <= 0 or rx < previous.rx_bytes or tx < previous.tx_bytes:
        return None, None
    return (rx - previous.rx_bytes) * 8 / elapsed, (tx - previous.tx_bytes) * 8 / elapsed
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest worker/tests/test_transform.py -v`
Expected: PASS

- [ ] **Step 6: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy worker/edgeio_worker`

```bash
git add pyproject.toml uv.lock worker
git commit -m "feat(worker): flatten reports into rows with derived rates"
```

---

### Task 8: Alert rules

**Files:**
- Create: `worker/edgeio_worker/rules.py`
- Test: `worker/tests/test_rules.py`

**Interfaces:**
- Consumes: `ReadingRow`, `to_row` (Task 7); `sample_report` (Task 1).
- Produces (`edgeio_worker.rules`): `Severity = Literal["warning","critical"]`; `DeviceStatus = Literal["healthy","warning","critical","offline"]`; `OFFLINE_RULE = "offline"`; `SERVICE_DOWN_RULE = "service_down"`; `THRESHOLD_RULES: tuple[ThresholdRule, ...]`; frozen dataclass `AlertAction(kind: Literal["upsert","resolve"], rule: str, severity: Severity | None = None, value: float | None = None, message: str = "")`; `evaluate(row, services: Mapping[str, str], open_alerts: Mapping[str, Severity]) -> list[AlertAction]`; `apply_actions(open_alerts, actions) -> dict[str, Severity]`; `device_status(open_alerts) -> DeviceStatus`.

- [ ] **Step 1: Write the failing tests**

`worker/tests/test_rules.py`:
```python
from typing import Any

from edgeio_contracts.samples import sample_report
from edgeio_worker.rules import (
    OFFLINE_RULE,
    SERVICE_DOWN_RULE,
    AlertAction,
    apply_actions,
    device_status,
    evaluate,
)
from edgeio_worker.transform import ReadingRow, to_row

RUNNING = {"docker": "running", "postgresql": "running", "edge_streamer": "running"}


def row_with(changes: dict[str, Any] | None = None) -> ReadingRow:
    """A healthy reading (no stopped containers) with dotted-path changes."""
    return to_row(sample_report({"containers.stopped": 0, **(changes or {})}), None)


def kinds(actions: list[AlertAction]) -> dict[str, tuple[str, str | None]]:
    return {a.rule: (a.kind, a.severity) for a in actions}


def test_healthy_reading_produces_no_actions() -> None:
    assert evaluate(row_with(), RUNNING, {}) == []


def test_temperature_opens_warning_then_critical() -> None:
    assert kinds(evaluate(row_with({"system.cpu_temperature_c": 80}), RUNNING, {})) == {
        "cpu_temp_high": ("upsert", "warning")
    }
    assert kinds(evaluate(row_with({"system.cpu_temperature_c": 90}), RUNNING, {})) == {
        "cpu_temp_high": ("upsert", "critical")
    }


def test_hysteresis_band_keeps_open_alert_at_its_severity() -> None:
    actions = evaluate(row_with({"system.cpu_temperature_c": 72}), RUNNING, {"cpu_temp_high": "critical"})
    assert kinds(actions) == {"cpu_temp_high": ("upsert", "critical")}
    assert actions[0].value == 72


def test_hysteresis_band_does_not_open_a_new_alert() -> None:
    assert evaluate(row_with({"system.cpu_temperature_c": 72}), RUNNING, {}) == []


def test_alert_resolves_at_clear_threshold() -> None:
    actions = evaluate(row_with({"system.cpu_temperature_c": 70}), RUNNING, {"cpu_temp_high": "warning"})
    assert kinds(actions) == {"cpu_temp_high": ("resolve", None)}


def test_critical_deescalates_to_warning() -> None:
    actions = evaluate(row_with({"system.cpu_temperature_c": 80}), RUNNING, {"cpu_temp_high": "critical"})
    assert kinds(actions) == {"cpu_temp_high": ("upsert", "warning")}


def test_ram_rule_has_no_critical_level() -> None:
    row = row_with({"system.ram_used_mb": 16000, "system.ram_usage_percent": 97.7})
    assert kinds(evaluate(row, RUNNING, {})) == {"ram_usage_high": ("upsert", "warning")}


def test_disk_and_packet_loss_thresholds() -> None:
    row = row_with(
        {
            "disk.root_used_gb": 460,
            "disk.root_free_gb": 16,
            "disk.root_usage_percent": 96.6,
            "network.packet_loss_percent": 3.0,
        }
    )
    assert kinds(evaluate(row, RUNNING, {})) == {
        "disk_usage_high": ("upsert", "critical"),
        "packet_loss_high": ("upsert", "warning"),
    }


def test_stopped_containers_warn_and_resolve_at_zero() -> None:
    assert kinds(evaluate(row_with({"containers.stopped": 1}), RUNNING, {})) == {
        "containers_stopped": ("upsert", "warning")
    }
    assert kinds(evaluate(row_with(), RUNNING, {"containers_stopped": "warning"})) == {
        "containers_stopped": ("resolve", None)
    }


def test_service_down_is_critical_and_names_services() -> None:
    services = {**RUNNING, "edge_streamer": "failed", "docker": "stopped"}
    actions = evaluate(row_with(), services, {})
    assert kinds(actions) == {SERVICE_DOWN_RULE: ("upsert", "critical")}
    assert actions[0].message == "Services not running: docker, edge_streamer"
    assert actions[0].value == 2


def test_service_down_resolves_when_all_running() -> None:
    assert kinds(evaluate(row_with(), RUNNING, {SERVICE_DOWN_RULE: "critical"})) == {
        SERVICE_DOWN_RULE: ("resolve", None)
    }


def test_any_reading_resolves_offline() -> None:
    assert kinds(evaluate(row_with(), RUNNING, {OFFLINE_RULE: "critical"})) == {
        OFFLINE_RULE: ("resolve", None)
    }


def test_apply_actions_updates_open_set() -> None:
    actions = [
        AlertAction("upsert", "cpu_temp_high", "critical", 90.0),
        AlertAction("resolve", "disk_usage_high"),
    ]
    assert apply_actions({"disk_usage_high": "warning"}, actions) == {"cpu_temp_high": "critical"}


def test_device_status_precedence() -> None:
    assert device_status({}) == "healthy"
    assert device_status({"ram_usage_high": "warning"}) == "warning"
    assert device_status({"ram_usage_high": "warning", "cpu_temp_high": "critical"}) == "critical"
    assert device_status({OFFLINE_RULE: "critical", "ram_usage_high": "warning"}) == "offline"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest worker/tests/test_rules.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_worker.rules'`

- [ ] **Step 3: Implement the rules**

`worker/edgeio_worker/rules.py`:
```python
"""Alert rules as data. Pure functions: no I/O."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from .transform import ReadingRow

Severity = Literal["warning", "critical"]
DeviceStatus = Literal["healthy", "warning", "critical", "offline"]

OFFLINE_RULE = "offline"
SERVICE_DOWN_RULE = "service_down"


@dataclass(frozen=True)
class ThresholdRule:
    name: str
    label: str
    unit: str
    metric: Callable[[ReadingRow], float]
    warning: float | None  # opens when value > warning
    critical: float | None  # opens/escalates when value > critical
    clear_at: float  # an open alert resolves when value <= clear_at (hysteresis)

    def severity_for(self, value: float) -> Severity | None:
        if self.critical is not None and value > self.critical:
            return "critical"
        if self.warning is not None and value > self.warning:
            return "warning"
        return None


THRESHOLD_RULES: tuple[ThresholdRule, ...] = (
    ThresholdRule("cpu_temp_high", "CPU temperature", "°C", lambda r: r.cpu_temperature_c, 75, 85, 70),
    ThresholdRule("disk_usage_high", "Disk usage", "%", lambda r: r.disk_usage_percent, 85, 95, 80),
    ThresholdRule("ram_usage_high", "RAM usage", "%", lambda r: r.ram_usage_percent, 90, None, 85),
    ThresholdRule("packet_loss_high", "Packet loss", "%", lambda r: r.packet_loss_percent, 2, 10, 1),
    ThresholdRule("containers_stopped", "Stopped containers", "", lambda r: r.containers_stopped, 0, None, 0),
)


@dataclass(frozen=True)
class AlertAction:
    kind: Literal["upsert", "resolve"]
    rule: str
    severity: Severity | None = None
    value: float | None = None
    message: str = ""


def evaluate(
    row: ReadingRow, services: Mapping[str, str], open_alerts: Mapping[str, Severity]
) -> list[AlertAction]:
    actions: list[AlertAction] = []
    if OFFLINE_RULE in open_alerts:
        actions.append(AlertAction("resolve", OFFLINE_RULE, message="Device reporting again"))

    for rule in THRESHOLD_RULES:
        value = rule.metric(row)
        message = f"{rule.label} at {value:g}{rule.unit}"
        severity = rule.severity_for(value)
        if severity is not None:
            actions.append(AlertAction("upsert", rule.name, severity, value, message))
        elif rule.name in open_alerts:
            if value <= rule.clear_at:
                actions.append(AlertAction("resolve", rule.name, value=value, message=message))
            else:  # inside the hysteresis band: stay open, refresh the value
                actions.append(
                    AlertAction("upsert", rule.name, open_alerts[rule.name], value, message)
                )

    down = sorted(name for name, state in services.items() if state != "running")
    if down:
        actions.append(
            AlertAction(
                "upsert",
                SERVICE_DOWN_RULE,
                "critical",
                float(len(down)),
                f"Services not running: {', '.join(down)}",
            )
        )
    elif SERVICE_DOWN_RULE in open_alerts:
        actions.append(AlertAction("resolve", SERVICE_DOWN_RULE, value=0.0))
    return actions


def apply_actions(
    open_alerts: Mapping[str, Severity], actions: Sequence[AlertAction]
) -> dict[str, Severity]:
    result = dict(open_alerts)
    for action in actions:
        if action.kind == "resolve":
            result.pop(action.rule, None)
        elif action.severity is not None:
            result[action.rule] = action.severity
    return result


def device_status(open_alerts: Mapping[str, Severity]) -> DeviceStatus:
    if OFFLINE_RULE in open_alerts:
        return "offline"
    if "critical" in open_alerts.values():
        return "critical"
    if open_alerts:
        return "warning"
    return "healthy"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest worker/tests/test_rules.py -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy worker/edgeio_worker`

```bash
git add worker
git commit -m "feat(worker): threshold and service alert rules with hysteresis"
```

---

### Task 9: Database migrations and migration runner

**Files:**
- Create: `db/migrations/0001_init.sql`, `db/migrations/0002_aggregates.sql`
- Create: `worker/edgeio_worker/config.py`, `worker/edgeio_worker/logs.py`, `worker/edgeio_worker/migrate.py`
- Create: `tests/integration/conftest.py`, `tests/integration/test_migrations.py`
- Modify: `CLAUDE.md` §8 (devices table also stores last counters)

**Interfaces:**
- Produces (`edgeio_worker.config`): `WorkerSettings` with fields `kafka_bootstrap, kafka_topic, kafka_dlq_topic, kafka_group_id, database_url, migrations_dir: Path, batch_size, poll_timeout_seconds, offline_after_seconds, sweep_interval_seconds, log_level`.
- Produces (`edgeio_worker.logs`): `configure_logging(level: str) -> None`.
- Produces (`edgeio_worker.migrate`): `apply_migrations(database_url: str, migrations_dir: Path) -> list[str]` (names newly applied); CLI `python -m edgeio_worker.migrate`.
- Produces (tables): `devices`, `health_readings` (hypertable), `service_status`, `alerts` (partial unique index `alerts_one_open_per_rule`), continuous aggregates `health_hourly`, `health_daily`.
- Produces (pytest fixtures in `tests/integration/conftest.py`): `migrations_dir: Path`, `database_url: str` (session, migrated), `conn: psycopg.Connection` (autocommit, tables truncated), `report_at(minutes, changes=None) -> HealthReport` (timestamp = 2026-10-06T09:00Z + minutes; `containers.stopped` defaults to 0).

- [ ] **Step 1: Write the migrations**

`db/migrations/0001_init.sql`:
```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE devices (
    device_id           inet PRIMARY KEY,
    hostname            text NOT NULL,
    os                  text NOT NULL,
    first_seen          timestamptz NOT NULL,
    last_seen           timestamptz NOT NULL,
    status              text NOT NULL DEFAULT 'healthy'
                        CHECK (status IN ('healthy', 'warning', 'critical', 'offline')),
    uptime_seconds      bigint,
    cpu_usage_percent   double precision,
    cpu_temperature_c   double precision,
    ram_usage_percent   double precision,
    disk_usage_percent  double precision,
    packet_loss_percent double precision,
    last_rx_bytes       bigint NOT NULL,
    last_tx_bytes       bigint NOT NULL
);

CREATE TABLE health_readings (
    device_id           inet NOT NULL,
    ts                  timestamptz NOT NULL,
    hostname            text NOT NULL,
    uptime_seconds      bigint NOT NULL,
    cpu_usage_percent   double precision NOT NULL,
    cpu_temperature_c   double precision NOT NULL,
    load_1m             double precision NOT NULL,
    ram_total_mb        integer NOT NULL,
    ram_used_mb         integer NOT NULL,
    ram_usage_percent   double precision NOT NULL,
    disk_total_gb       double precision NOT NULL,
    disk_used_gb        double precision NOT NULL,
    disk_free_gb        double precision NOT NULL,
    disk_usage_percent  double precision NOT NULL,
    disk_free_percent   double precision NOT NULL,
    net_interface       text NOT NULL,
    rx_bytes            bigint NOT NULL,
    tx_bytes            bigint NOT NULL,
    rx_rate_bps         double precision,
    tx_rate_bps         double precision,
    packet_loss_percent double precision NOT NULL,
    containers_running  integer NOT NULL,
    containers_stopped  integer NOT NULL,
    raw                 jsonb NOT NULL,
    PRIMARY KEY (device_id, ts)
);

SELECT create_hypertable('health_readings', by_range('ts', INTERVAL '1 day'));

CREATE TABLE service_status (
    device_id  inet NOT NULL,
    service    text NOT NULL,
    state      text NOT NULL,
    changed_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (device_id, service)
);

CREATE TABLE alerts (
    id          bigserial PRIMARY KEY,
    device_id   inet NOT NULL,
    rule        text NOT NULL,
    severity    text NOT NULL CHECK (severity IN ('warning', 'critical')),
    opened_at   timestamptz NOT NULL,
    resolved_at timestamptz,
    last_value  double precision,
    message     text NOT NULL
);

CREATE UNIQUE INDEX alerts_one_open_per_rule ON alerts (device_id, rule) WHERE resolved_at IS NULL;
CREATE INDEX alerts_opened_at_idx ON alerts (opened_at DESC);
```

`db/migrations/0002_aggregates.sql`:
```sql
-- WITH NO DATA lets continuous aggregates be created inside the migration transaction.

CREATE MATERIALIZED VIEW health_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT device_id,
       time_bucket(INTERVAL '1 hour', ts) AS bucket,
       avg(cpu_usage_percent)   AS cpu_avg,
       max(cpu_usage_percent)   AS cpu_max,
       avg(cpu_temperature_c)   AS temp_avg,
       max(cpu_temperature_c)   AS temp_max,
       avg(ram_usage_percent)   AS ram_avg,
       max(disk_usage_percent)  AS disk_max,
       avg(packet_loss_percent) AS packet_loss_avg,
       count(*)                 AS reading_count
FROM health_readings
GROUP BY device_id, bucket
WITH NO DATA;

CREATE MATERIALIZED VIEW health_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT device_id,
       time_bucket(INTERVAL '1 day', ts) AS bucket,
       avg(cpu_usage_percent)   AS cpu_avg,
       max(cpu_usage_percent)   AS cpu_max,
       avg(cpu_temperature_c)   AS temp_avg,
       max(cpu_temperature_c)   AS temp_max,
       avg(ram_usage_percent)   AS ram_avg,
       max(disk_usage_percent)  AS disk_max,
       avg(packet_loss_percent) AS packet_loss_avg,
       count(*)                 AS reading_count
FROM health_readings
GROUP BY device_id, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('health_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes');

SELECT add_continuous_aggregate_policy('health_daily',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour');

SELECT add_retention_policy('health_readings', INTERVAL '30 days');
SELECT add_retention_policy('health_hourly', INTERVAL '1 year');
```

- [ ] **Step 2: Write config, logging and the integration fixtures**

`worker/edgeio_worker/config.py`:
```python
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class WorkerSettings(BaseSettings):
    """Read from environment variables named after each field, upper-cased."""

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "device.health"
    kafka_dlq_topic: str = "device.health.dlq"
    kafka_group_id: str = "edgeio-worker"
    database_url: str = "postgresql://edgeio:edgeio@localhost:5432/edgeio"
    migrations_dir: Path = Path("db/migrations")
    batch_size: int = Field(default=500, ge=1)
    poll_timeout_seconds: float = Field(default=1.0, gt=0)
    offline_after_seconds: int = Field(default=900, gt=0)
    sweep_interval_seconds: float = Field(default=60.0, gt=0)
    log_level: str = "INFO"
```

`worker/edgeio_worker/logs.py`:
```python
import logging

from pythonjsonlogger.json import JsonFormatter


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.basicConfig(level=level, handlers=[handler], force=True)
```

`tests/integration/conftest.py`:
```python
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

from edgeio_contracts.models import HealthReport
from edgeio_contracts.samples import sample_report
from edgeio_worker.migrate import apply_migrations

TIMESCALE_IMAGE = "timescale/timescaledb:2.17.2-pg16"
BASE_TIME = datetime(2026, 10, 6, 9, 0, tzinfo=UTC)


@pytest.fixture(scope="session")
def migrations_dir() -> Path:
    return Path(__file__).parents[2] / "db" / "migrations"


@pytest.fixture(scope="session")
def database_url(migrations_dir: Path) -> Iterator[str]:
    with PostgresContainer(
        TIMESCALE_IMAGE, username="edgeio", password="edgeio", dbname="edgeio", driver=None
    ) as pg:
        url = pg.get_connection_url()
        apply_migrations(url, migrations_dir)
        yield url


@pytest.fixture
def conn(database_url: str) -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(database_url, autocommit=True) as c:
        c.execute("TRUNCATE health_readings, devices, service_status, alerts RESTART IDENTITY")
        yield c


@pytest.fixture
def report_at() -> Callable[..., HealthReport]:
    def build(minutes: int, changes: dict[str, Any] | None = None) -> HealthReport:
        ts = (BASE_TIME + timedelta(minutes=minutes)).isoformat()
        return sample_report({"timestamp": ts, "containers.stopped": 0, **(changes or {})})

    return build
```

- [ ] **Step 3: Write the failing migration tests**

`tests/integration/test_migrations.py`:
```python
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from edgeio_worker.migrate import apply_migrations


def test_tables_exist(conn: psycopg.Connection) -> None:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    ).fetchall()
    names = {r[0] for r in rows}
    assert {"devices", "health_readings", "service_status", "alerts", "schema_migrations"} <= names


def test_health_readings_is_a_hypertable(conn: psycopg.Connection) -> None:
    rows = conn.execute("SELECT hypertable_name FROM timescaledb_information.hypertables").fetchall()
    assert ("health_readings",) in rows


def test_continuous_aggregates_exist(conn: psycopg.Connection) -> None:
    rows = conn.execute("SELECT view_name FROM timescaledb_information.continuous_aggregates").fetchall()
    assert {r[0] for r in rows} == {"health_hourly", "health_daily"}


def test_reapplying_is_a_no_op(database_url: str, migrations_dir: Path) -> None:
    assert apply_migrations(database_url, migrations_dir) == []


def test_only_one_open_alert_per_device_and_rule(conn: psycopg.Connection) -> None:
    now = datetime(2026, 10, 6, tzinfo=UTC)
    insert = (
        "INSERT INTO alerts (device_id, rule, severity, opened_at, resolved_at, message) "
        "VALUES ('100.64.0.1', 'cpu_temp_high', 'warning', %s, %s, 'm')"
    )
    conn.execute(insert, (now, now))  # resolved
    conn.execute(insert, (now, None))  # open
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(insert, (now, None))  # second open
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_migrations.py -v` (Docker must be running)
Expected: FAIL / ERROR with `ModuleNotFoundError: No module named 'edgeio_worker.migrate'`

- [ ] **Step 5: Implement the migration runner**

`worker/edgeio_worker/migrate.py`:
```python
"""Apply db/migrations/*.sql in name order, each once, each in its own transaction."""

import logging
from pathlib import Path

import psycopg

from .config import WorkerSettings
from .logs import configure_logging

log = logging.getLogger(__name__)

MIGRATION_LOCK_ID = 7_262_001  # arbitrary; serializes concurrent migrators


def apply_migrations(database_url: str, migrations_dir: Path) -> list[str]:
    files = sorted(migrations_dir.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"no migrations found in {migrations_dir}")
    applied: list[str] = []
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK_ID,))
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version text PRIMARY KEY,"
                " applied_at timestamptz NOT NULL DEFAULT now())"
            )
            done = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
            for path in files:
                if path.name in done:
                    continue
                with conn.transaction():
                    conn.execute(path.read_text())
                    conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,))
                log.info("applied migration", extra={"migration": path.name})
                applied.append(path.name)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_ID,))
    return applied


def main() -> None:
    settings = WorkerSettings()
    configure_logging(settings.log_level)
    applied = apply_migrations(settings.database_url, settings.migrations_dir)
    log.info("migrations complete", extra={"applied": applied})


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_migrations.py -v`
Expected: PASS (first run pulls the Timescale image; may take a minute)

- [ ] **Step 7: Update CLAUDE.md §8**

In `CLAUDE.md`, replace:
```
- `devices` — `device_id` (PK, `inet`), `hostname`, `os`, `first_seen`, `last_seen`, `status` (`healthy|warning|critical|offline`), latest key metrics.
```
with:
```
- `devices` — `device_id` (PK, `inet`), `hostname`, `os`, `first_seen`, `last_seen`, `status` (`healthy|warning|critical|offline`), latest key metrics, `last_rx_bytes`/`last_tx_bytes` (previous counters for rate derivation).
```

- [ ] **Step 8: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy worker/edgeio_worker`

```bash
git add db worker tests CLAUDE.md
git commit -m "feat(db): timescale schema, continuous aggregates, migration runner"
```

---

### Task 10: Store — persist a report and evaluate alerts

**Files:**
- Create: `worker/edgeio_worker/store.py`
- Test: `tests/integration/test_store.py`
- Modify: `CLAUDE.md` §7 (counters come from the devices row)

**Interfaces:**
- Consumes: `to_row`, `PreviousCounters`, `ReadingRow` (Task 7); `evaluate`, `apply_actions`, `device_status`, `AlertAction`, `Severity`, `DeviceStatus` (Task 8); tables (Task 9); fixtures `conn`, `report_at` (Task 9).
- Produces (`edgeio_worker.store`): `process_report(conn, report: HealthReport) -> None`; `store_batch(conn, reports: Sequence[HealthReport]) -> None`; `load_open_alerts(conn, device_id: str) -> dict[str, Severity]`; `apply_alert_actions(conn, device_id: str, actions, at: datetime) -> None`; `set_status(conn, device_id: str, status: DeviceStatus) -> None`. All expect the caller to own the transaction.

- [ ] **Step 1: Write the failing tests**

`tests/integration/test_store.py`:
```python
from typing import Any

import psycopg

from edgeio_worker.store import process_report

DEVICE = "100.101.12.7"


def scalar(conn: psycopg.Connection, query: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(query, params).fetchone()
    assert row is not None
    return row[0]


def open_alerts(conn: psycopg.Connection) -> dict[str, str]:
    rows = conn.execute(
        "SELECT rule, severity FROM alerts WHERE resolved_at IS NULL ORDER BY rule"
    ).fetchall()
    return dict(rows)


def test_first_report_creates_device_reading_and_services(conn, report_at) -> None:
    process_report(conn, report_at(0))
    assert scalar(conn, "SELECT count(*) FROM health_readings") == 1
    device = conn.execute(
        "SELECT host(device_id), hostname, status, last_rx_bytes FROM devices"
    ).fetchone()
    assert device == (DEVICE, "edge-001", "healthy", 482938192)
    assert scalar(conn, "SELECT count(*) FROM service_status WHERE state = 'running'") == 3
    assert scalar(conn, "SELECT rx_rate_bps FROM health_readings") is None
    assert open_alerts(conn) == {}


def test_duplicate_report_is_ignored(conn, report_at) -> None:
    hot = report_at(0, {"system.cpu_temperature_c": 90})
    process_report(conn, hot)
    process_report(conn, hot)
    assert scalar(conn, "SELECT count(*) FROM health_readings") == 1
    assert scalar(conn, "SELECT count(*) FROM alerts") == 1


def test_second_report_derives_rates_from_device_row(conn, report_at) -> None:
    process_report(conn, report_at(0, {"network.rx_bytes": 1_000_000}))
    process_report(conn, report_at(5, {"network.rx_bytes": 4_000_000}))
    rate = scalar(conn, "SELECT rx_rate_bps FROM health_readings ORDER BY ts DESC LIMIT 1")
    assert rate == 80_000.0


def test_overheat_opens_holds_and_resolves_alert(conn, report_at) -> None:
    process_report(conn, report_at(0, {"system.cpu_temperature_c": 90}))
    assert open_alerts(conn) == {"cpu_temp_high": "critical"}
    assert scalar(conn, "SELECT status FROM devices") == "critical"

    process_report(conn, report_at(5, {"system.cpu_temperature_c": 72}))  # hysteresis band
    assert open_alerts(conn) == {"cpu_temp_high": "critical"}
    assert scalar(conn, "SELECT last_value FROM alerts") == 72

    process_report(conn, report_at(10, {"system.cpu_temperature_c": 65}))
    assert open_alerts(conn) == {}
    assert scalar(conn, "SELECT status FROM devices") == "healthy"
    resolved = conn.execute("SELECT opened_at, resolved_at FROM alerts").fetchone()
    assert resolved is not None and resolved[1] > resolved[0]


def test_late_reading_is_stored_but_does_not_rewind_state(conn, report_at) -> None:
    newer = report_at(10)
    process_report(conn, newer)
    process_report(conn, report_at(5, {"system.cpu_temperature_c": 95}))
    assert scalar(conn, "SELECT count(*) FROM health_readings") == 2
    assert scalar(conn, "SELECT last_seen FROM devices") == newer.timestamp
    assert scalar(conn, "SELECT status FROM devices") == "healthy"
    assert open_alerts(conn) == {}


def test_service_failure_opens_alert_and_tracks_change_time(conn, report_at) -> None:
    process_report(conn, report_at(0))
    failed = report_at(5, {"services.edge_streamer": "failed"})
    process_report(conn, failed)
    assert open_alerts(conn) == {"service_down": "critical"}
    row = conn.execute(
        "SELECT state, changed_at FROM service_status WHERE service = 'edge_streamer'"
    ).fetchone()
    assert row == ("failed", failed.timestamp)
    unchanged = scalar(conn, "SELECT changed_at FROM service_status WHERE service = 'docker'")
    assert unchanged < failed.timestamp
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_store.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_worker.store'`

- [ ] **Step 3: Implement the store**

`worker/edgeio_worker/store.py`:
```python
"""Persist validated reports and their alert consequences. Caller owns the transaction."""

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime
from typing import Any, cast

from psycopg import Connection, sql
from psycopg.types.json import Jsonb

from edgeio_contracts.models import HealthReport

from .rules import AlertAction, DeviceStatus, Severity, apply_actions, device_status, evaluate
from .transform import PreviousCounters, ReadingRow, to_row

_READING_COLUMNS = (
    "device_id", "ts", "hostname", "uptime_seconds", "cpu_usage_percent", "cpu_temperature_c",
    "load_1m", "ram_total_mb", "ram_used_mb", "ram_usage_percent", "disk_total_gb",
    "disk_used_gb", "disk_free_gb", "disk_usage_percent", "disk_free_percent", "net_interface",
    "rx_bytes", "tx_bytes", "rx_rate_bps", "tx_rate_bps", "packet_loss_percent",
    "containers_running", "containers_stopped", "raw",
)  # fmt: skip

_INSERT_READING = sql.SQL(
    "INSERT INTO health_readings ({columns}) VALUES ({values}) "
    "ON CONFLICT (device_id, ts) DO NOTHING"
).format(
    columns=sql.SQL(", ").join(map(sql.Identifier, _READING_COLUMNS)),
    values=sql.SQL(", ").join(map(sql.Placeholder, _READING_COLUMNS)),
)

_UPSERT_DEVICE = """
INSERT INTO devices (
    device_id, hostname, os, first_seen, last_seen, uptime_seconds, cpu_usage_percent,
    cpu_temperature_c, ram_usage_percent, disk_usage_percent, packet_loss_percent,
    last_rx_bytes, last_tx_bytes
) VALUES (
    %(device_id)s, %(hostname)s, %(os)s, %(ts)s, %(ts)s, %(uptime_seconds)s,
    %(cpu_usage_percent)s, %(cpu_temperature_c)s, %(ram_usage_percent)s,
    %(disk_usage_percent)s, %(packet_loss_percent)s, %(rx_bytes)s, %(tx_bytes)s
)
ON CONFLICT (device_id) DO UPDATE SET
    hostname = EXCLUDED.hostname,
    os = EXCLUDED.os,
    last_seen = EXCLUDED.last_seen,
    uptime_seconds = EXCLUDED.uptime_seconds,
    cpu_usage_percent = EXCLUDED.cpu_usage_percent,
    cpu_temperature_c = EXCLUDED.cpu_temperature_c,
    ram_usage_percent = EXCLUDED.ram_usage_percent,
    disk_usage_percent = EXCLUDED.disk_usage_percent,
    packet_loss_percent = EXCLUDED.packet_loss_percent,
    last_rx_bytes = EXCLUDED.last_rx_bytes,
    last_tx_bytes = EXCLUDED.last_tx_bytes
"""

_UPSERT_SERVICE = """
INSERT INTO service_status (device_id, service, state, changed_at, updated_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (device_id, service) DO UPDATE SET
    changed_at = CASE WHEN service_status.state = EXCLUDED.state
                      THEN service_status.changed_at ELSE EXCLUDED.updated_at END,
    state = EXCLUDED.state,
    updated_at = EXCLUDED.updated_at
"""

_UPSERT_ALERT = """
INSERT INTO alerts (device_id, rule, severity, opened_at, last_value, message)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (device_id, rule) WHERE resolved_at IS NULL DO UPDATE SET
    severity = EXCLUDED.severity,
    last_value = EXCLUDED.last_value,
    message = EXCLUDED.message
"""

_RESOLVE_ALERT = """
UPDATE alerts SET resolved_at = %s, last_value = COALESCE(%s, last_value)
WHERE device_id = %s AND rule = %s AND resolved_at IS NULL
"""


def store_batch(conn: Connection[Any], reports: Sequence[HealthReport]) -> None:
    for report in reports:
        process_report(conn, report)


def process_report(conn: Connection[Any], report: HealthReport) -> None:
    device_id = str(report.device_id)
    snapshot = conn.execute(
        "SELECT last_seen, last_rx_bytes, last_tx_bytes FROM devices "
        "WHERE device_id = %s FOR UPDATE",
        (device_id,),
    ).fetchone()
    previous = PreviousCounters(*snapshot) if snapshot is not None else None
    row = to_row(report, previous)

    if not _insert_reading(conn, row):
        return  # duplicate delivery: already processed
    if previous is not None and row.ts <= previous.ts:
        return  # late arrival: keep the history, don't rewind current state

    params = asdict(row)
    conn.execute(_UPSERT_DEVICE, params)
    with conn.cursor() as cur:
        cur.executemany(
            _UPSERT_SERVICE,
            [(device_id, name, state, row.ts, row.ts) for name, state in report.services.items()],
        )

    open_alerts = load_open_alerts(conn, device_id)
    actions = evaluate(row, report.services, open_alerts)
    apply_alert_actions(conn, device_id, actions, row.ts)
    set_status(conn, device_id, device_status(apply_actions(open_alerts, actions)))


def load_open_alerts(conn: Connection[Any], device_id: str) -> dict[str, Severity]:
    rows = conn.execute(
        "SELECT rule, severity FROM alerts WHERE device_id = %s AND resolved_at IS NULL",
        (device_id,),
    ).fetchall()
    return {rule: cast(Severity, severity) for rule, severity in rows}


def apply_alert_actions(
    conn: Connection[Any], device_id: str, actions: Sequence[AlertAction], at: datetime
) -> None:
    for action in actions:
        if action.kind == "upsert":
            conn.execute(
                _UPSERT_ALERT,
                (device_id, action.rule, action.severity, at, action.value, action.message),
            )
        else:
            conn.execute(_RESOLVE_ALERT, (at, action.value, device_id, action.rule))


def set_status(conn: Connection[Any], device_id: str, status: DeviceStatus) -> None:
    conn.execute("UPDATE devices SET status = %s WHERE device_id = %s", (status, device_id))


def _insert_reading(conn: Connection[Any], row: ReadingRow) -> bool:
    params: Mapping[str, Any] = {**asdict(row), "raw": Jsonb(row.raw)}
    return conn.execute(_INSERT_READING, params).rowcount == 1
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_store.py -v`
Expected: PASS

- [ ] **Step 5: Update CLAUDE.md §7**

In `CLAUDE.md`, replace:
```
2. **Transform** — normalize timestamp to UTC; flatten into a row; derive `disk_free_percent`, `rx_rate_bps`/`tx_rate_bps` from the previous counter values (in-memory cache per device, warmed from DB on startup; counter decrease = reboot → rate null).
```
with:
```
2. **Transform** — normalize timestamp to UTC; flatten into a row; derive `disk_free_percent`, `rx_rate_bps`/`tx_rate_bps` (bits/s) from the previous counters stored on the `devices` row (read with `SELECT … FOR UPDATE`; no in-memory cache; counter decrease = reboot → rate null). Readings older than `last_seen` are stored as history but don't update device state or alerts.
```

- [ ] **Step 6: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy worker/edgeio_worker`

```bash
git add worker tests CLAUDE.md
git commit -m "feat(worker): idempotent report storage with alert evaluation"
```

---

### Task 11: Offline sweeper

**Files:**
- Create: `worker/edgeio_worker/sweeper.py`
- Test: `tests/integration/test_sweeper.py`

**Interfaces:**
- Consumes: `OFFLINE_RULE` (Task 8); `process_report` (Task 10); fixtures `conn`, `report_at` (Task 9).
- Produces (`edgeio_worker.sweeper`): `sweep_offline(conn, now: datetime, offline_after: timedelta) -> list[str]` — device IDs that were newly marked offline. Caller owns the transaction.

- [ ] **Step 1: Write the failing tests**

`tests/integration/test_sweeper.py`:
```python
from datetime import timedelta

from edgeio_worker.store import process_report
from edgeio_worker.sweeper import sweep_offline

WINDOW = timedelta(minutes=15)


def test_silent_device_goes_offline_once(conn, report_at) -> None:
    last = report_at(0)
    process_report(conn, last)
    now = last.timestamp + timedelta(minutes=20)

    assert sweep_offline(conn, now, WINDOW) == ["100.101.12.7"]
    assert sweep_offline(conn, now, WINDOW) == []  # no duplicate alert

    alert = conn.execute(
        "SELECT severity, last_value, message FROM alerts WHERE rule = 'offline'"
    ).fetchone()
    assert alert == ("critical", 1200.0, "No health report for over 15 minutes")
    assert conn.execute("SELECT status FROM devices").fetchone() == ("offline",)


def test_recent_device_is_left_alone(conn, report_at) -> None:
    last = report_at(0)
    process_report(conn, last)
    assert sweep_offline(conn, last.timestamp + timedelta(minutes=5), WINDOW) == []
    assert conn.execute("SELECT status FROM devices").fetchone() == ("healthy",)


def test_next_report_brings_device_back(conn, report_at) -> None:
    process_report(conn, report_at(0))
    sweep_offline(conn, report_at(20).timestamp, WINDOW)
    process_report(conn, report_at(25))
    open_count = conn.execute(
        "SELECT count(*) FROM alerts WHERE resolved_at IS NULL"
    ).fetchone()
    assert open_count == (0,)
    assert conn.execute("SELECT status FROM devices").fetchone() == ("healthy",)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_sweeper.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_worker.sweeper'`

- [ ] **Step 3: Implement the sweeper**

`worker/edgeio_worker/sweeper.py`:
```python
"""Periodic detection of devices that stopped reporting. Caller owns the transaction."""

from datetime import datetime, timedelta
from typing import Any

from psycopg import Connection

from .rules import OFFLINE_RULE

_OPEN_OFFLINE_ALERTS = """
INSERT INTO alerts (device_id, rule, severity, opened_at, last_value, message)
SELECT device_id, %(rule)s, 'critical', %(now)s,
       EXTRACT(EPOCH FROM %(now)s - last_seen), %(message)s
FROM devices
WHERE last_seen < %(cutoff)s
ON CONFLICT (device_id, rule) WHERE resolved_at IS NULL DO NOTHING
RETURNING host(device_id)
"""

_MARK_OFFLINE = """
UPDATE devices SET status = 'offline'
WHERE last_seen < %(cutoff)s AND status <> 'offline'
"""


def sweep_offline(conn: Connection[Any], now: datetime, offline_after: timedelta) -> list[str]:
    minutes = int(offline_after.total_seconds() // 60)
    params = {
        "rule": OFFLINE_RULE,
        "now": now,
        "cutoff": now - offline_after,
        "message": f"No health report for over {minutes} minutes",
    }
    newly_offline = [row[0] for row in conn.execute(_OPEN_OFFLINE_ALERTS, params).fetchall()]
    conn.execute(_MARK_OFFLINE, params)
    return newly_offline
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_sweeper.py -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy worker/edgeio_worker`

```bash
git add worker tests
git commit -m "feat(worker): offline sweeper opens alerts for silent devices"
```

---

### Task 12: DLQ, retrying database access, and the consumer loop

**Files:**
- Create: `worker/edgeio_worker/dlq.py`, `worker/edgeio_worker/db.py`, `worker/edgeio_worker/consumer.py`, `worker/edgeio_worker/__main__.py`
- Modify: `tests/integration/conftest.py` (add Kafka fixture)
- Test: `worker/tests/test_dlq.py`, `worker/tests/test_db.py`, `tests/integration/test_pipeline.py`

**Interfaces:**
- Consumes: `validate_report`, `ContractError` (Task 2); `WorkerSettings`, `configure_logging` (Task 9); `store_batch` (Task 10); `sweep_offline` (Task 11).
- Produces (`edgeio_worker.dlq`): frozen dataclass `DlqRecord(key: bytes | None, value: bytes, headers: list[tuple[str, bytes]])`; `build_dlq_record(*, key, value, source_topic, partition, offset, error: ContractError, failed_at: datetime) -> DlqRecord`.
- Produces (`edgeio_worker.db`): `Database(connect: Callable[[], Connection], stop: threading.Event, *, initial_backoff_seconds=1.0, max_backoff_seconds=30.0)`, `Database.from_url(url, stop)`, `run_in_transaction(fn: Callable[[Connection], T]) -> T` (retries `psycopg.OperationalError` with backoff; re-raises once `stop` is set), `close()`.
- Produces (`edgeio_worker.consumer`): `run(settings: WorkerSettings, stop: threading.Event) -> None`; `handle_batch(messages, settings, producer, db) -> None`.
- Produces: `python -m edgeio_worker` entry point; integration fixture `kafka_bootstrap: str`.

- [ ] **Step 1: Write the failing unit tests**

`worker/tests/test_dlq.py`:
```python
from datetime import UTC, datetime

from edgeio_contracts.validation import ContractError
from edgeio_worker.dlq import build_dlq_record

FAILED_AT = datetime(2026, 10, 6, 9, 45, tzinfo=UTC)


def test_record_preserves_original_bytes_and_explains_failure() -> None:
    record = build_dlq_record(
        key=b"100.64.0.9",
        value=b"{not json",
        source_topic="device.health",
        partition=3,
        offset=42,
        error=ContractError("decode", "Invalid JSON"),
        failed_at=FAILED_AT,
    )
    assert record.key == b"100.64.0.9"
    assert record.value == b"{not json"
    assert dict(record.headers) == {
        "error": b"Invalid JSON",
        "error_stage": b"decode",
        "source_topic": b"device.health",
        "source_partition": b"3",
        "source_offset": b"42",
        "failed_at": b"2026-10-06T09:45:00+00:00",
    }


def test_missing_value_becomes_empty_bytes() -> None:
    record = build_dlq_record(
        key=None,
        value=None,
        source_topic="device.health",
        partition=0,
        offset=0,
        error=ContractError("decode", "message has no value"),
        failed_at=FAILED_AT,
    )
    assert record.key is None and record.value == b""
```

`worker/tests/test_db.py`:
```python
import threading
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
import pytest

from edgeio_worker.db import Database


class FakeConn:
    def __init__(self) -> None:
        self.closed = False

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield

    def close(self) -> None:
        self.closed = True


def test_retries_after_database_outage() -> None:
    connections: list[FakeConn] = []

    def connect() -> FakeConn:
        connections.append(FakeConn())
        return connections[-1]

    calls = {"n": 0}

    def work(conn: FakeConn) -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        return "stored"

    db = Database(connect, threading.Event(), initial_backoff_seconds=0.01)  # type: ignore[arg-type]
    assert db.run_in_transaction(work) == "stored"
    assert len(connections) == 2
    assert connections[0].closed


def test_reuses_healthy_connection() -> None:
    connections: list[FakeConn] = []

    def connect() -> FakeConn:
        connections.append(FakeConn())
        return connections[-1]

    db = Database(connect, threading.Event())  # type: ignore[arg-type]
    db.run_in_transaction(lambda c: None)
    db.run_in_transaction(lambda c: None)
    assert len(connections) == 1


def test_gives_up_when_shutting_down() -> None:
    stop = threading.Event()
    stop.set()

    def always_down(conn: FakeConn) -> None:
        raise psycopg.OperationalError("down")

    db = Database(FakeConn, stop, initial_backoff_seconds=0.01)  # type: ignore[arg-type]
    with pytest.raises(psycopg.OperationalError):
        db.run_in_transaction(always_down)


def test_non_transient_errors_propagate_immediately() -> None:
    def broken(conn: FakeConn) -> None:
        raise ValueError("bug")

    db = Database(FakeConn, threading.Event())  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        db.run_in_transaction(broken)
```

- [ ] **Step 2: Run unit tests to verify they fail**

Run: `uv run pytest worker/tests/test_dlq.py worker/tests/test_db.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_worker.dlq'`

- [ ] **Step 3: Implement DLQ record and Database**

`worker/edgeio_worker/dlq.py`:
```python
"""Dead-letter records: the original bytes plus headers explaining the rejection."""

from dataclasses import dataclass
from datetime import datetime

from edgeio_contracts.validation import ContractError


@dataclass(frozen=True)
class DlqRecord:
    key: bytes | None
    value: bytes
    headers: list[tuple[str, bytes]]


def build_dlq_record(
    *,
    key: bytes | None,
    value: bytes | None,
    source_topic: str,
    partition: int,
    offset: int,
    error: ContractError,
    failed_at: datetime,
) -> DlqRecord:
    return DlqRecord(
        key=key,
        value=value or b"",
        headers=[
            ("error", error.message.encode()),
            ("error_stage", error.stage.encode()),
            ("source_topic", source_topic.encode()),
            ("source_partition", str(partition).encode()),
            ("source_offset", str(offset).encode()),
            ("failed_at", failed_at.isoformat().encode()),
        ],
    )
```

`worker/edgeio_worker/db.py`:
```python
"""Postgres access with retry: a transient outage blocks and retries, never drops data."""

import logging
import threading
from collections.abc import Callable
from typing import Any

import psycopg

log = logging.getLogger(__name__)

Connect = Callable[[], psycopg.Connection[Any]]


class Database:
    def __init__(
        self,
        connect: Connect,
        stop: threading.Event,
        *,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 30.0,
    ) -> None:
        self._connect = connect
        self._stop = stop
        self._initial_backoff = initial_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._conn: psycopg.Connection[Any] | None = None

    @classmethod
    def from_url(cls, url: str, stop: threading.Event) -> "Database":
        return cls(lambda: psycopg.connect(url, autocommit=True), stop)

    def run_in_transaction[T](self, fn: Callable[[psycopg.Connection[Any]], T]) -> T:
        backoff = self._initial_backoff
        while True:
            try:
                conn = self._connection()
                with conn.transaction():
                    return fn(conn)
            except psycopg.OperationalError:
                log.warning(
                    "database unavailable, retrying",
                    exc_info=True,
                    extra={"backoff_seconds": backoff},
                )
                self.close()
                if self._stop.wait(backoff):
                    raise
                backoff = min(backoff * 2, self._max_backoff)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _connection(self) -> psycopg.Connection[Any]:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn
```

- [ ] **Step 4: Run unit tests to verify they pass**

Run: `uv run pytest worker/tests/test_dlq.py worker/tests/test_db.py -v`
Expected: PASS

- [ ] **Step 5: Add the Kafka fixture and write the failing pipeline test**

Append to `tests/integration/conftest.py`:
```python
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    with KafkaContainer().with_kraft() as kafka:
        yield kafka.get_bootstrap_server()
```
(Move the `from testcontainers.kafka import KafkaContainer` line up into the import block so ruff's import sorting passes.)

`tests/integration/test_pipeline.py`:
```python
import json
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from edgeio_contracts.samples import sample_payload, set_path
from edgeio_worker.config import WorkerSettings
from edgeio_worker.consumer import run


def create_topics(bootstrap: str) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    topic, dlq = f"device.health.{suffix}", f"device.health.dlq.{suffix}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(topic, 3, 1), NewTopic(dlq, 1, 1)])
    for future in futures.values():
        future.result(timeout=30)
    return topic, dlq


def wait_for(condition: Callable[[], bool], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.5)
    raise AssertionError("condition not met in time")


def read_topic(bootstrap: str, topic: str, expected: int, timeout: float) -> list[Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"reader-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    messages: list[Any] = []
    deadline = time.monotonic() + timeout
    while len(messages) < expected and time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is not None and msg.error() is None:
            messages.append(msg)
    consumer.close()
    return messages


def count_readings(conn: psycopg.Connection) -> int:
    row = conn.execute("SELECT count(*) FROM health_readings").fetchone()
    return int(row[0]) if row else 0


def test_valid_reports_are_stored_and_invalid_ones_dead_lettered(
    kafka_bootstrap: str, database_url: str, conn: psycopg.Connection
) -> None:
    topic, dlq = create_topics(kafka_bootstrap)
    now = datetime.now(UTC).replace(microsecond=0)
    reports = [
        json.dumps(
            set_path(sample_payload(), "timestamp", (now - timedelta(minutes=5 * i)).isoformat())
        ).encode()
        for i in (2, 1, 0)
    ]
    producer = Producer({"bootstrap.servers": kafka_bootstrap})
    for value in reports:
        producer.produce(topic, key=b"100.101.12.7", value=value)
    producer.produce(topic, key=b"bad-device", value=b"{not json")
    producer.produce(topic, key=b"100.101.12.7", value=reports[-1])  # redelivered duplicate
    assert producer.flush(10) == 0

    settings = WorkerSettings(
        kafka_bootstrap=kafka_bootstrap,
        kafka_topic=topic,
        kafka_dlq_topic=dlq,
        kafka_group_id=f"worker-{uuid.uuid4()}",
        database_url=database_url,
        poll_timeout_seconds=0.2,
        sweep_interval_seconds=3600,
    )
    stop = threading.Event()
    worker = threading.Thread(target=run, args=(settings, stop), daemon=True)
    worker.start()
    try:
        wait_for(lambda: count_readings(conn) == 3, timeout=60)
        dead = read_topic(kafka_bootstrap, dlq, expected=1, timeout=30)
    finally:
        stop.set()
        worker.join(timeout=30)

    assert not worker.is_alive()
    assert count_readings(conn) == 3
    assert len(dead) == 1
    assert dead[0].value() == b"{not json"
    assert dict(dead[0].headers())["error_stage"] == b"decode"
    status = conn.execute("SELECT status, last_seen FROM devices").fetchone()
    assert status == ("warning", now)  # sample has 1 stopped container → warning
```

- [ ] **Step 6: Run the pipeline test to verify it fails**

Run: `uv run pytest tests/integration/test_pipeline.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_worker.consumer'`

- [ ] **Step 7: Implement the consumer loop and entry point**

`worker/edgeio_worker/consumer.py`:
```python
"""Kafka consume loop: validate → (DLQ | store) → commit offsets, plus the offline sweep."""

import logging
import threading
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from confluent_kafka import Consumer, Producer

from edgeio_contracts.models import HealthReport
from edgeio_contracts.validation import ContractError, validate_report

from .config import WorkerSettings
from .db import Database
from .dlq import build_dlq_record
from .store import store_batch
from .sweeper import sweep_offline

log = logging.getLogger(__name__)


def run(settings: WorkerSettings, stop: threading.Event) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.kafka_group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer(
        {"bootstrap.servers": settings.kafka_bootstrap, "enable.idempotence": True, "acks": "all"}
    )
    db = Database.from_url(settings.database_url, stop)
    offline_after = timedelta(seconds=settings.offline_after_seconds)
    consumer.subscribe([settings.kafka_topic])
    log.info("worker started", extra={"topic": settings.kafka_topic})
    next_sweep = time.monotonic()
    try:
        while not stop.is_set():
            messages = consumer.consume(
                num_messages=settings.batch_size, timeout=settings.poll_timeout_seconds
            )
            if messages:
                handle_batch(messages, settings, producer, db)
                # Only after the DB transaction committed: at-least-once delivery.
                consumer.commit(asynchronous=False)
            if time.monotonic() >= next_sweep:
                offline = db.run_in_transaction(
                    lambda conn: sweep_offline(conn, datetime.now(UTC), offline_after)
                )
                if offline:
                    log.warning("devices went offline", extra={"device_ids": offline})
                next_sweep = time.monotonic() + settings.sweep_interval_seconds
    finally:
        consumer.close()
        producer.flush(10)
        db.close()
        log.info("worker stopped")


def handle_batch(
    messages: Sequence[Any], settings: WorkerSettings, producer: Any, db: Database
) -> None:
    now = datetime.now(UTC)
    reports: list[HealthReport] = []
    rejected = 0
    for msg in messages:
        if msg.error() is not None:
            log.warning("kafka message error", extra={"error": str(msg.error())})
            continue
        try:
            reports.append(validate_report(msg.value(), now))
        except ContractError as exc:
            record = build_dlq_record(
                key=msg.key(),
                value=msg.value(),
                source_topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                error=exc,
                failed_at=now,
            )
            producer.produce(
                settings.kafka_dlq_topic, key=record.key, value=record.value, headers=record.headers
            )
            rejected += 1
            log.warning(
                "message rejected",
                extra={"stage": exc.stage, "error": exc.message, "offset": msg.offset()},
            )
    if rejected and producer.flush(10) > 0:
        raise RuntimeError("dead-letter messages not delivered; refusing to commit offsets")
    if reports:
        db.run_in_transaction(lambda conn: store_batch(conn, reports))
    log.info("batch processed", extra={"stored": len(reports), "rejected": rejected})
```

`worker/edgeio_worker/__main__.py`:
```python
"""Entry point: python -m edgeio_worker"""

import signal
import threading

from .config import WorkerSettings
from .consumer import run
from .logs import configure_logging


def main() -> None:
    settings = WorkerSettings()
    configure_logging(settings.log_level)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    run(settings, stop)


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: Run all tests to verify they pass**

Run: `uv run pytest -v && uv run pytest tests/integration -v`
Expected: PASS (unit and integration)

- [ ] **Step 9: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy contracts/edgeio_contracts simulator/edgeio_simulator worker/edgeio_worker`

```bash
git add worker tests
git commit -m "feat(worker): consumer loop with DLQ, retrying DB access, offset commit after store"
```

---

### Task 13: Docker Compose stack, Makefile, end-to-end smoke check

**Files:**
- Create: `docker/python.Dockerfile`, `.dockerignore`, `docker-compose.yml`, `Makefile`
- Modify: `CLAUDE.md` §12 and §15

**Interfaces:**
- Consumes: `python -m edgeio_simulator` (Task 6), `python -m edgeio_worker.migrate` (Task 9), `python -m edgeio_worker` (Task 12).
- Produces: `make up/down/logs/test/test-int/lint/fmt/psql/topics/schema`.

- [ ] **Step 1: Write the image recipe**

`.dockerignore`:
```
.git
.venv
**/__pycache__
.mypy_cache
.ruff_cache
.pytest_cache
dashboard
node_modules
docs
```

`docker/python.Dockerfile`:
```dockerfile
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app

# PACKAGE selects which workspace member (and its dependencies) to install.
ARG PACKAGE
COPY . .
RUN uv sync --frozen --no-dev --no-editable --package "${PACKAGE}"
```

- [ ] **Step 2: Write the Compose file**

`docker-compose.yml`:
```yaml
name: edgeio

services:
  kafka:
    image: apache/kafka:3.8.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 12
      start_period: 15s

  kafka-init:
    image: apache/kafka:3.8.0
    depends_on:
      kafka:
        condition: service_healthy
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        set -e
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic device.health --partitions 6 --replication-factor 1
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic device.health.dlq --partitions 1 --replication-factor 1
    restart: "no"

  timescaledb:
    image: timescale/timescaledb:2.17.2-pg16
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: edgeio
      POSTGRES_PASSWORD: edgeio
      POSTGRES_DB: edgeio
    volumes:
      - timescale-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U edgeio -d edgeio"]
      interval: 5s
      timeout: 5s
      retries: 20

  migrate:
    image: edgeio/worker:dev
    build:
      context: .
      dockerfile: docker/python.Dockerfile
      args:
        PACKAGE: edgeio-worker
    command: ["python", "-m", "edgeio_worker.migrate"]
    environment:
      DATABASE_URL: postgresql://edgeio:edgeio@timescaledb:5432/edgeio
      MIGRATIONS_DIR: /app/db/migrations
    depends_on:
      timescaledb:
        condition: service_healthy
    restart: "no"

  worker:
    image: edgeio/worker:dev
    command: ["python", "-m", "edgeio_worker"]
    environment:
      KAFKA_BOOTSTRAP: kafka:29092
      DATABASE_URL: postgresql://edgeio:edgeio@timescaledb:5432/edgeio
    depends_on:
      migrate:
        condition: service_completed_successfully
      kafka-init:
        condition: service_completed_successfully
    restart: unless-stopped

  simulator:
    image: edgeio/simulator:dev
    build:
      context: .
      dockerfile: docker/python.Dockerfile
      args:
        PACKAGE: edgeio-simulator
    command: ["python", "-m", "edgeio_simulator"]
    environment:
      KAFKA_BOOTSTRAP: kafka:29092
      SIM_DEVICE_COUNT: "50"
      SIM_INTERVAL_SECONDS: "300"
    depends_on:
      kafka-init:
        condition: service_completed_successfully
    restart: unless-stopped

volumes:
  timescale-data:
```

- [ ] **Step 3: Write the Makefile**

`Makefile` (recipe lines are indented with a tab):
```make
.PHONY: up down logs test test-int lint fmt psql topics schema

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f $(s)

test:
	uv run pytest

test-int:
	uv run pytest tests/integration

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy contracts/edgeio_contracts simulator/edgeio_simulator worker/edgeio_worker

fmt:
	uv run ruff format .
	uv run ruff check --fix .

psql:
	docker compose exec timescaledb psql -U edgeio -d edgeio

topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group edgeio-worker

schema:
	uv run python -m edgeio_contracts.export_schema > contracts/health.schema.json
```

- [ ] **Step 4: Bring the stack up**

Run: `make up && docker compose ps`
Expected: `kafka` and `timescaledb` healthy; `kafka-init` and `migrate` exited 0; `worker` and `simulator` running.

Run: `make logs s=migrate`
Expected: JSON log lines `applied migration` for `0001_init.sql` and `0002_aggregates.sql`.

- [ ] **Step 5: Smoke-check data flow (real time: allow ~6 minutes)**

Every device's first tick is jittered across the 300 s interval, so wait at least 5 minutes after the simulator starts.

Run: `docker compose exec timescaledb psql -U edgeio -d edgeio -c "SELECT count(*) AS readings, count(DISTINCT device_id) AS devices FROM health_readings;"`
Expected: `devices` = 50 (or very close if a device happens to be in an `offline` fault), `readings` ≥ 50.

Run: `docker compose exec timescaledb psql -U edgeio -d edgeio -c "SELECT status, count(*) FROM devices GROUP BY status;"`
Expected: mostly `healthy`, a few `warning`/`critical` over time.

Run: `make topics`
Expected: `device.health` with 6 partitions; consumer group `edgeio-worker` with LAG 0 or near 0.

Run: `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic device.health.dlq --from-beginning --timeout-ms 5000 --property print.headers=true`
Expected: zero or a few malformed messages with `error_stage` headers (rate 0.5%, so possibly none yet — that's fine).

- [ ] **Step 6: Update CLAUDE.md §12 and §15**

In `CLAUDE.md` §12, replace:
```
(Targets to be created with the scaffold; keep this section in sync with the Makefile.)
```
with:
```
(Keep this section in sync with the Makefile. `make test` = unit tests only; `make test-int` needs Docker.)
```
and add this line inside the command block after `make topics ...`:
```
make schema      # regenerate contracts/health.schema.json from the Pydantic model
```

Replace the body of §15 with:
```
Design approved 2026-10-06. Plan 1 (pipeline: contracts, simulator, worker, TimescaleDB, Compose) implemented — see `docs/superpowers/plans/2026-10-06-edgeio-pipeline.md`. Next: Plan 2 — REST API (§10) and dashboard (§11).
```

- [ ] **Step 7: Final verification and commit**

Run: `make lint && make test && make test-int`
Expected: all pass.

```bash
git add docker .dockerignore docker-compose.yml Makefile CLAUDE.md
git commit -m "feat: docker compose stack and Makefile for the pipeline"
```
