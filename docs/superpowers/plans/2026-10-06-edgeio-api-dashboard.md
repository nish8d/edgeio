# edgeio API + Dashboard (Plan 2 of 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the pipeline's data through a read-only FastAPI REST API and a React dashboard (fleet overview, device detail with charts, alerts), both started by `make up`.

**Architecture:** A fourth uv workspace member, `api` (`edgeio_api`), serves `/api/v1/*` from TimescaleDB through a psycopg connection pool, using plain SQL in one `queries.py` module and Pydantic response models in `schemas.py`. The OpenAPI document is exported to `dashboard/openapi.json`, and the dashboard generates its TypeScript types from it (`openapi-typescript`) and calls the API with `openapi-fetch` + TanStack Query. In Compose, nginx serves the built dashboard and proxies `/api` to the API container.

**Tech Stack:** FastAPI, uvicorn, psycopg 3 + psycopg-pool, pydantic-settings; React 19, Vite, TypeScript (strict), React Router, TanStack Query, Recharts, openapi-typescript, openapi-fetch, Vitest + Testing Library, ESLint, Prettier; nginx.

**Spec:** `CLAUDE.md` (repo root) — §8 (rollups), §10 (API), §11 (dashboard), §12–§15. Builds on Plan 1 (`docs/superpowers/plans/2026-10-06-edgeio-pipeline.md`), already merged.

## Global Constraints

- Python 3.12; `mypy --strict` and `ruff` clean (line length 100; `docs/` excluded).
- API is read-only and never talks to Kafka; plain parameterized SQL via psycopg 3, no ORM; SQL identifiers only from fixed allow-lists via `psycopg.sql.Identifier`.
- All endpoints under `/api/v1`; OpenAPI docs at `/docs`.
- Config only via env vars (pydantic-settings). Host-side DB port is 5433 (Plan 1 ruling).
- Database unavailable → API answers `503 {"detail": "database unavailable"}`, never a 500 or a hang.
- Dashboard API types are generated from `dashboard/openapi.json`; never hand-write API types.
- TanStack Query refetches every 30 s; refetch keeps the previous render (no skeleton flash).
- Data viz: one metric per chart (never dual axes), single series per chart (title names it, no legend), 2px lines, hairline horizontal grid, crosshair tooltip, and a data-table view for every chart. Status is always color **+ icon + label**, using the reserved status colors (healthy `#0ca30c`, warning `#fab219`, critical `#d03b3b`, offline `#898781`). Series color is `#2a78d6` (light) / `#3987e5` (dark). Light and dark themes come from CSS tokens.
- Commits are authored by the user only. No AI co-author trailers.

## Review Focus

1. **A device ID in the path that isn't an IPv4 address** (`/devices/edge-001`) → 422, not 500. Test: Task 3 `test_device_id_must_be_an_ip`.
2. **Metrics with `from` ≥ `to`, or with naive timestamps** → 422 for the inverted range; a naive timestamp is treated as UTC. Tests: Task 4 `test_inverted_range_is_rejected`, `test_naive_timestamps_are_treated_as_utc`.
3. **Database down** → API returns 503 JSON, and the dashboard shows an error message instead of a blank page. Tests: Task 2/3 `test_unavailable.py`; Task 10 `test_fleet_overview_shows_error_when_api_fails`.
4. **Empty fleet** (fresh DB, no devices yet) → summary is all zeros and the dashboard says "No devices…". Tests: Task 6 `test_empty_fleet_summary`; Task 10 `shows an empty state…`.
5. **No data in the selected chart range** → chart shows "No data in this range." instead of an empty plot. Test: Task 10 `test_metric_chart_empty_state`.

---

## File Structure

```
db/migrations/0003_rollup_metrics.sql   # rollups gain ram/disk/packet-loss max + rx/tx rate avg/max
api/
  pyproject.toml
  edgeio_api/
    __init__.py  py.typed  __main__.py
    config.py        # ApiSettings
    logs.py          # JSON logging
    db.py            # pool + get_conn dependency, DbConn alias
    app.py           # create_app(): lifespan, CORS, 503 handler, routers
    schemas.py       # Pydantic response models + Literal enums
    queries.py       # all SQL; METRICS allow-list; choose_bucket()
    export_openapi.py
    routers/__init__.py  health.py  devices.py  alerts.py  fleet.py
  tests/test_unavailable.py  test_buckets.py  test_openapi.py
tests/integration/conftest.py           # + refresh_rollups, seed, api fixtures; rollup reset
tests/integration/test_rollups.py  test_api_devices.py  test_api_metrics.py
tests/integration/test_api_alerts.py  test_api_fleet.py
dashboard/
  package.json  package-lock.json  index.html  vite.config.ts  tsconfig.json
  eslint.config.js  .prettierrc  .prettierignore  .dockerignore  Dockerfile  nginx.conf
  openapi.json                          # exported from the API (committed)
  src/
    main.tsx  App.tsx  styles.css
    api/schema.d.ts (generated)  client.ts  types.ts  hooks.ts  queryClient.ts
    lib/format.ts  range.ts  status.ts  metrics.ts  (+ *.test.ts)
    components/Layout.tsx  StatusBadge.tsx  StatusTiles.tsx  DeviceCard.tsx  AlertsTable.tsx
               ServicesPanel.tsx  RangePicker.tsx  MetricChart.tsx  (+ *.test.tsx)
    pages/FleetOverview.tsx  DeviceDetail.tsx  AlertsPage.tsx  (+ *.test.tsx)
    test/setup.ts  render.tsx  api.ts  fixtures.ts
```

---

### Task 1: Rollups for every charted metric

**Files:**
- Create: `db/migrations/0003_rollup_metrics.sql`
- Modify: `tests/integration/conftest.py` (rollup reset in `conn`, add `refresh_rollups` fixture)
- Test: `tests/integration/test_rollups.py`

**Interfaces:**
- Produces (columns on `health_hourly` and `health_daily`): `device_id, bucket, cpu_avg, cpu_max, temp_avg, temp_max, ram_avg, ram_max, disk_avg, disk_max, packet_loss_avg, packet_loss_max, rx_rate_avg, rx_rate_max, tx_rate_avg, tx_rate_max, reading_count`.
- Produces (fixture): `refresh_rollups() -> None` materializes both rollups over all data. Seeded tests call it after seeding, so the results don't depend on the wall clock.

- [ ] **Step 1: Write the failing test**

`tests/integration/test_rollups.py`:
```python
import pytest

from edgeio_worker.store import process_report

EXPECTED = {
    "cpu_avg", "cpu_max", "temp_avg", "temp_max", "ram_avg", "ram_max", "disk_avg", "disk_max",
    "packet_loss_avg", "packet_loss_max", "rx_rate_avg", "rx_rate_max", "tx_rate_avg",
    "tx_rate_max", "reading_count",
}  # fmt: skip


@pytest.mark.parametrize("view", ["health_hourly", "health_daily"])
def test_rollups_cover_every_charted_metric(conn, view: str) -> None:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s", (view,)
    ).fetchall()
    assert EXPECTED <= {r[0] for r in rows}


def test_hourly_rollup_aggregates_network_rates(conn, report_at, refresh_rollups) -> None:
    process_report(conn, report_at(0, {"network.rx_bytes": 1_000_000}))
    process_report(conn, report_at(5, {"network.rx_bytes": 4_000_000}))
    refresh_rollups()
    row = conn.execute("SELECT reading_count, rx_rate_avg, rx_rate_max FROM health_hourly").fetchone()
    assert row == (2, 80_000.0, 80_000.0)
```

Add to `tests/integration/conftest.py`. Put the `sql` import with the other imports. In the existing `conn` fixture, add the two lines marked `# new`:
```python
from psycopg import sql

ROLLUPS = ("health_hourly", "health_daily")


def _refresh(conn: psycopg.Connection[Any]) -> None:
    for view in ROLLUPS:
        conn.execute(
            sql.SQL("CALL refresh_continuous_aggregate({}, NULL, NULL)").format(sql.Literal(view))
        )


@pytest.fixture
def conn(database_url: str) -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(database_url, autocommit=True) as c:
        c.execute("TRUNCATE health_readings, devices, service_status, alerts RESTART IDENTITY")
        _refresh(c)  # new: drop rollup rows materialized by earlier tests
        yield c


@pytest.fixture
def refresh_rollups(conn: psycopg.Connection[Any]) -> Callable[[], None]:  # new
    return lambda: _refresh(conn)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_rollups.py -v`
Expected: FAIL — `test_rollups_cover_every_charted_metric` misses `ram_max`, `rx_rate_avg`, …; the rate test fails with `UndefinedColumn: column "rx_rate_avg" does not exist`.

- [ ] **Step 3: Write the migration**

`db/migrations/0003_rollup_metrics.sql`:
```sql
-- Recreate both rollups with avg + max for every metric the dashboard charts,
-- including network rates. Dropping a continuous aggregate also drops its policies,
-- so they are re-added below.

DROP MATERIALIZED VIEW health_daily;
DROP MATERIALIZED VIEW health_hourly;

CREATE MATERIALIZED VIEW health_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT device_id,
       time_bucket(INTERVAL '1 hour', ts) AS bucket,
       avg(cpu_usage_percent)   AS cpu_avg,
       max(cpu_usage_percent)   AS cpu_max,
       avg(cpu_temperature_c)   AS temp_avg,
       max(cpu_temperature_c)   AS temp_max,
       avg(ram_usage_percent)   AS ram_avg,
       max(ram_usage_percent)   AS ram_max,
       avg(disk_usage_percent)  AS disk_avg,
       max(disk_usage_percent)  AS disk_max,
       avg(packet_loss_percent) AS packet_loss_avg,
       max(packet_loss_percent) AS packet_loss_max,
       avg(rx_rate_bps)         AS rx_rate_avg,
       max(rx_rate_bps)         AS rx_rate_max,
       avg(tx_rate_bps)         AS tx_rate_avg,
       max(tx_rate_bps)         AS tx_rate_max,
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
       max(ram_usage_percent)   AS ram_max,
       avg(disk_usage_percent)  AS disk_avg,
       max(disk_usage_percent)  AS disk_max,
       avg(packet_loss_percent) AS packet_loss_avg,
       max(packet_loss_percent) AS packet_loss_max,
       avg(rx_rate_bps)         AS rx_rate_avg,
       max(rx_rate_bps)         AS rx_rate_max,
       avg(tx_rate_bps)         AS tx_rate_avg,
       max(tx_rate_bps)         AS tx_rate_max,
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

SELECT add_retention_policy('health_hourly', INTERVAL '1 year');
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration -v`
Expected: PASS (all integration tests, including the existing migration tests)

- [ ] **Step 5: Commit**

```bash
uv run ruff format . && uv run ruff check .
git add db tests
git commit -m "feat(db): rollups carry avg and max for every charted metric"
```

---

### Task 2: API package, app factory, health check, 503 on DB outage

**Files:**
- Create: `api/pyproject.toml`, `api/edgeio_api/__init__.py`, `api/edgeio_api/py.typed`, `api/edgeio_api/config.py`, `api/edgeio_api/logs.py`, `api/edgeio_api/db.py`, `api/edgeio_api/schemas.py`, `api/edgeio_api/app.py`, `api/edgeio_api/__main__.py`, `api/edgeio_api/routers/__init__.py`, `api/edgeio_api/routers/health.py`
- Modify: `pyproject.toml` (member, dependency, source, testpaths, dev dep `httpx`), `Makefile` (mypy path), `tests/integration/conftest.py` (`api` fixture)
- Test: `api/tests/test_unavailable.py`, `tests/integration/test_api_fleet.py` (health test only for now)

**Interfaces:**
- Produces (`edgeio_api.config`): `ApiSettings(database_url, cors_origins: list[str], pool_min_size, pool_max_size, pool_timeout_seconds, host, port, log_level)`.
- Produces (`edgeio_api.db`): `DbConn = Connection[dict[str, Any]]`; `create_pool(settings) -> ConnectionPool[Any]`; `get_conn(request) -> Iterator[DbConn]` (FastAPI dependency).
- Produces (`edgeio_api.app`): `API_PREFIX = "/api/v1"`; `create_app(settings: ApiSettings | None = None) -> FastAPI`.
- Produces (`edgeio_api.schemas`): `DeviceStatus`, `Severity`, `AlertState`, `MetricName`, `Bucket` Literals; `Health(status: Literal["ok","degraded"], database: Literal["ok","unavailable"])`. Tasks 3–6 add more models to this module.
- Produces (fixture `api` in integration conftest): a `TestClient` with the lifespan running, against the test DB.

- [ ] **Step 1: Create the package and register it**

`api/pyproject.toml`:
```toml
[project]
name = "edgeio-api"
version = "0.1.0"
description = "Read-only REST API over the edgeio TimescaleDB"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.30",
    "psycopg[binary]>=3.2",
    "psycopg-pool>=3.2",
    "pydantic-settings>=2.4",
    "python-json-logger>=3.1",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

`api/edgeio_api/__init__.py`:
```python
"""Read-only REST API for the edgeio platform."""
```
`api/edgeio_api/py.typed` and `api/edgeio_api/routers/__init__.py`: empty files.

Root `pyproject.toml`:
- `dependencies = ["edgeio-contracts", "edgeio-simulator", "edgeio-worker", "edgeio-api"]`
- `members = ["contracts", "simulator", "worker", "api"]`
- add `edgeio-api = { workspace = true }` under `[tool.uv.sources]`
- `testpaths = ["contracts/tests", "simulator/tests", "worker/tests", "api/tests"]`
- add `"httpx>=0.27",` to the `dev` dependency group (FastAPI's `TestClient` needs it)
- add `"edgeio_api"` to `known-first-party`

`Makefile` `lint` target: change the mypy line to
```make
	uv run mypy contracts/edgeio_contracts simulator/edgeio_simulator worker/edgeio_worker api/edgeio_api
```

Run: `uv sync`
Expected: installs `edgeio-api`, `fastapi`, `uvicorn`, `psycopg-pool`, `httpx`.

- [ ] **Step 2: Write the failing tests**

`api/tests/test_unavailable.py`:
```python
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from edgeio_api.app import create_app
from edgeio_api.config import ApiSettings


@pytest.fixture
def client() -> Iterator[TestClient]:
    settings = ApiSettings(
        database_url="postgresql://edgeio:edgeio@127.0.0.1:1/edgeio",  # nothing listens here
        pool_timeout_seconds=0.5,
    )
    with TestClient(create_app(settings)) as c:
        yield c


def test_health_reports_database_unavailable(client: TestClient) -> None:
    response = client.get("/api/v1/healthz")
    assert response.status_code == 503
    assert response.json() == {"status": "degraded", "database": "unavailable"}
```

Add the `api` fixture to `tests/integration/conftest.py` (imports go in the import block):
```python
from fastapi.testclient import TestClient

from edgeio_api.app import create_app
from edgeio_api.config import ApiSettings


@pytest.fixture
def api(database_url: str) -> Iterator[TestClient]:
    with TestClient(create_app(ApiSettings(database_url=database_url))) as client:
        yield client
```

`tests/integration/test_api_fleet.py`:
```python
from fastapi.testclient import TestClient


def test_health_ok_with_database(api: TestClient) -> None:
    response = api.get("/api/v1/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest api/tests -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_api.app'`

- [ ] **Step 4: Implement config, logging, db, schemas, health router, app**

`api/edgeio_api/config.py`:
```python
from pydantic import Field
from pydantic_settings import BaseSettings


class ApiSettings(BaseSettings):
    """Read from environment variables named after each field, upper-cased."""

    database_url: str = "postgresql://edgeio:edgeio@localhost:5433/edgeio"
    cors_origins: list[str] = ["http://localhost:5173"]
    pool_min_size: int = Field(default=1, ge=1)
    pool_max_size: int = Field(default=5, ge=1)
    pool_timeout_seconds: float = Field(default=5.0, gt=0)
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
```

`api/edgeio_api/logs.py`:
```python
import logging

from pythonjsonlogger.json import JsonFormatter


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.basicConfig(level=level, handlers=[handler], force=True)
```

`api/edgeio_api/db.py`:
```python
"""Connection pool and the per-request connection dependency."""

from collections.abc import Iterator
from typing import Any

from fastapi import Request
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import ApiSettings

DbConn = Connection[dict[str, Any]]


def create_pool(settings: ApiSettings) -> ConnectionPool[Any]:
    return ConnectionPool(
        settings.database_url,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        timeout=settings.pool_timeout_seconds,
        open=False,
        check=ConnectionPool.check_connection,  # drop connections broken by a DB restart
        kwargs={"autocommit": True, "row_factory": dict_row},
    )


def get_conn(request: Request) -> Iterator[DbConn]:
    pool: ConnectionPool[Any] = request.app.state.pool
    with pool.connection() as conn:
        yield conn
```

`api/edgeio_api/schemas.py`:
```python
"""Response models. These are the API's public contract; the dashboard's types are
generated from the OpenAPI document built from them."""

from typing import Literal

from pydantic import BaseModel

DeviceStatus = Literal["healthy", "warning", "critical", "offline"]
Severity = Literal["warning", "critical"]
AlertState = Literal["open", "resolved", "all"]
MetricName = Literal["cpu", "temperature", "ram", "disk", "packet_loss", "rx_rate", "tx_rate"]
Bucket = Literal["raw", "1h", "1d"]


class Health(BaseModel):
    status: Literal["ok", "degraded"]
    database: Literal["ok", "unavailable"]
```

`api/edgeio_api/routers/health.py`:
```python
from typing import Any

import psycopg
from fastapi import APIRouter, Request, Response
from psycopg_pool import ConnectionPool, PoolTimeout

from ..schemas import Health

router = APIRouter(tags=["health"])


@router.get("/healthz", responses={503: {"model": Health}})
def healthz(request: Request, response: Response) -> Health:
    pool: ConnectionPool[Any] = request.app.state.pool
    try:
        with pool.connection(timeout=2.0) as conn:
            conn.execute("SELECT 1")
    except (PoolTimeout, psycopg.Error):
        response.status_code = 503
        return Health(status="degraded", database="unavailable")
    return Health(status="ok", database="ok")
```

`api/edgeio_api/app.py`:
```python
"""FastAPI application factory."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg_pool import PoolTimeout

from .config import ApiSettings
from .db import create_pool
from .routers import health

log = logging.getLogger(__name__)

API_PREFIX = "/api/v1"


async def _database_unavailable(request: Request, exc: Exception) -> JSONResponse:
    log.warning("database unavailable", extra={"path": request.url.path, "error": str(exc)})
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    settings = settings or ApiSettings()
    pool = create_pool(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool.open(wait=False)  # don't block startup on the database
        try:
            yield
        finally:
            pool.close()

    app = FastAPI(title="edgeio API", version="1.0.0", lifespan=lifespan)
    app.state.pool = pool
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["GET"],
        allow_headers=["*"],
    )
    app.add_exception_handler(PoolTimeout, _database_unavailable)
    app.add_exception_handler(psycopg.OperationalError, _database_unavailable)
    app.include_router(health.router, prefix=API_PREFIX)
    return app
```

`api/edgeio_api/__main__.py`:
```python
"""Entry point: python -m edgeio_api"""

import uvicorn

from .app import create_app
from .config import ApiSettings
from .logs import configure_logging


def main() -> None:
    settings = ApiSettings()
    configure_logging(settings.log_level)
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port, log_config=None)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest api/tests -v && uv run pytest tests/integration/test_api_fleet.py -v`
Expected: PASS

- [ ] **Step 6: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy api/edgeio_api`

```bash
git add pyproject.toml uv.lock Makefile api tests
git commit -m "feat(api): app factory with pooled DB access and health check"
```

---

### Task 3: Devices list and detail

**Files:**
- Create: `api/edgeio_api/queries.py`, `api/edgeio_api/routers/devices.py`
- Modify: `api/edgeio_api/schemas.py`, `api/edgeio_api/app.py` (include router), `tests/integration/conftest.py` (`seed` fixture), `api/tests/test_unavailable.py`
- Test: `tests/integration/test_api_devices.py`

**Interfaces:**
- Consumes: `DbConn`, `get_conn`, `API_PREFIX`, `create_app` (Task 2); `process_report` (Plan 1), fixtures `conn`, `report_at` (Plan 1).
- Produces (`schemas`): `DeviceSummary(device_id: str, hostname, os, status: DeviceStatus, last_seen: datetime, uptime_seconds: int|None, cpu_usage_percent: float|None, cpu_temperature_c, ram_usage_percent, disk_usage_percent, packet_loss_percent, open_alert_count: int)`; `DeviceList(items, total, limit, offset)`; `ServiceStatus(name, state, changed_at)`; `LatestReading(ts, load_1m, ram_used_mb, ram_total_mb, disk_used_gb, disk_total_gb, net_interface, rx_rate_bps: float|None, tx_rate_bps: float|None, containers_running, containers_stopped)`; `Alert(id, device_id, hostname: str|None, rule, severity: Severity, opened_at, resolved_at: datetime|None, last_value: float|None, message)`; `DeviceDetail(DeviceSummary) + first_seen, services: list[ServiceStatus], latest: LatestReading|None, open_alerts: list[Alert]`.
- Produces (`queries`): `Row = dict[str, Any]`; `list_devices(conn, status, limit, offset) -> tuple[list[Row], int]`; `get_device(conn, device_id: IPv4Address) -> Row | None`; `device_exists(conn, device_id) -> bool`; `get_services(conn, device_id) -> list[Row]`; `get_latest_reading(conn, device_id) -> Row | None`; `list_alerts(conn, *, state, device_id, severity, limit, offset) -> tuple[list[Row], int]`.
- Produces (routes): `GET /api/v1/devices`, `GET /api/v1/devices/{device_id}`.
- Produces (fixture `seed`): `seed(n: int, minutes: int = 0, changes: dict | None = None) -> HealthReport` stores a report for device `100.64.0.{n}` / `edge-{n:03d}` via `process_report`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/conftest.py` (import `process_report` from `edgeio_worker.store` in the import block):
```python
@pytest.fixture
def seed(
    conn: psycopg.Connection[Any], report_at: Callable[..., HealthReport]
) -> Callable[..., HealthReport]:
    def store(n: int, minutes: int = 0, changes: dict[str, Any] | None = None) -> HealthReport:
        identity = {"device_id": f"100.64.0.{n}", "system.hostname": f"edge-{n:03d}"}
        report = report_at(minutes, {**identity, **(changes or {})})
        process_report(conn, report)
        return report

    return store
```

`tests/integration/test_api_devices.py`:
```python
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def fleet(seed) -> None:
    seed(1)
    seed(2, changes={"system.cpu_temperature_c": 90})  # critical
    seed(3, changes={"network.packet_loss_percent": 3.0})  # warning


def test_lists_devices_by_hostname(api: TestClient, fleet: None) -> None:
    body = api.get("/api/v1/devices").json()
    assert body["total"] == 3
    assert [d["hostname"] for d in body["items"]] == ["edge-001", "edge-002", "edge-003"]
    second = body["items"][1]
    assert second["device_id"] == "100.64.0.2"
    assert second["status"] == "critical"
    assert second["open_alert_count"] == 1
    assert second["last_seen"] == "2026-10-06T09:00:00Z"


def test_filters_by_status(api: TestClient, fleet: None) -> None:
    body = api.get("/api/v1/devices", params={"status": "critical"}).json()
    assert body["total"] == 1
    assert [d["hostname"] for d in body["items"]] == ["edge-002"]


def test_paginates(api: TestClient, fleet: None) -> None:
    body = api.get("/api/v1/devices", params={"limit": 1, "offset": 1}).json()
    assert (body["total"], body["limit"], body["offset"]) == (3, 1, 1)
    assert [d["hostname"] for d in body["items"]] == ["edge-002"]


def test_rejects_unknown_status(api: TestClient) -> None:
    assert api.get("/api/v1/devices", params={"status": "broken"}).status_code == 422


def test_device_detail(api: TestClient, fleet: None) -> None:
    body = api.get("/api/v1/devices/100.64.0.1").json()
    assert body["hostname"] == "edge-001"
    assert body["first_seen"] == "2026-10-06T09:00:00Z"
    assert [s["name"] for s in body["services"]] == ["docker", "edge_streamer", "postgresql"]
    assert body["latest"]["containers_running"] == 5
    assert body["latest"]["containers_stopped"] == 0
    assert body["open_alerts"] == []


def test_device_detail_includes_open_alerts(api: TestClient, fleet: None) -> None:
    alerts = api.get("/api/v1/devices/100.64.0.2").json()["open_alerts"]
    assert [(a["rule"], a["severity"], a["hostname"]) for a in alerts] == [
        ("cpu_temp_high", "critical", "edge-002")
    ]


def test_unknown_device_is_404(api: TestClient) -> None:
    assert api.get("/api/v1/devices/100.64.0.99").status_code == 404


def test_device_id_must_be_an_ip(api: TestClient) -> None:
    assert api.get("/api/v1/devices/edge-001").status_code == 422
```

Append to `api/tests/test_unavailable.py`:
```python
def test_endpoints_answer_503_when_database_is_down(client: TestClient) -> None:
    response = client.get("/api/v1/devices")
    assert response.status_code == 503
    assert response.json() == {"detail": "database unavailable"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest api/tests tests/integration/test_api_devices.py -v`
Expected: FAIL — `/api/v1/devices` returns 404 (no route yet)

- [ ] **Step 3: Implement schemas, queries and the router**

Append to `api/edgeio_api/schemas.py` (add `from datetime import datetime` to its imports):
```python
class DeviceSummary(BaseModel):
    device_id: str
    hostname: str
    os: str
    status: DeviceStatus
    last_seen: datetime
    uptime_seconds: int | None
    cpu_usage_percent: float | None
    cpu_temperature_c: float | None
    ram_usage_percent: float | None
    disk_usage_percent: float | None
    packet_loss_percent: float | None
    open_alert_count: int


class DeviceList(BaseModel):
    items: list[DeviceSummary]
    total: int
    limit: int
    offset: int


class ServiceStatus(BaseModel):
    name: str
    state: str
    changed_at: datetime


class LatestReading(BaseModel):
    ts: datetime
    load_1m: float
    ram_used_mb: int
    ram_total_mb: int
    disk_used_gb: float
    disk_total_gb: float
    net_interface: str
    rx_rate_bps: float | None
    tx_rate_bps: float | None
    containers_running: int
    containers_stopped: int


class Alert(BaseModel):
    id: int
    device_id: str
    hostname: str | None
    rule: str
    severity: Severity
    opened_at: datetime
    resolved_at: datetime | None
    last_value: float | None
    message: str


class DeviceDetail(DeviceSummary):
    first_seen: datetime
    services: list[ServiceStatus]
    latest: LatestReading | None
    open_alerts: list[Alert]
```

`api/edgeio_api/queries.py`:
```python
"""All SQL the API runs. Read-only; identifiers come only from fixed allow-lists."""

from ipaddress import IPv4Address
from typing import Any

from .db import DbConn
from .schemas import AlertState, DeviceStatus, Severity

Row = dict[str, Any]

_SUMMARY_COLUMNS = """
    host(d.device_id) AS device_id, d.hostname, d.os, d.status, d.last_seen, d.uptime_seconds,
    d.cpu_usage_percent, d.cpu_temperature_c, d.ram_usage_percent, d.disk_usage_percent,
    d.packet_loss_percent,
    (SELECT count(*) FROM alerts a
      WHERE a.device_id = d.device_id AND a.resolved_at IS NULL) AS open_alert_count
"""
_DEVICE_FILTER = "(%(status)s::text IS NULL OR d.status = %(status)s::text)"

_LIST_DEVICES = f"""
SELECT {_SUMMARY_COLUMNS} FROM devices d WHERE {_DEVICE_FILTER}
ORDER BY d.hostname LIMIT %(limit)s OFFSET %(offset)s
"""
_COUNT_DEVICES = f"SELECT count(*) AS n FROM devices d WHERE {_DEVICE_FILTER}"
_GET_DEVICE = f"SELECT {_SUMMARY_COLUMNS}, d.first_seen FROM devices d WHERE d.device_id = %s"

_ALERT_COLUMNS = """
    a.id, host(a.device_id) AS device_id, d.hostname, a.rule, a.severity, a.opened_at,
    a.resolved_at, a.last_value, a.message
"""
_ALERT_FILTER = """
    (%(state)s::text = 'all' OR (%(state)s::text = 'open') = (a.resolved_at IS NULL))
    AND (%(device_id)s::inet IS NULL OR a.device_id = %(device_id)s::inet)
    AND (%(severity)s::text IS NULL OR a.severity = %(severity)s::text)
"""
_LIST_ALERTS = f"""
SELECT {_ALERT_COLUMNS} FROM alerts a LEFT JOIN devices d ON d.device_id = a.device_id
WHERE {_ALERT_FILTER}
ORDER BY a.opened_at DESC, a.id DESC LIMIT %(limit)s OFFSET %(offset)s
"""
_COUNT_ALERTS = f"SELECT count(*) AS n FROM alerts a WHERE {_ALERT_FILTER}"


def _count(conn: DbConn, query: str, params: dict[str, Any]) -> int:
    row = conn.execute(query, params).fetchone()
    return int(row["n"]) if row else 0


def list_devices(
    conn: DbConn, status: DeviceStatus | None, limit: int, offset: int
) -> tuple[list[Row], int]:
    params = {"status": status, "limit": limit, "offset": offset}
    return conn.execute(_LIST_DEVICES, params).fetchall(), _count(conn, _COUNT_DEVICES, params)


def get_device(conn: DbConn, device_id: IPv4Address) -> Row | None:
    return conn.execute(_GET_DEVICE, (device_id,)).fetchone()


def device_exists(conn: DbConn, device_id: IPv4Address) -> bool:
    return conn.execute("SELECT 1 FROM devices WHERE device_id = %s", (device_id,)).fetchone() is not None


def get_services(conn: DbConn, device_id: IPv4Address) -> list[Row]:
    return conn.execute(
        "SELECT service AS name, state, changed_at FROM service_status "
        "WHERE device_id = %s ORDER BY service",
        (device_id,),
    ).fetchall()


def get_latest_reading(conn: DbConn, device_id: IPv4Address) -> Row | None:
    return conn.execute(
        "SELECT ts, load_1m, ram_used_mb, ram_total_mb, disk_used_gb, disk_total_gb, "
        "net_interface, rx_rate_bps, tx_rate_bps, containers_running, containers_stopped "
        "FROM health_readings WHERE device_id = %s ORDER BY ts DESC LIMIT 1",
        (device_id,),
    ).fetchone()


def list_alerts(
    conn: DbConn,
    *,
    state: AlertState,
    device_id: IPv4Address | None,
    severity: Severity | None,
    limit: int,
    offset: int,
) -> tuple[list[Row], int]:
    params = {
        "state": state,
        "device_id": device_id,
        "severity": severity,
        "limit": limit,
        "offset": offset,
    }
    return conn.execute(_LIST_ALERTS, params).fetchall(), _count(conn, _COUNT_ALERTS, params)
```

`api/edgeio_api/routers/devices.py`:
```python
from ipaddress import IPv4Address
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from .. import queries
from ..db import DbConn, get_conn
from ..schemas import DeviceDetail, DeviceList, DeviceStatus, DeviceSummary

router = APIRouter(tags=["devices"])
Conn = Annotated[DbConn, Depends(get_conn)]
NOT_FOUND = {404: {"description": "Device not found"}}


@router.get("/devices")
def list_devices(
    conn: Conn,
    status: DeviceStatus | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> DeviceList:
    rows, total = queries.list_devices(conn, status, limit, offset)
    return DeviceList(
        items=[DeviceSummary.model_validate(r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/devices/{device_id}", responses=NOT_FOUND)
def get_device(conn: Conn, device_id: IPv4Address) -> DeviceDetail:
    row = queries.get_device(conn, device_id)
    if row is None:
        raise HTTPException(status_code=404, detail="device not found")
    alerts, _ = queries.list_alerts(
        conn, state="open", device_id=device_id, severity=None, limit=100, offset=0
    )
    return DeviceDetail.model_validate(
        {
            **row,
            "services": queries.get_services(conn, device_id),
            "latest": queries.get_latest_reading(conn, device_id),
            "open_alerts": alerts,
        }
    )
```

In `api/edgeio_api/app.py`, change `from .routers import health` to `from .routers import devices, health` and add after the health router:
```python
    app.include_router(devices.router, prefix=API_PREFIX)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest api/tests tests/integration/test_api_devices.py -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy api/edgeio_api`

```bash
git add api tests
git commit -m "feat(api): device list and detail endpoints"
```

---

### Task 4: Device metrics time series

**Files:**
- Modify: `api/edgeio_api/schemas.py`, `api/edgeio_api/queries.py`, `api/edgeio_api/routers/devices.py`
- Test: `api/tests/test_buckets.py`, `tests/integration/test_api_metrics.py`

**Interfaces:**
- Consumes: `seed`, `refresh_rollups` fixtures (Tasks 1, 3); `device_exists` (Task 3).
- Produces (`schemas`): `MetricPoint(ts: datetime, value: float|None, max: float|None = None)`; `MetricSeries(device_id: str, metric: MetricName, unit: str, bucket: Bucket, start: datetime, end: datetime, points: list[MetricPoint])`.
- Produces (`queries`): frozen dataclass `MetricSource(raw, avg, max, unit)`; `METRICS: dict[str, MetricSource]`; `choose_bucket(start, end) -> Bucket` (≤ 24 h → `raw`, ≤ 30 d → `1h`, else `1d`); `metric_points(conn, device_id, metric, bucket, start, end) -> list[Row]`.
- Produces (route): `GET /api/v1/devices/{device_id}/metrics?metric=&from=&to=&bucket=`. The default range is the last 24 h. Naive timestamps are treated as UTC.

- [ ] **Step 1: Write the failing tests**

`api/tests/test_buckets.py`:
```python
from datetime import UTC, datetime, timedelta

import pytest

from edgeio_api.queries import METRICS, choose_bucket

T0 = datetime(2026, 10, 6, tzinfo=UTC)


@pytest.mark.parametrize(
    ("span", "bucket"),
    [
        (timedelta(hours=1), "raw"),
        (timedelta(hours=24), "raw"),
        (timedelta(hours=25), "1h"),
        (timedelta(days=30), "1h"),
        (timedelta(days=31), "1d"),
    ],
)
def test_choose_bucket(span: timedelta, bucket: str) -> None:
    assert choose_bucket(T0, T0 + span) == bucket


def test_every_metric_has_raw_and_rollup_columns() -> None:
    assert set(METRICS) == {"cpu", "temperature", "ram", "disk", "packet_loss", "rx_rate", "tx_rate"}
    assert all(m.raw and m.avg and m.max and m.unit for m in METRICS.values())
```

`tests/integration/test_api_metrics.py`:
```python
from typing import Any

import pytest
from fastapi.testclient import TestClient

URL = "/api/v1/devices/100.64.0.1/metrics"
T0 = "2026-10-06T09:00:00Z"


@pytest.fixture
def series(seed, refresh_rollups) -> None:
    # 13 readings, 5 min apart, 09:00-10:00; cpu 10..22; rx grows 3 MB per reading.
    for i in range(13):
        seed(
            1,
            minutes=5 * i,
            changes={"system.cpu_usage_percent": 10.0 + i, "network.rx_bytes": 1_000_000 + 3_000_000 * i},
        )
    refresh_rollups()


def get(api: TestClient, **params: Any) -> Any:
    response = api.get(URL, params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_raw_points_within_a_day(api: TestClient, series: None) -> None:
    body = get(api, metric="cpu", **{"from": T0, "to": "2026-10-06T11:00:00Z"})
    assert (body["bucket"], body["unit"], body["metric"]) == ("raw", "%", "cpu")
    assert len(body["points"]) == 13
    assert body["points"][0] == {"ts": T0, "value": 10.0, "max": None}
    assert body["points"][-1]["value"] == 22.0


def test_rates_start_null_then_bits_per_second(api: TestClient, series: None) -> None:
    body = get(api, metric="rx_rate", **{"from": T0, "to": "2026-10-06T11:00:00Z"})
    assert body["unit"] == "bps"
    assert body["points"][0]["value"] is None
    assert body["points"][1]["value"] == 80_000.0


def test_hourly_bucket_averages_and_maxes(api: TestClient, series: None) -> None:
    body = get(api, metric="cpu", bucket="1h", **{"from": T0, "to": "2026-10-06T11:00:00Z"})
    assert body["points"] == [
        {"ts": "2026-10-06T09:00:00Z", "value": 15.5, "max": 21.0},
        {"ts": "2026-10-06T10:00:00Z", "value": 22.0, "max": 22.0},
    ]


def test_bucket_is_chosen_from_the_range(api: TestClient, series: None) -> None:
    week = get(api, metric="cpu", **{"from": "2026-10-04T00:00:00Z", "to": "2026-10-07T00:00:00Z"})
    assert week["bucket"] == "1h"
    quarter = get(api, metric="cpu", **{"from": "2026-08-01T00:00:00Z", "to": "2026-10-07T00:00:00Z"})
    assert quarter["bucket"] == "1d"
    assert quarter["points"] == [{"ts": "2026-10-06T00:00:00Z", "value": 16.0, "max": 22.0}]


def test_naive_timestamps_are_treated_as_utc(api: TestClient, series: None) -> None:
    body = get(api, metric="cpu", **{"from": "2026-10-06T09:00:00", "to": "2026-10-06T09:10:00"})
    assert [p["ts"] for p in body["points"]] == [T0, "2026-10-06T09:05:00Z"]
    assert body["start"] == T0


def test_inverted_range_is_rejected(api: TestClient, series: None) -> None:
    response = api.get(URL, params={"metric": "cpu", "from": "2026-10-06T10:00:00Z", "to": T0})
    assert response.status_code == 422


def test_unknown_metric_is_rejected(api: TestClient, series: None) -> None:
    assert api.get(URL, params={"metric": "gpu"}).status_code == 422


def test_unknown_device_is_404(api: TestClient) -> None:
    response = api.get("/api/v1/devices/100.64.0.99/metrics", params={"metric": "cpu"})
    assert response.status_code == 404
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest api/tests/test_buckets.py tests/integration/test_api_metrics.py -v`
Expected: FAIL — `ImportError: cannot import name 'METRICS'`; metrics route 404

- [ ] **Step 3: Implement**

Append to `api/edgeio_api/schemas.py`:
```python
class MetricPoint(BaseModel):
    ts: datetime
    value: float | None
    max: float | None = None


class MetricSeries(BaseModel):
    device_id: str
    metric: MetricName
    unit: str
    bucket: Bucket
    start: datetime
    end: datetime
    points: list[MetricPoint]
```

Append to `api/edgeio_api/queries.py`. Add `from dataclasses import dataclass`, `from datetime import datetime, timedelta` and `from psycopg import sql` to its imports, and extend the schemas import with `Bucket, MetricName`:
```python
@dataclass(frozen=True)
class MetricSource:
    raw: str  # column in health_readings
    avg: str  # column in the rollup views
    max: str
    unit: str


METRICS: dict[str, MetricSource] = {
    "cpu": MetricSource("cpu_usage_percent", "cpu_avg", "cpu_max", "%"),
    "temperature": MetricSource("cpu_temperature_c", "temp_avg", "temp_max", "°C"),
    "ram": MetricSource("ram_usage_percent", "ram_avg", "ram_max", "%"),
    "disk": MetricSource("disk_usage_percent", "disk_avg", "disk_max", "%"),
    "packet_loss": MetricSource("packet_loss_percent", "packet_loss_avg", "packet_loss_max", "%"),
    "rx_rate": MetricSource("rx_rate_bps", "rx_rate_avg", "rx_rate_max", "bps"),
    "tx_rate": MetricSource("tx_rate_bps", "tx_rate_avg", "tx_rate_max", "bps"),
}

_ROLLUPS: dict[str, tuple[str, str]] = {
    "1h": ("health_hourly", "1 hour"),
    "1d": ("health_daily", "1 day"),
}
RAW_MAX_SPAN = timedelta(hours=24)
HOURLY_MAX_SPAN = timedelta(days=30)


def choose_bucket(start: datetime, end: datetime) -> Bucket:
    span = end - start
    if span <= RAW_MAX_SPAN:
        return "raw"
    if span <= HOURLY_MAX_SPAN:
        return "1h"
    return "1d"


def metric_points(
    conn: DbConn,
    device_id: IPv4Address,
    metric: MetricName,
    bucket: Bucket,
    start: datetime,
    end: datetime,
) -> list[Row]:
    source = METRICS[metric]
    if bucket == "raw":
        query = sql.SQL(
            "SELECT ts, {value} AS value, NULL::double precision AS max FROM health_readings "
            "WHERE device_id = %s AND ts >= %s AND ts < %s ORDER BY ts"
        ).format(value=sql.Identifier(source.raw))
        return conn.execute(query, (device_id, start, end)).fetchall()
    view, width = _ROLLUPS[bucket]
    query = sql.SQL(
        "SELECT bucket AS ts, {avg} AS value, {max} AS max FROM {view} "
        "WHERE device_id = %s AND bucket >= time_bucket(%s::interval, %s::timestamptz) "
        "AND bucket < %s ORDER BY bucket"
    ).format(
        avg=sql.Identifier(source.avg),
        max=sql.Identifier(source.max),
        view=sql.Identifier(view),
    )
    return conn.execute(query, (device_id, width, start, end)).fetchall()
```

Add to `api/edgeio_api/routers/devices.py`. Add `from datetime import UTC, datetime, timedelta` to its imports, and extend the schemas import with `Bucket, MetricName, MetricPoint, MetricSeries`:
```python
def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@router.get("/devices/{device_id}/metrics", responses=NOT_FOUND)
def get_metrics(
    conn: Conn,
    device_id: IPv4Address,
    metric: MetricName,
    start: Annotated[datetime | None, Query(alias="from")] = None,
    end: Annotated[datetime | None, Query(alias="to")] = None,
    bucket: Bucket | None = None,
) -> MetricSeries:
    end = _utc(end) if end else datetime.now(UTC)
    start = _utc(start) if start else end - timedelta(hours=24)
    if start >= end:
        raise HTTPException(status_code=422, detail="'from' must be earlier than 'to'")
    if not queries.device_exists(conn, device_id):
        raise HTTPException(status_code=404, detail="device not found")
    chosen = bucket or queries.choose_bucket(start, end)
    rows = queries.metric_points(conn, device_id, metric, chosen, start, end)
    return MetricSeries(
        device_id=str(device_id),
        metric=metric,
        unit=queries.METRICS[metric].unit,
        bucket=chosen,
        start=start,
        end=end,
        points=[MetricPoint.model_validate(r) for r in rows],
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest api/tests tests/integration/test_api_metrics.py -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy api/edgeio_api`

```bash
git add api tests
git commit -m "feat(api): device metric time series with automatic rollup selection"
```

---

### Task 5: Alerts endpoint

**Files:**
- Create: `api/edgeio_api/routers/alerts.py`
- Modify: `api/edgeio_api/schemas.py` (`AlertList`), `api/edgeio_api/app.py` (include router)
- Test: `tests/integration/test_api_alerts.py`

**Interfaces:**
- Consumes: `queries.list_alerts`, `Alert` (Task 3); `seed` fixture.
- Produces: `AlertList(items: list[Alert], total, limit, offset)`; `GET /api/v1/alerts?state=open|resolved|all&device_id=&severity=&limit=&offset=`. The default state is `open`. Results are ordered by `opened_at` descending.

- [ ] **Step 1: Write the failing tests**

`tests/integration/test_api_alerts.py`:
```python
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def history(seed) -> None:
    seed(1, 0, {"system.cpu_temperature_c": 90})  # opens cpu_temp_high 09:00 ...
    seed(1, 5, {"system.cpu_temperature_c": 65})  # ... resolved 09:05
    seed(2, 5, {"services.edge_streamer": "failed"})  # service_down critical, 09:05
    seed(3, 10, {"system.cpu_temperature_c": 80})  # cpu_temp_high warning, 09:10


def rules(body: dict) -> list[tuple[str, str]]:
    return [(a["hostname"], a["rule"]) for a in body["items"]]


def test_open_alerts_by_default_newest_first(api: TestClient, history: None) -> None:
    body = api.get("/api/v1/alerts").json()
    assert body["total"] == 2
    assert rules(body) == [("edge-003", "cpu_temp_high"), ("edge-002", "service_down")]
    assert body["items"][0]["resolved_at"] is None


def test_resolved_alerts(api: TestClient, history: None) -> None:
    body = api.get("/api/v1/alerts", params={"state": "resolved"}).json()
    assert rules(body) == [("edge-001", "cpu_temp_high")]
    assert body["items"][0]["resolved_at"] == "2026-10-06T09:05:00Z"


def test_all_alerts(api: TestClient, history: None) -> None:
    body = api.get("/api/v1/alerts", params={"state": "all"}).json()
    assert [a["hostname"] for a in body["items"]] == ["edge-003", "edge-002", "edge-001"]


def test_filter_by_device_and_severity(api: TestClient, history: None) -> None:
    by_device = api.get("/api/v1/alerts", params={"state": "all", "device_id": "100.64.0.1"})
    assert rules(by_device.json()) == [("edge-001", "cpu_temp_high")]
    critical = api.get("/api/v1/alerts", params={"severity": "critical"}).json()
    assert rules(critical) == [("edge-002", "service_down")]


def test_rejects_unknown_state(api: TestClient) -> None:
    assert api.get("/api/v1/alerts", params={"state": "closed"}).status_code == 422
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_api_alerts.py -v`
Expected: FAIL — `/api/v1/alerts` returns 404

- [ ] **Step 3: Implement**

Append to `api/edgeio_api/schemas.py`:
```python
class AlertList(BaseModel):
    items: list[Alert]
    total: int
    limit: int
    offset: int
```

`api/edgeio_api/routers/alerts.py`:
```python
from ipaddress import IPv4Address
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from .. import queries
from ..db import DbConn, get_conn
from ..schemas import Alert, AlertList, AlertState, Severity

router = APIRouter(tags=["alerts"])
Conn = Annotated[DbConn, Depends(get_conn)]


@router.get("/alerts")
def list_alerts(
    conn: Conn,
    state: AlertState = "open",
    device_id: IPv4Address | None = None,
    severity: Severity | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> AlertList:
    rows, total = queries.list_alerts(
        conn, state=state, device_id=device_id, severity=severity, limit=limit, offset=offset
    )
    return AlertList(
        items=[Alert.model_validate(r) for r in rows], total=total, limit=limit, offset=offset
    )
```

In `app.py`: `from .routers import alerts, devices, health` and `app.include_router(alerts.router, prefix=API_PREFIX)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_api_alerts.py -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy api/edgeio_api`

```bash
git add api tests
git commit -m "feat(api): alerts endpoint with state, device and severity filters"
```

---

### Task 6: Fleet summary

**Files:**
- Create: `api/edgeio_api/routers/fleet.py`
- Modify: `api/edgeio_api/schemas.py`, `api/edgeio_api/queries.py`, `api/edgeio_api/app.py`
- Test: `tests/integration/test_api_fleet.py` (extend)

**Interfaces:**
- Consumes: `seed` fixture; `sweep_offline` (Plan 1).
- Produces (`schemas`): `StatusCounts(healthy, warning, critical, offline, total)`, `AlertCounts(warning, critical)`, `RankedDevice(device_id, hostname, value: float)`, `FleetSummary(devices: StatusCounts, open_alerts: AlertCounts, hottest: list[RankedDevice], fullest_disks: list[RankedDevice])`.
- Produces (`queries`): `status_counts(conn) -> Row`, `open_alert_counts(conn) -> Row`, `top_devices(conn, column: Literal["cpu_temperature_c","disk_usage_percent"], limit) -> list[Row]` (offline devices excluded).
- Produces: `GET /api/v1/fleet/summary?top=5` (1–20).

- [ ] **Step 1: Write the failing tests**

Append to `tests/integration/test_api_fleet.py`. Merge the imports into the file's import block:
```python
from datetime import UTC, datetime, timedelta

import pytest

from edgeio_worker.sweeper import sweep_offline

BASE = datetime(2026, 10, 6, 9, 0, tzinfo=UTC)
FULL_DISK = {"disk.root_used_gb": 430, "disk.root_free_gb": 46, "disk.root_usage_percent": 90.3}


@pytest.fixture
def fleet(seed, conn) -> None:
    seed(1, 30)  # healthy, 57.2 °C, disk 61.1 %
    seed(2, 30, {"system.cpu_temperature_c": 80})  # warning
    seed(3, 30, {"system.cpu_temperature_c": 90, **FULL_DISK})  # critical + disk warning
    seed(4, 0, {"system.cpu_temperature_c": 99})  # critical, then silent → offline
    sweep_offline(conn, BASE + timedelta(minutes=40), timedelta(minutes=15))


def test_fleet_summary(api: TestClient, fleet: None) -> None:
    body = api.get("/api/v1/fleet/summary", params={"top": 2}).json()
    assert body["devices"] == {"healthy": 1, "warning": 1, "critical": 1, "offline": 1, "total": 4}
    assert body["open_alerts"] == {"warning": 2, "critical": 3}
    assert [(d["hostname"], d["value"]) for d in body["hottest"]] == [
        ("edge-003", 90.0),
        ("edge-002", 80.0),
    ]  # the offline device's stale 99 °C is excluded
    assert [(d["hostname"], d["value"]) for d in body["fullest_disks"]] == [
        ("edge-003", 90.3),
        ("edge-001", 61.1),
    ]


def test_empty_fleet_summary(api: TestClient, conn) -> None:
    body = api.get("/api/v1/fleet/summary").json()
    assert body["devices"] == {"healthy": 0, "warning": 0, "critical": 0, "offline": 0, "total": 0}
    assert body["open_alerts"] == {"warning": 0, "critical": 0}
    assert body["hottest"] == [] and body["fullest_disks"] == []


def test_top_is_bounded(api: TestClient) -> None:
    assert api.get("/api/v1/fleet/summary", params={"top": 0}).status_code == 422
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_api_fleet.py -v`
Expected: FAIL — `/api/v1/fleet/summary` returns 404

- [ ] **Step 3: Implement**

Append to `api/edgeio_api/schemas.py`:
```python
class StatusCounts(BaseModel):
    healthy: int
    warning: int
    critical: int
    offline: int
    total: int


class AlertCounts(BaseModel):
    warning: int
    critical: int


class RankedDevice(BaseModel):
    device_id: str
    hostname: str
    value: float


class FleetSummary(BaseModel):
    devices: StatusCounts
    open_alerts: AlertCounts
    hottest: list[RankedDevice]
    fullest_disks: list[RankedDevice]
```

Append to `api/edgeio_api/queries.py` (add `Literal` to the `typing` import):
```python
def status_counts(conn: DbConn) -> Row:
    row = conn.execute(
        "SELECT count(*) FILTER (WHERE status = 'healthy') AS healthy, "
        "count(*) FILTER (WHERE status = 'warning') AS warning, "
        "count(*) FILTER (WHERE status = 'critical') AS critical, "
        "count(*) FILTER (WHERE status = 'offline') AS offline, "
        "count(*) AS total FROM devices"
    ).fetchone()
    assert row is not None  # an aggregate always returns one row
    return row


def open_alert_counts(conn: DbConn) -> Row:
    row = conn.execute(
        "SELECT count(*) FILTER (WHERE severity = 'warning') AS warning, "
        "count(*) FILTER (WHERE severity = 'critical') AS critical "
        "FROM alerts WHERE resolved_at IS NULL"
    ).fetchone()
    assert row is not None
    return row


def top_devices(
    conn: DbConn, column: Literal["cpu_temperature_c", "disk_usage_percent"], limit: int
) -> list[Row]:
    query = sql.SQL(
        "SELECT host(device_id) AS device_id, hostname, {col} AS value FROM devices "
        "WHERE {col} IS NOT NULL AND status <> 'offline' "
        "ORDER BY {col} DESC, hostname LIMIT %s"
    ).format(col=sql.Identifier(column))
    return conn.execute(query, (limit,)).fetchall()
```

`api/edgeio_api/routers/fleet.py`:
```python
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from .. import queries
from ..db import DbConn, get_conn
from ..schemas import AlertCounts, FleetSummary, RankedDevice, StatusCounts

router = APIRouter(tags=["fleet"])
Conn = Annotated[DbConn, Depends(get_conn)]


@router.get("/fleet/summary")
def fleet_summary(conn: Conn, top: Annotated[int, Query(ge=1, le=20)] = 5) -> FleetSummary:
    return FleetSummary(
        devices=StatusCounts.model_validate(queries.status_counts(conn)),
        open_alerts=AlertCounts.model_validate(queries.open_alert_counts(conn)),
        hottest=[
            RankedDevice.model_validate(r)
            for r in queries.top_devices(conn, "cpu_temperature_c", top)
        ],
        fullest_disks=[
            RankedDevice.model_validate(r)
            for r in queries.top_devices(conn, "disk_usage_percent", top)
        ],
    )
```

In `app.py`: `from .routers import alerts, devices, fleet, health` and `app.include_router(fleet.router, prefix=API_PREFIX)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_api_fleet.py -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy api/edgeio_api`

```bash
git add api tests
git commit -m "feat(api): fleet summary with status counts and top devices"
```

---

### Task 7: OpenAPI export and Makefile API targets

**Files:**
- Create: `api/edgeio_api/export_openapi.py`, `dashboard/openapi.json` (generated)
- Modify: `Makefile`
- Test: `api/tests/test_openapi.py`

**Interfaces:**
- Produces: `build_openapi() -> dict[str, Any]`; CLI `python -m edgeio_api.export_openapi`; committed `dashboard/openapi.json` (Task 8 generates TS types from it); `make openapi`, `make dev-api`.

- [ ] **Step 1: Write the failing test**

`api/tests/test_openapi.py`:
```python
import json
from pathlib import Path

from edgeio_api.export_openapi import build_openapi

OPENAPI_FILE = Path(__file__).parents[2] / "dashboard" / "openapi.json"


def test_committed_openapi_matches_the_app() -> None:
    assert json.loads(OPENAPI_FILE.read_text()) == build_openapi(), "run `make openapi`"


def test_every_endpoint_is_documented() -> None:
    assert set(build_openapi()["paths"]) == {
        "/api/v1/healthz",
        "/api/v1/devices",
        "/api/v1/devices/{device_id}",
        "/api/v1/devices/{device_id}/metrics",
        "/api/v1/alerts",
        "/api/v1/fleet/summary",
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest api/tests/test_openapi.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgeio_api.export_openapi'`

- [ ] **Step 3: Implement the export and generate the file**

`api/edgeio_api/export_openapi.py`:
```python
"""Write the OpenAPI document the dashboard generates its types from."""

import json
from typing import Any

from .app import create_app
from .config import ApiSettings


def build_openapi() -> dict[str, Any]:
    # Building the schema never touches the database; the pool is not opened.
    return create_app(ApiSettings(database_url="postgresql://unused")).openapi()


def main() -> None:
    print(json.dumps(build_openapi(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
```

Add to `Makefile`. Add `openapi dev-api dev-dashboard` to `.PHONY`:
```make
openapi:
	uv run python -m edgeio_api.export_openapi > dashboard/openapi.json
	@if [ -d dashboard/node_modules ]; then npm --prefix dashboard run gen:api; fi

dev-api:
	uv run python -m edgeio_api

dev-dashboard:
	npm --prefix dashboard run dev
```

Run: `mkdir -p dashboard && make openapi`
Expected: `dashboard/openapi.json` exists; `grep -c '"/api/v1/' dashboard/openapi.json` prints 6.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest api/tests -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
uv run ruff format . && uv run ruff check . && uv run mypy api/edgeio_api
git add api Makefile dashboard/openapi.json
git commit -m "feat(api): export OpenAPI document for the dashboard"
```

---

### Task 8: Dashboard scaffold, generated API client, formatting helpers

**Files:**
- Create: `dashboard/package.json` (+ `package-lock.json` via npm), `dashboard/index.html`, `dashboard/vite.config.ts`, `dashboard/tsconfig.json`, `dashboard/eslint.config.js`, `dashboard/.prettierrc`, `dashboard/.prettierignore`
- Create: `dashboard/src/api/schema.d.ts` (generated), `dashboard/src/api/client.ts`, `dashboard/src/api/types.ts`, `dashboard/src/api/queryClient.ts`, `dashboard/src/api/hooks.ts`
- Create: `dashboard/src/lib/format.ts`, `dashboard/src/lib/range.ts`, `dashboard/src/lib/status.ts`, `dashboard/src/lib/metrics.ts`
- Create: `dashboard/src/test/setup.ts`, `dashboard/src/test/render.tsx`, `dashboard/src/test/api.ts`, `dashboard/src/test/fixtures.ts`
- Modify: `.gitignore` (dashboard build output)
- Test: `dashboard/src/lib/format.test.ts`, `dashboard/src/lib/range.test.ts`, `dashboard/src/api/client.test.ts`

**Interfaces:**
- Consumes: `dashboard/openapi.json` (Task 7).
- Produces (`api/client.ts`): `api` (openapi-fetch client; fetch is resolved per call); `class ApiError(status, message)`; `unwrap<T>(request) -> Promise<T>` (throws `ApiError` on non-2xx).
- Produces (`api/types.ts`): `DeviceStatus, DeviceSummary, DeviceDetail, Alert, AlertList, FleetSummary, StatusCounts, MetricSeries, MetricPoint, MetricName, Severity, ServiceStatus, LatestReading, AlertState`.
- Produces (`api/queryClient.ts`): `REFRESH_INTERVAL_MS = 30_000`; `createQueryClient()`.
- Produces (`api/hooks.ts`): `useFleetSummary()`, `useDevices(status?)`, `useDevice(deviceId)`, `useMetric(deviceId, metric, range)`, `useAlerts({state, deviceId?, severity?})`.
- Produces (`lib`): `formatUptime, formatBitsPerSecond, formatPercent, formatCelsius, formatRelative, formatTimestamp, formatAxisTime`; `RANGES, RANGE_KEYS, RangeKey, rangeBounds(range, now?)`; `STATUS_ORDER, STATUS_META, isDeviceStatus`; `ChartSpec, CHARTS`.
- Produces (test utils): `renderRoute(ui, {path?, route?})`; `stubApi(routes) -> URL[]` (records requests; unknown paths → 404 JSON); fixtures `device()`, `deviceDetail()`, `alert()`, `fleetSummary()`, `metricSeries()`.

- [ ] **Step 1: Create the project and install dependencies**

`dashboard/package.json`:
```json
{
  "name": "edgeio-dashboard",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc --noEmit && vite build",
    "preview": "vite preview",
    "test": "vitest run",
    "lint": "eslint . && prettier --check . && tsc --noEmit",
    "format": "prettier --write .",
    "gen:api": "openapi-typescript openapi.json -o src/api/schema.d.ts"
  }
}
```

Run (from `dashboard/`):
```bash
npm install react react-dom react-router @tanstack/react-query recharts openapi-fetch
npm install -D vite @vitejs/plugin-react typescript @types/react @types/react-dom \
  vitest jsdom @testing-library/react @testing-library/dom @testing-library/jest-dom \
  @testing-library/user-event openapi-typescript eslint @eslint/js typescript-eslint \
  eslint-plugin-react-hooks globals prettier
npm run gen:api
```
Expected: `package-lock.json` and `node_modules/` exist; `src/api/schema.d.ts` contains `export interface paths`.

`dashboard/index.html`:
```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>edgeio fleet</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

`dashboard/vite.config.ts`:
```ts
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { "/api": "http://localhost:8000" },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
  },
});
```

`dashboard/tsconfig.json`:
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2022", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "isolatedModules": true,
    "skipLibCheck": true,
    "noEmit": true,
    "types": ["vite/client"]
  },
  "include": ["src", "vite.config.ts"]
}
```

`dashboard/eslint.config.js`:
```js
import js from "@eslint/js";
import reactHooks from "eslint-plugin-react-hooks";
import globals from "globals";
import tseslint from "typescript-eslint";

export default [
  { ignores: ["dist", "src/api/schema.d.ts"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: { globals: globals.browser },
    plugins: { "react-hooks": reactHooks },
    rules: {
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",
    },
  },
];
```

`dashboard/.prettierrc`:
```json
{ "printWidth": 100 }
```

`dashboard/.prettierignore`:
```
dist
node_modules
package-lock.json
openapi.json
src/api/schema.d.ts
```

Append to root `.gitignore`:
```
dashboard/dist/
```

- [ ] **Step 2: Write the failing tests**

`dashboard/src/lib/format.test.ts`:
```ts
import { describe, expect, it } from "vitest";
import {
  formatBitsPerSecond,
  formatCelsius,
  formatPercent,
  formatRelative,
  formatUptime,
} from "./format";

describe("formatUptime", () => {
  it("shows days and hours for long uptimes", () => expect(formatUptime(382941)).toBe("4d 10h"));
  it("shows hours and minutes under a day", () => expect(formatUptime(3 * 3600 + 300)).toBe("3h 5m"));
  it("shows minutes under an hour", () => expect(formatUptime(42 * 60 + 5)).toBe("42m"));
  it("shows a dash when unknown", () => expect(formatUptime(null)).toBe("—"));
});

describe("formatBitsPerSecond", () => {
  it("keeps small rates in bps", () => expect(formatBitsPerSecond(950)).toBe("950 bps"));
  it("scales to kbps", () => expect(formatBitsPerSecond(80_000)).toBe("80.0 kbps"));
  it("scales to Mbps", () => expect(formatBitsPerSecond(1_234_567)).toBe("1.2 Mbps"));
  it("shows a dash for missing rates", () => expect(formatBitsPerSecond(null)).toBe("—"));
});

describe("percent and temperature", () => {
  it("formats percent with one decimal", () => expect(formatPercent(61.14)).toBe("61.1%"));
  it("formats celsius", () => expect(formatCelsius(57.2)).toBe("57.2 °C"));
  it("dashes missing values", () => expect(formatPercent(undefined)).toBe("—"));
});

describe("formatRelative", () => {
  const now = Date.parse("2026-10-06T10:00:00Z");
  it("says just now under a minute", () =>
    expect(formatRelative("2026-10-06T09:59:30Z", now)).toBe("just now"));
  it("uses minutes", () => expect(formatRelative("2026-10-06T09:55:00Z", now)).toBe("5 min ago"));
  it("uses hours", () => expect(formatRelative("2026-10-06T07:00:00Z", now)).toBe("3 h ago"));
  it("uses days", () => expect(formatRelative("2026-10-04T10:00:00Z", now)).toBe("2 d ago"));
});
```

`dashboard/src/lib/range.test.ts`:
```ts
import { describe, expect, it } from "vitest";
import { RANGE_KEYS, rangeBounds } from "./range";

describe("rangeBounds", () => {
  it("ends now and starts one range earlier", () => {
    expect(rangeBounds("24h", new Date("2026-10-06T12:00:00Z"))).toEqual({
      from: "2026-10-05T12:00:00.000Z",
      to: "2026-10-06T12:00:00.000Z",
    });
  });
  it("offers the presets in order", () => expect(RANGE_KEYS).toEqual(["1h", "24h", "7d", "30d"]));
});
```

`dashboard/src/api/client.test.ts`:
```ts
import { describe, expect, it } from "vitest";
import { stubApi } from "../test/api";
import { fleetSummary } from "../test/fixtures";
import { ApiError, api, unwrap } from "./client";

describe("unwrap", () => {
  it("returns the parsed body on success", async () => {
    stubApi({ "/api/v1/fleet/summary": fleetSummary() });
    const body = await unwrap(api.GET("/api/v1/fleet/summary"));
    expect(body.devices.total).toBe(50);
  });

  it("throws ApiError with the status and detail on failure", async () => {
    stubApi({});
    const request = unwrap(
      api.GET("/api/v1/devices/{device_id}", { params: { path: { device_id: "100.64.0.9" } } }),
    );
    await expect(request).rejects.toEqual(new ApiError(404, "Not Found"));
  });
});
```

`dashboard/src/test/setup.ts`:
```ts
import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";

// Recharts' ResponsiveContainer needs ResizeObserver, which jsdom lacks.
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}
globalThis.ResizeObserver ??= ResizeObserverStub as unknown as typeof ResizeObserver;

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});
```

`dashboard/src/test/api.ts`:
```ts
import { vi } from "vitest";

type RouteBody = unknown | ((url: URL) => unknown);

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/** Stub global fetch with JSON bodies keyed by URL pathname. Returns the requested URLs. */
export function stubApi(routes: Record<string, RouteBody>): URL[] {
  const requests: URL[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: Request) => {
      const url = new URL(input.url);
      requests.push(url);
      const route = routes[url.pathname];
      if (route === undefined) return json({ detail: "Not Found" }, 404);
      return json(typeof route === "function" ? (route as (u: URL) => unknown)(url) : route);
    }),
  );
  return requests;
}

/** Stub global fetch so every request fails like an API whose database is down. */
export function stubApiDown(): void {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => json({ detail: "database unavailable" }, 503)),
  );
}
```

`dashboard/src/test/render.tsx`:
```tsx
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactElement } from "react";
import { MemoryRouter, Route, Routes } from "react-router";

export function renderRoute(ui: ReactElement, { path = "/", route = "/" } = {}) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[route]}>
        <Routes>
          <Route path={path} element={ui} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}
```

`dashboard/src/test/fixtures.ts`:
```ts
import type {
  Alert,
  DeviceDetail,
  DeviceSummary,
  FleetSummary,
  MetricName,
  MetricSeries,
} from "../api/types";

export function device(overrides: Partial<DeviceSummary> = {}): DeviceSummary {
  return {
    device_id: "100.64.0.1",
    hostname: "edge-001",
    os: "Ubuntu 24.04",
    status: "healthy",
    last_seen: "2026-10-06T09:45:00Z",
    uptime_seconds: 382941,
    cpu_usage_percent: 43.7,
    cpu_temperature_c: 57.2,
    ram_usage_percent: 56.6,
    disk_usage_percent: 61.1,
    packet_loss_percent: 0,
    open_alert_count: 0,
    ...overrides,
  };
}

export function deviceDetail(overrides: Partial<DeviceDetail> = {}): DeviceDetail {
  return {
    ...device(),
    first_seen: "2026-10-01T00:00:00Z",
    services: [
      { name: "docker", state: "running", changed_at: "2026-10-01T00:00:00Z" },
      { name: "edge_streamer", state: "failed", changed_at: "2026-10-06T09:40:00Z" },
    ],
    latest: {
      ts: "2026-10-06T09:45:00Z",
      load_1m: 1.42,
      ram_used_mb: 9271,
      ram_total_mb: 16384,
      disk_used_gb: 291,
      disk_total_gb: 476,
      net_interface: "eth0",
      rx_rate_bps: 80000,
      tx_rate_bps: 8000,
      containers_running: 5,
      containers_stopped: 0,
    },
    open_alerts: [],
    ...overrides,
  };
}

export function alert(overrides: Partial<Alert> = {}): Alert {
  return {
    id: 1,
    device_id: "100.64.0.2",
    hostname: "edge-002",
    rule: "cpu_temp_high",
    severity: "critical",
    opened_at: "2026-10-06T09:00:00Z",
    resolved_at: null,
    last_value: 90,
    message: "CPU temperature at 90°C",
    ...overrides,
  };
}

export function fleetSummary(overrides: Partial<FleetSummary> = {}): FleetSummary {
  return {
    devices: { healthy: 47, warning: 1, critical: 2, offline: 0, total: 50 },
    open_alerts: { warning: 1, critical: 2 },
    hottest: [],
    fullest_disks: [],
    ...overrides,
  };
}

export function metricSeries(metric: MetricName, values: (number | null)[] = [40, 42]): MetricSeries {
  return {
    device_id: "100.64.0.1",
    metric,
    unit: "%",
    bucket: "raw",
    start: "2026-10-06T09:00:00Z",
    end: "2026-10-06T10:00:00Z",
    points: values.map((value, i) => ({
      ts: new Date(Date.parse("2026-10-06T09:00:00Z") + i * 300_000).toISOString(),
      value,
      max: null,
    })),
  };
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `npm --prefix dashboard test`
Expected: FAIL — cannot resolve `./format`, `./range`, `./client`, `../api/types`

- [ ] **Step 4: Implement the client, hooks and helpers**

`dashboard/src/api/client.ts`:
```ts
import createClient from "openapi-fetch";
import type { paths } from "./schema";

export const api = createClient<paths>({
  baseUrl: import.meta.env.VITE_API_BASE_URL || window.location.origin,
  // Look fetch up per call so tests can stub it.
  fetch: (request: Request) => globalThis.fetch(request),
});

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

/** Resolve an openapi-fetch call to its data, or throw ApiError for a non-2xx response. */
export async function unwrap<T>(
  request: Promise<{ data?: T; error?: unknown; response: Response }>,
): Promise<T> {
  const { data, error, response } = await request;
  if (data === undefined) {
    const detail =
      typeof error === "object" && error !== null && "detail" in error
        ? String(error.detail)
        : response.statusText;
    throw new ApiError(response.status, detail);
  }
  return data;
}
```

`dashboard/src/api/types.ts`:
```ts
import type { components, paths } from "./schema";

type Schemas = components["schemas"];
type AlertsQuery = NonNullable<paths["/api/v1/alerts"]["get"]["parameters"]["query"]>;

export type DeviceSummary = Schemas["DeviceSummary"];
export type DeviceDetail = Schemas["DeviceDetail"];
export type DeviceStatus = DeviceSummary["status"];
export type StatusCounts = Schemas["StatusCounts"];
export type FleetSummary = Schemas["FleetSummary"];
export type Alert = Schemas["Alert"];
export type AlertList = Schemas["AlertList"];
export type Severity = Alert["severity"];
export type AlertState = NonNullable<AlertsQuery["state"]>;
export type ServiceStatus = Schemas["ServiceStatus"];
export type LatestReading = Schemas["LatestReading"];
export type MetricSeries = Schemas["MetricSeries"];
export type MetricPoint = Schemas["MetricPoint"];
export type MetricName = MetricSeries["metric"];
```

`dashboard/src/api/queryClient.ts`:
```ts
import { QueryClient } from "@tanstack/react-query";
import { ApiError } from "./client";

export const REFRESH_INTERVAL_MS = 30_000;

export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        refetchInterval: REFRESH_INTERVAL_MS,
        staleTime: 10_000,
        // Client errors (404/422) won't fix themselves; retry only server/network failures.
        retry: (failureCount, error) =>
          !(error instanceof ApiError && error.status < 500) && failureCount < 2,
      },
    },
  });
}
```

`dashboard/src/api/hooks.ts`:
```ts
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { type RangeKey, rangeBounds } from "../lib/range";
import { api, unwrap } from "./client";
import type { AlertState, DeviceStatus, MetricName, Severity } from "./types";

export function useFleetSummary() {
  return useQuery({
    queryKey: ["fleet-summary"],
    queryFn: () => unwrap(api.GET("/api/v1/fleet/summary")),
  });
}

export function useDevices(status: DeviceStatus | undefined) {
  return useQuery({
    queryKey: ["devices", status ?? "all"],
    queryFn: () =>
      unwrap(api.GET("/api/v1/devices", { params: { query: { status, limit: 500 } } })),
    placeholderData: keepPreviousData,
  });
}

export function useDevice(deviceId: string) {
  return useQuery({
    queryKey: ["device", deviceId],
    queryFn: () =>
      unwrap(api.GET("/api/v1/devices/{device_id}", { params: { path: { device_id: deviceId } } })),
  });
}

export function useMetric(deviceId: string, metric: MetricName, range: RangeKey) {
  return useQuery({
    queryKey: ["metric", deviceId, metric, range],
    queryFn: () => {
      const { from, to } = rangeBounds(range);
      return unwrap(
        api.GET("/api/v1/devices/{device_id}/metrics", {
          params: { path: { device_id: deviceId }, query: { metric, from, to } },
        }),
      );
    },
    placeholderData: keepPreviousData,
  });
}

export interface AlertFilters {
  state: AlertState;
  deviceId?: string;
  severity?: Severity;
}

export function useAlerts({ state, deviceId, severity }: AlertFilters) {
  return useQuery({
    queryKey: ["alerts", state, deviceId ?? null, severity ?? null],
    queryFn: () =>
      unwrap(
        api.GET("/api/v1/alerts", {
          params: { query: { state, device_id: deviceId, severity, limit: 200 } },
        }),
      ),
    placeholderData: keepPreviousData,
  });
}
```

`dashboard/src/lib/range.ts`:
```ts
const HOUR = 60 * 60 * 1000;

export const RANGES = { "1h": HOUR, "24h": 24 * HOUR, "7d": 7 * 24 * HOUR, "30d": 30 * 24 * HOUR };
export type RangeKey = keyof typeof RANGES;
export const RANGE_KEYS = Object.keys(RANGES) as RangeKey[];

export function rangeBounds(range: RangeKey, now: Date = new Date()): { from: string; to: string } {
  return { from: new Date(now.getTime() - RANGES[range]).toISOString(), to: now.toISOString() };
}
```

`dashboard/src/lib/format.ts`:
```ts
import type { RangeKey } from "./range";

const DASH = "—";

export function formatUptime(seconds: number | null | undefined): string {
  if (seconds == null) return DASH;
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

export function formatBitsPerSecond(bps: number | null | undefined): string {
  if (bps == null) return DASH;
  const units = ["bps", "kbps", "Mbps", "Gbps"];
  let value = bps;
  let unit = 0;
  while (value >= 1000 && unit < units.length - 1) {
    value /= 1000;
    unit += 1;
  }
  return unit === 0 ? `${Math.round(value)} bps` : `${value.toFixed(1)} ${units[unit]}`;
}

export function formatPercent(value: number | null | undefined): string {
  return value == null ? DASH : `${value.toFixed(1)}%`;
}

export function formatCelsius(value: number | null | undefined): string {
  return value == null ? DASH : `${value.toFixed(1)} °C`;
}

export function formatRelative(iso: string, now: number = Date.now()): string {
  const seconds = Math.max(0, Math.round((now - Date.parse(iso)) / 1000));
  if (seconds < 60) return "just now";
  if (seconds < 3_600) return `${Math.floor(seconds / 60)} min ago`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3_600)} h ago`;
  return `${Math.floor(seconds / 86_400)} d ago`;
}

export function formatTimestamp(iso: string): string {
  return new Date(iso).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

export function formatAxisTime(iso: string, range: RangeKey): string {
  const date = new Date(iso);
  return range === "1h" || range === "24h"
    ? date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })
    : date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}
```

`dashboard/src/lib/status.ts`:
```ts
import type { DeviceStatus } from "../api/types";

export const STATUS_ORDER: DeviceStatus[] = ["healthy", "warning", "critical", "offline"];

// Status is never color alone: every badge/tile pairs the color with this icon and label.
export const STATUS_META: Record<DeviceStatus, { label: string; icon: string }> = {
  healthy: { label: "Healthy", icon: "✓" },
  warning: { label: "Warning", icon: "!" },
  critical: { label: "Critical", icon: "✕" },
  offline: { label: "Offline", icon: "○" },
};

export function isDeviceStatus(value: string | null): value is DeviceStatus {
  return value !== null && (STATUS_ORDER as string[]).includes(value);
}
```

`dashboard/src/lib/metrics.ts`:
```ts
import type { MetricName } from "../api/types";
import { formatBitsPerSecond, formatCelsius, formatPercent } from "./format";

export interface ChartSpec {
  metric: MetricName;
  title: string;
  format: (value: number) => string;
  domain?: [number, number];
}

// One metric per chart: never two scales on one plot.
export const CHARTS: ChartSpec[] = [
  { metric: "cpu", title: "CPU usage", format: formatPercent, domain: [0, 100] },
  { metric: "temperature", title: "CPU temperature", format: formatCelsius },
  { metric: "ram", title: "RAM usage", format: formatPercent, domain: [0, 100] },
  { metric: "disk", title: "Disk usage", format: formatPercent, domain: [0, 100] },
  { metric: "rx_rate", title: "Network receive", format: formatBitsPerSecond },
  { metric: "tx_rate", title: "Network transmit", format: formatBitsPerSecond },
  { metric: "packet_loss", title: "Packet loss", format: formatPercent },
];
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `npm --prefix dashboard test && npm --prefix dashboard run format && npm --prefix dashboard run lint`
Expected: tests PASS; lint (eslint, prettier check, tsc) clean. If `tsc` reports a fixture field mismatch against the generated schema, the schema is the authority: fix the fixture.

- [ ] **Step 6: Commit**

```bash
git add .gitignore dashboard
git commit -m "feat(dashboard): scaffold with generated API client and formatting helpers"
```

---

### Task 9: Dashboard components

**Files:**
- Create: `dashboard/src/components/StatusBadge.tsx`, `StatusTiles.tsx`, `DeviceCard.tsx`, `AlertsTable.tsx`, `ServicesPanel.tsx`, `RangePicker.tsx`
- Test: `dashboard/src/components/StatusTiles.test.tsx`, `DeviceCard.test.tsx`, `AlertsTable.test.tsx`, `ServicesPanel.test.tsx`

**Interfaces:**
- Consumes: types, lib helpers, test utils (Task 8).
- Produces: `StatusBadge({status: DeviceStatus})`; `StatusTiles({counts: StatusCounts, selected?: DeviceStatus, onSelect(status | undefined)})`; `DeviceCard({device: DeviceSummary, now?: number})`; `AlertsTable({alerts: Alert[], showDevice?: boolean})`; `ServicesPanel({services: ServiceStatus[], latest?: LatestReading | null})`; `RangePicker({value: RangeKey, onChange(range)})`.

- [ ] **Step 1: Write the failing tests**

`dashboard/src/components/StatusTiles.test.tsx`:
```tsx
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { StatusTiles } from "./StatusTiles";

const counts = { healthy: 47, warning: 1, critical: 2, offline: 0, total: 50 };

describe("StatusTiles", () => {
  it("shows a count, icon and label for every status", () => {
    render(<StatusTiles counts={counts} onSelect={() => {}} />);
    expect(screen.getByRole("button", { name: /47\s*Healthy/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /2\s*Critical/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /0\s*Offline/ })).toBeInTheDocument();
  });

  it("selects a status and clears it on a second click", async () => {
    const onSelect = vi.fn();
    const { rerender } = render(<StatusTiles counts={counts} onSelect={onSelect} />);
    await userEvent.click(screen.getByRole("button", { name: /Critical/ }));
    expect(onSelect).toHaveBeenLastCalledWith("critical");

    rerender(<StatusTiles counts={counts} selected="critical" onSelect={onSelect} />);
    const tile = screen.getByRole("button", { name: /Critical/ });
    expect(tile).toHaveAttribute("aria-pressed", "true");
    await userEvent.click(tile);
    expect(onSelect).toHaveBeenLastCalledWith(undefined);
  });
});
```

`dashboard/src/components/DeviceCard.test.tsx`:
```tsx
import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { renderRoute } from "../test/render";
import { device } from "../test/fixtures";
import { DeviceCard } from "./DeviceCard";

describe("DeviceCard", () => {
  it("links to the device and shows its key metrics", () => {
    const now = Date.parse("2026-10-06T09:50:00Z");
    renderRoute(<DeviceCard device={device({ status: "critical", open_alert_count: 2 })} now={now} />);
    const link = screen.getByRole("link");
    expect(link).toHaveAttribute("href", "/devices/100.64.0.1");
    expect(screen.getByText("edge-001")).toBeInTheDocument();
    expect(screen.getByText("100.64.0.1")).toBeInTheDocument();
    expect(screen.getByText("Critical")).toBeInTheDocument();
    expect(screen.getByText("57.2 °C")).toBeInTheDocument();
    expect(screen.getByText(/5 min ago · 2 open alerts/)).toBeInTheDocument();
  });
});
```

`dashboard/src/components/AlertsTable.test.tsx`:
```tsx
import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { alert } from "../test/fixtures";
import { renderRoute } from "../test/render";
import { AlertsTable } from "./AlertsTable";

describe("AlertsTable", () => {
  it("lists alerts with a device link and open/resolved state", () => {
    renderRoute(
      <AlertsTable
        alerts={[alert(), alert({ id: 2, severity: "warning", resolved_at: "2026-10-06T09:30:00Z" })]}
      />,
    );
    const rows = screen.getAllByRole("row").slice(1);
    expect(rows).toHaveLength(2);
    expect(within(rows[0]).getByText("Critical")).toBeInTheDocument();
    expect(within(rows[0]).getByText("Open")).toBeInTheDocument();
    expect(within(rows[0]).getByRole("link", { name: "edge-002" })).toHaveAttribute(
      "href",
      "/devices/100.64.0.2",
    );
    expect(within(rows[1]).getByText("Warning")).toBeInTheDocument();
    expect(within(rows[1]).queryByText("Open")).not.toBeInTheDocument();
  });

  it("can hide the device column", () => {
    renderRoute(<AlertsTable alerts={[alert()]} showDevice={false} />);
    expect(screen.queryByRole("columnheader", { name: "Device" })).not.toBeInTheDocument();
  });

  it("shows an empty state", () => {
    renderRoute(<AlertsTable alerts={[]} />);
    expect(screen.getByText("No alerts.")).toBeInTheDocument();
  });
});
```

`dashboard/src/components/ServicesPanel.test.tsx`:
```tsx
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { deviceDetail } from "../test/fixtures";
import { ServicesPanel } from "./ServicesPanel";

describe("ServicesPanel", () => {
  it("shows each service state and container counts", () => {
    const detail = deviceDetail();
    render(<ServicesPanel services={detail.services} latest={detail.latest} />);
    expect(screen.getByText("docker")).toBeInTheDocument();
    expect(screen.getByText("running")).toBeInTheDocument();
    expect(screen.getByText("failed")).toBeInTheDocument();
    expect(screen.getByText(/Containers:/)).toHaveTextContent("Containers: 5 running, 0 stopped");
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm --prefix dashboard test`
Expected: FAIL — cannot resolve `./StatusTiles`, `./DeviceCard`, `./AlertsTable`, `./ServicesPanel`

- [ ] **Step 3: Implement the components**

`dashboard/src/components/StatusBadge.tsx`:
```tsx
import type { DeviceStatus } from "../api/types";
import { STATUS_META } from "../lib/status";

export function StatusBadge({ status }: { status: DeviceStatus }) {
  const { label, icon } = STATUS_META[status];
  return (
    <span className={`badge badge-${status}`}>
      <span aria-hidden="true" className="badge-icon">
        {icon}
      </span>
      {label}
    </span>
  );
}
```

`dashboard/src/components/StatusTiles.tsx`:
```tsx
import type { DeviceStatus, StatusCounts } from "../api/types";
import { STATUS_META, STATUS_ORDER } from "../lib/status";

interface Props {
  counts: StatusCounts;
  selected?: DeviceStatus;
  onSelect: (status: DeviceStatus | undefined) => void;
}

export function StatusTiles({ counts, selected, onSelect }: Props) {
  return (
    <div className="tiles" role="group" aria-label="Filter devices by status">
      {STATUS_ORDER.map((status) => {
        const { label, icon } = STATUS_META[status];
        const active = selected === status;
        return (
          <button
            key={status}
            type="button"
            className={`tile badge-${status}`}
            aria-pressed={active}
            onClick={() => onSelect(active ? undefined : status)}
          >
            <span className="tile-value">{counts[status]}</span>
            <span className="tile-label">
              <span aria-hidden="true" className="badge-icon">
                {icon}
              </span>
              {label}
            </span>
          </button>
        );
      })}
    </div>
  );
}
```

`dashboard/src/components/DeviceCard.tsx`:
```tsx
import { Link } from "react-router";
import type { DeviceSummary } from "../api/types";
import { formatCelsius, formatPercent, formatRelative } from "../lib/format";
import { StatusBadge } from "./StatusBadge";

export function DeviceCard({ device, now }: { device: DeviceSummary; now?: number }) {
  const alerts = device.open_alert_count;
  return (
    <Link to={`/devices/${device.device_id}`} className={`device-card badge-${device.status}`}>
      <div className="device-card-head">
        <span className="device-name">{device.hostname}</span>
        <StatusBadge status={device.status} />
      </div>
      <div className="muted mono">{device.device_id}</div>
      <dl className="device-metrics">
        <div>
          <dt>CPU</dt>
          <dd>{formatPercent(device.cpu_usage_percent)}</dd>
        </div>
        <div>
          <dt>Temp</dt>
          <dd>{formatCelsius(device.cpu_temperature_c)}</dd>
        </div>
        <div>
          <dt>Disk</dt>
          <dd>{formatPercent(device.disk_usage_percent)}</dd>
        </div>
      </dl>
      <div className="muted small">
        Last seen {formatRelative(device.last_seen, now)}
        {alerts > 0 && ` · ${alerts} open alert${alerts === 1 ? "" : "s"}`}
      </div>
    </Link>
  );
}
```

`dashboard/src/components/AlertsTable.tsx`:
```tsx
import { Link } from "react-router";
import type { Alert } from "../api/types";
import { formatTimestamp } from "../lib/format";
import { StatusBadge } from "./StatusBadge";

export function AlertsTable({ alerts, showDevice = true }: { alerts: Alert[]; showDevice?: boolean }) {
  if (alerts.length === 0) return <p className="empty">No alerts.</p>;
  return (
    <div className="table-wrap">
      <table className="table">
        <thead>
          <tr>
            <th>Severity</th>
            {showDevice && <th>Device</th>}
            <th>Rule</th>
            <th>Details</th>
            <th>Opened</th>
            <th>Resolved</th>
          </tr>
        </thead>
        <tbody>
          {alerts.map((a) => (
            <tr key={a.id}>
              <td>
                <StatusBadge status={a.severity} />
              </td>
              {showDevice && (
                <td>
                  <Link to={`/devices/${a.device_id}`}>{a.hostname ?? a.device_id}</Link>
                </td>
              )}
              <td className="mono">{a.rule}</td>
              <td>{a.message}</td>
              <td>{formatTimestamp(a.opened_at)}</td>
              <td>{a.resolved_at ? formatTimestamp(a.resolved_at) : <strong>Open</strong>}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

`dashboard/src/components/ServicesPanel.tsx`:
```tsx
import type { LatestReading, ServiceStatus } from "../api/types";

interface Props {
  services: ServiceStatus[];
  latest?: LatestReading | null;
}

export function ServicesPanel({ services, latest }: Props) {
  return (
    <section className="panel">
      <h2>Services</h2>
      {services.length === 0 ? (
        <p className="empty">No services reported.</p>
      ) : (
        <ul className="service-list">
          {services.map((service) => {
            const running = service.state === "running";
            return (
              <li key={service.name}>
                <span className="mono">{service.name}</span>
                <span className={`badge badge-${running ? "healthy" : "critical"}`}>
                  <span aria-hidden="true" className="badge-icon">
                    {running ? "✓" : "✕"}
                  </span>
                  {service.state}
                </span>
              </li>
            );
          })}
        </ul>
      )}
      {latest && (
        <p className="containers">
          Containers: <strong>{latest.containers_running}</strong> running,{" "}
          <strong>{latest.containers_stopped}</strong> stopped
        </p>
      )}
    </section>
  );
}
```

`dashboard/src/components/RangePicker.tsx`:
```tsx
import { RANGE_KEYS, type RangeKey } from "../lib/range";

export function RangePicker({
  value,
  onChange,
}: {
  value: RangeKey;
  onChange: (range: RangeKey) => void;
}) {
  return (
    <div className="segmented" role="group" aria-label="Time range">
      {RANGE_KEYS.map((key) => (
        <button key={key} type="button" aria-pressed={value === key} onClick={() => onChange(key)}>
          {key}
        </button>
      ))}
    </div>
  );
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm --prefix dashboard test && npm --prefix dashboard run format && npm --prefix dashboard run lint`
Expected: PASS; lint clean

- [ ] **Step 5: Commit**

```bash
git add dashboard
git commit -m "feat(dashboard): status, device, alert and service components"
```

---

### Task 10: Pages, charts, app shell and styles

**Files:**
- Create: `dashboard/src/components/MetricChart.tsx`, `dashboard/src/components/Layout.tsx`
- Create: `dashboard/src/pages/FleetOverview.tsx`, `dashboard/src/pages/DeviceDetail.tsx`, `dashboard/src/pages/AlertsPage.tsx`
- Create: `dashboard/src/App.tsx`, `dashboard/src/main.tsx`, `dashboard/src/styles.css`
- Test: `dashboard/src/pages/FleetOverview.test.tsx`, `dashboard/src/pages/DeviceDetail.test.tsx`, `dashboard/src/pages/AlertsPage.test.tsx`, `dashboard/src/components/MetricChart.test.tsx`

**Interfaces:**
- Consumes: hooks, components, lib, test utils (Tasks 8–9).
- Produces: routes `/` (fleet; `?status=`), `/devices/:deviceId`, `/alerts` (`?state=&severity=`); `MetricChart({deviceId, spec: ChartSpec, range: RangeKey})`.

- [ ] **Step 1: Write the failing tests**

`dashboard/src/pages/FleetOverview.test.tsx`:
```tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { stubApi, stubApiDown } from "../test/api";
import { device, fleetSummary } from "../test/fixtures";
import { renderRoute } from "../test/render";
import { FleetOverview } from "./FleetOverview";

const deviceList = (items = [device()]) => ({ items, total: items.length, limit: 500, offset: 0 });

describe("FleetOverview", () => {
  it("filters devices by the status in the URL", async () => {
    const hot = device({ device_id: "100.64.0.7", hostname: "edge-007", status: "critical" });
    const requests = stubApi({
      "/api/v1/fleet/summary": fleetSummary(),
      "/api/v1/devices": deviceList([hot]),
    });
    renderRoute(<FleetOverview />, { route: "/?status=critical" });
    expect(await screen.findByText("edge-007")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: /2\s*Critical/ })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    const call = requests.find((u) => u.pathname === "/api/v1/devices");
    expect(call?.searchParams.get("status")).toBe("critical");
  });

  it("clicking a tile requests that status", async () => {
    const requests = stubApi({
      "/api/v1/fleet/summary": fleetSummary(),
      "/api/v1/devices": deviceList(),
    });
    renderRoute(<FleetOverview />);
    await userEvent.click(await screen.findByRole("button", { name: /Warning/ }));
    await waitFor(() =>
      expect(
        requests.some(
          (u) => u.pathname === "/api/v1/devices" && u.searchParams.get("status") === "warning",
        ),
      ).toBe(true),
    );
  });

  it("shows an empty state when no devices match", async () => {
    stubApi({ "/api/v1/fleet/summary": fleetSummary(), "/api/v1/devices": deviceList([]) });
    renderRoute(<FleetOverview />);
    expect(await screen.findByText("No devices match this filter.")).toBeInTheDocument();
  });

  it("test_fleet_overview_shows_error_when_api_fails", async () => {
    stubApiDown();
    renderRoute(<FleetOverview />);
    expect(await screen.findByText("Couldn't load devices.")).toBeInTheDocument();
  });
});
```

`dashboard/src/pages/DeviceDetail.test.tsx`:
```tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { MetricName } from "../api/types";
import { stubApi } from "../test/api";
import { alert, deviceDetail, metricSeries } from "../test/fixtures";
import { renderRoute } from "../test/render";
import { DeviceDetail } from "./DeviceDetail";

const ROUTE = { path: "/devices/:deviceId", route: "/devices/100.64.0.2" };

function stubDevice() {
  return stubApi({
    "/api/v1/devices/100.64.0.2": deviceDetail({
      device_id: "100.64.0.2",
      hostname: "edge-002",
      status: "critical",
    }),
    "/api/v1/devices/100.64.0.2/metrics": (url: URL) =>
      metricSeries(url.searchParams.get("metric") as MetricName),
    "/api/v1/alerts": { items: [alert()], total: 1, limit: 200, offset: 0 },
  });
}

const spanHours = (u: URL) =>
  (Date.parse(u.searchParams.get("to") ?? "") - Date.parse(u.searchParams.get("from") ?? "")) /
  3_600_000;

describe("DeviceDetail", () => {
  it("shows the device header, services, charts and alert history", async () => {
    stubDevice();
    renderRoute(<DeviceDetail />, ROUTE);
    expect(await screen.findByRole("heading", { name: "edge-002" })).toBeInTheDocument();
    expect(screen.getByText("edge_streamer")).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "CPU usage" })).toBeInTheDocument();
    expect(await screen.findByText("CPU temperature at 90°C")).toBeInTheDocument();
  });

  it("requests a wider window when the range changes", async () => {
    const requests = stubDevice();
    renderRoute(<DeviceDetail />, ROUTE);
    await userEvent.click(await screen.findByRole("button", { name: "7d" }));
    await waitFor(() =>
      expect(requests.some((u) => u.pathname.endsWith("/metrics") && spanHours(u) === 168)).toBe(
        true,
      ),
    );
  });

  it("says when the device does not exist", async () => {
    stubApi({});
    renderRoute(<DeviceDetail />, ROUTE);
    expect(await screen.findByText("Device not found.")).toBeInTheDocument();
  });
});
```

`dashboard/src/pages/AlertsPage.test.tsx`:
```tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { stubApi } from "../test/api";
import { alert } from "../test/fixtures";
import { renderRoute } from "../test/render";
import { AlertsPage } from "./AlertsPage";

describe("AlertsPage", () => {
  it("loads open alerts, then filters by state and severity", async () => {
    const requests = stubApi({
      "/api/v1/alerts": { items: [alert()], total: 1, limit: 200, offset: 0 },
    });
    renderRoute(<AlertsPage />);
    expect(await screen.findByText("CPU temperature at 90°C")).toBeInTheDocument();
    expect(requests[0].searchParams.get("state")).toBe("open");

    await userEvent.click(screen.getByRole("button", { name: "resolved" }));
    await userEvent.selectOptions(screen.getByLabelText("Severity"), "critical");
    await waitFor(() => {
      const last = requests[requests.length - 1];
      expect(last.searchParams.get("state")).toBe("resolved");
      expect(last.searchParams.get("severity")).toBe("critical");
    });
  });
});
```

`dashboard/src/components/MetricChart.test.tsx`:
```tsx
import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { CHARTS } from "../lib/metrics";
import { stubApi } from "../test/api";
import { metricSeries } from "../test/fixtures";
import { renderRoute } from "../test/render";
import { MetricChart } from "./MetricChart";

const cpu = CHARTS[0];

describe("MetricChart", () => {
  it("shows the latest value and a data table", async () => {
    stubApi({ "/api/v1/devices/100.64.0.1/metrics": metricSeries("cpu", [40, 42.25]) });
    renderRoute(<MetricChart deviceId="100.64.0.1" spec={cpu} range="24h" />);
    expect(await screen.findAllByText("42.3%")).not.toHaveLength(0);
    expect(screen.getByText(/Data table/)).toBeInTheDocument();
  });

  it("test_metric_chart_empty_state", async () => {
    stubApi({ "/api/v1/devices/100.64.0.1/metrics": metricSeries("cpu", []) });
    renderRoute(<MetricChart deviceId="100.64.0.1" spec={cpu} range="24h" />);
    expect(await screen.findByText("No data in this range.")).toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm --prefix dashboard test`
Expected: FAIL — cannot resolve `./FleetOverview`, `./DeviceDetail`, `./AlertsPage`, `./MetricChart`

- [ ] **Step 3: Implement chart, pages, shell and styles**

`dashboard/src/components/MetricChart.tsx`:
```tsx
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useMetric } from "../api/hooks";
import type { MetricPoint } from "../api/types";
import { formatAxisTime, formatTimestamp } from "../lib/format";
import type { ChartSpec } from "../lib/metrics";
import type { RangeKey } from "../lib/range";

interface Props {
  deviceId: string;
  spec: ChartSpec;
  range: RangeKey;
}

const TICK = { fill: "var(--text-muted)", fontSize: 12 };

export function MetricChart({ deviceId, spec, range }: Props) {
  const { data, isPending, isError } = useMetric(deviceId, spec.metric, range);
  const points = data?.points ?? [];
  const latest = [...points].reverse().find((p) => p.value != null)?.value ?? null;
  const rollup = data !== undefined && data.bucket !== "raw";

  return (
    <section className="panel chart-card" aria-label={spec.title}>
      <header className="chart-head">
        <h3>{spec.title}</h3>
        <span className="chart-latest">{latest == null ? "—" : spec.format(latest)}</span>
      </header>
      {isError ? (
        <p className="error">Couldn't load this metric.</p>
      ) : isPending ? (
        <div className="chart-placeholder" />
      ) : points.length === 0 ? (
        <p className="empty">No data in this range.</p>
      ) : (
        <>
          <div className="chart-plot">
            <ResponsiveContainer width="100%" height={180}>
              <LineChart data={points} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                <CartesianGrid vertical={false} stroke="var(--grid)" />
                <XAxis
                  dataKey="ts"
                  tickFormatter={(ts: string) => formatAxisTime(ts, range)}
                  stroke="var(--axis)"
                  tick={TICK}
                  tickLine={false}
                  minTickGap={32}
                />
                <YAxis
                  domain={spec.domain ?? ["auto", "auto"]}
                  tickFormatter={(v: number) => spec.format(v)}
                  tick={TICK}
                  tickLine={false}
                  axisLine={false}
                  width={76}
                />
                <Tooltip
                  cursor={{ stroke: "var(--axis)", strokeWidth: 1 }}
                  content={<ChartTooltip format={spec.format} />}
                />
                <Line
                  type="monotone"
                  dataKey="value"
                  stroke="var(--series-1)"
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface)" }}
                  isAnimationActive={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <details className="chart-table">
            <summary>Data table ({rollup ? `${data.bucket} averages` : "raw readings"})</summary>
            <table className="table compact">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Value</th>
                  {rollup && <th>Max</th>}
                </tr>
              </thead>
              <tbody>
                {points.map((p) => (
                  <tr key={p.ts}>
                    <td>{formatTimestamp(p.ts)}</td>
                    <td>{p.value == null ? "—" : spec.format(p.value)}</td>
                    {rollup && <td>{p.max == null ? "—" : spec.format(p.max)}</td>}
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        </>
      )}
    </section>
  );
}

interface TooltipContent {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: unknown }>;
  format: (value: number) => string;
}

function ChartTooltip({ active, payload, format }: TooltipContent) {
  const point = payload?.[0]?.payload as MetricPoint | undefined;
  if (!active || !point) return null;
  return (
    <div className="chart-tooltip">
      <strong>{point.value == null ? "—" : format(point.value)}</strong>
      {point.max != null && <span>max {format(point.max)}</span>}
      <span className="muted">{formatTimestamp(point.ts)}</span>
    </div>
  );
}
```

`dashboard/src/components/Layout.tsx`:
```tsx
import { NavLink, Outlet } from "react-router";

export function Layout() {
  return (
    <div className="app">
      <header className="app-header">
        <span className="brand">edgeio</span>
        <nav>
          <NavLink to="/" end>
            Fleet
          </NavLink>
          <NavLink to="/alerts">Alerts</NavLink>
        </nav>
      </header>
      <main className="app-main">
        <Outlet />
      </main>
    </div>
  );
}
```

`dashboard/src/pages/FleetOverview.tsx`:
```tsx
import { Link, useSearchParams } from "react-router";
import { useDevices, useFleetSummary } from "../api/hooks";
import type { DeviceStatus } from "../api/types";
import { DeviceCard } from "../components/DeviceCard";
import { StatusTiles } from "../components/StatusTiles";
import { STATUS_META, isDeviceStatus } from "../lib/status";

export function FleetOverview() {
  const [params, setParams] = useSearchParams();
  const raw = params.get("status");
  const status = isDeviceStatus(raw) ? raw : undefined;
  const summary = useFleetSummary();
  const devices = useDevices(status);
  const select = (next: DeviceStatus | undefined) => setParams(next ? { status: next } : {});

  return (
    <>
      <div className="page-head">
        <h1>Fleet</h1>
        {summary.data && (
          <Link to="/alerts" className="muted">
            {summary.data.open_alerts.critical} critical · {summary.data.open_alerts.warning}{" "}
            warning alerts open
          </Link>
        )}
      </div>
      {summary.isError ? (
        <p className="error">Couldn't load the fleet summary.</p>
      ) : summary.data ? (
        <StatusTiles counts={summary.data.devices} selected={status} onSelect={select} />
      ) : (
        <div className="tiles tiles-placeholder" />
      )}
      <h2 className="section-title">
        {status ? `${STATUS_META[status].label} devices` : "All devices"}
        {devices.data && <span className="muted"> ({devices.data.total})</span>}
      </h2>
      {devices.isError ? (
        <p className="error">Couldn't load devices.</p>
      ) : !devices.data ? (
        <p className="muted">Loading devices…</p>
      ) : devices.data.items.length === 0 ? (
        <p className="empty">No devices match this filter.</p>
      ) : (
        <div className="device-grid">
          {devices.data.items.map((d) => (
            <DeviceCard key={d.device_id} device={d} />
          ))}
        </div>
      )}
    </>
  );
}
```

`dashboard/src/pages/DeviceDetail.tsx`:
```tsx
import { useState } from "react";
import { useParams } from "react-router";
import { ApiError } from "../api/client";
import { useAlerts, useDevice } from "../api/hooks";
import { AlertsTable } from "../components/AlertsTable";
import { MetricChart } from "../components/MetricChart";
import { RangePicker } from "../components/RangePicker";
import { ServicesPanel } from "../components/ServicesPanel";
import { StatusBadge } from "../components/StatusBadge";
import { formatRelative, formatUptime } from "../lib/format";
import { CHARTS } from "../lib/metrics";
import type { RangeKey } from "../lib/range";

export function DeviceDetail() {
  const { deviceId = "" } = useParams();
  const [range, setRange] = useState<RangeKey>("24h");
  const device = useDevice(deviceId);
  const alerts = useAlerts({ state: "all", deviceId });

  if (device.isError) {
    const missing =
      device.error instanceof ApiError && [404, 422].includes(device.error.status);
    return <p className="error">{missing ? "Device not found." : "Couldn't load this device."}</p>;
  }
  if (!device.data) return <p className="muted">Loading device…</p>;
  const d = device.data;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>{d.hostname}</h1>
          <p className="muted">
            <span className="mono">{d.device_id}</span> · {d.os} · up {formatUptime(d.uptime_seconds)}{" "}
            · last seen {formatRelative(d.last_seen)}
          </p>
        </div>
        <StatusBadge status={d.status} />
      </div>
      <div className="filters">
        <RangePicker value={range} onChange={setRange} />
      </div>
      <div className="chart-grid">
        {CHARTS.map((spec) => (
          <MetricChart key={spec.metric} deviceId={d.device_id} spec={spec} range={range} />
        ))}
      </div>
      <div className="detail-grid">
        <ServicesPanel services={d.services} latest={d.latest} />
        <section className="panel">
          <h2>Alert history</h2>
          {alerts.isError ? (
            <p className="error">Couldn't load alerts.</p>
          ) : alerts.data ? (
            <AlertsTable alerts={alerts.data.items} showDevice={false} />
          ) : (
            <p className="muted">Loading alerts…</p>
          )}
        </section>
      </div>
    </>
  );
}
```

`dashboard/src/pages/AlertsPage.tsx`:
```tsx
import { useSearchParams } from "react-router";
import { useAlerts } from "../api/hooks";
import type { AlertState, Severity } from "../api/types";
import { AlertsTable } from "../components/AlertsTable";

const STATES: AlertState[] = ["open", "resolved", "all"];

function parseState(value: string | null): AlertState {
  return STATES.find((s) => s === value) ?? "open";
}

function parseSeverity(value: string | null): Severity | undefined {
  return value === "warning" || value === "critical" ? value : undefined;
}

export function AlertsPage() {
  const [params, setParams] = useSearchParams();
  const state = parseState(params.get("state"));
  const severity = parseSeverity(params.get("severity"));
  const alerts = useAlerts({ state, severity });

  const update = (next: { state?: AlertState; severity?: Severity | undefined }) => {
    const merged = { state, severity, ...next };
    const query: Record<string, string> = { state: merged.state };
    if (merged.severity) query.severity = merged.severity;
    setParams(query);
  };

  return (
    <>
      <div className="page-head">
        <h1>Alerts</h1>
        {alerts.data && <span className="muted">{alerts.data.total} total</span>}
      </div>
      <div className="filters">
        <div className="segmented" role="group" aria-label="Alert state">
          {STATES.map((s) => (
            <button key={s} type="button" aria-pressed={state === s} onClick={() => update({ state: s })}>
              {s}
            </button>
          ))}
        </div>
        <label className="select">
          Severity
          <select
            value={severity ?? ""}
            onChange={(e) => update({ severity: parseSeverity(e.target.value) })}
          >
            <option value="">All</option>
            <option value="warning">Warning</option>
            <option value="critical">Critical</option>
          </select>
        </label>
      </div>
      {alerts.isError ? (
        <p className="error">Couldn't load alerts.</p>
      ) : alerts.data ? (
        <AlertsTable alerts={alerts.data.items} />
      ) : (
        <p className="muted">Loading alerts…</p>
      )}
    </>
  );
}
```

`dashboard/src/App.tsx`:
```tsx
import { Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import { AlertsPage } from "./pages/AlertsPage";
import { DeviceDetail } from "./pages/DeviceDetail";
import { FleetOverview } from "./pages/FleetOverview";

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<FleetOverview />} />
        <Route path="devices/:deviceId" element={<DeviceDetail />} />
        <Route path="alerts" element={<AlertsPage />} />
        <Route path="*" element={<p className="empty">Page not found.</p>} />
      </Route>
    </Routes>
  );
}
```

`dashboard/src/main.tsx`:
```tsx
import { QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router";
import { createQueryClient } from "./api/queryClient";
import { App } from "./App";
import "./styles.css";

createRoot(document.getElementById("root") as HTMLElement).render(
  <StrictMode>
    <QueryClientProvider client={createQueryClient()}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);
```

`dashboard/src/styles.css`:
```css
/* Tokens: chart chrome & status palette from the data-viz reference instance. */
:root {
  color-scheme: light;
  --page: #f9f9f7;
  --surface: #fcfcfb;
  --text: #0b0b0b;
  --text-secondary: #52514e;
  --text-muted: #898781;
  --grid: #e1e0d9;
  --axis: #c3c2b7;
  --border: rgba(11, 11, 11, 0.1);
  --series-1: #2a78d6;
  --accent: #2a78d6;
  --status-healthy: #0ca30c;
  --status-warning: #fab219;
  --status-critical: #d03b3b;
  --status-offline: #898781;
  --radius: 8px;
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
  line-height: 1.4;
}

@media (prefers-color-scheme: dark) {
  :root {
    color-scheme: dark;
    --page: #0d0d0d;
    --surface: #1a1a19;
    --text: #ffffff;
    --text-secondary: #c3c2b7;
    --grid: #2c2c2a;
    --axis: #383835;
    --border: rgba(255, 255, 255, 0.1);
    --series-1: #3987e5;
    --accent: #3987e5;
  }
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  background: var(--page);
  color: var(--text);
}

a {
  color: var(--accent);
}

h1 {
  margin: 0;
  font-size: 24px;
}

h2 {
  font-size: 16px;
  margin: 0 0 12px;
}

h3 {
  font-size: 14px;
  margin: 0;
}

.app-header {
  display: flex;
  align-items: center;
  gap: 24px;
  padding: 12px 24px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
}

.brand {
  font-weight: 700;
}

.app-header nav {
  display: flex;
  gap: 16px;
}

.app-header nav a {
  color: var(--text-secondary);
  text-decoration: none;
}

.app-header nav a.active {
  color: var(--text);
  font-weight: 600;
}

.app-main {
  max-width: 1280px;
  margin: 0 auto;
  padding: 24px;
}

.page-head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 20px;
}

.page-head p {
  margin: 4px 0 0;
}

.section-title {
  margin-top: 28px;
}

.muted {
  color: var(--text-muted);
}

.small {
  font-size: 12px;
}

.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.92em;
}

.empty,
.error {
  padding: 16px;
  border-radius: var(--radius);
  background: var(--surface);
  border: 1px solid var(--border);
}

.error {
  border-color: var(--status-critical);
}

/* Status: color + icon + label, never color alone. */
.badge-healthy {
  --status-color: var(--status-healthy);
}
.badge-warning {
  --status-color: var(--status-warning);
}
.badge-critical {
  --status-color: var(--status-critical);
}
.badge-offline {
  --status-color: var(--status-offline);
}

.badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 2px 8px 2px 4px;
  border-radius: 999px;
  border: 1px solid var(--border);
  color: var(--text-secondary);
  font-size: 12px;
  white-space: nowrap;
}

.badge-icon {
  display: inline-grid;
  place-items: center;
  width: 16px;
  height: 16px;
  border-radius: 50%;
  background: var(--status-color);
  color: #ffffff;
  font-size: 10px;
  font-weight: 700;
}

.badge-warning .badge-icon {
  color: #0b0b0b;
}

.tiles {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
  min-height: 92px;
}

.tile {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 8px;
  padding: 16px;
  border: 1px solid var(--border);
  border-left: 4px solid var(--status-color);
  border-radius: var(--radius);
  background: var(--surface);
  color: var(--text);
  font: inherit;
  cursor: pointer;
  text-align: left;
}

.tile[aria-pressed="true"] {
  outline: 2px solid var(--accent);
  outline-offset: 1px;
}

.tile-value {
  font-size: 32px;
  font-weight: 700;
  line-height: 1;
}

.tile-label {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: var(--text-secondary);
}

.device-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(min(220px, 100%), 1fr));
  gap: 12px;
}

.device-card {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 14px;
  border: 1px solid var(--border);
  border-left: 4px solid var(--status-color);
  border-radius: var(--radius);
  background: var(--surface);
  color: var(--text);
  text-decoration: none;
}

.device-card:hover {
  border-color: var(--accent);
  border-left-color: var(--status-color);
}

.device-card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.device-name {
  font-weight: 600;
}

.device-metrics {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 4px;
  margin: 0;
}

.device-metrics dt {
  font-size: 11px;
  color: var(--text-muted);
}

.device-metrics dd {
  margin: 0;
  font-weight: 600;
}

.filters {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
}

.segmented {
  display: inline-flex;
  border: 1px solid var(--border);
  border-radius: var(--radius);
  overflow: hidden;
}

.segmented button {
  padding: 6px 14px;
  border: 0;
  background: var(--surface);
  color: var(--text-secondary);
  font: inherit;
  cursor: pointer;
}

.segmented button[aria-pressed="true"] {
  background: var(--accent);
  color: #ffffff;
}

.select {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: var(--text-secondary);
}

.select select {
  font: inherit;
  padding: 5px 8px;
  border-radius: var(--radius);
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--text);
}

.panel {
  padding: 16px;
  border: 1px solid var(--border);
  border-radius: var(--radius);
  background: var(--surface);
  min-width: 0;
}

.chart-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(min(360px, 100%), 1fr));
  gap: 12px;
}

.chart-head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  margin-bottom: 8px;
}

.chart-latest {
  font-size: 20px;
  font-weight: 700;
}

.chart-plot,
.chart-placeholder {
  height: 180px;
}

.chart-table summary {
  margin-top: 8px;
  font-size: 12px;
  color: var(--text-muted);
  cursor: pointer;
}

.chart-table {
  max-height: 240px;
  overflow: auto;
}

.chart-tooltip {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding: 8px 10px;
  border: 1px solid var(--border);
  border-radius: var(--radius);
  background: var(--surface);
  color: var(--text);
  font-size: 12px;
}

.detail-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 2fr);
  gap: 12px;
  margin-top: 12px;
}

@media (max-width: 800px) {
  .detail-grid {
    grid-template-columns: minmax(0, 1fr);
  }
  .app-main {
    padding: 16px;
  }
}

.service-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: 8px;
}

.service-list li {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.containers {
  margin: 16px 0 0;
  color: var(--text-secondary);
}

.table-wrap {
  overflow-x: auto;
}

.table {
  width: 100%;
  border-collapse: collapse;
  font-size: 14px;
}

.table th {
  text-align: left;
  font-weight: 600;
  color: var(--text-muted);
  font-size: 12px;
}

.table th,
.table td {
  padding: 8px;
  border-bottom: 1px solid var(--grid);
}

.table td {
  font-variant-numeric: tabular-nums;
}

.table.compact th,
.table.compact td {
  padding: 4px 8px;
  font-size: 12px;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm --prefix dashboard test && npm --prefix dashboard run format && npm --prefix dashboard run lint && npm --prefix dashboard run build`
Expected: tests PASS; lint clean; `dist/index.html` built

- [ ] **Step 5: Commit**

```bash
git add dashboard
git commit -m "feat(dashboard): fleet, device detail and alerts pages with charts"
```

---

### Task 11: Containers, compose, Makefile, docs, end-to-end smoke check

**Files:**
- Create: `dashboard/Dockerfile`, `dashboard/nginx.conf`, `dashboard/.dockerignore`
- Modify: `docker-compose.yml` (api + dashboard services), `Makefile` (`test`, `lint`, `fmt`), `CLAUDE.md` §8, §10, §11, §12, §15

**Interfaces:**
- Consumes: `python -m edgeio_api` (Task 2), dashboard build (Task 10).
- Produces: `make up` serves the API on `:8000` and the dashboard on `:${DASHBOARD_PORT:-5173}`, with nginx proxying `/api`.

- [ ] **Step 1: Write the dashboard image recipe**

`dashboard/.dockerignore`:
```
node_modules
dist
```

`dashboard/nginx.conf`:
```nginx
server {
    listen 80;
    root /usr/share/nginx/html;

    location /api/ {
        proxy_pass http://api:8000;
        proxy_set_header Host $host;
    }

    location / {
        try_files $uri /index.html;
    }
}
```

`dashboard/Dockerfile`:
```dockerfile
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
```

- [ ] **Step 2: Add the services to Compose**

Append under `services:` in `docker-compose.yml` (before the top-level `volumes:`):
```yaml
  api:
    image: edgeio/api:dev
    build:
      context: .
      dockerfile: docker/python.Dockerfile
      args:
        PACKAGE: edgeio-api
    command: ["python", "-m", "edgeio_api"]
    environment:
      DATABASE_URL: postgresql://edgeio:edgeio@timescaledb:5432/edgeio
    ports:
      - "8000:8000"
    depends_on:
      migrate:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/v1/healthz')"]
      interval: 10s
      timeout: 5s
      retries: 6
    restart: unless-stopped

  dashboard:
    image: edgeio/dashboard:dev
    build: ./dashboard
    ports:
      - "${DASHBOARD_PORT:-5173}:80"
    depends_on:
      api:
        condition: service_healthy
    restart: unless-stopped
```

- [ ] **Step 3: Extend the Makefile**

Replace the `test`, `lint` and `fmt` targets with:
```make
test:
	uv run pytest
	npm --prefix dashboard test

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy contracts/edgeio_contracts simulator/edgeio_simulator worker/edgeio_worker api/edgeio_api
	npm --prefix dashboard run lint

fmt:
	uv run ruff format .
	uv run ruff check --fix .
	npm --prefix dashboard run format
```

- [ ] **Step 4: Update CLAUDE.md**

§8: replace the continuous-aggregates bullet with:
```
- Continuous aggregates: `health_hourly`, `health_daily` — avg + max of CPU, temperature, RAM %, disk %, packet loss and rx/tx rate (bits/s), plus reading count (migration `0003` added the max/rate columns). Real-time aggregation is on (`materialized_only = false`).
```

§10: replace the endpoint bullets with:
```
- `GET /devices?status=&limit=&offset=` — list with latest status/metrics and open-alert count, ordered by hostname.
- `GET /devices/{device_id}` — latest metrics, services, latest reading (load, containers, rates), open alerts. Non-IP id → 422, unknown → 404.
- `GET /devices/{device_id}/metrics?metric=&from=&to=&bucket=` — `metric` ∈ cpu, temperature, ram, disk, packet_loss, rx_rate, tx_rate; `from`/`to` ISO-8601 (default last 24 h; naive = UTC); `bucket` ∈ raw, 1h, 1d, auto-selected when omitted: raw (≤ 24 h), `health_hourly` (≤ 30 d), `health_daily` beyond. Points carry `value` (avg for rollups) and `max`.
- `GET /alerts?state=open|resolved|all&device_id=&severity=` — paginated, newest first; default `open`.
- `GET /fleet/summary?top=5` — counts per status, open alert counts by severity, top-N hottest / fullest-disk devices (offline devices excluded).
- `GET /healthz` — 200 `{status: ok}` or 503 `{status: degraded}`. Any endpoint answers 503 `{"detail": "database unavailable"}` when the DB is down.
```

§11: add after the first paragraph:
```
In Compose, nginx serves the built dashboard on `DASHBOARD_PORT` (default 5173) and proxies `/api` to the API. For development run `make dev-api` and `make dev-dashboard` (Vite on 5173, proxying `/api` to :8000) — stop the Compose dashboard first or set `DASHBOARD_PORT`. After changing API response models run `make openapi` to refresh `dashboard/openapi.json` and the generated types.
```

§12: update the command block so it matches the Makefile:
```
make up          # docker compose up -d --build (full stack)
make down        # stop stack
make logs s=worker   # tail a service's logs
make test        # Python unit tests + dashboard Vitest
make test-int    # integration tests incl. API (Testcontainers; needs Docker)
make lint        # ruff + mypy + eslint/prettier/tsc
make fmt         # ruff format + prettier
make psql        # psql shell into timescaledb
make topics      # list topics / consumer lag
make schema      # regenerate contracts/health.schema.json from the Pydantic model
make openapi     # regenerate dashboard/openapi.json + TS types from the API
make dev-api     # run the API locally on :8000
make dev-dashboard  # Vite dev server on :5173
```
and the ports line:
```
Ports: dashboard `5173` (`DASHBOARD_PORT`), API `8000`, Kafka `9092` (host) / `kafka:29092` (in-network), Timescale `5433` on the host (override with `TIMESCALE_PORT`; 5432 is often taken by a local Postgres) / `timescaledb:5432` in-network.
```

§15 body:
```
Design approved 2026-10-06. Plan 1 (pipeline) and Plan 2 (REST API + dashboard) implemented — see `docs/superpowers/plans/`. The platform is feature-complete for the simulated fleet; next steps are real devices running `health.py` against the `device.health` contract.
```

- [ ] **Step 5: Bring up the full stack and smoke-check**

Run: `make up && docker compose ps --format '{{.Service}} {{.State}} {{.Status}}'`
Expected: `api` running (healthy), `dashboard` running, along with the Plan 1 services.

Run: `make logs s=migrate`, or `docker compose logs migrate --no-log-prefix | tail -3`
Expected: `applied migration` for `0003_rollup_metrics.sql`; `0001`/`0002` are already applied on the existing volume.

Run:
```bash
curl -s localhost:8000/api/v1/healthz
curl -s localhost:8000/api/v1/fleet/summary | python3 -m json.tool | head -20
curl -s "localhost:5173/api/v1/devices?limit=2" | python3 -m json.tool | head -20
curl -s localhost:5173/ | grep -o '<div id="root"></div>'
curl -s -o /dev/null -w '%{http_code}\n' localhost:5173/devices/100.64.0.1
```
Expected: `{"status":"ok","database":"ok"}`; summary with `"total": 50`; device list through the nginx proxy; the root div; `200` (SPA fallback).

Then open `http://localhost:5173` in a browser and look at it (data-viz step 7: the validator doesn't check layout):
- Fleet: four status tiles, each showing a count, icon and label; a grid of 50 cards; clicking a tile filters, and the URL gets `?status=`.
- Device detail: header, a range picker in one row above the charts, 7 single-metric line charts with crosshair tooltips, a data-table toggle on each chart, a services panel, alert history.
- Alerts: state and severity filters; device links.
- Dark mode (OS setting): surfaces and series color switch; no illegible text.
- Narrow window (≈ 375 px): no horizontal page scroll.

- [ ] **Step 6: Final verification and commit**

Run: `make lint && make test && make test-int`
Expected: all pass.

```bash
git add dashboard docker-compose.yml Makefile CLAUDE.md
git commit -m "feat: serve API and dashboard from docker compose"
```
