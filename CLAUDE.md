# CLAUDE.md — edgeio: Edge Device Health Monitoring Platform

This file is both the **design spec** and the **working guide** for this repo. Read it fully before making changes. If an implementation decision contradicts this file, update this file in the same change (or ask first).

## 1. Purpose

A portfolio / learning project that simulates a fleet of ~50 edge devices reporting health telemetry, and builds a realistic data platform around it:

```
Simulated devices ──JSON every 5 min──▶ Kafka (device.health)
                                            │
                                            ▼
                                  Stream worker: validate → transform → store → alert
                                            │
                                            ▼
                                  PostgreSQL + TimescaleDB
                                       │            │
                                       ▼            ▼
                                   REST API ──▶ Dashboard
```

**Future direction:** real edge devices running a real `health.py` over Tailscale will eventually replace the simulator. Therefore the **device → Kafka message contract is the most important interface in the repo**: it must stay strict, versioned, and independent of simulator internals. Nothing downstream may depend on anything the simulator does that a real device wouldn't.

**Success criteria**
- `make up` (docker compose) brings up the full stack from a clean checkout.
- All 50 simulated devices appear on the dashboard with live status, history charts, and alerts.
- Invalid messages land in the DLQ, never crash the worker, never reach the DB.
- Tests pass: unit, integration (Testcontainers), API, dashboard.

**Non-goals (YAGNI):** auth/multi-tenancy, notifications (Slack/email), Kubernetes, ML anomaly detection, accelerated/backfilled simulation time, a schema registry. Don't add these without being asked.

## 2. Decisions (already made — don't relitigate)

| Area | Decision |
|---|---|
| Orchestration | Docker Compose, local only |
| Broker | Kafka, single broker, **KRaft** mode (no ZooKeeper) |
| Storage | PostgreSQL + **TimescaleDB** (hypertable + continuous aggregates + retention) |
| Simulator | Python, **one process, 50 asyncio virtual devices** (can run multiple replicas with device-range sharding for N > 50) |
| Stream worker | Python, plain `confluent-kafka` consumer, Pydantic validation, `psycopg` (v3) |
| API | FastAPI, read-only |
| Dashboard | React + Vite + TypeScript, TanStack Query, Recharts |
| Alerting | Rule-based in the worker, stored in `alerts` table; no outbound notifications |
| Simulation time | **Real time only.** Default interval 300 s. No acceleration, no backfill. |
| Python tooling | `uv`, `ruff`, `mypy --strict`, `pytest` |
| Delivery semantics | At-least-once + idempotent inserts |

## 3. Repo layout

```
edgeio/
  contracts/            # Single source of truth for the wire format
    health.schema.json  #   JSON Schema (for real devices / non-Python producers)
    edgeio_contracts/   #   Pydantic models used by simulator, worker, api
    fixtures/valid/     #   example payloads (invalid cases are generated in tests)
  simulator/            # Virtual device fleet → Kafka
  worker/               # Kafka consumer: validate → transform → store → alert
  api/                  # FastAPI read API over TimescaleDB
  dashboard/            # React + Vite + TS
  db/migrations/        # Ordered SQL migrations (plain .sql, applied at startup)
  tests/integration/    # Cross-service tests with Testcontainers
  docker-compose.yml
  Makefile
  CLAUDE.md
```

Each Python package is its own `uv` project depending on `contracts` as a path dependency. Keep units small and single-purpose; if a file passes ~300 lines, split it.

## 4. The message contract (`device.health`)

Wire format = the JSON below, plus `schema_version`. Keys are snake_case. Timestamps are ISO-8601 UTC with `Z`.

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

**Validation rules (worker rejects → DLQ if any fail):**
- `schema_version` is a supported version (currently `1`).
- `device_id` is an IPv4 address inside Tailscale CGNAT range `100.64.0.0/10` (100.64.0.0–100.127.255.255).
- `timestamp` parses as UTC and is not more than 10 minutes in the future.
- All `*_percent` fields in `[0, 100]`; counts, bytes, sizes, uptime ≥ 0.
- `ram_used_mb ≤ ram_total_mb`; `root_used_gb ≤ root_total_gb`; `|root_used_gb + root_free_gb − root_total_gb|` within 1 GB tolerance.
- `cpu_temperature_c` in `[-40, 125]`.
- Service values ∈ `{"running", "stopped", "failed", "unknown"}`. The service *keys* are an open map (devices may report extra services); `docker`, `postgresql`, `edge_streamer` are expected.
- Unknown top-level fields: reject (strict) — contract changes must bump `schema_version`.

**Changing the contract:** update `health.schema.json` and the Pydantic models together, add fixtures for valid/invalid cases, bump `schema_version` for breaking changes, and keep the worker accepting the previous version until explicitly dropped.

## 5. Kafka

| Topic | Partitions | Key | Purpose |
|---|---|---|---|
| `device.health` | 6 | `device_id` | Health readings (per-device ordering preserved) |
| `device.health.dlq` | 1 | `device_id` or null | Rejected messages: original bytes + headers `error`, `error_stage`, `failed_at` |

Topics are created explicitly by an init container (auto-create disabled). Consumer group: `edgeio-worker`. Producer: `acks=all`, idempotence enabled.

## 6. Simulator (`simulator/`)

Stands in for `health.py` on each device. Publishes **directly to Kafka** (no intermediate files).

- **Fleet generation:** deterministic from `SIM_SEED`. Each device gets: Tailscale IP in `100.64.0.0/10`, hostname `edge-001`…`edge-050`, OS (mostly `Ubuntu 24.04`, some `Ubuntu 22.04`), RAM size (4/8/16/32 GB), disk size, interface (`eth0`/`wlan0`), baseline load profile.
- **Stateful per device** (not independent random draws per tick):
  - `uptime_seconds` increases monotonically; resets on simulated reboot.
  - `rx_bytes`/`tx_bytes` are monotonic counters; reset only on reboot.
  - Disk usage drifts slowly upward; occasional cleanup drops it.
  - CPU follows a diurnal curve + noise; `load_1m` and temperature correlate with CPU.
  - RAM noisy around a baseline; `ram_usage_percent` is computed from used/total (keep fields self-consistent).
- **Fault scenarios** — small per-tick probability to start, last several ticks, then recover:
  `overheat`, `disk_fill`, `memory_leak`, `packet_loss`, `service_crash` (a service → `failed`/`stopped`), `container_crash`, `offline` (device stops publishing), `reboot`.
- **Malformed messages:** with probability `SIM_MALFORMED_RATE` (default `0.005`, `0` disables) emit an invalid payload to exercise the DLQ path.
- **Scheduling:** each device publishes every `SIM_INTERVAL_SECONDS` (default `300`) with a random initial jitter so the fleet doesn't publish in lockstep.
- Config via env: `KAFKA_BOOTSTRAP`, `SIM_DEVICE_COUNT` (50), `SIM_DEVICE_OFFSET` (0, for sharding replicas), `SIM_SEED`, `SIM_INTERVAL_SECONDS`, `SIM_MALFORMED_RATE`, `SIM_FAULT_RATE`.
- Device model logic must be **pure and testable** (state + rng in → new state + payload out); the asyncio/Kafka layer is a thin shell around it.

## 7. Stream worker (`worker/`)

Pipeline per poll batch:

1. **Validate** — decode JSON → Pydantic model. Failure → produce to DLQ with error details, continue.
2. **Transform** — normalize timestamp to UTC; flatten into a row; derive `disk_free_percent`, `rx_rate_bps`/`tx_rate_bps` (bits/s) from the previous counters stored on the `devices` row (read with `SELECT … FOR UPDATE`; no in-memory cache; counter decrease = reboot → rate null). Readings older than `last_seen` are stored as history but don't update device state or alerts.
3. **Store** — single transaction: insert into `health_readings` (`ON CONFLICT (device_id, ts) DO NOTHING`), upsert `devices` (`last_seen`, latest metrics, status), upsert `service_status`.
4. **Alert** — evaluate rules (§9) against the reading; open/update/resolve rows in `alerts` in the same transaction.
5. **Commit offsets** only after the DB transaction commits. → at-least-once; duplicates are harmless due to idempotent inserts.

Plus a periodic **offline sweeper** (every 60 s) that opens `offline` alerts / sets status for devices with `last_seen` older than 15 min, and resolves them when readings resume.

Error handling: DB unavailable → don't commit offsets, back off and retry (never drop data). Poison messages never block the partition — they go to the DLQ. Graceful shutdown on SIGTERM (finish batch, commit, close).

## 8. Database (`db/migrations/`)

Plain ordered SQL files (`0001_init.sql`, …), applied idempotently at startup by a small migrate step. Never edit an applied migration; add a new one.

- `devices` — `device_id` (PK, `inet`), `hostname`, `os`, `first_seen`, `last_seen`, `status` (`healthy|warning|critical|offline`), latest key metrics, `last_rx_bytes`/`last_tx_bytes` (previous counters for rate derivation).
- `health_readings` — **hypertable** on `ts`; unique `(device_id, ts)`; flattened columns for all numeric metrics + derived fields; `raw JSONB` with the full original payload.
- `service_status` — latest state per `(device_id, service)` with `changed_at`.
- `alerts` — `id`, `device_id`, `rule`, `severity` (`warning|critical`), `opened_at`, `resolved_at` (null = open), `last_value`, `message`. Partial unique index guarantees **at most one open alert per (device_id, rule)**.
- Continuous aggregates: `health_hourly`, `health_daily` (avg/max CPU, avg/max temp, avg RAM %, max disk %, avg packet loss, reading count).
- Retention: raw `health_readings` 30 days; `health_hourly` 1 year; `health_daily` kept.

## 9. Alert rules

Defined as data in one module (`worker/.../rules.py`), not scattered `if`s. Each rule has warning/critical thresholds and a **hysteresis** clear threshold to avoid flapping.

| Rule | Warning | Critical | Clears below |
|---|---|---|---|
| `cpu_temp_high` | > 75 °C | > 85 °C | 70 °C |
| `disk_usage_high` | > 85 % | > 95 % | 80 % |
| `ram_usage_high` | > 90 % | — | 85 % |
| `packet_loss_high` | > 2 % | > 10 % | 1 % |
| `service_down` | — | any service ≠ `running` | service `running` |
| `containers_stopped` | `stopped > 0` | — | `stopped == 0` |
| `offline` | — | `last_seen` > 15 min | reading received |

Device `status` = `offline` if offline, else worst open alert severity, else `healthy`.

## 10. API (`api/`)

FastAPI, read-only, prefix `/api/v1`, Pydantic response models, OpenAPI docs at `/docs`.

- `GET /devices?status=&limit=&offset=` — list with latest status/metrics.
- `GET /devices/{device_id}` — latest reading, services, containers, open alerts.
- `GET /devices/{device_id}/metrics?metric=&from=&to=&bucket=` — time series; auto-selects source: raw (≤ 24 h), `health_hourly` (≤ 30 d), `health_daily` beyond.
- `GET /alerts?state=open|resolved&device_id=&severity=` — paginated.
- `GET /fleet/summary` — counts per status, top-N hottest / fullest-disk devices, open alert counts.
- `GET /healthz` — liveness + DB connectivity.

CORS allows the dashboard dev origin. The API never talks to Kafka.

## 11. Dashboard (`dashboard/`)

React + Vite + TypeScript. TanStack Query (refetch every 30 s), Recharts, React Router. API types generated from the FastAPI OpenAPI schema (`openapi-typescript`) — don't hand-write API types.

- **Fleet overview** — status tiles (healthy/warning/critical/offline), grid of 50 device cards colored by status, filter by status.
- **Device detail** — header (hostname, IP, OS, uptime), time-series charts (CPU, temp, RAM, disk, network rate, packet loss) with range picker, services/containers panel, alert history.
- **Alerts** — open and resolved alerts table, filterable.

## 12. Commands

(Keep this section in sync with the Makefile. `make test` = unit tests only; `make test-int` needs Docker.)

```
make up          # docker compose up -d --build (full stack)
make down        # stop stack
make logs s=worker   # tail a service's logs
make test        # all unit + API tests
make test-int    # integration tests (Testcontainers; needs Docker)
make lint        # ruff + mypy + eslint/tsc
make fmt         # ruff format + prettier
make psql        # psql shell into timescaledb
make topics      # list topics / consumer lag
make schema      # regenerate contracts/health.schema.json from the Pydantic model
```

Ports: dashboard `5173`, API `8000`, Kafka `9092` (host) / `kafka:29092` (in-network), Timescale `5433` on the host (override with `TIMESCALE_PORT`; 5432 is often taken by a local Postgres) / `timescaledb:5432` in-network.

## 13. Testing

- **TDD** for logic: write the failing test first.
- **Unit (pytest):** simulator state transitions (monotonic counters, uptime, fault lifecycles, payload always valid unless malformed injected); contract validation — valid example payloads in `contracts/fixtures/valid/`, invalid cases as a mutation table over `edgeio_contracts.samples.sample_payload()`; transform + rate derivation (incl. counter reset); alert rule evaluation incl. hysteresis and open/resolve transitions.
- **Integration (`tests/integration/`, Testcontainers):** produce valid + invalid messages → assert rows in `health_readings`, `devices`, `alerts`; assert DLQ receives invalid ones; assert redelivery doesn't duplicate rows.
- **API:** FastAPI `TestClient` against a migrated, seeded Timescale container.
- **Dashboard:** Vitest + Testing Library for components.
- Don't mock the database in worker/API tests — use the Timescale container.

## 14. Conventions

- Python 3.12, type hints everywhere, `mypy --strict` clean, `ruff` clean.
- Config only via environment variables (parsed with `pydantic-settings`); no hard-coded hosts.
- Structured JSON logging; include `device_id` in log context where relevant.
- All times stored and transmitted in UTC (`timestamptz`).
- Shared models live in `contracts/` only — never duplicate the payload schema in another package.
- SQL in plain `.sql` / parameterized queries via psycopg; no ORM.

## 15. Build status

Design approved 2026-10-06. Plan 1 (pipeline: contracts, simulator, worker, TimescaleDB, Compose) implemented — see `docs/superpowers/plans/2026-10-06-edgeio-pipeline.md`. Next: Plan 2 — REST API (§10) and dashboard (§11).
