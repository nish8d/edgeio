# CLAUDE.md — edgeio: Edge Device Health Monitoring Platform

This file is both the **design spec** and the **working guide** for this repo. Read it fully before making changes. If an implementation decision contradicts this file, update this file in the same change (or ask first).

## 1. Purpose

A health monitoring platform for real edge devices. Each device runs the edge agent (`agent/`), which reports health telemetry over Tailscale into this data platform:

```
Edge agents (Tailscale) ──JSON every 5 min──▶ Kafka (device.health)
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

**History:** the platform was built against a 50-device simulator. That portfolio version, simulator included, lives in the separate `edgeiosim` repo; this repo is the deployment project and carries real devices only. The **device → Kafka message contract is the most important interface in the repo**: it must stay strict, versioned, and independent of any one producer.

**Success criteria**
- `make up` (docker compose) brings up the full stack from a clean checkout; `make up-tailnet` also accepts real agents over Tailscale.
- Every device running the agent appears on the dashboard with live status, history charts, and alerts.
- Invalid messages land in the DLQ, never crash the worker, never reach the DB.
- Tests pass: unit, integration (Testcontainers), API, dashboard.

**Non-goals (YAGNI):** auth/multi-tenancy, notifications (Slack/email), Kubernetes, ML anomaly detection, a schema registry. Don't add these without being asked.

## 2. Decisions (already made — don't relitigate)

| Area | Decision |
|---|---|
| Orchestration | Docker Compose, local only |
| Broker | Kafka, single broker, **KRaft** mode (no ZooKeeper) |
| Storage | PostgreSQL + **TimescaleDB** (hypertable + continuous aggregates + retention) |
| Device agent | Python in a Docker image on each device, producing directly to Kafka over Tailscale |
| Stream worker | Python, plain `confluent-kafka` consumer, Pydantic validation, `psycopg` (v3) |
| API | FastAPI, read-only |
| Dashboard | React + Vite + TypeScript, TanStack Query, Recharts |
| Alerting | Rule-based in the worker, stored in `alerts` table; no outbound notifications |
| Reporting interval | Real time. Default 300 s per device. |
| Python tooling | `uv`, `ruff`, `mypy --strict`, `pytest` |
| Delivery semantics | At-least-once + idempotent inserts |

## 3. Repo layout

```
edgeio/
  contracts/            # Single source of truth for the wire format
    health.schema.json  #   JSON Schema (for real devices / non-Python producers)
    edgeio_contracts/   #   Pydantic models used by agent, worker, api
    fixtures/valid/     #   example payloads (invalid cases are generated in tests)
  agent/                # Health agent for real edge devices (Docker image) → Kafka over Tailscale
  worker/               # Kafka consumer: validate → transform → store → alert
  api/                  # FastAPI read API over TimescaleDB
  dashboard/            # React + Vite + TS
  db/migrations/        # Ordered SQL migrations (plain .sql, applied at startup)
  tests/integration/    # Cross-service tests with Testcontainers
  deploy/agent/         # How to run the agent on a device (env example, docker run)
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

**Disk convention:** `root_free_gb` is the space available to unprivileged processes; filesystem-reserved blocks count as used (`used = total − free`). This keeps the 1 GB sum rule exact on ext4 (which reserves ~5 % for root) and makes `disk_usage_high` fire on space applications can actually use.

**Changing the contract:** update `health.schema.json` and the Pydantic models together, add fixtures for valid/invalid cases, bump `schema_version` for breaking changes, and keep the worker accepting the previous version until explicitly dropped.

## 5. Kafka

| Topic | Partitions | Key | Purpose |
|---|---|---|---|
| `device.health` | 6 | `device_id` | Health readings (per-device ordering preserved) |
| `device.health.dlq` | 1 | `device_id` or null | Rejected messages: original bytes + headers `error`, `error_stage`, `failed_at` |

Real agents reach Kafka through an opt-in third listener, `TAILNET`, on `<server tailscale ip>:9094` (`docker-compose.tailscale.yml`, `make up-tailnet`). The `EXTERNAL` listener advertises `localhost:9092`, so only clients on the server itself can use it.

**Server deployment.** Published ports bind to `HOST_BIND` (Kafka 9092, Timescale, API; default `0.0.0.0`) and `DASHBOARD_BIND` (dashboard). The DB password comes from `POSTGRES_PASSWORD` (default `edgeio` for development). On a server, put these in a `.env` next to `docker-compose.yml` (template: `deploy/server/server.env.example`): `HOST_BIND=127.0.0.1`, `DASHBOARD_BIND` = the server's Tailscale IP, a random hex `POSTGRES_PASSWORD`, so only the dashboard and Kafka's TAILNET listener are reachable, and only over Tailscale. Ship runtime files only (no docs, tests or CLAUDE.md). Topics are created explicitly by an init container (auto-create disabled). The Kafka log lives on the `kafka-data` volume, so recreating the container keeps topics and unconsumed readings; every long-running service is `restart: unless-stopped`. Consumer group: `edgeio-worker`. Producer: `acks=all`, idempotence enabled.

## 6b. Edge agent (`agent/`)

Runs on each real device and publishes the v1 contract. It's a Docker image (`docker/agent.Dockerfile`, `make agent-image`), run with `--network host --pid host --uts host` and read-only mounts of `/` (→ `/host`), `/sys/fs/cgroup` (→ `/host-cgroup`) and the Docker socket, plus a named volume for the spool. See `deploy/agent/README.md`.

- **Functional core, imperative shell.** `host.py` / `containers.py` / `ping.py` are pure readers over paths or text. `report.py` turns a `HostSnapshot` into a validated `HealthReport`. `collect.py` is the only module that touches the live machine.
- **Field sources:**
  - `device_id`: the IPv4 address in 100.64.0.0/10, preferring `tailscale0`.
  - CPU temperature: hwmon, in the order `k10temp` → `coretemp` → `zenpower` → `cpu_thermal`, else the hottest sensor.
  - Disk: `statvfs` of the host root (see the §4 disk convention).
  - Interface: the default-route interface from `/proc/net/route`.
  - Packet loss: the **minimum** loss across `AGENT_PING_TARGETS` (internet reachability, not the Tailscale link).
- **Services:**
  - Host units (`AGENT_HOST_SERVICES`) are `running` if their cgroup v2 `cgroup.procs` lists a process, otherwise `stopped`.
  - Every container comes from the Docker API: running → running; exit 0, created or paused → stopped; non-zero exit, restarting or dead → failed.
  - `AGENT_SERVICE_RENAMES` maps container names to contract keys (e.g. the streamer → `edge_streamer`).
  - If the Docker API is unreachable, `docker` is `unknown`.
- **Delivery:** each tick validates the payload locally (`validate_report`) and writes it atomically to the spool. It then sends every pending file oldest first, deleting a file only after the broker acks it. The spool is capped at `AGENT_SPOOL_MAX_FILES` (7 days), oldest dropped. A failed collection skips the reading but still drains the backlog.
- **Never commit customer identifiers** (container names, registries, hostnames, IPs). Per-device settings live in the device's env file.

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

Plain ordered SQL files (`0001_init.sql`, …), applied idempotently at startup by a small migrate step, each in its own transaction. A file whose first line is `-- migrate: no-transaction` runs statement by statement outside a transaction (needed for `refresh_continuous_aggregate`). Never edit an applied migration; add a new one. Recreating a continuous aggregate must be followed by a backfill refresh (see `0004`), or history outside the policy window disappears.

- `devices` — `device_id` (PK, `inet`), `hostname`, `os`, `first_seen`, `last_seen`, `status` (`healthy|warning|critical|offline`), latest key metrics, `last_rx_bytes`/`last_tx_bytes` (previous counters for rate derivation).
- `health_readings` — **hypertable** on `ts`; unique `(device_id, ts)`; flattened columns for all numeric metrics + derived fields; `raw JSONB` with the full original payload.
- `service_status` — latest state per `(device_id, service)` with `changed_at`.
- `alerts` — `id`, `device_id`, `rule`, `severity` (`warning|critical`), `opened_at`, `resolved_at` (null = open), `last_value`, `message`. Partial unique index guarantees **at most one open alert per (device_id, rule)**.
- Continuous aggregates: `health_hourly`, `health_daily` — avg + max of CPU, temperature, RAM %, disk %, packet loss and rx/tx rate (bits/s), plus reading count (migration `0003` added the max/rate columns). Real-time aggregation is on (`materialized_only = false`).
- Refresh policies re-materialize the last 8 days (hourly) / 10 days (daily) so late readings from an agent's spool (≤ 7 days) reach the rollups (`0005`).
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

- `GET /devices?status=&limit=&offset=` — list with latest status/metrics and open-alert count, ordered by hostname.
- `GET /devices/{device_id}` — latest metrics, services, latest reading (load, containers, rates), open alerts. Non-IP id → 422, unknown → 404.
- `GET /devices/{device_id}/metrics?metric=&from=&to=&bucket=` — `metric` ∈ cpu, temperature, ram, disk, packet_loss, rx_rate, tx_rate; `from`/`to` ISO-8601 (default last 24 h; naive = UTC); `bucket` ∈ raw, 1h, 1d, auto-selected when omitted: raw (≤ 24 h), `health_hourly` (≤ 30 d), `health_daily` beyond. Points carry `value` (avg for rollups) and `max`.
- `GET /alerts?state=open|resolved|all&device_id=&severity=` — paginated, newest first; default `open`.
- `GET /fleet/summary?top=5` — counts per status, open alert counts by severity, top-N hottest / fullest-disk devices (offline devices excluded).
- `GET /healthz` — 200 `{status: ok}` or 503 `{status: degraded}`. Any endpoint answers 503 `{"detail": "database unavailable"}` when the DB is down.

CORS allows the dashboard dev origin. The API never talks to Kafka.

## 11. Dashboard (`dashboard/`)

React + Vite + TypeScript. TanStack Query (refetch every 30 s), Recharts, React Router. API types generated from the FastAPI OpenAPI schema (`openapi-typescript`) — don't hand-write API types.

In Compose, nginx serves the built dashboard on `DASHBOARD_PORT` (default 5173) and proxies `/api` to the API. For development run `make dev-api` and `make dev-dashboard` (Vite on 5173, proxying `/api` to :8000) — stop the Compose dashboard first or set `DASHBOARD_PORT`. After changing API response models run `make openapi` to refresh `dashboard/openapi.json` and the generated types.

- **Fleet overview** — status tiles (healthy/warning/critical/offline), grid of device cards colored by status, filter by status.
- **Device detail** — header (hostname, IP, OS, uptime), time-series charts (CPU, temp, RAM, disk, network rate, packet loss) with range picker, services/containers panel, alert history.
- **Alerts** — open and resolved alerts table, filterable.

## 12. Commands

(Keep this section in sync with the Makefile. `make test-int` needs Docker.)

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
make up-tailnet  # full stack + Kafka listener on this machine's Tailscale IP :9094
make agent-image # build the edge agent image (edgeio/agent:dev)
make dev-dashboard  # Vite dev server on :5173
```

Ports: dashboard `5173` (`DASHBOARD_PORT`), API `8000`, Kafka `9092` (host) / `kafka:29092` (in-network), Timescale `5433` on the host (override with `TIMESCALE_PORT`; 5432 is often taken by a local Postgres) / `timescaledb:5432` in-network.

## 13. Testing

- **TDD** for logic: write the failing test first.
- **Unit (pytest):** agent readers against fake host trees, spool and delivery; contract validation — valid example payloads in `contracts/fixtures/valid/`, invalid cases as a mutation table over `edgeio_contracts.samples.sample_payload()`; transform + rate derivation (incl. counter reset); alert rule evaluation incl. hysteresis and open/resolve transitions.
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

Design approved 2026-10-06. Plan 1 (pipeline), Plan 2 (REST API + dashboard) and Plan 3 (edge agent for real devices) implemented — see `docs/superpowers/plans/`. On 2026-10-07 the simulator was removed (kept in `edgeiosim`); only real devices report here. The server deployment (no simulator, ports bound per `deploy/server/server.env.example`) is described in §5.
