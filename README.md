# edgeio

[![CI](https://github.com/nish8d/edgeio/actions/workflows/ci.yml/badge.svg)](https://github.com/nish8d/edgeio/actions/workflows/ci.yml)

A health monitoring platform for a fleet of real edge devices. Each device runs a small agent container that reports telemetry every five minutes to Kafka over Tailscale. A stream worker validates and stores the readings in TimescaleDB and raises alerts. A REST API and a React dashboard show live status, history and alerts.

```
Edge agents ──Tailscale, every 5 min──▶ Kafka (device.health)
                                            │
                                            ▼
                                  Stream worker: validate → transform → store → alert
                                            │                      │
                                            ▼                      ▼
                                  PostgreSQL + TimescaleDB    Kafka (device.health.dlq)
                                       │            │
                                       ▼            ▼
                                   REST API ──▶ Dashboard
```

The device → Kafka message contract is the most important interface in the repo. It's strict and versioned, and nothing downstream depends on how a device produces it. The original portfolio version, built against a 50-device simulator, lives in [edgeiosim](https://github.com/nish8d/edgeiosim).

## Features

- **Edge agent.** A container on each device reports CPU, temperature, RAM, disk, uplink traffic, internet packet loss, host services and every Docker container. Readings are spooled on disk while Kafka is unreachable and sent when it's back.
- **Strict message contract.** Pydantic v2 models plus a generated JSON Schema (`contracts/health.schema.json`) for producers that aren't written in Python. `device_id` must be a Tailscale CGNAT address (`100.64.0.0/10`).
- **Reliable stream worker:**
  - At-least-once delivery with idempotent inserts. Offsets are committed only after the database transaction commits.
  - Invalid or poison messages go to a dead-letter topic and never block a partition.
  - Network rates are derived from counters, and a counter reset is treated as a reboot.
- **Rule-based alerts with hysteresis.** Rules cover CPU temperature, disk, RAM, packet loss, services, stopped containers and offline devices. A device has at most one open alert per rule.
- **TimescaleDB storage.** Readings go in a hypertable. Hourly and daily continuous aggregates keep long-range charts fast. Retention policies apply: raw data for 30 days, hourly rollups for 1 year.
- **Read-only FastAPI service.** OpenAPI docs are served at `/docs`. The resolution of metric history (raw, hourly or daily) is picked automatically from the time range. If the database is down, the API answers 503 instead of hanging.
- **Dashboard:**
  - Built with React, TypeScript, TanStack Query and Recharts.
  - Pages: fleet overview, per-device charts with a range picker, and alert history.
  - API types are generated from the OpenAPI schema.

## Quick start

Requirements: Docker with Compose v2.

```bash
git clone https://github.com/nish8d/edgeio.git
cd edgeio
make up
```

| Service | URL |
|---|---|
| Dashboard | http://localhost:5173 |
| API + OpenAPI docs | http://localhost:8000/docs |
| Kafka (host) | `localhost:9092` |
| TimescaleDB (host) | `localhost:5433` (user/db `edgeio`) |

The dashboard is empty until a device runs the agent (see [Running on a real device](#running-on-a-real-device)). Stop the stack with `make down`.

Ports can be overridden with `DASHBOARD_PORT` and `TIMESCALE_PORT`.

## Development

Requirements: [uv](https://docs.astral.sh/uv/), Python 3.12, Node 22, Docker (for integration tests).

```bash
uv sync                          # Python workspace + dev tools
npm --prefix dashboard ci        # dashboard dependencies
```

| Command | What it does |
|---|---|
| `make test` | Python unit tests + dashboard Vitest |
| `make test-int` | Integration tests against real Kafka and TimescaleDB (Testcontainers) |
| `make lint` | ruff, mypy `--strict`, eslint, prettier, tsc |
| `make fmt` | Format Python and TypeScript |
| `make dev-api` | API on :8000 with the local Python env |
| `make dev-dashboard` | Vite dev server on :5173, proxying `/api` to :8000 |
| `make logs s=worker` | Tail one service's logs |
| `make psql` | psql shell into TimescaleDB |
| `make topics` | Describe topics and consumer-group lag |
| `make schema` | Regenerate `contracts/health.schema.json` from the Pydantic model |
| `make openapi` | Regenerate `dashboard/openapi.json` and the TypeScript API types |

The dev servers bind the same ports as the Compose stack. To run them alongside it, stop the Compose `dashboard` and `api` services first, or set `DASHBOARD_PORT`.

## Repository layout

```
contracts/          Wire format: Pydantic models, JSON Schema, example payloads
agent/              Health agent for real edge devices → Kafka over Tailscale
worker/             Kafka consumer: validate → transform → store → alert; DB migrator
api/                FastAPI read API over TimescaleDB
dashboard/          React + Vite + TypeScript dashboard (served by nginx in Compose)
db/migrations/      Ordered SQL migrations, applied at startup
tests/integration/  Cross-service tests with Testcontainers
docker/             Python service and agent Dockerfiles
deploy/agent/       Running the agent on a device
deploy/server/      Server .env template
```

## Running on a real device

`agent/` is a small container that reads the host it runs on and publishes the same `device.health` contract to Kafka over Tailscale.
- **What it reports:**
  - CPU, temperature, RAM and disk;
  - uplink traffic and internet packet loss;
  - host services, and every Docker container as a service.
- **Delivery:** readings are spooled on disk while Kafka is unreachable and sent when it's back.

```bash
make up-tailnet     # stack + Kafka listener on this machine's Tailscale IP (:9094)
make agent-image    # build edgeio/agent:dev, then ship and run it per deploy/agent/README.md
```

The device appears on the dashboard under its hostname within one interval.

On a server, copy `deploy/server/server.env.example` to `.env` next to `docker-compose.yml` first. It binds the database, API and the local Kafka port to `127.0.0.1`, puts the dashboard on the Tailscale IP, and sets a real database password.

## The message contract

Each device publishes one JSON message per interval to `device.health`, keyed by `device_id`:

```json
{
  "schema_version": 1,
  "device_id": "100.101.12.7",
  "timestamp": "2026-10-06T09:45:00Z",
  "system": {
    "hostname": "edge-001", "os": "Ubuntu 24.04", "uptime_seconds": 382941,
    "cpu_usage_percent": 43.7, "cpu_temperature_c": 57.2, "load_1m": 1.42,
    "ram_total_mb": 16384, "ram_used_mb": 9271, "ram_usage_percent": 56.6
  },
  "disk": { "root_total_gb": 476, "root_used_gb": 291, "root_free_gb": 185, "root_usage_percent": 61.1 },
  "network": { "interface": "eth0", "rx_bytes": 482938192, "tx_bytes": 182938291, "packet_loss_percent": 0.0 },
  "services": { "docker": "running", "postgresql": "running", "edge_streamer": "running" },
  "containers": { "running": 5, "stopped": 1 }
}
```

The full validation rules are in [`CLAUDE.md`](CLAUDE.md#4-the-message-contract-devicehealth) and the schema is in [`contracts/health.schema.json`](contracts/health.schema.json). Messages that fail validation go to `device.health.dlq` with the original bytes. Headers on the DLQ record describe the error.

## API

All endpoints are under `/api/v1`:

| Endpoint | Description |
|---|---|
| `GET /healthz` | Liveness + database reachability |
| `GET /devices` | Devices with latest status and metrics (`status`, `limit`, `offset`) |
| `GET /devices/{device_id}` | Latest metrics, services, containers and open alerts |
| `GET /devices/{device_id}/metrics` | History for one metric (`metric`, `from`, `to`, `bucket`) |
| `GET /alerts` | Open / resolved alerts, newest first (`state`, `device_id`, `severity`) |
| `GET /fleet/summary` | Status counts, open alerts by severity, hottest / fullest-disk devices |

## CI

GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs on every push to `main` and on pull requests. It has four jobs:

- **Python:** lint, mypy, unit tests, and a check that the generated schema and OpenAPI files are up to date.
- **Integration:** the integration tests, run with Testcontainers.
- **Dashboard:** lint, tests and the production build.
- **Docker:** builds every image in the Compose stack, plus the agent image.

## Tech stack

Python 3.12 · uv · Pydantic v2 · confluent-kafka · psycopg 3 · FastAPI · Apache Kafka 3.8 (KRaft) · TimescaleDB 2.17 (PostgreSQL 16) · React 19 · Vite · TypeScript · TanStack Query · Recharts · Vitest · Docker Compose · GitHub Actions
