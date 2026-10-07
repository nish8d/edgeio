# Edge agent (`health.py`) — design

Status: approved 2026-10-07. Implements CLAUDE.md §1 "future direction": real edge devices report over Tailscale against the `device.health` contract, replacing the simulator for those devices.

## 1. Goal

A health agent that runs on a real edge device. It reads that device's health, builds a `HealthReport` (schema v1, unchanged), and produces it to Kafka every 300 s. The deliverable is the agent plus an opt-in way for the local stack to accept producers over Tailscale. It is proved by a live run from a real customer device into the local stack. That device shows up on the dashboard next to the simulated fleet.

Non-goals: authentication/TLS on Kafka (the Tailscale ACLs are the boundary for now), remote install/fleet management of agents, a contract v2, and alerting on container restart counts (the contract has no field for them).

## 2. What the reference device looks like

This is one surveyed customer device. Names are generalised here. Customer-specific names never enter the repo.

- Ubuntu 24.04 on an x86 mini PC (4-core AMD Ryzen, about 7 GB RAM, 116 GB NVMe, ext4 root). Python 3.12 with `psutil`. No Kafka client, no passwordless sudo, and the user is in the `docker` group.
- The workload is about 7 Docker containers. Each was started with plain `docker run` (no Compose), uses host networking and has `restart=always`. None has a healthcheck. One container is the edge streamer.
- There is no PostgreSQL. The relevant host services are `docker` and `tailscaled`.
- The uplink is wired (`enp1s0`, not `eth0`). Wi-Fi, `docker0` and `tailscale0` also exist.
- Temperature sensors are exposed through hwmon: `k10temp` (CPU Tctl), `nvme` and `amdgpu`.
- On ext4, `used + free` is about 6.4 GB less than `total` because of reserved blocks. The v1 contract only tolerates 1 GB.
- The kernel has no packet-loss counter for the uplink.

## 3. Architecture

There is a new uv workspace member `agent/` with the package `edgeio_agent`. It depends on `edgeio-contracts`, `confluent-kafka`, `psutil` and `pydantic-settings`. It follows the simulator's "functional core, imperative shell" split:

```
collectors (pure: host paths/inputs → values)   ─┐
                                                 ├─▶ build_report() → HealthReport (validated)
docker / systemd / ping probes (thin I/O)       ─┘                          │
                                                                            ▼
                                              publisher: spool-first, then Kafka (acks=all, idempotent)
```

- `collect.py` (or split by area if it grows past about 300 lines) holds the collectors. Each takes a host root path or raw input, so tests can use fake host trees built from the reference device's real readings.
- `report.py` holds `build_report(...) -> HealthReport`, which validates with the shared Pydantic contract. Every payload passes `validate_report` on the device before it's sent. A broken collector shows up in the agent's own logs, not as DLQ noise.
- `publisher.py` holds the Kafka producer plus the on-disk spool.
- `runtime.py` / `__main__.py` hold the loop, which runs on a fixed 300 s cadence anchored to monotonic time, handles SIGTERM, and writes structured JSON logs.

## 4. Field sources

| Field | Source |
|---|---|
| `device_id` | The IPv4 address on `tailscale0` inside 100.64.0.0/10, from `psutil.net_if_addrs()` (needs host networking). It's overridable with `AGENT_DEVICE_ID`. If no address is found, the agent logs an error and skips the tick. |
| `hostname` | The host's `/etc/hostname` (via the read-only host root mount). |
| `os` | `PRETTY_NAME` from the host's `/etc/os-release`, trimmed to the `Ubuntu 24.04` form (name plus major.minor). |
| `uptime_seconds` | `now − psutil.boot_time()`. |
| `cpu_usage_percent` | `psutil.cpu_percent(interval=1)`. |
| `load_1m` | `os.getloadavg()[0]`. |
| RAM | `psutil.virtual_memory()`: total and used in MB. The percent is computed from used/total to keep the fields consistent. |
| `cpu_temperature_c` | The first hwmon chip present in the order `k10temp` → `coretemp` → `zenpower` → `cpu_thermal`/thermal_zone, using the package/Tctl label where available. If none is present it falls back to the hottest sensor, and is clamped to the contract's [−40, 125]. If there's no sensor at all, the reading is skipped with an error log, because the v1 contract requires a temperature. |
| Disk | `os.statvfs(<host root>)`: `total = f_blocks·f_frsize`, `free = f_bavail·f_frsize` (space usable by normal processes), `used = total − free`, `percent = used / total`. Reserved blocks therefore count as used. The sum is exact, and alerts fire on the space applications can actually use. |
| `interface` | The default-route interface from the host's `/proc/net/route` (the lowest metric, destination `00000000`). |
| `rx_bytes` / `tx_bytes` | `psutil.net_io_counters(pernic=True)[interface]`. These are monotonic and reset on reboot, which matches the worker's rate derivation. |
| `packet_loss_percent` | Internet connectivity, not the Tailscale link. 5 ICMP pings each to `1.1.1.1` and `8.8.8.8` (configurable through `AGENT_PING_TARGETS`), run in parallel. The result is the **minimum** loss across targets, so a single provider dropping ICMP doesn't count as an outage, while 100 % means no internet. |
| `services` | Host services (`AGENT_HOST_SERVICES`, default `docker,tailscaled`) come from the host's cgroup v2 tree, mounted read-only at `/host-cgroup`. If `system.slice/<unit>.service/cgroup.procs` exists and lists any process, the service is `running`. Otherwise it is `stopped`, because a unit that isn't running has no cgroup. This was verified on the reference device. Without systemd, `failed` can't be told apart from `stopped`, but either one raises `service_down`. Every container comes from the Docker Engine API over the read-only mounted socket: running → `running`; exited with code 0 or created → `stopped`; exited non-zero, restarting or dead → `failed`; paused → `stopped`. Container names are mapped through `AGENT_SERVICE_RENAMES` (e.g. `streamer-app=edge_streamer`) and truncated to the contract's 64 characters. If the Docker API is unreachable, `docker` is reported as `unknown` and no container keys are added. |
| `containers` | `running` = count of containers in the running state; `stopped` = all others. |

## 5. Delivery and the spool

- Producer settings match the simulator: `acks=all`, idempotence on, keyed by `device_id`, topic `device.health`. Each tick, the agent waits up to `AGENT_SEND_TIMEOUT_SECONDS` (default 30) for delivery reports.
- The spool is a directory on a named volume holding one file per reading, named by timestamp. Each tick:
  1. Build and validate the new report.
  2. Write it to the spool, atomically (temp file, then rename).
  3. Send every spooled file, oldest first, in one flush. Delete each one only after its delivery report succeeds; failed ones stay for the next tick. Unreadable or invalid spool files are discarded with a warning so they can't block the queue.
- The spool is capped at `AGENT_SPOOL_MAX_FILES` (default 2016, which is 7 days at 300 s). When over the cap, the oldest files are dropped with a warning.
- Late readings are fine downstream. The worker stores readings older than `last_seen` as history without rewinding device state, and the `ON CONFLICT DO NOTHING` key makes redelivery harmless.

## 6. Packaging and deployment

- The image is `edgeio/agent`, built from the shared `docker/python.Dockerfile` (`PACKAGE=edgeio-agent`), plus `iputils-ping`.
- The image is shipped by `docker save | ssh <device> docker load`. A `make agent-image` target builds it, and `deploy/agent/README.md` documents the run command.
- Run command:

```
docker run -d --name edgeio-agent --restart=always \
  --network host --pid host --uts host \
  --log-opt max-size=10m --log-opt max-file=3 \
  -v /:/host:ro -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -v /sys/fs/cgroup:/host-cgroup:ro \
  -v edgeio-agent-spool:/spool \
  --env-file ~/edgeio-agent.env edgeio/agent:dev
```

  The env file (in the user's home, since there's no sudo) holds `KAFKA_BOOTSTRAP`, `AGENT_SERVICE_RENAMES` and any per-device overrides. It lives on the device, not in the repo.
- Security trade-off, accepted: access to the Docker socket is root-equivalent even when mounted read-only. The agent only issues GET requests, and the reference device already runs a privileged watchdog container.
- On the server side, an opt-in `docker-compose.tailscale.yml` adds a Kafka listener `TAILNET://0.0.0.0:9094`. It is advertised as `${TAILSCALE_IP}:9094` and published only on `${TAILSCALE_IP}:9094`. The listener map is extended to cover it. `make up-tailnet` uses it. Plain `make up` stays localhost-only.

## 7. Testing

- **Unit tests** (`agent/tests`):
  - each collector against fake host trees built from the reference device's readings (hwmon layout, `/proc/net/route`, os-release, statvfs numbers);
  - the container-state and systemd-state mapping;
  - renames and truncation;
  - min-loss across ping targets, including all targets unreachable → 100;
  - the spool: ordering, atomic write, cap and delete-after-ack;
  - `build_report` output passes `validate_report`, including the reserved-blocks disk case.
- **Integration** (`tests/integration`): the agent's publisher sends a built report and a spooled backlog to a Testcontainers Kafka. The test asserts the messages arrive in order with the right key.
- **Live**: `make up-tailnet` on the laptop, then the agent deployed on the reference device. The device appears on the dashboard with real metrics and per-container services. Stopping the laptop's stack and restarting it later drains the spool.

## 8. Privacy

The repo is public. The following never enter it: customer container names, image registry, hostnames, IPs, or this device's identity. Fixtures use generic names (`edge_streamer`, `app-1`). Device-specific configuration lives only in the device's env file.

## 9. Doc updates

`CLAUDE.md`: add an agent section (§6b), the Tailscale listener in §5/§12, the disk convention in §4, and new `make` targets. The README gets a "Running on a real device" section.
