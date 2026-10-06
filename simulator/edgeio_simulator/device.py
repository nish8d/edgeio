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

    leaked = state.leaked_ram_mb + profile.ram_total_mb * 0.04 if kind is Fault.MEMORY_LEAK else 0.0

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
