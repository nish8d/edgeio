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
        return (
            replace(current, ticks_left=current.ticks_left - 1) if current.ticks_left > 1 else None
        )
    if rng.random() < fault_rate:
        return start_fault(rng.choice(list(Fault)), rng, profile)
    return None
