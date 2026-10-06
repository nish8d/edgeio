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
    profiles = generate_fleet(
        settings.sim_seed, settings.sim_device_count, settings.sim_device_offset
    )
    log.info(
        "starting fleet",
        extra={"devices": len(profiles), "interval_seconds": settings.sim_interval_seconds},
    )
    async with asyncio.TaskGroup() as group:
        for profile in profiles:
            rng = random.Random(f"{settings.sim_seed}:{profile.index}:runtime")
            group.create_task(run_device(profile, settings, sink, stop, rng))
        group.create_task(_serve_delivery_callbacks(sink, stop))
