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
