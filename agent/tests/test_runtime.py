import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from edgeio_agent.collect import CollectError
from edgeio_agent.report import HostSnapshot
from edgeio_agent.runtime import producer_error_handler, run, tick
from edgeio_agent.spool import Spool
from edgeio_contracts.validation import validate_report

NOW = datetime(2026, 10, 7, 9, 0, tzinfo=UTC)


def test_tick_spools_a_valid_report_and_sends_it(
    tmp_path: Path, make_snapshot: Callable[..., HostSnapshot], fake_producer: type[Any]
) -> None:
    spool, producer = Spool(tmp_path, 10), fake_producer()
    tick(make_snapshot, spool, producer, "device.health", 5.0, NOW)
    assert len(producer.sent) == 1
    topic, key, value = producer.sent[0]
    assert (topic, key) == ("device.health", b"100.70.1.2")
    assert validate_report(value, NOW).timestamp == NOW
    assert spool.pending() == []


def test_failed_collection_still_drains_backlog(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes], fake_producer: type[Any]
) -> None:
    spool, producer = Spool(tmp_path, 10), fake_producer()
    earlier = NOW - timedelta(minutes=5)
    spool.add(earlier, report_bytes(earlier))

    def broken() -> HostSnapshot:
        raise CollectError("no default route")

    tick(broken, spool, producer, "device.health", 5.0, NOW)
    assert len(producer.sent) == 1
    assert spool.pending() == []


def test_invalid_snapshot_is_skipped_not_spooled(
    tmp_path: Path, make_snapshot: Callable[..., HostSnapshot], fake_producer: type[Any]
) -> None:
    spool, producer = Spool(tmp_path, 10), fake_producer()
    tick(lambda: make_snapshot(interface=""), spool, producer, "t", 5.0, NOW)  # contract: min 1
    assert producer.sent == [] and spool.pending() == []


def test_run_ticks_until_stopped(
    tmp_path: Path, make_snapshot: Callable[..., HostSnapshot], fake_producer: type[Any]
) -> None:
    stop = threading.Event()
    calls: list[int] = []

    def source() -> HostSnapshot:
        calls.append(1)
        if len(calls) == 3:
            stop.set()
        return make_snapshot()

    run(source, Spool(tmp_path, 10), fake_producer(), "t", 5.0, interval=0.01, stop=stop)
    assert len(calls) == 3


def test_unexpected_collection_error_still_drains_backlog(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes], fake_producer: type[Any]
) -> None:
    spool, producer = Spool(tmp_path, 10), fake_producer()
    earlier = NOW - timedelta(minutes=5)
    spool.add(earlier, report_bytes(earlier))

    def missing_mount() -> HostSnapshot:
        raise FileNotFoundError("/host")

    tick(missing_mount, spool, producer, "device.health", 5.0, NOW)
    assert len(producer.sent) == 1


def test_spool_write_failure_still_sends_the_reading(
    tmp_path: Path, make_snapshot: Callable[..., HostSnapshot], fake_producer: type[Any]
) -> None:
    directory = tmp_path / "spool"
    spool, producer = Spool(directory, 10), fake_producer()
    directory.chmod(0o500)  # e.g. disk full / read-only volume
    try:
        tick(make_snapshot, spool, producer, "device.health", 5.0, NOW)
    finally:
        directory.chmod(0o700)
    assert len(producer.sent) == 1
    assert validate_report(producer.sent[0][2], NOW).timestamp == NOW


def test_producer_error_does_not_stop_the_loop(
    tmp_path: Path, make_snapshot: Callable[..., HostSnapshot]
) -> None:
    class QueueFull:
        def produce(self, topic: str, **kwargs: Any) -> None:
            raise BufferError("local queue full")

        def flush(self, timeout: float) -> int:
            return 0

    spool = Spool(tmp_path, 10)
    tick(make_snapshot, spool, QueueFull(), "device.health", 5.0, NOW)
    assert len(spool.pending()) == 1


class _KafkaError:
    def __init__(self, fatal: bool) -> None:
        self._fatal = fatal

    def fatal(self) -> bool:
        return self._fatal

    def __str__(self) -> str:
        return "producer fenced"


def test_fatal_producer_error_stops_the_agent() -> None:
    stop, fatal = threading.Event(), threading.Event()
    on_error = producer_error_handler(stop, fatal)
    on_error(_KafkaError(fatal=False))
    assert not stop.is_set()
    on_error(_KafkaError(fatal=True))
    assert stop.is_set() and fatal.is_set()
