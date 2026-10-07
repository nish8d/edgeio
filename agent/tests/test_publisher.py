import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Producer

from edgeio_agent.publisher import send_pending
from edgeio_agent.spool import Spool

BASE = datetime(2026, 10, 7, 9, 0, tzinfo=UTC)


def test_delivers_oldest_first_keyed_by_device_and_deletes_acked(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes], fake_producer: type[Any]
) -> None:
    spool = Spool(tmp_path, max_files=10)
    payloads = [report_bytes(BASE + timedelta(minutes=5 * i)) for i in range(3)]
    for i, payload in enumerate(payloads):
        spool.add(BASE + timedelta(minutes=5 * i), payload)
    producer = fake_producer()
    assert send_pending(spool, producer, "device.health", timeout=5) == 3
    assert producer.sent == [("device.health", b"100.70.1.2", p) for p in payloads]
    assert spool.pending() == []


def test_failed_delivery_stays_in_the_spool(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes], fake_producer: type[Any]
) -> None:
    spool = Spool(tmp_path, max_files=10)
    ok, bad = report_bytes(BASE), report_bytes(BASE + timedelta(minutes=5))
    spool.add(BASE, ok)
    spool.add(BASE + timedelta(minutes=5), bad)
    assert send_pending(spool, fake_producer(fail=frozenset({bad})), "t", timeout=5) == 1
    assert [path.read_bytes() for path in spool.pending()] == [bad]


def test_corrupt_spool_entry_is_discarded(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes], fake_producer: type[Any]
) -> None:
    spool = Spool(tmp_path, max_files=10)
    spool.add(BASE, b'{"truncated": ')
    spool.add(BASE + timedelta(minutes=5), report_bytes(BASE + timedelta(minutes=5)))
    producer = fake_producer()
    assert send_pending(spool, producer, "t", timeout=5) == 1
    assert spool.pending() == []


def test_unreachable_broker_keeps_spool(
    tmp_path: Path, report_bytes: Callable[[datetime], bytes]
) -> None:
    spool = Spool(tmp_path, max_files=10)
    spool.add(BASE, report_bytes(BASE))
    producer = Producer({"bootstrap.servers": "127.0.0.1:1", "message.timeout.ms": 1000})
    assert send_pending(spool, producer, "device.health", timeout=10) == 0
    assert len(spool.pending()) == 1


def test_failed_deliveries_are_logged_once_per_flush(
    tmp_path: Path,
    report_bytes: Callable[[datetime], bytes],
    fake_producer: type[Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    spool = Spool(tmp_path, max_files=10)
    payloads = [report_bytes(BASE + timedelta(minutes=5 * i)) for i in range(3)]
    for i, payload in enumerate(payloads):
        spool.add(BASE + timedelta(minutes=5 * i), payload)
    with caplog.at_level(logging.WARNING, logger="edgeio_agent.publisher"):
        send_pending(spool, fake_producer(fail=frozenset(payloads)), "t", timeout=5)
    failures = [r for r in caplog.records if "deliver" in r.getMessage()]
    assert len(failures) == 1
    assert failures[0].__dict__["failed"] == 3
