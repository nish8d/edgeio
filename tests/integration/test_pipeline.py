import json
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from edgeio_contracts.samples import sample_payload, set_path
from edgeio_worker.config import WorkerSettings
from edgeio_worker.consumer import run


def create_topics(bootstrap: str) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    topic, dlq = f"device.health.{suffix}", f"device.health.dlq.{suffix}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(topic, 3, 1), NewTopic(dlq, 1, 1)])
    for future in futures.values():
        future.result(timeout=30)
    return topic, dlq


def wait_for(condition: Callable[[], bool], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.5)
    raise AssertionError("condition not met in time")


def read_topic(bootstrap: str, topic: str, expected: int, timeout: float) -> list[Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"reader-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    messages: list[Any] = []
    deadline = time.monotonic() + timeout
    while len(messages) < expected and time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is not None and msg.error() is None:
            messages.append(msg)
    consumer.close()
    return messages


def count_readings(conn: psycopg.Connection) -> int:
    row = conn.execute("SELECT count(*) FROM health_readings").fetchone()
    return int(row[0]) if row else 0


def test_valid_reports_are_stored_and_invalid_ones_dead_lettered(
    kafka_bootstrap: str, database_url: str, conn: psycopg.Connection
) -> None:
    topic, dlq = create_topics(kafka_bootstrap)
    now = datetime.now(UTC).replace(microsecond=0)
    reports = [
        json.dumps(
            set_path(sample_payload(), "timestamp", (now - timedelta(minutes=5 * i)).isoformat())
        ).encode()
        for i in (2, 1, 0)
    ]
    producer = Producer({"bootstrap.servers": kafka_bootstrap})
    for value in reports:
        producer.produce(topic, key=b"100.101.12.7", value=value)
    producer.produce(topic, key=b"bad-device", value=b"{not json")
    producer.produce(topic, key=b"100.101.12.7", value=reports[-1])  # redelivered duplicate
    assert producer.flush(10) == 0

    settings = WorkerSettings(
        kafka_bootstrap=kafka_bootstrap,
        kafka_topic=topic,
        kafka_dlq_topic=dlq,
        kafka_group_id=f"worker-{uuid.uuid4()}",
        database_url=database_url,
        poll_timeout_seconds=0.2,
        sweep_interval_seconds=3600,
    )
    stop = threading.Event()
    worker = threading.Thread(target=run, args=(settings, stop), daemon=True)
    worker.start()
    try:
        wait_for(lambda: count_readings(conn) == 3, timeout=60)
        dead = read_topic(kafka_bootstrap, dlq, expected=1, timeout=30)
    finally:
        stop.set()
        worker.join(timeout=30)

    assert not worker.is_alive()
    assert count_readings(conn) == 3
    assert len(dead) == 1
    assert dead[0].value() == b"{not json"
    assert dict(dead[0].headers())["error_stage"] == b"decode"
    status = conn.execute("SELECT status, last_seen FROM devices").fetchone()
    assert status == ("warning", now)  # sample has 1 stopped container → warning
