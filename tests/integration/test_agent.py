import json
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from edgeio_agent.publisher import send_pending
from edgeio_agent.spool import Spool
from edgeio_contracts.samples import sample_report


def test_spooled_backlog_reaches_kafka_in_order_keyed_by_device(
    kafka_bootstrap: str, tmp_path: Path
) -> None:
    topic = f"device.health.agent.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    admin.create_topics([NewTopic(topic, 3, 1)])[topic].result(timeout=30)

    spool = Spool(tmp_path, max_files=100)
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=15)
    stamps = [base + timedelta(minutes=5 * i) for i in range(3)]
    for ts in stamps:
        changes = {"device_id": "100.70.1.2", "timestamp": ts.isoformat()}
        spool.add(ts, sample_report(changes).model_dump_json().encode())

    producer = Producer(
        {"bootstrap.servers": kafka_bootstrap, "enable.idempotence": True, "acks": "all"}
    )
    assert send_pending(spool, producer, topic, timeout=30) == 3
    assert spool.pending() == []

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": f"reader-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    messages = []
    deadline = time.monotonic() + 30
    while len(messages) < 3 and time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is not None and msg.error() is None:
            messages.append(msg)
    consumer.close()

    assert {msg.key() for msg in messages} == {b"100.70.1.2"}
    received = [datetime.fromisoformat(json.loads(msg.value())["timestamp"]) for msg in messages]
    assert received == stamps
