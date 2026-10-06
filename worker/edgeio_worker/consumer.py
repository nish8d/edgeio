"""Kafka consume loop: validate → (DLQ | store) → commit offsets, plus the offline sweep."""

import logging
import threading
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from confluent_kafka import Consumer, Producer

from edgeio_contracts.models import HealthReport
from edgeio_contracts.validation import ContractError, validate_report

from .config import WorkerSettings
from .db import Database
from .dlq import build_dlq_record
from .store import store_batch
from .sweeper import sweep_offline

log = logging.getLogger(__name__)


def run(settings: WorkerSettings, stop: threading.Event) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.kafka_group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer(
        {"bootstrap.servers": settings.kafka_bootstrap, "enable.idempotence": True, "acks": "all"}
    )
    db = Database.from_url(settings.database_url, stop)
    offline_after = timedelta(seconds=settings.offline_after_seconds)
    consumer.subscribe([settings.kafka_topic])
    log.info("worker started", extra={"topic": settings.kafka_topic})
    next_sweep = time.monotonic()
    try:
        while not stop.is_set():
            messages = consumer.consume(
                num_messages=settings.batch_size, timeout=settings.poll_timeout_seconds
            )
            if messages:
                handle_batch(messages, settings, producer, db)
                # Only after the DB transaction committed: at-least-once delivery.
                consumer.commit(asynchronous=False)
            if time.monotonic() >= next_sweep:
                offline = db.run_in_transaction(
                    lambda conn: sweep_offline(conn, datetime.now(UTC), offline_after)
                )
                if offline:
                    log.warning("devices went offline", extra={"device_ids": offline})
                next_sweep = time.monotonic() + settings.sweep_interval_seconds
    finally:
        consumer.close()
        producer.flush(10)
        db.close()
        log.info("worker stopped")


def handle_batch(
    messages: Sequence[Any], settings: WorkerSettings, producer: Any, db: Database
) -> None:
    now = datetime.now(UTC)
    reports: list[HealthReport] = []
    rejected = 0
    for msg in messages:
        if msg.error() is not None:
            log.warning("kafka message error", extra={"error": str(msg.error())})
            continue
        try:
            reports.append(validate_report(msg.value(), now))
        except ContractError as exc:
            record = build_dlq_record(
                key=msg.key(),
                value=msg.value(),
                source_topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                error=exc,
                failed_at=now,
            )
            producer.produce(
                settings.kafka_dlq_topic, key=record.key, value=record.value, headers=record.headers
            )
            rejected += 1
            log.warning(
                "message rejected",
                extra={"stage": exc.stage, "error": exc.message, "offset": msg.offset()},
            )
    if rejected and producer.flush(10) > 0:
        raise RuntimeError("dead-letter messages not delivered; refusing to commit offsets")
    if reports:
        db.run_in_transaction(lambda conn: store_batch(conn, reports))
    log.info("batch processed", extra={"stored": len(reports), "rejected": rejected})
