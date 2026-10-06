"""Kafka consume loop: validate → (DLQ | store) → commit offsets, plus the offline sweep."""

import logging
import threading
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any

import psycopg
from confluent_kafka import Consumer, Producer

from edgeio_contracts.models import HealthReport
from edgeio_contracts.validation import ContractError, validate_report

from .config import WorkerSettings
from .db import Database
from .dlq import DeliveryTracker, build_dlq_record
from .lag import consumer_lag
from .store import process_report, store_batch
from .sweeper import sweep_offline

log = logging.getLogger(__name__)

LAG_RECHECK_SECONDS = 5.0


def run(settings: WorkerSettings, stop: threading.Event) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.kafka_group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    deliveries = DeliveryTracker()
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "on_delivery": deliveries,
        }
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
                handle_batch(messages, settings, producer, db, deliveries)
                # Only after the DB transaction committed: at-least-once delivery.
                consumer.commit(asynchronous=False)
            if time.monotonic() >= next_sweep:
                # last_seen is only trustworthy once we've caught up with the topic; sweeping
                # while behind (startup, after an outage) would flag the whole fleet offline.
                if consumer_lag(consumer) == 0:
                    offline = db.run_in_transaction(
                        lambda conn: sweep_offline(conn, datetime.now(UTC), offline_after)
                    )
                    if offline:
                        log.warning("devices went offline", extra={"device_ids": offline})
                    next_sweep = time.monotonic() + settings.sweep_interval_seconds
                else:
                    next_sweep = time.monotonic() + LAG_RECHECK_SECONDS
    finally:
        consumer.close()
        producer.flush(10)
        db.close()
        log.info("worker stopped")


def handle_batch(
    messages: Sequence[Any],
    settings: WorkerSettings,
    producer: Any,
    db: Database,
    deliveries: DeliveryTracker,
) -> None:
    now = datetime.now(UTC)
    accepted: list[tuple[Any, HealthReport]] = []
    rejected = 0
    for msg in messages:
        if msg.error() is not None:
            log.warning("kafka message error", extra={"error": str(msg.error())})
            continue
        try:
            accepted.append((msg, validate_report(msg.value(), now)))
        except ContractError as exc:
            _dead_letter(producer, settings, msg, exc.stage, exc.message, now)
            rejected += 1

    reports = [report for _, report in accepted]
    try:
        if reports:
            db.run_in_transaction(lambda conn: store_batch(conn, reports))
    except psycopg.OperationalError:
        raise  # only raised while shutting down; nothing is committed
    except psycopg.Error:
        # A report the contract accepted but the database refused. Store the batch one
        # report at a time so a single poison message can't block the partition.
        log.exception("batch failed to store; isolating reports")
        rejected += _store_individually(accepted, settings, producer, db, now)

    if rejected:
        if producer.flush(10) > 0:
            raise RuntimeError("dead-letter messages not delivered; refusing to commit offsets")
        deliveries.raise_if_failed()
    log.info("batch processed", extra={"stored": len(reports), "rejected": rejected})


def _store_individually(
    accepted: Sequence[tuple[Any, HealthReport]],
    settings: WorkerSettings,
    producer: Any,
    db: Database,
    now: datetime,
) -> int:
    failed = 0
    for msg, report in accepted:
        try:
            db.run_in_transaction(partial(process_report, report=report))
        except psycopg.OperationalError:
            raise
        except psycopg.Error as exc:
            _dead_letter(producer, settings, msg, "store", str(exc), now)
            failed += 1
    return failed


def _dead_letter(
    producer: Any, settings: WorkerSettings, msg: Any, stage: str, message: str, now: datetime
) -> None:
    record = build_dlq_record(
        key=msg.key(),
        value=msg.value(),
        source_topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
        stage=stage,
        message=message,
        failed_at=now,
    )
    producer.produce(
        settings.kafka_dlq_topic, key=record.key, value=record.value, headers=record.headers
    )
    log.warning(
        "message dead-lettered",
        extra={"stage": stage, "error": message, "offset": msg.offset()},
    )
