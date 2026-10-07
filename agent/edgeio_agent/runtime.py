"""The agent loop: collect → validate → spool → deliver, on a fixed cadence."""

import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from edgeio_contracts.validation import ContractError, validate_report

from .collect import CollectError
from .publisher import Producer, send_pending
from .report import HostSnapshot, build_report
from .spool import Spool

log = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def tick(
    snapshot_source: Callable[[], HostSnapshot],
    spool: Spool,
    producer: Producer,
    topic: str,
    flush_timeout: float,
    now: datetime,
) -> None:
    """One reading. Never raises: whatever fails, the backlog still gets a delivery attempt."""
    payload: bytes | None = None
    try:
        payload = build_report(snapshot_source(), now).model_dump_json().encode()
        validate_report(payload, now)  # the exact bytes the worker will see
    except (CollectError, ContractError, ValueError) as exc:  # ValidationError is a ValueError
        log.error("skipping reading: could not build a valid report", extra={"error": str(exc)})
        payload = None
    except Exception:
        log.exception("skipping reading: unexpected collection error")
        payload = None

    unspooled: bytes | None = None
    if payload is not None:
        try:
            dropped = spool.add(now, payload)
        except OSError as exc:
            # Disk full or a broken volume: still try to deliver this reading directly.
            log.error("cannot write to spool; sending directly", extra={"error": str(exc)})
            unspooled = payload
        else:
            if dropped:
                log.warning("spool full; dropped oldest readings", extra={"dropped": dropped})

    try:
        sent = send_pending(spool, producer, topic, flush_timeout, unspooled=unspooled)
    except Exception:
        log.exception("delivery attempt failed")
        return
    log.info("tick", extra={"sent": sent, "backlog": len(spool.pending())})


def run(
    snapshot_source: Callable[[], HostSnapshot],
    spool: Spool,
    producer: Producer,
    topic: str,
    flush_timeout: float,
    interval: float,
    stop: threading.Event,
) -> None:
    next_at = time.monotonic()
    while not stop.is_set():
        tick(snapshot_source, spool, producer, topic, flush_timeout, utc_now())
        next_at = max(next_at + interval, time.monotonic())
        stop.wait(next_at - time.monotonic())


def producer_error_handler(stop: threading.Event, fatal: threading.Event) -> Callable[[Any], None]:
    """librdkafka error_cb. A fatal error (e.g. a fenced idempotent producer) can't recover
    in-process, so stop and let the container's restart policy start a fresh producer."""

    def on_error(err: Any) -> None:
        if err.fatal():
            log.error("fatal producer error; stopping for restart", extra={"error": str(err)})
            fatal.set()
            stop.set()
        else:
            log.warning("producer error", extra={"error": str(err)})

    return on_error
