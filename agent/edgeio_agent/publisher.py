"""Deliver spooled reports to Kafka; a file is deleted only after the broker acknowledges it."""

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from pydantic import ValidationError

from edgeio_contracts.models import HealthReport

from .spool import Spool

log = logging.getLogger(__name__)


class Producer(Protocol):
    def produce(
        self, topic: str, *, key: bytes, value: bytes, on_delivery: Callable[[Any, Any], None]
    ) -> None: ...

    def flush(self, timeout: float) -> int: ...


def send_pending(
    spool: Spool,
    producer: Producer,
    topic: str,
    timeout: float,
    unspooled: bytes | None = None,
) -> int:
    """Send every spooled report oldest first, plus `unspooled` (a reading the spool couldn't
    store); delete spool files only once the broker acks them. Returns how many were acked."""
    outcome = _Outcome()
    for path in spool.pending():
        try:
            payload = path.read_bytes()
            report = HealthReport.model_validate_json(payload)
        except (OSError, ValidationError) as exc:
            log.warning(
                "discarding unreadable spool entry", extra={"file": path.name, "error": str(exc)}
            )
            path.unlink(missing_ok=True)
            continue
        _produce(producer, topic, report, payload, outcome.callback(path))
    if unspooled is not None:
        report = HealthReport.model_validate_json(unspooled)
        _produce(producer, topic, report, unspooled, outcome.callback(None))
    remaining = producer.flush(timeout)
    if remaining:
        log.warning("delivery still pending at flush timeout", extra={"pending": remaining})
    if outcome.failed:
        # One line per flush, not per file: a week-long outage must not fill the device's disk.
        log.warning(
            "deliveries failed; kept for retry",
            extra={"failed": outcome.failed, "first_error": outcome.first_error},
        )
    for done in outcome.delivered:
        if done is not None:
            done.unlink(missing_ok=True)
    return len(outcome.delivered)


def _produce(
    producer: Producer,
    topic: str,
    report: HealthReport,
    payload: bytes,
    on_delivery: Callable[[Any, Any], None],
) -> None:
    producer.produce(
        topic, key=str(report.device_id).encode(), value=payload, on_delivery=on_delivery
    )


class _Outcome:
    def __init__(self) -> None:
        self.delivered: list[Path | None] = []
        self.failed = 0
        self.first_error: str | None = None

    def callback(self, path: Path | None) -> Callable[[Any, Any], None]:
        def on_delivery(err: Any, _msg: Any) -> None:
            if err is None:
                self.delivered.append(path)
                return
            self.failed += 1
            if self.first_error is None:
                self.first_error = str(err)

        return on_delivery
