"""Entry point: python -m edgeio_simulator"""

import asyncio
import logging
import signal
from typing import Any

from confluent_kafka import Producer

from .config import SimulatorSettings
from .logs import configure_logging
from .runtime import run_fleet

log = logging.getLogger("edgeio_simulator")


def _on_delivery(err: Any, msg: Any) -> None:
    if err is not None:
        key = msg.key().decode(errors="replace") if msg.key() else None
        log.error("delivery failed", extra={"error": str(err), "device_id": key})


async def _run(settings: SimulatorSettings, producer: Any) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await run_fleet(settings, producer, stop)
    finally:
        remaining = producer.flush(10)
        if remaining:
            log.warning("undelivered messages at shutdown", extra={"count": remaining})


def main() -> None:
    settings = SimulatorSettings()
    configure_logging(settings.log_level)
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "on_delivery": _on_delivery,
        }
    )
    asyncio.run(_run(settings, producer))


if __name__ == "__main__":
    main()
