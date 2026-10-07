"""Entry point: python -m edgeio_agent"""

import functools
import logging
import shutil
import signal
import threading
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from .collect import collect
from .config import AgentSettings
from .logs import configure_logging
from .runtime import producer_error_handler, run
from .spool import Spool

log = logging.getLogger("edgeio_agent")


def main() -> None:
    settings = AgentSettings()
    configure_logging(settings.log_level)
    _ = settings.service_renames  # fail fast on a malformed AGENT_SERVICE_RENAMES
    if shutil.which("ping") is None:
        raise SystemExit("ping is not installed; packet loss cannot be measured")

    stop, fatal = threading.Event(), threading.Event()
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            # Give up on a message within one send window so flush() reports every outcome.
            "message.timeout.ms": int(settings.agent_send_timeout_seconds * 1000),
            "error_cb": producer_error_handler(stop, fatal),
        }
    )
    spool = Spool(Path(settings.agent_spool_dir), settings.agent_spool_max_files)

    def request_stop(signum: int, _frame: Any) -> None:
        log.info("stopping", extra={"signal": signum})
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, request_stop)

    log.info("agent starting", extra={"bootstrap": settings.kafka_bootstrap})
    run(
        functools.partial(collect, settings),
        spool,
        producer,
        settings.kafka_topic,
        settings.agent_send_timeout_seconds + 5,
        settings.agent_interval_seconds,
        stop,
    )

    if fatal.is_set():
        raise SystemExit(1)  # non-zero so --restart=always brings up a fresh producer


if __name__ == "__main__":
    main()
