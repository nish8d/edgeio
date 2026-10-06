"""Entry point: python -m edgeio_worker"""

import signal
import threading

from .config import WorkerSettings
from .consumer import run
from .logs import configure_logging


def main() -> None:
    settings = WorkerSettings()
    configure_logging(settings.log_level)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    run(settings, stop)


if __name__ == "__main__":
    main()
