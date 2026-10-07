"""On-disk queue of encoded reports awaiting delivery, oldest first."""

import os
from datetime import datetime
from pathlib import Path


class Spool:
    def __init__(self, directory: Path, max_files: int) -> None:
        self.directory = directory
        self.max_files = max_files
        directory.mkdir(parents=True, exist_ok=True)

    def add(self, timestamp: datetime, payload: bytes) -> int:
        """Write atomically (temp file + rename); return how many oldest entries were dropped."""
        name = f"{timestamp:%Y%m%dT%H%M%S}Z.json"
        tmp = self.directory / f".{name}.tmp"
        with tmp.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.directory / name)
        pending = self.pending()
        excess = pending[: max(0, len(pending) - self.max_files)]
        for path in excess:
            path.unlink(missing_ok=True)
        return len(excess)

    def pending(self) -> list[Path]:
        return sorted(self.directory.glob("*.json"))
