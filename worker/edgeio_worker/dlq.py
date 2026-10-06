"""Dead-letter records: the original bytes plus headers explaining the rejection."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

# Keeps DLQ records well under the 1 MB default Kafka message size limit.
MAX_DLQ_VALUE_BYTES = 512 * 1024


@dataclass(frozen=True)
class DlqRecord:
    key: bytes | None
    value: bytes
    headers: list[tuple[str, bytes]]


def build_dlq_record(
    *,
    key: bytes | None,
    value: bytes | None,
    source_topic: str,
    partition: int,
    offset: int,
    stage: str,
    message: str,
    failed_at: datetime,
) -> DlqRecord:
    original = value or b""
    headers = [
        ("error", message.encode()),
        ("error_stage", stage.encode()),
        ("source_topic", source_topic.encode()),
        ("source_partition", str(partition).encode()),
        ("source_offset", str(offset).encode()),
        ("failed_at", failed_at.isoformat().encode()),
    ]
    if len(original) > MAX_DLQ_VALUE_BYTES:
        headers += [("truncated", b"true"), ("original_size", str(len(original)).encode())]
    return DlqRecord(key=key, value=original[:MAX_DLQ_VALUE_BYTES], headers=headers)


class DeliveryTracker:
    """Producer on_delivery callback: remembers failures that flush() alone won't report."""

    def __init__(self) -> None:
        self._errors: list[str] = []

    def __call__(self, err: Any, msg: Any) -> None:
        if err is not None:
            self._errors.append(str(err))

    def raise_if_failed(self) -> None:
        if self._errors:
            errors, self._errors = self._errors, []
            raise RuntimeError(f"dead-letter delivery failed: {'; '.join(errors)}")
