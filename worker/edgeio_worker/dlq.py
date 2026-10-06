"""Dead-letter records: the original bytes plus headers explaining the rejection."""

from dataclasses import dataclass
from datetime import datetime


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
    return DlqRecord(
        key=key,
        value=value or b"",
        headers=[
            ("error", message.encode()),
            ("error_stage", stage.encode()),
            ("source_topic", source_topic.encode()),
            ("source_partition", str(partition).encode()),
            ("source_offset", str(offset).encode()),
            ("failed_at", failed_at.isoformat().encode()),
        ],
    )
