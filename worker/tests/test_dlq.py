from datetime import UTC, datetime

from edgeio_contracts.validation import ContractError
from edgeio_worker.dlq import build_dlq_record

FAILED_AT = datetime(2026, 10, 6, 9, 45, tzinfo=UTC)


def test_record_preserves_original_bytes_and_explains_failure() -> None:
    record = build_dlq_record(
        key=b"100.64.0.9",
        value=b"{not json",
        source_topic="device.health",
        partition=3,
        offset=42,
        error=ContractError("decode", "Invalid JSON"),
        failed_at=FAILED_AT,
    )
    assert record.key == b"100.64.0.9"
    assert record.value == b"{not json"
    assert dict(record.headers) == {
        "error": b"Invalid JSON",
        "error_stage": b"decode",
        "source_topic": b"device.health",
        "source_partition": b"3",
        "source_offset": b"42",
        "failed_at": b"2026-10-06T09:45:00+00:00",
    }


def test_missing_value_becomes_empty_bytes() -> None:
    record = build_dlq_record(
        key=None,
        value=None,
        source_topic="device.health",
        partition=0,
        offset=0,
        error=ContractError("decode", "message has no value"),
        failed_at=FAILED_AT,
    )
    assert record.key is None and record.value == b""
