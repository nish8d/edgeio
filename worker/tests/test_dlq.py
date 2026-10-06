from datetime import UTC, datetime

import pytest

from edgeio_worker.dlq import MAX_DLQ_VALUE_BYTES, DeliveryTracker, build_dlq_record

FAILED_AT = datetime(2026, 10, 6, 9, 45, tzinfo=UTC)


def test_record_preserves_original_bytes_and_explains_failure() -> None:
    record = build_dlq_record(
        key=b"100.64.0.9",
        value=b"{not json",
        source_topic="device.health",
        partition=3,
        offset=42,
        stage="decode",
        message="Invalid JSON",
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
        stage="decode",
        message="message has no value",
        failed_at=FAILED_AT,
    )
    assert record.key is None and record.value == b""


def test_oversized_value_is_truncated_and_marked() -> None:
    big = b"x" * (MAX_DLQ_VALUE_BYTES + 10)
    record = build_dlq_record(
        key=None,
        value=big,
        source_topic="device.health",
        partition=0,
        offset=0,
        stage="schema",
        message="too big",
        failed_at=FAILED_AT,
    )
    assert len(record.value) == MAX_DLQ_VALUE_BYTES
    headers = dict(record.headers)
    assert headers["truncated"] == b"true"
    assert headers["original_size"] == str(len(big)).encode()


def test_delivery_tracker_passes_when_all_delivered() -> None:
    tracker = DeliveryTracker()
    tracker(None, object())
    tracker.raise_if_failed()


def test_delivery_tracker_raises_after_a_failed_delivery() -> None:
    tracker = DeliveryTracker()
    tracker("Broker: Message size too large", object())
    with pytest.raises(RuntimeError, match="Message size too large"):
        tracker.raise_if_failed()
