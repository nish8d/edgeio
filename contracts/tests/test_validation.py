import json
from datetime import UTC, datetime, timedelta

import pytest

from edgeio_contracts.samples import sample_payload, set_path
from edgeio_contracts.validation import ContractError, validate_report

NOW = datetime(2026, 10, 6, 9, 50, tzinfo=UTC)


def encode(payload: dict) -> bytes:
    return json.dumps(payload).encode()


def test_valid_payload_returns_report() -> None:
    report = validate_report(encode(sample_payload()), NOW)
    assert report.system.hostname == "edge-001"


def test_garbage_bytes_fail_at_decode_stage() -> None:
    with pytest.raises(ContractError) as exc:
        validate_report(b"{not json", NOW)
    assert exc.value.stage == "decode"


def test_missing_value_fails_at_decode_stage() -> None:
    with pytest.raises(ContractError) as exc:
        validate_report(None, NOW)
    assert exc.value.stage == "decode"


def test_schema_violation_names_the_field() -> None:
    payload = set_path(sample_payload(), "system.cpu_usage_percent", 143.7)
    with pytest.raises(ContractError) as exc:
        validate_report(encode(payload), NOW)
    assert exc.value.stage == "schema"
    assert "system.cpu_usage_percent" in exc.value.message


def test_json_that_is_not_an_object_fails_schema() -> None:
    with pytest.raises(ContractError) as exc:
        validate_report(b"[]", NOW)
    assert exc.value.stage == "schema"


def test_timestamp_more_than_ten_minutes_ahead_is_rejected() -> None:
    future = (NOW + timedelta(minutes=11)).isoformat()
    payload = set_path(sample_payload(), "timestamp", future)
    with pytest.raises(ContractError) as exc:
        validate_report(encode(payload), NOW)
    assert exc.value.stage == "semantic"


def test_small_clock_skew_is_accepted() -> None:
    ahead = (NOW + timedelta(minutes=9)).isoformat()
    payload = set_path(sample_payload(), "timestamp", ahead)
    assert validate_report(encode(payload), NOW).timestamp == NOW + timedelta(minutes=9)
