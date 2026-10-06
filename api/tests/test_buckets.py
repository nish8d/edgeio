from datetime import UTC, datetime, timedelta

import pytest

from edgeio_api.queries import METRICS, choose_bucket

T0 = datetime(2026, 10, 6, tzinfo=UTC)


@pytest.mark.parametrize(
    ("span", "bucket"),
    [
        (timedelta(hours=1), "raw"),
        (timedelta(hours=24), "raw"),
        (timedelta(hours=25), "1h"),
        (timedelta(days=30), "1h"),
        (timedelta(days=31), "1d"),
    ],
)
def test_choose_bucket(span: timedelta, bucket: str) -> None:
    assert choose_bucket(T0, T0 + span) == bucket


def test_every_metric_has_raw_and_rollup_columns() -> None:
    assert set(METRICS) == {
        "cpu",
        "temperature",
        "ram",
        "disk",
        "packet_loss",
        "rx_rate",
        "tx_rate",
    }
    assert all(m.raw and m.avg and m.max and m.unit for m in METRICS.values())
