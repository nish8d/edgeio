from typing import Any

import pytest
from fastapi.testclient import TestClient

URL = "/api/v1/devices/100.64.0.1/metrics"
T0 = "2026-10-06T09:00:00Z"


@pytest.fixture
def series(seed, refresh_rollups) -> None:
    # 13 readings, 5 min apart, 09:00-10:00; cpu 10..22; rx grows 3 MB per reading.
    for i in range(13):
        seed(
            1,
            minutes=5 * i,
            changes={
                "system.cpu_usage_percent": 10.0 + i,
                "network.rx_bytes": 1_000_000 + 3_000_000 * i,
            },
        )
    refresh_rollups()


def get(api: TestClient, **params: Any) -> Any:
    response = api.get(URL, params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_raw_points_within_a_day(api: TestClient, series: None) -> None:
    body = get(api, metric="cpu", **{"from": T0, "to": "2026-10-06T11:00:00Z"})
    assert (body["bucket"], body["unit"], body["metric"]) == ("raw", "%", "cpu")
    assert len(body["points"]) == 13
    assert body["points"][0] == {"ts": T0, "value": 10.0, "max": None}
    assert body["points"][-1]["value"] == 22.0


def test_rates_start_null_then_bits_per_second(api: TestClient, series: None) -> None:
    body = get(api, metric="rx_rate", **{"from": T0, "to": "2026-10-06T11:00:00Z"})
    assert body["unit"] == "bps"
    assert body["points"][0]["value"] is None
    assert body["points"][1]["value"] == 80_000.0


def test_hourly_bucket_averages_and_maxes(api: TestClient, series: None) -> None:
    body = get(api, metric="cpu", bucket="1h", **{"from": T0, "to": "2026-10-06T11:00:00Z"})
    assert body["points"] == [
        {"ts": "2026-10-06T09:00:00Z", "value": 15.5, "max": 21.0},
        {"ts": "2026-10-06T10:00:00Z", "value": 22.0, "max": 22.0},
    ]


def test_bucket_is_chosen_from_the_range(api: TestClient, series: None) -> None:
    week = get(api, metric="cpu", **{"from": "2026-10-04T00:00:00Z", "to": "2026-10-07T00:00:00Z"})
    assert week["bucket"] == "1h"
    quarter = get(
        api, metric="cpu", **{"from": "2026-08-01T00:00:00Z", "to": "2026-10-07T00:00:00Z"}
    )
    assert quarter["bucket"] == "1d"
    assert quarter["points"] == [{"ts": "2026-10-06T00:00:00Z", "value": 16.0, "max": 22.0}]


def test_naive_timestamps_are_treated_as_utc(api: TestClient, series: None) -> None:
    body = get(api, metric="cpu", **{"from": "2026-10-06T09:00:00", "to": "2026-10-06T09:10:00"})
    assert [p["ts"] for p in body["points"]] == [T0, "2026-10-06T09:05:00Z"]
    assert body["start"] == T0


def test_inverted_range_is_rejected(api: TestClient, series: None) -> None:
    response = api.get(URL, params={"metric": "cpu", "from": "2026-10-06T10:00:00Z", "to": T0})
    assert response.status_code == 422


def test_unknown_metric_is_rejected(api: TestClient, series: None) -> None:
    assert api.get(URL, params={"metric": "gpu"}).status_code == 422


def test_unknown_device_is_404(api: TestClient) -> None:
    response = api.get("/api/v1/devices/100.64.0.99/metrics", params={"metric": "cpu"})
    assert response.status_code == 404
