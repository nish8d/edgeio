from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from edgeio_worker.sweeper import sweep_offline

BASE = datetime(2026, 10, 6, 9, 0, tzinfo=UTC)
FULL_DISK = {"disk.root_used_gb": 430, "disk.root_free_gb": 46, "disk.root_usage_percent": 90.3}


def test_health_ok_with_database(api: TestClient) -> None:
    response = api.get("/api/v1/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}


@pytest.fixture
def fleet(seed, conn) -> None:
    seed(1, 30)  # healthy, 57.2 °C, disk 61.1 %
    seed(2, 30, {"system.cpu_temperature_c": 80})  # warning
    seed(3, 30, {"system.cpu_temperature_c": 90, **FULL_DISK})  # critical + disk warning
    seed(4, 0, {"system.cpu_temperature_c": 99})  # critical, then silent → offline
    sweep_offline(conn, BASE + timedelta(minutes=40), timedelta(minutes=15))


def test_fleet_summary(api: TestClient, fleet: None) -> None:
    body = api.get("/api/v1/fleet/summary", params={"top": 2}).json()
    assert body["devices"] == {"healthy": 1, "warning": 1, "critical": 1, "offline": 1, "total": 4}
    assert body["open_alerts"] == {"warning": 2, "critical": 3}
    assert [(d["hostname"], d["value"]) for d in body["hottest"]] == [
        ("edge-003", 90.0),
        ("edge-002", 80.0),
    ]  # the offline device's stale 99 °C is excluded
    assert [(d["hostname"], d["value"]) for d in body["fullest_disks"]] == [
        ("edge-003", 90.3),
        ("edge-001", 61.1),
    ]


def test_empty_fleet_summary(api: TestClient, conn) -> None:
    body = api.get("/api/v1/fleet/summary").json()
    assert body["devices"] == {"healthy": 0, "warning": 0, "critical": 0, "offline": 0, "total": 0}
    assert body["open_alerts"] == {"warning": 0, "critical": 0}
    assert body["hottest"] == [] and body["fullest_disks"] == []


def test_top_is_bounded(api: TestClient) -> None:
    assert api.get("/api/v1/fleet/summary", params={"top": 0}).status_code == 422
