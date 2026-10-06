import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def fleet(seed) -> None:
    seed(1)
    seed(2, changes={"system.cpu_temperature_c": 90})  # critical
    seed(3, changes={"network.packet_loss_percent": 3.0})  # warning


def test_lists_devices_by_hostname(api: TestClient, fleet: None) -> None:
    body = api.get("/api/v1/devices").json()
    assert body["total"] == 3
    assert [d["hostname"] for d in body["items"]] == ["edge-001", "edge-002", "edge-003"]
    second = body["items"][1]
    assert second["device_id"] == "100.64.0.2"
    assert second["status"] == "critical"
    assert second["open_alert_count"] == 1
    assert second["last_seen"] == "2026-10-06T09:00:00Z"


def test_filters_by_status(api: TestClient, fleet: None) -> None:
    body = api.get("/api/v1/devices", params={"status": "critical"}).json()
    assert body["total"] == 1
    assert [d["hostname"] for d in body["items"]] == ["edge-002"]


def test_paginates(api: TestClient, fleet: None) -> None:
    body = api.get("/api/v1/devices", params={"limit": 1, "offset": 1}).json()
    assert (body["total"], body["limit"], body["offset"]) == (3, 1, 1)
    assert [d["hostname"] for d in body["items"]] == ["edge-002"]


def test_rejects_unknown_status(api: TestClient) -> None:
    assert api.get("/api/v1/devices", params={"status": "broken"}).status_code == 422


def test_device_detail(api: TestClient, fleet: None) -> None:
    body = api.get("/api/v1/devices/100.64.0.1").json()
    assert body["hostname"] == "edge-001"
    assert body["first_seen"] == "2026-10-06T09:00:00Z"
    assert [s["name"] for s in body["services"]] == ["docker", "edge_streamer", "postgresql"]
    assert body["latest"]["containers_running"] == 5
    assert body["latest"]["containers_stopped"] == 0
    assert body["open_alerts"] == []


def test_device_detail_includes_open_alerts(api: TestClient, fleet: None) -> None:
    alerts = api.get("/api/v1/devices/100.64.0.2").json()["open_alerts"]
    assert [(a["rule"], a["severity"], a["hostname"]) for a in alerts] == [
        ("cpu_temp_high", "critical", "edge-002")
    ]


def test_unknown_device_is_404(api: TestClient) -> None:
    assert api.get("/api/v1/devices/100.64.0.99").status_code == 404


def test_device_id_must_be_an_ip(api: TestClient) -> None:
    assert api.get("/api/v1/devices/edge-001").status_code == 422
