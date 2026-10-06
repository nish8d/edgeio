import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def history(seed) -> None:
    seed(1, 0, {"system.cpu_temperature_c": 90})  # opens cpu_temp_high 09:00 ...
    seed(1, 5, {"system.cpu_temperature_c": 65})  # ... resolved 09:05
    seed(2, 5, {"services.edge_streamer": "failed"})  # service_down critical, 09:05
    seed(3, 10, {"system.cpu_temperature_c": 80})  # cpu_temp_high warning, 09:10


def rules(body: dict) -> list[tuple[str, str]]:
    return [(a["hostname"], a["rule"]) for a in body["items"]]


def test_open_alerts_by_default_newest_first(api: TestClient, history: None) -> None:
    body = api.get("/api/v1/alerts").json()
    assert body["total"] == 2
    assert rules(body) == [("edge-003", "cpu_temp_high"), ("edge-002", "service_down")]
    assert body["items"][0]["resolved_at"] is None


def test_resolved_alerts(api: TestClient, history: None) -> None:
    body = api.get("/api/v1/alerts", params={"state": "resolved"}).json()
    assert rules(body) == [("edge-001", "cpu_temp_high")]
    assert body["items"][0]["resolved_at"] == "2026-10-06T09:05:00Z"


def test_all_alerts(api: TestClient, history: None) -> None:
    body = api.get("/api/v1/alerts", params={"state": "all"}).json()
    assert [a["hostname"] for a in body["items"]] == ["edge-003", "edge-002", "edge-001"]


def test_filter_by_device_and_severity(api: TestClient, history: None) -> None:
    by_device = api.get("/api/v1/alerts", params={"state": "all", "device_id": "100.64.0.1"})
    assert rules(by_device.json()) == [("edge-001", "cpu_temp_high")]
    critical = api.get("/api/v1/alerts", params={"severity": "critical"}).json()
    assert rules(critical) == [("edge-002", "service_down")]


def test_rejects_unknown_state(api: TestClient) -> None:
    assert api.get("/api/v1/alerts", params={"state": "closed"}).status_code == 422
