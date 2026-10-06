from fastapi.testclient import TestClient


def test_health_ok_with_database(api: TestClient) -> None:
    response = api.get("/api/v1/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}
