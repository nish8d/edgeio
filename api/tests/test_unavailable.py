from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from edgeio_api.app import create_app
from edgeio_api.config import ApiSettings


@pytest.fixture
def client() -> Iterator[TestClient]:
    settings = ApiSettings(
        database_url="postgresql://edgeio:edgeio@127.0.0.1:1/edgeio",  # nothing listens here
        pool_timeout_seconds=0.5,
    )
    with TestClient(create_app(settings)) as c:
        yield c


def test_health_reports_database_unavailable(client: TestClient) -> None:
    response = client.get("/api/v1/healthz")
    assert response.status_code == 503
    assert response.json() == {"status": "degraded", "database": "unavailable"}
