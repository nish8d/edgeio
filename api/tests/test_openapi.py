import json
from pathlib import Path

from edgeio_api.export_openapi import build_openapi

OPENAPI_FILE = Path(__file__).parents[2] / "dashboard" / "openapi.json"


def test_committed_openapi_matches_the_app() -> None:
    assert json.loads(OPENAPI_FILE.read_text()) == build_openapi(), "run `make openapi`"


def test_every_endpoint_is_documented() -> None:
    assert set(build_openapi()["paths"]) == {
        "/api/v1/healthz",
        "/api/v1/devices",
        "/api/v1/devices/{device_id}",
        "/api/v1/devices/{device_id}/metrics",
        "/api/v1/alerts",
        "/api/v1/fleet/summary",
    }
