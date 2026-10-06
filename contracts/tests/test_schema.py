import json
from pathlib import Path

import jsonschema
import pytest

from edgeio_contracts.export_schema import health_json_schema
from edgeio_contracts.samples import sample_payload, set_path

SCHEMA_FILE = Path(__file__).parents[1] / "health.schema.json"


def test_committed_schema_is_up_to_date() -> None:
    committed = json.loads(SCHEMA_FILE.read_text())
    assert committed == health_json_schema(), "run `make schema` to regenerate"


def test_schema_accepts_sample() -> None:
    jsonschema.Draft202012Validator(health_json_schema()).validate(sample_payload())


def test_schema_rejects_out_of_range_percent() -> None:
    payload = set_path(sample_payload(), "system.cpu_usage_percent", 143.7)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(health_json_schema()).validate(payload)
