"""Export the HealthReport JSON Schema for non-Python producers (e.g. a real health.py)."""

import json
from typing import Any

from .models import HealthReport


def health_json_schema() -> dict[str, Any]:
    schema = HealthReport.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://edgeio.local/schemas/health.v1.json"
    schema["description"] = (
        "Health report published by an edge device to the device.health Kafka topic. "
        "Cross-field rules (Tailscale address range, RAM/disk consistency, clock skew) "
        "are enforced by the worker, not by this schema."
    )
    return schema


def main() -> None:
    print(json.dumps(health_json_schema(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
