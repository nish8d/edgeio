"""Write the OpenAPI document the dashboard generates its types from."""

import json
from typing import Any

from .app import create_app
from .config import ApiSettings


def build_openapi() -> dict[str, Any]:
    # Building the schema never touches the database; the pool is not opened.
    return create_app(ApiSettings(database_url="postgresql://unused")).openapi()


def main() -> None:
    print(json.dumps(build_openapi(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
