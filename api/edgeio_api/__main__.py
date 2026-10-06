"""Entry point: python -m edgeio_api"""

import uvicorn

from .app import create_app
from .config import ApiSettings
from .logs import configure_logging


def main() -> None:
    settings = ApiSettings()
    configure_logging(settings.log_level)
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port, log_config=None)


if __name__ == "__main__":
    main()
