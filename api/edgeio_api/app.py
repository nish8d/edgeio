"""FastAPI application factory."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg_pool import PoolTimeout

from .config import ApiSettings
from .db import create_pool
from .routers import devices, health

log = logging.getLogger(__name__)

API_PREFIX = "/api/v1"


async def _database_unavailable(request: Request, exc: Exception) -> JSONResponse:
    log.warning("database unavailable", extra={"path": request.url.path, "error": str(exc)})
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    settings = settings or ApiSettings()
    pool = create_pool(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool.open(wait=False)  # don't block startup on the database
        try:
            yield
        finally:
            pool.close()

    app = FastAPI(title="edgeio API", version="1.0.0", lifespan=lifespan)
    app.state.pool = pool
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["GET"],
        allow_headers=["*"],
    )
    app.add_exception_handler(PoolTimeout, _database_unavailable)
    app.add_exception_handler(psycopg.OperationalError, _database_unavailable)
    app.include_router(health.router, prefix=API_PREFIX)
    app.include_router(devices.router, prefix=API_PREFIX)
    return app
