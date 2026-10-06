from typing import Any

import psycopg
from fastapi import APIRouter, Request, Response
from psycopg_pool import ConnectionPool, PoolTimeout

from ..schemas import Health

router = APIRouter(tags=["health"])


@router.get("/healthz", responses={503: {"model": Health}})
def healthz(request: Request, response: Response) -> Health:
    pool: ConnectionPool[Any] = request.app.state.pool
    try:
        with pool.connection(timeout=2.0) as conn:
            conn.execute("SELECT 1")
    except (PoolTimeout, psycopg.Error):
        response.status_code = 503
        return Health(status="degraded", database="unavailable")
    return Health(status="ok", database="ok")
