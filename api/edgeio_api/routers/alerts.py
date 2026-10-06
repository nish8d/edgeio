from ipaddress import IPv4Address
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from .. import queries
from ..db import DbConn, get_conn
from ..schemas import Alert, AlertList, AlertState, Severity

router = APIRouter(tags=["alerts"])
Conn = Annotated[DbConn, Depends(get_conn)]


@router.get("/alerts")
def list_alerts(
    conn: Conn,
    state: AlertState = "open",
    device_id: IPv4Address | None = None,
    severity: Severity | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> AlertList:
    rows, total = queries.list_alerts(
        conn, state=state, device_id=device_id, severity=severity, limit=limit, offset=offset
    )
    return AlertList(
        items=[Alert.model_validate(r) for r in rows], total=total, limit=limit, offset=offset
    )
