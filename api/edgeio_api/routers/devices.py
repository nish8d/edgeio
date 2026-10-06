from ipaddress import IPv4Address
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query

from .. import queries
from ..db import DbConn, get_conn
from ..schemas import DeviceDetail, DeviceList, DeviceStatus, DeviceSummary

router = APIRouter(tags=["devices"])
Conn = Annotated[DbConn, Depends(get_conn)]
NOT_FOUND: dict[int | str, dict[str, Any]] = {404: {"description": "Device not found"}}


@router.get("/devices")
def list_devices(
    conn: Conn,
    status: DeviceStatus | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> DeviceList:
    rows, total = queries.list_devices(conn, status, limit, offset)
    return DeviceList(
        items=[DeviceSummary.model_validate(r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/devices/{device_id}", responses=NOT_FOUND)
def get_device(conn: Conn, device_id: IPv4Address) -> DeviceDetail:
    row = queries.get_device(conn, device_id)
    if row is None:
        raise HTTPException(status_code=404, detail="device not found")
    alerts, _ = queries.list_alerts(
        conn, state="open", device_id=device_id, severity=None, limit=100, offset=0
    )
    return DeviceDetail.model_validate(
        {
            **row,
            "services": queries.get_services(conn, device_id),
            "latest": queries.get_latest_reading(conn, device_id),
            "open_alerts": alerts,
        }
    )
