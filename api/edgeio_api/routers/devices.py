from datetime import UTC, datetime, timedelta
from ipaddress import IPv4Address
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query

from .. import queries
from ..db import DbConn, get_conn
from ..schemas import (
    Bucket,
    DeviceDetail,
    DeviceList,
    DeviceStatus,
    DeviceSummary,
    MetricName,
    MetricPoint,
    MetricSeries,
)

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


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@router.get("/devices/{device_id}/metrics", responses=NOT_FOUND)
def get_metrics(
    conn: Conn,
    device_id: IPv4Address,
    metric: MetricName,
    start: Annotated[datetime | None, Query(alias="from")] = None,
    end: Annotated[datetime | None, Query(alias="to")] = None,
    bucket: Bucket | None = None,
) -> MetricSeries:
    end = _utc(end) if end else datetime.now(UTC)
    start = _utc(start) if start else end - timedelta(hours=24)
    if start >= end:
        raise HTTPException(status_code=422, detail="'from' must be earlier than 'to'")
    if not queries.device_exists(conn, device_id):
        raise HTTPException(status_code=404, detail="device not found")
    chosen = bucket or queries.choose_bucket(start, end)
    rows = queries.metric_points(conn, device_id, metric, chosen, start, end)
    return MetricSeries(
        device_id=str(device_id),
        metric=metric,
        unit=queries.METRICS[metric].unit,
        bucket=chosen,
        start=start,
        end=end,
        points=[MetricPoint.model_validate(r) for r in rows],
    )
