from typing import Annotated

from fastapi import APIRouter, Depends, Query

from .. import queries
from ..db import DbConn, get_conn
from ..schemas import AlertCounts, FleetSummary, RankedDevice, StatusCounts

router = APIRouter(tags=["fleet"])
Conn = Annotated[DbConn, Depends(get_conn)]


@router.get("/fleet/summary")
def fleet_summary(conn: Conn, top: Annotated[int, Query(ge=1, le=20)] = 5) -> FleetSummary:
    return FleetSummary(
        devices=StatusCounts.model_validate(queries.status_counts(conn)),
        open_alerts=AlertCounts.model_validate(queries.open_alert_counts(conn)),
        hottest=[
            RankedDevice.model_validate(r)
            for r in queries.top_devices(conn, "cpu_temperature_c", top)
        ],
        fullest_disks=[
            RankedDevice.model_validate(r)
            for r in queries.top_devices(conn, "disk_usage_percent", top)
        ],
    )
