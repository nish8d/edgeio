"""Periodic detection of devices that stopped reporting. Caller owns the transaction."""

from datetime import datetime, timedelta
from typing import Any

from psycopg import Connection

from .rules import OFFLINE_RULE

_OPEN_OFFLINE_ALERTS = """
INSERT INTO alerts (device_id, rule, severity, opened_at, last_value, message)
SELECT device_id, %(rule)s, 'critical', %(now)s,
       EXTRACT(EPOCH FROM %(now)s - last_seen), %(message)s
FROM devices
WHERE last_seen < %(cutoff)s
ON CONFLICT (device_id, rule) WHERE resolved_at IS NULL DO NOTHING
RETURNING host(device_id)
"""

_MARK_OFFLINE = """
UPDATE devices SET status = 'offline'
WHERE last_seen < %(cutoff)s AND status <> 'offline'
"""


def sweep_offline(conn: Connection[Any], now: datetime, offline_after: timedelta) -> list[str]:
    minutes = int(offline_after.total_seconds() // 60)
    params = {
        "rule": OFFLINE_RULE,
        "now": now,
        "cutoff": now - offline_after,
        "message": f"No health report for over {minutes} minutes",
    }
    newly_offline = [row[0] for row in conn.execute(_OPEN_OFFLINE_ALERTS, params).fetchall()]
    conn.execute(_MARK_OFFLINE, params)
    return newly_offline
