from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from edgeio_contracts.samples import sample_report
from edgeio_worker.store import process_report

EXPECTED = {
    "cpu_avg", "cpu_max", "temp_avg", "temp_max", "ram_avg", "ram_max", "disk_avg", "disk_max",
    "packet_loss_avg", "packet_loss_max", "rx_rate_avg", "rx_rate_max", "tx_rate_avg",
    "tx_rate_max", "reading_count",
}  # fmt: skip


@pytest.mark.parametrize("view", ["health_hourly", "health_daily"])
def test_rollups_cover_every_charted_metric(conn, view: str) -> None:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s", (view,)
    ).fetchall()
    assert {r[0] for r in rows} >= EXPECTED


def test_hourly_rollup_aggregates_network_rates(conn, report_at, refresh_rollups) -> None:
    process_report(conn, report_at(0, {"network.rx_bytes": 1_000_000}))
    process_report(conn, report_at(5, {"network.rx_bytes": 4_000_000}))
    refresh_rollups()
    row = conn.execute(
        "SELECT reading_count, rx_rate_avg, rx_rate_max FROM health_hourly"
    ).fetchone()
    assert row == (2, 80_000.0, 80_000.0)


def _refresh_job(conn: psycopg.Connection, view: str) -> int:
    row = conn.execute(
        "SELECT j.job_id FROM timescaledb_information.jobs j"
        " JOIN timescaledb_information.continuous_aggregates c"
        "   ON j.hypertable_name = c.materialization_hypertable_name"
        " WHERE c.view_name = %s AND j.proc_name = 'policy_refresh_continuous_aggregate'",
        (view,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_hourly_policy_picks_up_readings_delivered_days_late(conn: psycopg.Connection) -> None:
    # An agent's spool can deliver a week-old backlog after an outage. The scheduled refresh
    # must still materialize those buckets, or long-range charts keep a permanent hole.
    now = datetime.now(UTC).replace(microsecond=0)
    job = _refresh_job(conn, "health_hourly")
    process_report(conn, sample_report({"timestamp": (now - timedelta(hours=2)).isoformat()}))
    conn.execute("CALL run_job(%s)", (job,))  # live data moves the watermark to ~now

    two_days_ago = now - timedelta(days=2)
    process_report(conn, sample_report({"timestamp": two_days_ago.isoformat()}))  # backlog
    conn.execute("CALL run_job(%s)", (job,))

    count = conn.execute(
        "SELECT count(*) FROM health_hourly WHERE bucket = time_bucket('1 hour', %s::timestamptz)",
        (two_days_ago,),
    ).fetchone()
    assert count == (1,)
