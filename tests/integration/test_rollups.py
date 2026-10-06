import pytest

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
