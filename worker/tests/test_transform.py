from datetime import timedelta

from edgeio_contracts.samples import sample_report
from edgeio_worker.transform import PreviousCounters, to_row

RX, TX = 482938192, 182938291  # sample counters


def test_maps_report_fields() -> None:
    report = sample_report()
    row = to_row(report, previous=None)
    assert row.device_id == "100.101.12.7"
    assert row.ts == report.timestamp
    assert (row.hostname, row.os) == ("edge-001", "Ubuntu 24.04")
    assert row.cpu_temperature_c == 57.2
    assert row.disk_usage_percent == 61.1
    assert row.net_interface == "eth0"
    assert (row.containers_running, row.containers_stopped) == (5, 1)
    assert row.raw["system"]["hostname"] == "edge-001"
    assert row.raw["timestamp"] == "2026-10-06T09:45:00Z"


def test_disk_free_percent_is_derived() -> None:
    assert to_row(sample_report(), None).disk_free_percent == round(185 / 476 * 100, 2)


def test_no_previous_reading_gives_no_rates() -> None:
    row = to_row(sample_report(), None)
    assert row.rx_rate_bps is None and row.tx_rate_bps is None


def test_rates_are_bits_per_second() -> None:
    report = sample_report()
    previous = PreviousCounters(
        ts=report.timestamp - timedelta(seconds=300), rx_bytes=RX - 3_000_000, tx_bytes=TX - 300_000
    )
    row = to_row(report, previous)
    assert row.rx_rate_bps == 80_000.0
    assert row.tx_rate_bps == 8_000.0


def test_counter_reset_after_reboot_gives_no_rates() -> None:
    report = sample_report()
    previous = PreviousCounters(
        ts=report.timestamp - timedelta(seconds=300), rx_bytes=RX + 1, tx_bytes=TX - 1
    )
    row = to_row(report, previous)
    assert row.rx_rate_bps is None and row.tx_rate_bps is None


def test_non_increasing_timestamp_gives_no_rates() -> None:
    report = sample_report()
    previous = PreviousCounters(ts=report.timestamp, rx_bytes=RX - 10, tx_bytes=TX - 10)
    row = to_row(report, previous)
    assert row.rx_rate_bps is None and row.tx_rate_bps is None
