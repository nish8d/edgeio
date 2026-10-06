import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from edgeio_contracts.models import HealthReport
from edgeio_contracts.validation import validate_report
from edgeio_simulator.device import DeviceState, initial_state, step
from edgeio_simulator.faults import ActiveFault, Fault
from edgeio_simulator.fleet import make_profile

PROFILE = make_profile(0, seed=7)
START = datetime(2026, 10, 6, 0, 0, tzinfo=UTC)
TICK = timedelta(seconds=300)


def fresh() -> tuple[DeviceState, random.Random]:
    rng = random.Random(1)
    return initial_state(PROFILE, START, rng), rng


def with_fault(state: DeviceState, kind: Fault, ticks: int = 2, **kw: object) -> DeviceState:
    # ticks_left is decremented before use, so ticks=2 applies the fault to the next step.
    return replace(state, fault=ActiveFault(kind, ticks_left=ticks, **kw))  # type: ignore[arg-type]


def run(state: DeviceState, rng: random.Random, ticks: int, start: datetime = START):
    reports: list[HealthReport] = []
    for i in range(1, ticks + 1):
        state, report = step(PROFILE, state, start + i * TICK, rng, fault_rate=0.0)
        assert report is not None
        reports.append(report)
    return state, reports


def test_a_day_of_normal_operation_is_valid_and_consistent() -> None:
    state, rng = fresh()
    _, reports = run(state, rng, ticks=288)
    for prev, cur in zip(reports, reports[1:], strict=False):
        validate_report(cur.model_dump_json(), now=cur.timestamp)
        assert cur.network.rx_bytes > prev.network.rx_bytes
        assert cur.network.tx_bytes > prev.network.tx_bytes
        assert cur.system.uptime_seconds - prev.system.uptime_seconds == 300
        assert cur.disk.root_used_gb >= prev.disk.root_used_gb
        assert all(state == "running" for state in cur.services.values())
    assert reports[0].system.hostname == "edge-001"
    assert str(reports[0].device_id) == str(PROFILE.device_id)


def test_overheat_pushes_temperature_past_critical() -> None:
    state, rng = fresh()
    _, report = step(PROFILE, with_fault(state, Fault.OVERHEAT), START + TICK, rng, 0.0)
    assert report is not None and report.system.cpu_temperature_c > 85


def test_packet_loss_fault_exceeds_warning_threshold() -> None:
    state, rng = fresh()
    _, report = step(PROFILE, with_fault(state, Fault.PACKET_LOSS), START + TICK, rng, 0.0)
    assert report is not None and report.network.packet_loss_percent > 2


def test_service_crash_marks_target_failed() -> None:
    state, rng = fresh()
    faulty = with_fault(state, Fault.SERVICE_CRASH, target="edge_streamer")
    _, report = step(PROFILE, faulty, START + TICK, rng, 0.0)
    assert report is not None
    assert report.services["edge_streamer"] == "failed"
    assert report.services["docker"] == "running"


def test_container_crash_moves_containers_to_stopped() -> None:
    state, rng = fresh()
    faulty = with_fault(state, Fault.CONTAINER_CRASH, magnitude=2)
    _, report = step(PROFILE, faulty, START + TICK, rng, 0.0)
    assert report is not None
    assert report.containers.stopped == 2
    assert report.containers.running == PROFILE.container_count - 2


def test_reboot_resets_uptime_and_counters() -> None:
    state, rng = fresh()
    state, before = run(state, rng, ticks=1)
    _, after = step(PROFILE, with_fault(state, Fault.REBOOT), START + 2 * TICK, rng, 0.0)
    assert after is not None
    assert after.system.uptime_seconds < 300
    assert after.network.rx_bytes < before[0].network.rx_bytes


def test_offline_device_publishes_nothing_but_keeps_running() -> None:
    state, rng = fresh()
    new_state, report = step(PROFILE, with_fault(state, Fault.OFFLINE), START + TICK, rng, 0.0)
    assert report is None
    assert new_state.rx_bytes > state.rx_bytes


def test_memory_leak_grows_ram_usage() -> None:
    state, rng = fresh()
    state = with_fault(state, Fault.MEMORY_LEAK, ticks=9)
    usages = []
    for i in range(1, 9):
        state, report = step(PROFILE, state, START + i * TICK, rng, 0.0)
        assert report is not None
        usages.append(report.system.ram_usage_percent)
    assert usages[-1] >= usages[0] + 15


def test_disk_fill_grows_then_cleans_up() -> None:
    state, rng = fresh()
    state, report = step(PROFILE, with_fault(state, Fault.DISK_FILL), START + TICK, rng, 0.0)
    assert report is not None
    baseline = PROFILE.disk_total_gb * PROFILE.initial_disk_fraction
    assert state.disk_used_gb >= baseline + PROFILE.disk_total_gb * 0.03
    state, _ = step(PROFILE, state, START + 2 * TICK, rng, 0.0)  # fault ends this tick
    assert state.fault is None
    assert abs(state.disk_used_gb - baseline) <= 0.05
